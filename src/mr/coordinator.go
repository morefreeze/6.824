package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	idxMap           map[int]string // [idx]->[work id]
	nReduce          int
	files            []string
	nFinishMap       int
	intermediateFile [][]string
	readyReduce      bool
	idxReduce        map[int]string // [idx]->[work id]
	outFiles         map[string]bool
	// worker management
	liveWorkers map[string]time.Time // [id]->ttl
	refreshTime time.Duration
	workerFiles map[string][]int // [id]->[idx1, idx2]
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	if reply == nil {
		reply = &AskTaskReply{}
	}
	defer log.Printf("distribute task reply: %+v", reply)
	reply.TaskType = TaskTypeNothing
	c.mu.Lock()
	defer c.mu.Unlock()
	// still distribute map task
	if !c.readyReduce {
		reply.TaskType = TaskTypeWait
		// find first occupy map task
		for i := 0; i < len(c.files); i++ {
			if _, ok := c.idxMap[i]; !ok {
				reply.TaskType = TaskTypeMap
				reply.Index = i
				reply.Filename = c.files[reply.Index]
				reply.NumR = c.nReduce
				c.idxMap[i] = args.ID
			}
		}
	} else {
		for i := 0; i < c.nReduce; i++ {
			if _, ok := c.idxReduce[i]; !ok {
				// all map finish, distribute reduce task
				reply.TaskType = TaskTypeReduce
				reply.IntermediateFiles = c.intermediateFile[i]
				reply.Index = i
				c.idxReduce[i] = args.ID
			}
		}
	}
	return nil

}

func (c *Coordinator) NoticeTaskDone(args *NoticeTaskDoneArgs, reply *NoticeTaskDoneReply) error {
	if args.TaskType == TaskTypeMap {
		// mark map task done and check if can enter reduce
		/* last mapper finish then sort kvs */
		c.mu.Lock()
		defer c.mu.Unlock()
		for i, filename := range args.MapOutputFilenames {
			c.intermediateFile[i] = append(c.intermediateFile[i], filename)
		}
		c.nFinishMap += 1
		// all map finish, ready to distribute reduce task
		c.readyReduce = c.nFinishMap == len(c.files)
		return nil
	} else if args.TaskType == TaskTypeReduce {
		// mark reduce task done and check if all finish
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, exist := c.outFiles[args.ReduceOutputFilename]; !exist {
			c.outFiles[args.ReduceOutputFilename] = true
		}
	} else {
		log.Fatalf("bad task type %v", args.TaskType)
	}
	return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	if reply == nil {
		reply = &HeartbeatReply{}
	}
	if args.Init {
		reply.ID = c.genWorkerID()
	} else {
		reply.ID = args.ID
	}
	// refresh worker id
	c.mu.Lock()
	defer c.mu.Unlock()
	c.liveWorkers[reply.ID] = time.Now().Add(c.refreshTime)
	return nil
}

func (c *Coordinator) genWorkerID() (id string) {
	id = uuid.New().String()
	tomb := make(chan struct{}, 1)
	go func(id string) {
		<-tomb
		log.Printf("receive dead letter id[%v]", id)
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.liveWorkers, id)
	}(id)
	expireTime := time.After(c.refreshTime)
	go func(id string) {
		for t := range expireTime {
			log.Printf("check time %v", t)
			c.mu.Lock()
			if newTime, ok := c.liveWorkers[id]; ok && newTime.After(t) {
				expireTime = time.After(time.Until(newTime))
				c.mu.Unlock()
			} else {
				log.Printf("%v die at %v, starting clean", id, time.Now())
				// id is dead, clean its task
				if !c.readyReduce {
					// clean map task
					// TODO: use [id]->idx
					for i, workerID := range c.idxMap {
						if workerID == id {
							delete(c.idxMap, i)
						}
					}
				} else {
					// clean reduce task
					for i, workerID := range c.idxReduce {
						if workerID == id {
							delete(c.idxReduce, i)
						}
					}
				}
				c.mu.Unlock()
				break
			}
		}
		tomb <- struct{}{}
	}(id)
	return
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = len(c.outFiles) == c.nReduce
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.intermediateFile = make([][]string, nReduce)
	c.readyReduce = false
	c.outFiles = make(map[string]bool, nReduce)
	c.liveWorkers = make(map[string]time.Time, 10)
	c.liveWorkers = make(map[string]time.Time)
	c.refreshTime = time.Second * 10
	c.workerFiles = make(map[string][]int)
	c.idxMap = make(map[int]string)
	c.idxReduce = make(map[int]string, nReduce)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)
	colorGreen := "\033[32m"
	log.SetPrefix(colorGreen)

	log.Printf("starting coordinator server with %v files", len(files))
	c.server()
	return &c
}
