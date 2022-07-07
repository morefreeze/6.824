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

const TASK_DONE = "done"

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	idxMap           map[int]string // [idx]->[work id] or [idx]=TASK_DONE
	nReduce          int
	files            []string
	nFinishMap       int
	intermediateFile [][]string
	readyReduce      bool
	idxReduce        map[int]string  // [idx]->[work id] or [idx]=TASK_DONE
	outFiles         map[string]bool // [output filename]=true
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
	defer log.Printf("distribute task to %v reply: %+v", args.ID, reply)
	reply.TaskType = TaskTypeNothing
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.liveWorkers[args.ID]; !ok {
		log.Printf("warning: %v not register in coord", args.ID)
		return nil
	}
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
				break
			}
		}
	} else {
		reply.TaskType = TaskTypeWait
		for i := 0; i < c.nReduce; i++ {
			if _, ok := c.idxReduce[i]; !ok {
				// all map finish, distribute reduce task
				reply.TaskType = TaskTypeReduce
				reply.IntermediateFiles = c.intermediateFile[i]
				reply.Index = i
				c.idxReduce[i] = args.ID
				break
			}
		}
	}
	return nil

}

func (c *Coordinator) NoticeTaskDone(args *NoticeTaskDoneArgs, reply *NoticeTaskDoneReply) error {
	log.Printf("receive notice task done %+v", args)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.liveWorkers[args.ID]; !ok {
		log.Printf("warning: notice done but %v not register in coord", args.ID)
		return nil
	}
	if args.TaskType == TaskTypeMap && !c.readyReduce {
		// mark map task done and check if can enter reduce
		/* last mapper finish then sort kvs */
		for i, filename := range args.MapOutputFilenames {
			c.intermediateFile[i] = append(c.intermediateFile[i], filename)
		}
		for idx, id := range c.idxMap {
			if id == args.ID {
				c.idxMap[idx] = TASK_DONE
				break
			}
		}
		c.nFinishMap += 1
		// all map finish, ready to distribute reduce task
		c.readyReduce = c.nFinishMap == len(c.files)
		if c.readyReduce {
			log.Printf("=============begin reduce")
		}
		log.Printf("distribute %+v map task(s) and %v done", c.idxMap, c.nFinishMap)
		return nil
	} else if args.TaskType == TaskTypeReduce && c.readyReduce {
		// mark reduce task done and check if all finish
		c.outFiles[args.ReduceOutputFilename] = true
		for idx, id := range c.idxReduce {
			if id == args.ID {
				c.idxReduce[idx] = TASK_DONE
				break
			}
		}
		log.Printf("distribute %+v reduce task(s) and %v done", c.idxReduce, len(c.outFiles))
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
		// log.Printf("receive %v heartbeat", args.ID)
	}
	// refresh worker id
	c.mu.Lock()
	defer c.mu.Unlock()
	c.liveWorkers[reply.ID] = time.Now().Add(c.refreshTime)
	return nil
}

func (c *Coordinator) genWorkerID() (id string) {
	id = uuid.New().String()[:8]
	tomb := make(chan struct{}, 1)
	go func(id string) {
		<-tomb
		log.Printf("receive dead letter id[%v]", id)
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.liveWorkers, id)
	}(id)
	go func(id string) {
		expireTime := time.After(c.refreshTime)
		for {
			select {
			case <-expireTime:
				// log.Printf("check time id[%v] @%v", id, t)
				c.mu.Lock()
				if newTime, ok := c.liveWorkers[id]; ok && time.Until(newTime) > 200*time.Millisecond {
					// log.Printf("new time id[%v] @%v", id, newTime)
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
						// log.Printf("========map just confirm %+v", c.idxMap)
					} else {
						// clean reduce task
						for i, workerID := range c.idxReduce {
							if workerID == id {
								delete(c.idxReduce, i)
							}
						}
						// log.Printf("=======reduce just confirm %+v", c.idxReduce)
					}
					c.mu.Unlock()
					tomb <- struct{}{}
					return
				}
			}
		}
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
