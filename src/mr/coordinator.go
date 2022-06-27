package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	nMap             int
	nReduce          int
	files            []string
	nFinishMap       int
	intermediateFile [][]string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	if reply == nil {
		reply = &AskTaskReply{}
	}
	// distribute all mapper but not finish all yet
	needWait := c.nMap >= len(c.files) && c.nFinishMap < c.nMap
	if needWait {
		reply.TaskType = TaskTypeWait
		return nil
	}
	// still distribute map task
	if c.nMap < len(c.files) {
		reply.Index = c.nMap
		reply.Filename = c.files[reply.Index]
		c.nMap += 1
	} else {
		// all map finish, distribute reduce task

	}
	return nil

}

func (c *Coordinator) NoticeTaskDone(args *NoticeTaskDoneArgs, reply *NoticeTaskDoneReply) error {
	if args.TaskType == TaskTypeMap {
		// mark map task done and check if can enter reduce

		/* last mapper finish then sort kvs */
	} else if args.TaskType == TaskTypeReduce {
		// mark reduce task done and check if all finish
	} else {
		log.Fatalf("bad task type %v", args.TaskType)
	}
	return nil
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
	log.Print("starting coordinator server")
	c.server()
	return &c
}
