package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	TaskTypeNothing TaskType = iota
	TaskTypeMap
	TaskTypeReduce
	TaskTypeWait
)

type BaseTaskArgs struct {
	ID string
}

// Add your RPC definitions here.
type AskTaskArgs struct {
	BaseTaskArgs
}

type AskTaskReply struct {
	TaskType TaskType
	// Error    string
	// mapper task
	Index    int
	Filename string
	NumR     int
	// reducer task
	IntermediateFiles []string
}

type NoticeTaskDoneArgs struct {
	BaseTaskArgs
	TaskType TaskType
	// map task
	MapOutputFilenames []string
	// reduce task
	ReduceOutputFilename string
}

type NoticeTaskDoneReply struct{}

type HeartbeatArgs struct {
	BaseTaskArgs
	Init bool // this is a new worker, so coordinator will distribute a new id
}

type HeartbeatReply struct {
	ID string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
