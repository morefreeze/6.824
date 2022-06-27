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
	TaskTypeUnknown TaskType = iota
	TaskTypeMap
	TaskTypeReduce
	TaskTypeWait
)

// Add your RPC definitions here.
type AskTaskArgs struct{}

type AskTaskReply struct {
	TaskType TaskType
	// Error    string
	// mapper task
	Index    int
	Filename string
	NumR     int
	// reducer task
	Key    string
	Values []string
}

type NoticeTaskDoneArgs struct {
	TaskType TaskType
	// map task
	MapOutputFilenames []string
}

type NoticeTaskDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
