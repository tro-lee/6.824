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

// Add your RPC definitions here.
type TaskArgs struct {
}

type TaskReply struct {
	Type        string
	TaskId      string
	TaskContent interface{}
}

type MapTask struct {
	Filename string
	Content  string
}

type ReduceTask struct {
	Key    string
	Values []string
}

type DoneMapTaskArgs struct {
	Kva []KeyValue
}

type DoneReduceTaskArgs struct {
	Output string
}

type EmptyReply struct{}
type DoneMapTaskReply EmptyReply
type DoneReduceTaskReply EmptyReply

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
