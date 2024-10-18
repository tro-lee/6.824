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
// Task
type Task[Content any] struct {
	Type    string
	Id      int
	Content Content
}

type MapTaskContent struct {
	Filename string
	NReduce  int
}

type ReduceTaskContent struct {
	FilenameNum int
}

type AssignTaskArgs EmptyStruct
type AssignTaskReply struct {
	Task *Task[interface{}]
}

type DoneMapTaskArgs struct {
	TaskId int
}
type DoneReduceTaskArgs struct {
	TaskId int
}

type EmptyStruct struct{}
type DoneMapTaskReply EmptyStruct
type DoneReduceTaskReply EmptyStruct

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
