package mr

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    chan *MapTask
	reduceTasks chan *ReduceTask

	genReduceTask SyncTask

	intermediate chan KeyValue
	outputs      chan string
}

// Your code here -- RPC handlers for the worker to call.
type SyncTask struct {
	HasDone bool
	sync.Mutex
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 100
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	// assign Task

	if len(c.mapTasks) >= 1 {
		println("Assign MapTask")
		fristMapTask := <-c.mapTasks

		reply.Type = "map"
		reply.TaskContent = fristMapTask
		return nil
	}
	if len(c.reduceTasks) >= 1 {
		println("Assign ReduceTask")
		firstReduceTask := <-c.reduceTasks

		reply.Type = "reduce"
		reply.TaskContent = firstReduceTask
		return nil
	}

	// generate ReduceTasks
	c.genReduceTask.Lock()
	if len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 && c.genReduceTask.HasDone {
		print("Generate ReduceTasks")
		// get temp
		temp := []KeyValue{}
		for {
			if data, ok := <-c.intermediate; ok {
				temp = append(temp, data)
			} else {
				break
			}
		}
		sort.Sort(ByKey(temp))

		// gen
		i := 0
		for i < len(temp) {
			j := i + 1
			for j < len(c.intermediate) && temp[j].Key == temp[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, temp[k].Value)
			}

			task := &ReduceTask{temp[i].Key, values}
			c.reduceTasks <- task

			i = j
		}

		// done
		c.genReduceTask.HasDone = false
		c.genReduceTask.Unlock()
	}
	return nil
}

func (c *Coordinator) DoneMapTask(args *DoneMapTaskArgs, reply *DoneMapTaskReply) error {
	for _, data := range args.Kva {
		c.intermediate <- data
	}

	fmt.Printf("完成%d，还剩%d", len(c.intermediate), len(c.mapTasks))
	return nil
}

func (c *Coordinator) DoneReduceTask(args *DoneReduceTaskArgs, reply interface{}) error {
	c.outputs <- args.Output
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	gob.Register(new(MapTask))
	gob.Register(new(ReduceTask))
	gob.Register(new(DoneMapTaskArgs))
	gob.Register(new(DoneReduceTaskArgs))

	// init channel
	c.mapTasks = make(chan *MapTask, len(files))
	c.reduceTasks = make(chan *ReduceTask, nReduce)
	c.intermediate = make(chan KeyValue, 2048)
	c.outputs = make(chan string, 1024)

	// init maptask
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		mapTask := MapTask{
			Filename: filename,
			Content:  string(content),
		}

		c.mapTasks <- &mapTask
	}

	c.server()
	return &c
}
