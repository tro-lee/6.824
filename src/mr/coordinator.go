package mr

import (
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks         []*Task[*MapTaskContent]
	mapTasksDoing    []*Task[*MapTaskContent]
	reduceTasks      []*Task[*ReduceTaskContent]
	reduceTasksDoing []*Task[*ReduceTaskContent]
}

// Your code here -- RPC handlers for the worker to call.
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
} // 用于DoneMapTask上锁

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = len(c.reduceTasksDoing) == 0 && len(c.reduceTasks) == 0

	return ret
}

var mu sync.Mutex

// 若有可分配的Map任务，则分配Map任务
// 若有可分配的Reduce任务，则分配Reduce任务
// 若都没有，则表示已经完成
// 为了防止争夺资源，采用Locker
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	mu.Lock()
	defer mu.Unlock()

	// MapTask分配
	if len(c.mapTasks) >= 1 {
		task := c.mapTasks[0]
		reply.Task = &Task[interface{}]{
			Id:      task.Id,
			Type:    task.Type,
			Content: task.Content,
		}

		c.mapTasks = c.mapTasks[1:]
		c.mapTasksDoing = append(c.mapTasksDoing, task)
		return nil
	}

	// 等待所有MapTaskDoing完成
	for len(c.mapTasksDoing) >= 1 {
	}

	// ReduceTask分配
	if len(c.reduceTasks) >= 1 {
		task := c.reduceTasks[0]
		reply.Task = &Task[interface{}]{
			Id:      task.Id,
			Type:    task.Type,
			Content: task.Content,
		}

		c.reduceTasks = c.reduceTasks[1:]
		c.reduceTasksDoing = append(c.reduceTasksDoing, task)
		return nil
	}

	// 等待所有ReduceTaskDoing完成
	for len(c.reduceTasksDoing) >= 1 {
	}

	//
	// 下面是收尾工作
	// 停止worker任务
	// 删除文件
	//

	reply.Task = &Task[interface{}]{
		Id:      -1,
		Type:    "stop",
		Content: "",
	}

	matches, err := filepath.Glob("mr-map*")
	if err != nil {
		log.Fatal("匹配异常")
	}
	for _, match := range matches {
		if err := os.Remove(match); err != nil {
			log.Fatal("删除异常")
		}
	}

	return nil
}

// 用于DoneMapTask上锁

var doneMapMu sync.Mutex

func (c *Coordinator) DoneMapTask(args *DoneMapTaskArgs, reply *DoneMapTaskReply) error {
	doneMapMu.Lock()
	defer doneMapMu.Unlock()

	for i, v := range c.mapTasksDoing {
		if v.Id == args.TaskId {
			c.mapTasksDoing = append(c.mapTasksDoing[:i], c.mapTasksDoing[i+1:]...)
			break
		}
	}
	return nil
}

var doneReduceMu sync.Mutex

func (c *Coordinator) DoneReduceTask(args *DoneReduceTaskArgs, reply *DoneReduceTaskReply) error {
	doneReduceMu.Lock()
	defer doneReduceMu.Unlock()

	for i, v := range c.reduceTasksDoing {
		if v.Id == args.TaskId {
			c.reduceTasksDoing = append(c.reduceTasksDoing[:i], c.reduceTasksDoing[i+1:]...)
			break
		}
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// 初始化
	// 注册接口
	gob.Register(new(Task[interface{}]))
	gob.Register(new(MapTaskContent))
	gob.Register(new(ReduceTaskContent))
	gob.Register(new(EmptyStruct))

	c.mapTasks = []*Task[*MapTaskContent]{}
	c.mapTasksDoing = []*Task[*MapTaskContent]{}
	c.reduceTasks = []*Task[*ReduceTaskContent]{}
	c.reduceTasksDoing = []*Task[*ReduceTaskContent]{}

	// 生成所有mapTask
	for i, filename := range files {
		mapTask := Task[*MapTaskContent]{
			Id:   i,
			Type: "map",
			Content: &MapTaskContent{
				Filename: filename,
				NReduce:  nReduce,
			},
		}
		c.mapTasks = append(c.mapTasks, &mapTask)
	}

	// 生成所有reduceTask
	for i := 0; i < nReduce; i++ {
		reduceTask := Task[*ReduceTaskContent]{
			Id:   len(files) + 10 + i,
			Type: "reduce",
			Content: &ReduceTaskContent{
				FilenameNum: i,
			},
		}
		c.reduceTasks = append(c.reduceTasks, &reduceTask)
	}

	c.server()
	return &c
}
