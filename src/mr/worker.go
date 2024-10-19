package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	gob.Register(new(AssignTaskArgs))
	gob.Register(new(Task[interface{}]))
	gob.Register(new(MapTaskContent))
	gob.Register(new(ReduceTaskContent))
	gob.Register(new(EmptyStruct))

	for {
		reply := &AssignTaskReply{}
		ok := call("Coordinator.AssignTask", &AssignTaskArgs{}, reply)
		if !ok {
			log.Fatal("调用失败")
		}

		task := reply.Task
		if task.Type == "map" {
			doMapTask(task, mapf)
		}
		if task.Type == "reduce" {
			doReduceTask(task, reducef)
		}
		if task.Type == "stop" {
			break
		}
	}
}

// 执行Map任务，判断是Map类型任务后，传入相关参数即可
// 负责拿到并解析文件，执行自定义map任务 然后保存到另一文件，最终通知已经完成
func doMapTask(task *Task[interface{}], mapf func(string, string) []KeyValue) {
	mapTask := task.Content.(*MapTaskContent)

	// 拿到并解析文件
	file, err := os.Open(mapTask.Filename)
	if err != nil {
		log.Fatalln("未找到该文件")
	}
	content, _ := io.ReadAll(file)
	file.Close()

	// 执行自定义map任务
	kva := mapf(mapTask.Filename, string(content))

	// 保存到文件
	for _, kv := range kva {
		filename := "mr-map" + strconv.Itoa(ihash(kv.Key)%mapTask.NReduce)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalln(err)
		}

		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		file.Close()
	}

	// 通知已经完成
	empty := &EmptyStruct{}
	ok := call("Coordinator.DoneMapTask", &DoneMapTaskArgs{task.Id}, empty)
	if !ok {
		fmt.Printf("doneMapTask failed\n")
		return
	}
}

func doReduceTask(task *Task[interface{}], reducef func(string, []string) string) {
	reduceTask := task.Content.(*ReduceTaskContent)
	//
	// 通用的变量，当前任务task和中间过渡变量intermediate
	//
	intermediate := []KeyValue{}

	//
	// 读取文本内容, 拿到任务后转成文件内容，并读取出来
	// 文件内容类似 XX 10, 所以直接分行处理
	//
	file, err := os.Open("mr-map" + strconv.Itoa(reduceTask.FilenameNum))
	if err != nil {
		os.Exit(1)
	}
	content, _ := io.ReadAll(file)
	file.Close()
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		intermediate = append(intermediate, KeyValue{parts[0], parts[1]})
	}

	sort.Sort(ByKey(intermediate))

	//
	// 保存到文件，将intermediate相同Key的合并，然后交给reducef处理
	//
	file, _ = os.Create("mr-out" + strconv.Itoa(reduceTask.FilenameNum))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	// 通知已经完成
	ok := call("Coordinator.DoneReduceTask", &DoneReduceTaskArgs{task.Id}, &EmptyStruct{})
	if !ok {
		fmt.Printf("doneReduceTask failed\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
