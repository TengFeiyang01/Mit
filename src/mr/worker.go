package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReducer to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 启动worker
	for {
		// worker从master获取任务
		task := getTask()

		// 拿到task后，根据task的State, Map | Reduce Map交给mapper Reduce交给reducer
		// 额外加两个状态 等待/退出
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	// call 函数使用反射来调用名为 "Coordinator.AssignTask" 的方法，其中 args 是传递给方法的参数，reply 是方法执行后的结果。
	call("Coordinator.AssignTask", &args, &reply)
	return reply
}

func TaskCompleted(task *Task) {
	// 通过 RPC， 把task信息发送给coordinator
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", task, &reply)
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	//从文件名读取content
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}
	// 将content交给mapf，缓存结果
	intermediates := mapf(task.Input, string(content))

	// 缓存后的结果会写到本地磁盘，并切成R份
	// 切分方式是根据key做hash
	bucket := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		bucket[slot] = append(bucket[slot], intermediate)
	}

	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskNumber, i, &bucket[i]))
	}

	// R个文件的位置发送给coordinator
	task.Intermediates = mapOutput
	TaskCompleted(task)
}

func reducer(task *Task, reducef func(string, []string) string) {
	// 先从filepath读取intermediate的KeyValue
	intermediate := *readFromLocalFile(task.Intermediates)
	// 根据kv排序
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	// 这部分代码修改自mrsequential.go
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
		// 交给reducef，拿到结果
		output := reducef(intermediate[i].Key, values)
		// 写入到对应的output文件
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%v", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	TaskCompleted(task)
}

// writeToLocalFile 写入临时文件 并返回临时文件的目录列表
func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

// readFromLocalFile 从临时文件中读取所有的KeyValue pair 返回给reducer处理
func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("Failed to open file"+filename, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
