package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorTaskStatus int
type State int

const (
	Map = iota
	Reduce
	Wait
	Exit
)

const (
	Idle CoordinatorTaskStatus = iota // 未执行
	InProgress
	Completed
)

var mu sync.Mutex

// Task 存储任务信息
type Task struct {
	Input         string // 该任务需要处理的文件 map 任务处理原数据 reduce 处理map产生的中间结果文件
	TaskState     State  // 任务状态 Map | Reduce | Wait | Exit
	NReducer      int
	TaskNumber    int      // 任务编号 生成中间文件需要
	Intermediates []string // 产生的中间文件的文件名列表
}

type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus // CoordinatorTask的状态
	TaskReference *Task                 // Task的指针
	StartTime     time.Time             // Task的启动时间
}

type Coordinator struct {
	TaskQueue        chan *Task               // 等待执行的task
	TaskMeta         map[int]*CoordinatorTask // 当前所有task的信息
	CoordinatorPhase State                    // Coordinator 的阶段
	NReduce          int                      // Reducer的个数
	InputFiles       []string                 // 要分配給map处理的文件列表
	Intermediates    [][]string               // Map任务产生的R个中间文件的信息
}

// Your code here -- RPC handlers for the worker to call.

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
	mu.Lock()
	defer mu.Unlock()
	ret := c.CoordinatorPhase == Exit
	return ret
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(len(files), nReduce)),
		TaskMeta:         make(map[int]*CoordinatorTask),
		CoordinatorPhase: Map,
		NReduce:          nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}

	// Your code here.
	// 创建 Map 任务
	c.createMapTask()

	// 一个程序启动coordinator 其他程序成为worker
	// 这里是启动coordinate服务器
	// 拥有coordinate代码的是coordinator 发送RPC过来的是worker
	c.server()
	// 启动一个goroutine检查Time Out 超市任务
	go c.catchTimeOut()
	return &c
}

// catchTimeOut 超时检测
func (c *Coordinator) catchTimeOut() {
	for {
		// 每5秒检查一次
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.CoordinatorPhase == Exit {
			return
		}
		for _, coordinatorTask := range c.TaskMeta {
			if coordinatorTask.TaskStatus == InProgress && time.Now().Sub(coordinatorTask.StartTime) > 10*time.Second {
				c.TaskQueue <- coordinatorTask.TaskReference
				coordinatorTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

// createMapTask 创建Map任务
func (c *Coordinator) createMapTask() {
	for idx, filename := range c.InputFiles {
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   c.NReduce,
			TaskNumber: idx,
		}
		c.TaskQueue <- &taskMeta
		// 数据表
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

// createReduceTask 创建Reduce任务
func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*CoordinatorTask)
	// c.Intermediates 是一个二维列表 第一维是reduceTaskID 第二维是Map处理后得到的中间文件
	// 遍历所有的中间结果 对每个中间结果去创建一个Task信息
	for idx, files := range c.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NReducer:      c.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		// 新的ReduceTask放入TaskQueue 等待分配
		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

// AssignTask 等待worker调用 向 worker 发送任务
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	// assignTask就看看自己队列中是否还有task
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		// 有任务 发出去
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		// 记录启动时间 超时kill
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// 没有task可分配 worker等待
		// eg: 两个task 三个worker 第三个worker没有任务 但是此时并没有结束 他(worker)不能退出 应该让其等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	// 更新task状态
	mu.Lock()
	defer mu.Unlock()
	//
	if task.TaskState != c.CoordinatorPhase || c.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 因为worker卸载同一个磁盘上 对应同一个结果要丢弃
		return nil
	}
	c.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		// 收集 Map 任务的 intermediate信息
		for reduceTaskID, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskID] = append(c.Intermediates[reduceTaskID], filePath)
		}
		// 所有的Map任务处理结束后 进入Reduce阶段
		if c.allTaskDone() {
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			// 获得所有reduce task之后 退出
			c.CoordinatorPhase = Exit
		}
	}
}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
