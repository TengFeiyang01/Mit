# Lab1: MapReduce 

## 环境准备

**Go 1.13 + WSL2** + **Goland** (PS：一定要更新到WSL2，不然会有奇奇怪怪的BUG...)

## 1. 准备工作

使用[git](https://git-scm.com/)（版本控制系统）拉取实验初始版本代码。如果您不熟悉 git，可以通过查阅[Git-Book](https://git-scm.com/book/en/v2)和[Git User Manual](http://www.kernel.org/pub/software/scm/git/docs/user-manual.html)自行学习。执行以下命令，即可从远端拉取 6.824 的初始实验代码：

````shell
$ git clone git://g.csail.mit.edu/6.824-golabs-2020 6.824
$ cd 6.824
$ ls
Makefile src
````

### 1.1 非分布式的实现

官方给我们提供了一个简单的顺序 mapreduce 实现。它在一个进程中一次运行一个映射和还原。

```shell
$ cd ~/6.824
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
A 509
ABOUT 2
ACT 8
...
```

这个wc.go定义了`Map`和`Reduce`函数，这两个函数在`mrsequential.go`被加载和调用。mrsequential.go 会在 mr-out-0 文件中留下输出结果。输入来自名为 pg-xxx.txt 的文本文件。

### 1.2 your job

​	们的任务是实现一个分布式 MapReduce，它由两个程序组成：协调程序coordinator和工作程序worker。一个coordinator,一个worker（启动多个）,在这次实验都在一个机器上运行。工作进程将通过 RPC 与协调程序对话。每个 Worker 进程都会向协调器请求任务，从一个或多个文件中读取任务输入，执行任务，并将任务输出写入一个或多个文件。协调器应该注意到某个 Worker 是否超时，若超时将任务重新交给不同的 Worker。
​	协调器和 Worker 的 "主 "例程位于 `main/mrcoordinator.go` 和 `main/mrworker.go`；不要修改这些文件。**我们要写的代码就是`mr`文件夹下的：`mr/coordinator.go`、`mr/worker.go` 和 `mr/rpc.go` 。**

当我们完成后，可以执行`bash test-mr.sh`，若我们成功了，就会出现这样的代码：

```shell
$ bash test-mr.sh
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting job count test.
--- job count test: PASS
*** Starting early exit test.
--- early exit test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

### 1.3 Some rules

- map 阶段应将中间键分割为不同的桶（译者注：中间键指的是 MapReduce 论文中的 intermediate key，这句话的意思是将 map Task 的输出以某种形式保存为 reduce 函数能够读取的输入），方便后续`nReduce`个 reduce task 读取，而`nReduce`则是`main/mrmaster.go`传递给`MakeMaster`的参数；
- worker 进程应把第 X 个 reduce task 的输出保存到文件`mr-out-X`中；
- `mr-out-X`中每行都应该是调用一次 Reduce 函数的输出，应该按照 Go 语言的`"%v %v"`的格式生成，也即 key 和 value，如在`main/mrsequential.go`中注释"this is the correct format"的位置所示。如果您的实现和这一格式相差太多，测试脚本将会执行失败；
- 您需要修改的文件为：`mr/worker.go`，`mr/master.go`和`mr/rpc.go`。尽管您可以暂时地修改其他文件来辅助您测试，但请确保其他文件被还原为初始状态（原始版本）后您的程序仍能正常工作，我们将会使用原始版本的代码进行评判；
- woker 进程应该将 Map 函数的输出（intermediate key）保存在当前目录的文件中，使得后续 worker 进程可以读取它们并将其作为 Reduce task 的输入；
- 当 MapReduce Job 被计算完毕后，`main/mrmaster.go`希望您实现的`mr/master.go`中的`Done()`方法会返回 true。这样`mrmaster.go`就能知道 Job 已经顺利完成，进程即可退出；
- 当 MapReduce job 被做完后，worker 进程就应该退出。实现这一功能的一种笨方法就是令 woker 程序检查`call()`函数的返回值：如果 woker 进程无法和 master 进程通信，那么 worker 进程就可以认为整个 Job 已经全被做完了，自己也就可以退出了。当然是否这么做还是要取决于您的设计，您也可以设计一个”please exit“的伪任务，当 worker 进程收到这一任务，就自动退出（译者注：笔者就是这么做的，看起来更优雅一些）。

### 1.4 提示Hints

- 如果您觉得无从下手，可以从修改`mr/worker.go`中的`Worker()`函数开始，在函数中首先实现以下逻辑：向 master 发送 RPC 来请求 task。然后修改 master：将文件名作为尚未开始的 map task 响应给 worker。然后修改 worker：读取文件并像`mrsequential.go`程序一样，调用 Map 方法来处理读取的数;

- MapReduce 应用程序的 Map/Reduce 函数被保存在以`.so`结尾的文件中，在运行时使用 Go plugin 包读取；

- 如果您改变了`mr/`文件夹中的文件，并且该文件也被用到，那您需要将该文件使用类似于`go build -buildmode=plugin ../mrapps/wc.go`的命令重新编译成 MapReduce 插件；

- 本实验要求 worker 进程使用同一个文件系统，因此所有的 worker 进程必须运行于一台机器上。如果想要让 worker 程序运行在不同的机器上，那您需要为他们提供一个全局的文件系统，比如 GFS；

- **一个命名中间文件的合理形式是`mr-X-Y`，其中 X 是 Map task 的编号，Y 是 Reduce task 编号；**

- worker 程序处理 Map task 的代码中需要一种将中间键值对存储为文件的方式，也需要一种在处理 Reduce task 时能从文件中正确读回键值对的方式。一种可能的实现是使用 Go 语言的`encoding/json`包。将键值对写入 JSON 文件的代码如下：

  ```go
    enc := json.NewEncoder(file)
    for _, kv := ... {
        err := enc.Encode(&kv)
  ```

  从 JSON 文件中读回键值对的代码如下：

  ```go
    dec := json.NewDecoder(file)
    for {
        var kv KeyValue
        if err := dec.Decode(&kv); err != nil {
            break
        }
        kva = append(kva, kv)
    }
  ```

- **在 worker 中处理 map Task 的部分，对于一个给定键，您可以使用`ihash(key)`函数（在`worker.go`中）来选择它属于哪一个 reduce task；**

- 您可以将`mrsequential.go`中一些功能的代码拿来直接用，比如：读取 map task 的输入文件，在调用 Map 方法和调用 Reduce 方法之间对中间键值对进行排序，以及存储 Reduce 函数的输出到文件。

- 作为一个 RPC 服务器，**master 进程将是并发的，因此不要忘记给共享资源加锁。**

- **可以执行命令`go build -race`和`go run -race`来使用 Go 语言的 race detector，在`test.sh`中有一条注释为您展示如何为测试开启 race detector；**

- **worker 进程有时需要等待，比如在最后一个 map task 处理完之前，worker 不能开始对 reduce task 的处理。实现这一功能的一种可能方案是 worker 进程周期性的向 master 请求 task，在每次请求间使用`time.Sleep()`阻塞一段时间。另一种可能的方案是 master 在收到 rpc 请求后额外开启一个 RPC 处理线程，在这个线程中执行循环等待（也可以使用`time.Sleep()`和`sync.Cond`），这样使得阻塞的 RPC 不会影响 master 响应其他 RPC；**

- **master 进程无法可靠的区分崩溃的 worker 进程、活着但因某些原因停止运行的 worker 进程和正在运行但太慢导致无法使用的 worker 进程。这一问题最好的解决方案是令 master 等待一段时间，如果某个 worker 进程在这段时间（在本实验中，这段时间被设置为 10 秒）内没有完成相应的 task，就放弃继续等待并将该 task 重新分配给其他 worker。之后，master 应该假设这个 worker 进程已经死亡了（当然，它可能还活着）；**

- 为了测试容错，您可以使用`mrapps/crash.go`插件，它在 Map 和 Reduce 函数中增加了随机退出功能；

- 为了确保没人能看到被崩溃进程写了一半的文件，MapReduce 论文提到了一个小技巧，那就是使用临时文件，一旦该文件完全写完，就自动重命名。您可以使用 `ioutil.TempFile` 创建临时文件，并使用 `os.Rename` 去自动重命名它。

- `test-mr.sh`将会在子目录`mr-tmp`中运行所有的进程，因此如果有错误发生并且您想查看中间输出文件的话，可以查看这一目录中的文件。

## 2. MapReduce的实现

### 2.1 数据结构设计

论文提到每个(Map或者Reduce)Task有分为idle, in-progress, completed 三种状态。

```Go
type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)
```

Coordinator 存储这些Task的信息。这里我并没有保留worked的ID，因此master不会主动向worker发送`心跳检测`

```Go
type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus // CoordinatorTask的状态
	TaskReference *Task                 // 其所管理的Task的指针
	StartTime     time.Time             // 以及Task的启动时间
}
```

此外Coordinator存储Map任务产生的R个中间文件的信息。

```Go
type Coordinator struct {
	TaskQueue        chan *Task               // 等待执行的task
	TaskMeta         map[int]*CoordinatorTask // 当前所有task的信息
	CoordinatorPhase State                    // Coordinator 的阶段
	NReduce          int                      // Reducer的个数
	InputFiles       []string                 // 要分配給map处理的文件列表
	Intermediates    [][]string               // Map任务产生的R个中间文件的信息
}
```

Map和Reduce的Task应该负责不同的事情，在实现代码的过程中同一个Task结构完全可以兼顾两个阶段的任务。

```Go
type Task struct {
	Input         string // 该任务需要处理的文件 map 任务处理原数据 reduce 处理map产生的中间结果文件
	TaskState     State  // 任务状态 Map | Reduce | Wait | Exit
	NReducer      int
	TaskNumber    int      // 任务编号 生成中间文件需要
	Intermediates []string // 产生的中间文件的文件名列表
    Output        string
}				
```

将task和master的状态合并成一个State。task和master的状态应该一致。如果在Reduce阶段收到了迟来MapTask结果，应该直接丢弃。

```Go
const (
	Map = iota
	Reduce
	Wait
	Exit
)
```

### 2.2 MapReduce执行过程的实现

**1. 启动Coordinator**

```Go
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(len(files), nReduce)),
		TaskMeta:         make(map[int]*CoordinatorTask),
		CoordinatorPhase: Map,
		NReduce:          nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}

	// 创建 Map 任务
	c.createMapTask()

	// 一个程序成为coordinator 其他程序成为worker
	// 这里是启动 coordinator 服务器
	// 拥有coordinator代码的是coordinator, 别的发送RPC过来的是worker
	c.server()
	// 启动一个goroutine检查Time Out 超市任务
	go c.catchTimeOut()
	return &c
}
```

**2. Coordinator 监听 worker RPC调用，分配任务**

```Go
// AssignTask 等待worker调用 向 worker 发送任务
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	// assignTask就看看自己队列中是否还有task
	mu.Lock()
	defer mu.Unlock()
	if len(c.TaskQueue) > 0 {
		// 有任务 发出去
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		// 记录启动时间
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// 没有task可分配 worker等待
		// eg: 两个task 三个worker 第三个worker没有任务 但是此时并没有结束 (worker)又不能退出 应该让其等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}
```

**3. 启动 worker**

```Go
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
```

**4. worker向 Coordinator 发送RPC请求任务**

```Go
func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
    // call 函数使用反射来调用名为 "Coordinator.AssignTask" 的方法，其中 args 是传递给方法的参数，reply 是方法执行后的结果。
	call("Coordinator.AssignTask", &args, &reply)
	return reply
}
```

**5. worker 获得 MapTask，交给 mapper 处理**

```Go
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
```

**6. worker 任务完成后通知 Coordinator **

```Go
func TaskCompleted(task *Task) {
	// 通过 RPC， 把task信息发送给coordinator
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", task, &reply)  // 反射
}
```

**7. Coordinator 收到完成后的Task**

- 如果所有的MapTask都已经完成，创建ReduceTask，转入Reduce阶段
- 如果所有的ReduceTask都已经完成，转入Exit阶段

```Go
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
```

这里使用一个辅助函数判断是否当前阶段所有任务都已经完成

```Go
func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}	
```

**8. 转入Reduce阶段，worker获得ReduceTask，交给reducer处理** 

```go
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
```

**9. Coordinator 确认所有ReduceTask都已经完成，转入Exit阶段，终止所有Coordinator 和worker goroutine**

```Go
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := c.CoordinatorPhase == Exit
	return ret
}
```

**10. 上锁** master跟多个worker通信，master的数据是共享的，其中`TaskMeta, Phase, Intermediates, TaskQueue` 都有读写发生。`TaskQueue`使用`channel`实现，自己带锁。只有涉及`Intermediates, TaskMeta, Phase`的操作需要上锁。*PS.写的糙一点，那就是master每个方法都要上锁，master直接变成同步执行。。。*

另外go -race并不能检测出所有的datarace。我曾一度任务`Intermediates`写操作发生在map阶段，读操作发生在reduce阶段读，逻辑上存在`barrier`，所以不会有datarace. 但是后来想到两个write也可能造成datarace，然而Go Race Detector并没有检测出来。

**11. carsh处理**

test当中有容错的要求，不过只针对worker。mapreduce论文中提到了：

1. 周期性向worker发送心跳检测

   如果worker失联一段时间，master将worker标记成failed

   worker失效之后

   - 已完成的map task被重新标记为idle
   - 已完成的reduce task不需要改变
   - 原因是：map的结果被写在local disk，worker machine 宕机会导致map的结果丢失；reduce结果存储在GFS，不会随着machine down丢失

2. 对于in-progress 且超时的任务，启动backup执行

tricky的在于Lab1是在单机上用多进程模拟了多机器，但并不会因为进程终止导致写好的文件丢失，这也是为什么我前面没有按照论文保留workerID.

针对Lab1修改后的容错设计：

1. 周期性检查task是否完成。将超时未完成的任务，交给新的worker，backup执行

   ```go
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
   ```

2. 从第一个完成的worker获取结果，将后序的backup结果丢弃

   ```go
   if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
   	// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
   	return nil
   }
   m.TaskMeta[task.TaskNumber].TaskStatus = Completed
   ```

## 3. 测试

开启race detector，执行`test-mr.sh`

```shell
$ ./test-mr.sh        
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

PS. Lab1要求的Go版本是1.13，如果使用的更新的版本，会报出

```go
rpc.Register: method "Done" has 1 input parameters; needs exactly three
```

可以直接无视
