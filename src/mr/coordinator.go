package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
//import "fmt"
import "time"

type Coordinator struct {
	// Your definitions here.
	finishMapNumber int //已经完成了多少个map任务
	finishReduceNumber int //已经完成了多少个reduce任务
	nReduce int //分散的数量
	// map
	mapNumber int //总共的文件数量-对应几个worker
	files []string //对应的文件名
	mapQueue TaskQueue //map的任务队列
	//会生成mapNumber*nReduce个中间文件

	// reduce
	canReduce int //可以发送reduce任务了
	reduceQueue TaskQueue //map的任务队列
}

// Task 任务类
type Task struct {
	WorkMode int //
	// map任务
	Number     int		// mr-X-Y中的X，代表处理几个文件
	NReduce int         //基本是全局在传
	Filename string

	// post任务
	NReduceNumber int // mr-X-Y中的Y，处理所有Y结尾的文件，来reduce
}

// TaskQueue 带锁的任务队列
type TaskQueue struct {
	queue []Task
	mu    sync.Mutex
}

// NewTaskQueue 创建一个新的任务队列
func NewTaskQueue() *TaskQueue {
	return &TaskQueue{}
}

// Push 将任务添加到队列
func (q *TaskQueue) Push(task Task) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, task)
}

// Pop 从队列中取出任务
func (q *TaskQueue) Pop() (Task, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queue) == 0 {
		return Task{}, false
	}
	task := q.queue[0]
	q.queue = q.queue[1:]
	return task, true
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

func (c *Coordinator) GetTask(args *RPCArgs, reply *RPCReply) error {
	//fmt.Printf("c.canReduce=%v\n",c.canReduce)
	if c.canReduce==0 { //发送map任务
		task, ok := c.mapQueue.Pop()
		if !ok {
			return nil
		}
		reply.Number = task.Number
		reply.Filename = task.Filename
		reply.NReduce = task.NReduce
		//fmt.Printf("map Task send! %v\n",reply.WorkMode)
		reply.WorkMode = 0
		return nil
	} else { //发送reduce任务
		task, ok := c.reduceQueue.Pop()
		if !ok {
			return nil
		}
		reply.Number = task.Number
		reply.NReduceNumber = task.NReduceNumber
		reply.NReduce = task.NReduce
		reply.WorkMode = 1
		//fmt.Printf("reduce Task send!\n")
		return nil
	}
}

func (c *Coordinator) PostTask(args *RPCArgs, reply *RPCReply) error {
	//fmt.Printf("post!\n")
	if args.FinishCheck==1 && args.WorkMode==0 {
		c.finishMapNumber++
		if c.finishMapNumber==c.mapNumber{
			//可以写成chan 这样就能放到主函数里操作了，这里简单点写实现
			for i := 0; i < c.nReduce; i++ {
				task:=Task{
					WorkMode:1,
					NReduceNumber:i,
					NReduce:c.nReduce,
					Number : c.mapNumber,
				}
				c.reduceQueue.Push(task)
			}
			c.canReduce=1
			//fmt.Printf("c.canReduce=%v\n",c.canReduce)
		}
	}
	
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
	ret := false

	// Your code here.
	time.Sleep(30 * time.Second)

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:files,
		mapNumber:len(files),
		mapQueue:TaskQueue{},
		nReduce:nReduce,

		canReduce:0, // map任务上锁
		reduceQueue:TaskQueue{},

		finishMapNumber:0,
		finishReduceNumber:0,
	}


	for index, file := range files {
		task:=Task{
			WorkMode:0,
			Number:index,
			Filename:file,
			NReduce:nReduce,
		}
        c.mapQueue.Push(task)
    }
	// Your code here.


	c.server()
	return &c
}
