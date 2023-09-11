package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	// Your definitions here.

	// map
	mapNumber int
	files []string
	mapQueue TaskQueue
}

// Task 是一个任务类，包含一个 int 和一个 string 字段
type Task struct {
	Number     int
	Filename string
}

// TaskQueue 是一个带锁的任务队列
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

func (c *Coordinator) GetMapTask(args *RPCArgs, reply *RPCReply) error {
	task, _ := c.mapQueue.Pop()
	reply.Number = task.Number
	reply.Filename = task.Filename
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
	}

	for index, file := range files {
		task:=Task{
			Number:index,
			Filename:file,
		}
        c.mapQueue.Push(task)
    }
	// Your code here.


	c.server()
	return &c
}
