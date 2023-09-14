package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type RPCArgs struct {//发送完成请求相关
	WorkMode int //0代表map任务，1代表reduce任务
	//Number int //mr-x-y的x
	FinishCheck int //任务是否完成
	Index int
}

type RPCReply struct {

	//map任务
	Number int
	Filename string
	NReduce int
	WorkMode int //0代表map任务，1代表reduce任务
	//reduce任务
	NReduceNumber int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
