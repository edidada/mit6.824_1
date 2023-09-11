package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
//import "time"
import "sort"
//
// Map functions return a slice of KeyValue.
//
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
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for{
		args := RPCArgs{}
		reply := RPCReply{}
		_ = call("Coordinator.GetTask", &args, &reply)
		// 判断还能再改一下，这里是请求不到任何任务了
		if reply.WorkMode == 0 && reply.NReduce==0{ 
			//fmt.Printf("no task\n")
			//time.Sleep(time.Duration(10)*time.Second)
			break
		}
		//fmt.Printf("reply.WorkMode %v\n", reply.WorkMode)

		if reply.WorkMode == 0{
			//处理map任务
			//fmt.Printf("reply.filename %v %v %v\n", reply.Number,reply.NReduce,reply.Filename)

			//
			intermediate := []KeyValue{}
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()

			kva := mapf(reply.Filename, string(content))
			intermediate = append(intermediate, kva...)

			HashKv := make([][]KeyValue, reply.NReduce)
			for _, v := range(intermediate) {
				// if(v.Key=="Thatcher"){
				// 	fmt.Printf("%v\n",ihash(v.Key) % reply.NReduce)
				// }
				index := ihash(v.Key) % reply.NReduce
				HashKv[index] = append(HashKv[index], v)
			}

			for i := 0; i < reply.NReduce; i++ {
				oname := fmt.Sprintf("mr-%d-%d", reply.Number, i)
				ofile, err := os.Create(oname)
				if err != nil {
					log.Fatal("create file failed:", err)
				}
				enc := json.NewEncoder(ofile)
				for _, kv := range(HashKv[i]) {
					err := enc.Encode(kv)
					if err != nil {
						log.Fatal("encode failed:", err)
					}
				}
				ofile.Close()
			}
			args.WorkMode=0
			args.FinishCheck=1
			//args.Number=reply.Number
			_ = call("Coordinator.PostTask", &args, &reply)
		}else {
			//处理reduce任务
			//fmt.Printf("reply.NReduceNumber %v %v\n", reply.NReduceNumber,reply.NReduce)
			// Reduce
			var Filenames []string
			for i := 0; i < reply.Number; i++ {
				file:=fmt.Sprintf("mr-%d-%d", i,reply.NReduceNumber)
				Filenames=append(Filenames,file)
			}

			kva := make([]KeyValue, 0)

			for _, filename := range Filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open file %v", filename)
				}
				dec := json.NewDecoder(file)

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%d", reply.NReduceNumber)
			ofile, err := os.Create(oname)

			for i := 0; i < len(kva); {
				if err != nil {
					log.Fatal("create file failed:", err)
				}

				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
				
			}
			ofile.Close()

			
			
			args.WorkMode=1
			args.FinishCheck=1
			//args.Number=reply.Number
			_ = call("Coordinator.PostTask", &args, &reply)

		}
	}

}

//
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
