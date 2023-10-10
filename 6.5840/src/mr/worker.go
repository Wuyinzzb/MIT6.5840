package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "strconv"
import "encoding/json"

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
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writemapfile(intermediate []KeyValue, nummap int, nreduce int) error{
	buf := make([][]KeyValue, nreduce)
	for _, keyvalue := range(intermediate) {
		i := ihash(keyvalue.Key) % nreduce
		buf[i] = append(buf[i], keyvalue)
	}
	for i, keyvalue := range(buf) {
		oname := "mr-" + strconv.Itoa(nummap) + "-" + strconv.Itoa(i)
		tmpfile, err := ioutil.TempFile("", "mr-out-z")
		if err != nil {
			log.Fatal("Error creating temporary file:", err)
		}
		defer os.Remove(tmpfile.Name())
		enc := json.NewEncoder(tmpfile)
		err = enc.Encode(keyvalue)
		if err != nil {
			log.Fatal("enc.Encode():", err)
		}
		tmpfile.Close()
		os.Rename(tmpfile.Name(), oname)

	}
	return nil
	
}

func DoMap(mapf func(string, string) []KeyValue) {
	for {
		args := RequestTask{
			Mbp: ResponseTask{},
		}
		reply := ResponseTask{}
		pid := strconv.Itoa(os.Getpid())
		args.Pid = pid
		replys := false
		//每次执行map都会先发送一个心跳说明
		
		ok := call("Coordinator.GetMapTask", &args, &reply)
		if !ok {
			log.Fatal("call GetMapTask failure***")
		}	
		args.Mbp = reply
		ok = call("Coordinator.Heartbeat", pid, &replys)
		if !ok {
			log.Fatal("call Heartbeat failure***")
		}
		if reply.MapFin {
			break
		}
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

		err = writemapfile(intermediate, reply.IdMap, reply.NReduce)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		//fmt.Println("midmap***",args.Mbp.IdMap,pid)
		ok = call("Coordinator.MapTaskFin", &args, &reply)
		if !ok {
			log.Fatal("call MapTaskFin failure*****")
		}
		//fmt.Println("midmap***",args.Mbp.IdMap,pid)
		if reply.MapFin {
			break
		}
	}
}
func DoReduce(reducef func(string, []string) string) {
	for {
		args := RequestTask{
			Mbp: ResponseTask{},
		}
		reply := ResponseTask{}
		pid := strconv.Itoa(os.Getpid())
		args.Pid = pid
		replys := false
		ok := call("Coordinator.GetReduceTask", &args, &reply)
		if !ok {
			log.Fatal("call GetReduceTask failure***")
		}
		
		ok = call("Coordinator.Heartbeat", pid, &replys)
		if !ok {
			log.Fatal("call Heartbeat failure***")
		}
		if reply.ReduceFin {
			break
		}
		args.Mbp = reply
		intermediate := []KeyValue{}
		for i := 0; i < reply.IdMap; i++ {
			tmpfile := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.IdReduce)
			file, err := os.OpenFile(tmpfile, os.O_RDONLY, 0644)
			if err != nil {
				fmt.Println("openfile error")
			}
			dec := json.NewDecoder(file)
			for {
				var kv []KeyValue
				if err := dec.Decode(&kv);err != nil {
					break;
				}
				intermediate= append(intermediate, kv...)
			}
			file.Close()
		}
		sort.Sort(ByKey(intermediate))
		oname := "mr-out-" + strconv.Itoa(reply.IdReduce)
		ofile, _ := os.Create(oname)
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
			//fmt.Printf("%v %v %d %v\n", intermediate[i].Key, output,reply.IdReduce,pid)
			// this is the correct format for each line of Reduce output.
			//这一句可能会导致写入失败，还没有想到具体解决方法
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			i = j
		}
		err := ofile.Sync()
		if err != nil {
			log.Fatal("刷新文件缓冲区时发生错误：", err)
		}
		ofile.Close()
		//fmt.Println("midred***",args.Mbp.IdReduce,pid)
		ok = call("Coordinator.ReduceTaskFin", &args, &reply)
		//fmt.Println("midred***",args.Mbp.IdReduce,pid)
		if !ok {
			log.Fatal("call ReduceTaskFin failure*****")
		}
		

	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//每隔一秒向coordinator发送一次心跳
	

	DoMap(mapf)
	DoReduce(reducef)
	
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
