package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
//import "os/exec"
//import "fmt"

type ResponseTask struct {
	NReduce int
	Filename string
	IdMap int
	MapFin bool
	IdReduce int
	ReduceFin bool
}
type ClientHeartbeat struct {
	LastHeartbeat time.Time
	ClientID      string // 客户端标识信息
}
type RequestTask struct {
	Pid string
	Mbp ResponseTask
}

type Coordinator struct {
	// Your definitions here.
	NumReduce 	int//Reduce个数
	NumMap		int
	NumMapFin	int
	NumReduceFin int
	MapTasks chan ResponseTask
	MapTasksbp map[ResponseTask]string
	ReduceTasks chan ResponseTask
	ReduceTasksbp map[ResponseTask]string
	clients map[string]*ClientHeartbeat
	mu      sync.Mutex
}

func (c *Coordinator)Heartbeat(clientID string, reply *bool) error {
	c.mu.Lock()
	client, ok := c.clients[clientID]
	if !ok {
		client = &ClientHeartbeat{ClientID: clientID}
		c.clients[clientID] = client
	}

	client.LastHeartbeat = time.Now()
	c.mu.Unlock()
	return nil
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
func (c *Coordinator) GetMapTask(args *RequestTask, reply *ResponseTask) error {
	for{ 
		c.mu.Lock()	
		if len(c.MapTasks) > 0{
			maptask, ok := <- c.MapTasks
			//c.mu.Unlock()
			if ok {
				reply.Filename = maptask.Filename
				reply.IdMap = maptask.IdMap
				reply.MapFin = maptask.MapFin
				reply.NReduce = maptask.NReduce
				//c.mu.Lock()
				c.MapTasksbp[maptask] = args.Pid
				
				//fmt.Println("getmap***",reply.IdMap,args.Pid)
				c.mu.Unlock()
				return nil
			}else {
				c.mu.Unlock()
				log.Fatal("maptask error !")
				return nil
			}
		} else {
			if c.NumMap == c.NumMapFin{
				reply.MapFin = true
				c.mu.Unlock()
				return nil
			}
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
		
	}
}
func (c *Coordinator) GetReduceTask(args *RequestTask, reply *ResponseTask) error {
	for {
		c.mu.Lock()
		if len(c.ReduceTasks) > 0 {
			reducetask, ok := <- c.ReduceTasks
			if ok {
				reply.IdReduce = reducetask.IdReduce
				reply.ReduceFin = reducetask.ReduceFin
				reply.IdMap = reducetask.IdMap
				c.ReduceTasksbp[reducetask] = args.Pid
				//.Println("getred***",reply.IdReduce, args.Pid)
				c.mu.Unlock()
				return nil
			}else {
				c.mu.Unlock()
				log.Fatal("reducetask error !")
				return nil
			}
		}else {
			if c.NumReduce == c.NumReduceFin{
				reply.ReduceFin = true
				c.mu.Unlock()
				return nil
			}
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}
}
func (c *Coordinator) MapTaskFin(args *RequestTask, reply *ResponseTask) error {	
	c.mu.Lock()
	c.MapTasksbp[args.Mbp] = "0"
	//fmt.Println("Finmap***",args.Mbp.IdMap,args.Pid)
	c.NumMapFin += 1
	if c.NumMapFin == c.NumMap {
		reply.MapFin = true
	}
	c.mu.Unlock()
	return nil
}
func (c *Coordinator) ReduceTaskFin(args *RequestTask, reply *ResponseTask) error {
	c.mu.Lock()
	c.ReduceTasksbp[args.Mbp] = "0"
	//fmt.Println("Finred***",args.Mbp.IdReduce,args.Pid)
	c.NumReduceFin += 1
	if c.NumReduceFin == c.NumReduce {
		reply.ReduceFin = true
	}
	c.mu.Unlock()
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
	//因为主函数里每一秒循环一次done，直接在这里检查心跳
	c.mu.Lock()
	for clientID, client := range c.clients {
		elapsed := time.Since(client.LastHeartbeat)
		if elapsed > 10*time.Second {
			if c.NumMapFin != c.NumMap {//如何两个数相等，说明是reduce出现问题，否则就是map出现问题
				for key, value := range c.MapTasksbp {//如果进程id相等，说明此进程取出数据后十秒还没有处理完成
					if value == clientID {
						//fmt.Println("Donemap**",key.IdMap,clientID)
						c.MapTasks <- key//把数据重新放入管道
						c.MapTasksbp[key] = "0"//0表示未使用或者已处理
					}
				}
			}else {
				for key, value := range c.ReduceTasksbp {//如果进程id相等，说明此进程取出数据后十秒还没有处理完成
					if value == clientID {
						//fmt.Println("Donered**",key.IdReduce,clientID)
						c.ReduceTasks <- key//把数据重新放入管道
						c.ReduceTasksbp[key] = "0"//0表示未使用或者已处理
					}
				}
			}
			// cmd := exec.Command("kill", clientID)
			// err := cmd.Run()
			// if err != nil {
			// 	log.Fatal("Failed to terminate process:", err)
			// }
			// 执行额外操作，例如发送警报或断开连接等
		}
	}	
	//Your code here.
	if c.NumReduce == c.NumReduceFin {
		close(c.MapTasks)
		close(c.ReduceTasks)
		ret = true
	}
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduceFin: 0,
		NumReduce: nReduce,
		NumMap: len(files),
		MapTasks: make(chan ResponseTask, len(files)),
		ReduceTasks: make(chan ResponseTask , nReduce),
		MapTasksbp: make(map[ResponseTask]string),
		ReduceTasksbp: make(map[ResponseTask]string),
		NumMapFin: 0,
		clients: make(map[string]*ClientHeartbeat),
	}

	// Your code here.
 
	for id, filename := range(files) {
		maptask := ResponseTask{
			Filename: filename,
			IdMap: id,
			MapFin: false,
			NReduce: nReduce,
		}
		c.MapTasks <- maptask
		val := "0"
		c.MapTasksbp[maptask] = val
	}
	for i := 0; i < nReduce; i++ {
		reducetask := ResponseTask{
			IdReduce: i,
			ReduceFin: false,
			IdMap: len(files),
		}
		c.ReduceTasks <- reducetask
		c.ReduceTasksbp[reducetask] = "0"
	}

	
	c.server()
	return &c
}
