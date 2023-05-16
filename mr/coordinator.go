package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

type TaskPhase int

const (
	MapPhase TaskPhase = iota
	ReducePhase
	WaitPhase
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	NReduce int
	// finished reduce task and map task
	doneCounter int
	finishMapTask int
	files []string
	// mark map/reduce task's state
	// -1 means not assigned
	// 0 means assinged but not finished
	// 1 means finished
	mapTaskState []int
	reduceTaskState []int
	// count assigned task
	assignedMapTask int
	assignedReduceTask int
}

// get task phase: map / reduce / wait
func (c *Coordinator) getTaskPhase ()TaskPhase {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.files)
	if c.assignedMapTask < n {
		return MapPhase
	} else if c.finishMapTask == n && c.assignedReduceTask < c.NReduce {
		return ReducePhase
	} else {
		return WaitPhase
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *AskTask, reply *TaskReply) error {
	if c.Done() {
		//fmt.Println("coordinator: all work finished")
		reply.Task = Exit
	} else if c.getTaskPhase() == MapPhase {
		//fmt.Println("coordinator: assign a map task")
		id := c.getTask(MapPhase, reply)
		go c.spyTask(Map, id)
	} else if c.getTaskPhase() == ReducePhase {
		//fmt.Println("coordinator: assign a reduce task")
		id := c.getTask(ReducePhase, reply)
		go c.spyTask(Reduce, id)
	} else {
		//fmt.Println("coordinator: cannot assign any task, please wait")
		reply.Task = Wait
	}
	return nil
}

// get a map / reduce task
// return task id
func (c *Coordinator) getTask(taskP TaskPhase, reply *TaskReply) int{
	c.mu.Lock()
	defer c.mu.Unlock()
	id := 0
	if taskP == MapPhase {
		reply.Task = Map
		reply.NReduce = c.NReduce
		for i := 0; i < len(c.files); i++ {
			if c.mapTaskState[i] == -1 {
				reply.Filename = c.files[i]
				reply.TaskID = i
				c.assignedMapTask++
				c.mapTaskState[i] = 0
				id = i
				break
			}
		} 
	} else if taskP == ReducePhase {
		reply.Task = Reduce
		reply.NReduce = c.NReduce
		reply.MapTaskTotalNum = len(c.files)
		for i := 0; i < c.NReduce; i++ {
			if c.reduceTaskState[i] == -1 {
				c.assignedReduceTask++
				c.reduceTaskState[i] = 0
				reply.TaskID = i
				id = i
				break
			}
		}
	}
	return id
}


// deal with the finished tasks
func (c *Coordinator) FinishTask(args *FinishTaskRequest, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Task == Map {
		id := args.TaskID
		if args.TaskSucceed {
			// if the task has been done by another worker
			// ignore the message
			if c.mapTaskState[id] == 1 {
				return nil
			}
			c.mapTaskState[id] = 1
			c.finishMapTask++
			//fmt.Printf("receive map task %d finish message\n", id)
		}else {
			c.assignedMapTask--
			c.mapTaskState[id] = -1
		}
		
	} else {
		id := args.TaskID
		if args.TaskSucceed {
			if c.reduceTaskState[id] == 1 {
				return nil
			}
			c.reduceTaskState[id] = 1
			c.doneCounter++
		}else {
			c.assignedReduceTask--
			c.reduceTaskState[id] = -1
		}
		
		//fmt.Printf("receive reduce task %d finish message, task state: %d, finishReduceTask: %d\n", id, c.reduceTaskState[id], c.doneCounter)
	}
	return nil
}

// spy on the running tasks
// after 10 seconds, check the state of the task
// if task hasn't been finished, mark its state -1
// so that it can be assigned to another worker
func (c *Coordinator)spyTask (taskP TaskType, taskID int) {
	// TODO: modify to time.AfterFunc or keep as follow?
	time.Sleep(time.Second * 10)
	c.mu.Lock()
	defer c.mu.Unlock()
	if taskP == Map {
		// if has been finished or reset to unassigned, ignore
		if c.mapTaskState[taskID] == 0 {
			c.assignedMapTask--
			c.mapTaskState[taskID] = -1
		}
	} else {
		if c.reduceTaskState[taskID] == 0 {
			c.assignedReduceTask--
			c.reduceTaskState[taskID] = -1
		}
	}
}

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
	ret := false
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	if c.doneCounter == c.NReduce {
		return true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.doneCounter = 0
	c.finishMapTask = 0
	c.files = files
	n := len(files)
	c.mapTaskState = make([]int, n)
	// innitialize mapTaskState
	for i := 0; i < n; i++ {
		c.mapTaskState[i] = -1
	}
	c.reduceTaskState = make([]int, nReduce)
	// innitialize reduceTaskState
	for i := 0; i < nReduce; i++ {
		c.reduceTaskState[i] = -1
	}
	c.assignedMapTask = 0
	c.assignedReduceTask = 0
	fmt.Println("coordinator initialization completes, start server")
	c.server()
	return &c
}
