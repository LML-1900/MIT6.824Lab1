package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
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
	//fmt.Println("start workers")
	for {
		args := AskTask{}
		reply := TaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			log.Fatal("AssignTask call failed!\n")
		} else {
			switch reply.Task {
			case Exit:
				//fmt.Println("worker Eixt")
				return
			case Wait:
				//fmt.Println("worker Sleep")
				time.Sleep(time.Second)
			case Map:
				//fmt.Println("worker run map task")
				finishArgs := FinishTaskRequest{}
				finishArgs.TaskSucceed = DoMapTask(reply.Filename, reply.TaskID, reply.NReduce, mapf)
				finishArgs.Task = Map
				finishArgs.TaskID = reply.TaskID
				finishReply := FinishTaskReply{}
				//fmt.Printf("finished map task %d\n", reply.TaskID)
				ok := call("Coordinator.FinishTask", &finishArgs, &finishReply)
				if !ok {
					log.Fatal("FinishTask call failed!\n")
				}
			case Reduce:
				//fmt.Println("worker run reduce task")
				finishArgs := FinishTaskRequest{}
				finishArgs.TaskSucceed = DoReduceTask(reply.TaskID, reply.MapTaskTotalNum, reducef)
				//DoReduceTask(reply.TaskID, reducef)
				finishArgs.Task = Reduce
				finishArgs.TaskID = reply.TaskID
				finishReply := FinishTaskReply{}
				//fmt.Printf("finished reduce task %d\n", reply.TaskID)
				ok := call("Coordinator.FinishTask", &finishArgs, &finishReply)
				if !ok {
					log.Fatal("FinishTask call failed!\n")
				}
			}
		}
	}
}

func DoMapTask(mapFileName string, taskID int, NReduce int, mapf func(string, string) []KeyValue) (bool){
	// map the given file
	mapfile, err := os.Open(mapFileName)
	if err != nil {
		log.Fatalf("cannot open %v", mapFileName)
		return false;
	}
	content, err := ioutil.ReadAll(mapfile)
	if err != nil {
		log.Fatalf("cannot read %v", mapFileName)
		return false;
	}
	mapfile.Close()
	kva := mapf(mapFileName, string(content))

	// output NReduce files
	// first: create NReduce files
	outfiles := make([]*os.File, NReduce)
	encoders := make([]*json.Encoder, NReduce)
	//intermediateFiles := make([]string, NReduce)
	for i := 0; i < NReduce; i++ {
		// create tmpfile for results
		tmpfilename := fmt.Sprintf("mr-tmp-%d-%d", taskID, i)
		//"" means output to a default directory, /tmp in linux
		tmpfile, err := ioutil.TempFile("", tmpfilename)
		if err != nil {
			log.Fatalf("cannot create tmpfile: %v", tmpfilename)
			return false;
		}
		//tmpfile.Name() return file's path
		//delete tmpfile before return
		defer os.Remove(tmpfile.Name())

		outfiles[i] = tmpfile
		encoders[i] = json.NewEncoder(tmpfile)
	}
	// append keys to corresponding buckets
	for _, kv := range kva {
		bucket := ihash(kv.Key) % NReduce
		// file append write
		err := encoders[bucket].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv: %v", kv)
			//return nil, false;
			return false;
		}
	}
	// close outfiles
	for i, tmpfile := range outfiles {
		outfilename := fmt.Sprintf("mr-%d-%d", taskID, i)
		err = os.Rename(tmpfile.Name(), outfilename)
		if err != nil {
			log.Fatalf("cannot rename mr-%d-%d\n", taskID, i)
			return false;
		}
	}
	//return intermediateFiles, true;
	return true;
}

func DoReduceTask(taskID int, mapTaskTotalNum int,reducef func(string, []string) string) (bool){
	// read all intermediate files
	// write all keys in files to kva slice
	kva := []KeyValue{}
	for i := 0; i < mapTaskTotalNum; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, taskID);
		//fmt.Printf("mapTaskTotalNum: %d; current i: %d; intermediateFileName: %v\n", mapTaskTotalNum, i, intermediateFileName)
		intermediateFile, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
			return false;
		}
		dec := json.NewDecoder(intermediateFile)
		for  {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	
	// create tmpfile for results
	tmpfilename := fmt.Sprintf("mr-tmp-%d", taskID)
	//"" means output to a default directory, /tmp in linux
	tmpfile, err := ioutil.TempFile("", tmpfilename)
	if err != nil {
		log.Fatalf("cannot create tmpfile: %v", tmpfilename)
		return false;
	}
	//tmpfile.Name() return file's path
	//delete tmpfile before return
	defer os.Remove(tmpfile.Name())

	sort.Sort(ByKey(kva))
	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-tmp-taskID.
	//
	i := 0
	for i < len(kva) {
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
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		//fmt.Printf("%v %v\n", kva[i].Key, output)

		i = j
	}

	// finish reduce task, rename tmpfile
	// os.Rename automatically close file
	outfilename := fmt.Sprintf("mr-out-%d", taskID)
	//fmt.Printf("output file %v\n", outfilename)
	err = os.Rename(tmpfile.Name(), outfilename)
	if err != nil {
		log.Fatalf("cannot rename %v\n", tmpfilename)
		return false;
	}
	return true;
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
