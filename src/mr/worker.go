package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// wait for the coordinator to start
	time.Sleep(10 * time.Second)

	pid := os.Getpid()
	for {
		response := requestTask(pid)
		switch response.Phase {
		case MapPhase:
			doMapTask(mapf, response)
		case ReducePhase:
			doReduceTask(reducef, response)
		case WaitPhase:
			time.Sleep(2 * time.Second)
		case CompletePhase:
			return
		default:
			panic(fmt.Sprintf("unexpected taskType %v", response.Phase))
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, response RequestTaskReply) {
	task := response.Task

	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()

	kva := mapf(task.FileName, string(content))
	intermediateBucket := make([][]KeyValue, response.NReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % response.NReduce
		intermediateBucket[idx] = append(intermediateBucket[idx], kv)
	}
	for i, kva := range intermediateBucket {
		// filename : mr-<mapTaskId>-<reduceTaskId>
		file, err := os.Create(fmt.Sprintf("mr-%d-%d", task.Id, i))
		if err != nil {
			log.Fatalf("cannot create %v", fmt.Sprintf("mr-%d-%d", task.Id, i))
		}
		encoder := json.NewEncoder(file)
		err = encoder.Encode(kva)
		if err != nil {
			log.Fatalf("cannot encode %v", fmt.Sprintf("mr-%d-%d", task.Id, i))
			return
		}
		file.Close()
	}
	reportTask(task, response.Phase)
}

func doReduceTask(reducef func(string, []string) string, response RequestTaskReply) {
	task := response.Task
	intermediate := []KeyValue{}
	for i := 0; i < response.NMap; i++ {
		// filename : mr-<mapTaskId>-<reduceTaskId>
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, task.Id))
		if err != nil {
			log.Fatalf("cannot open %v", fmt.Sprintf("mr-%d-%d", i, task.Id))
		}
		decoder := json.NewDecoder(file)
		kva := []KeyValue{}
		err = decoder.Decode(&kva)
		if err != nil {
			log.Fatalf("cannot decode %v", fmt.Sprintf("mr-%d-%d", i, task.Id))
			return
		}
		intermediate = append(intermediate, kva...)
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// filename : mr-out-<reduceTaskId>
	oname := fmt.Sprintf("mr-out-%d", task.Id)
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	reportTask(task, response.Phase)
}

func requestTask(id int) RequestTaskReply {
	args := RequestTaskArgs{WorkerId: id}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		log.Printf("request task failed %v", args)
	}

	return reply
}

func reportTask(task Task, phase SchedulePhase) ReportTaskReply {
	args := ReportTaskRequest{task, phase}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		log.Printf("report task failed %v", args)
	}

	return reply
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
