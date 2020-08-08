package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func toBucketID(key string, bucketSum int) int {
	return ihash(key) % bucketSum
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	pid := os.Getpid()
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	executor := &Executor{taskChan: make(chan *CompletedTask, 100)}
	executor.id = "worker-" + name + "-" + strconv.Itoa(pid)
	executor.mapF = mapf
	executor.reduceF = reducef
	//parallelism测试这里必须设置为1才能通过
	executor.slot = 1
	executor.remainSlot = executor.slot
	executor.logger = log.New(os.Stdout, executor.id+" ", log.Lshortfile|log.LstdFlags)
	executor.logger.Println("worker created")
	for {
		executor.heartBeat()
		time.Sleep(time.Second * 1)
	}
}

type CompletedTask struct {
	id     int
	state  State
	output []string
}

type Executor struct {
	id         string
	slot       int
	remainSlot int
	mapF       func(string, string) []KeyValue
	reduceF    func(string, []string) string
	logger     *log.Logger
	workerMut  sync.Mutex
	taskChan   chan *CompletedTask
}

func (executor *Executor) heartBeat() {
	executor.logger.Println("worker")
	taskChan := executor.taskChan
	req := Request{}
	req.WorkerId = executor.id
	req.RemainSlot = executor.remainSlot
	taskReports := []*TaskReport{}
	completedTasks := []*CompletedTask{}
	completedLen := len(taskChan)
	executor.remainSlot = executor.remainSlot + completedLen
	executor.logger.Println("completed task:", completedLen)
	for i := 0; i < completedLen; i++ {
		completedTasks = append(completedTasks, <-taskChan)
	}
	for _, task := range completedTasks {
		report := &TaskReport{}
		report.TaskId = task.id
		report.Output = task.output
		report.State = task.state
		taskReports = append(taskReports, report)
	}
	req.TaskReports = taskReports
	response := Response{}
	executor.logger.Println("assign tasks", response.AssignTasks)
	executor.remainSlot = executor.remainSlot - len(response.AssignTasks)
	if call("Master.HeartBeat", &req, &response) {
		executor.logger.Println("current remainSlot", executor.remainSlot,
			"receive task:", len(response.AssignTasks))
		executor.remainSlot = executor.remainSlot - len(response.AssignTasks)
		executor.logger.Println("acquire task:", len(response.AssignTasks))
		for _, task := range response.AssignTasks {
			task := task
			switch response.CurrentStage {
			case MapStage:
				go executor.executeMapF(task, response.ReduceCount)
			case ReduceStage:
				go executor.executorReduceF(task)
			default:
				panic("Invalid stage:" + response.CurrentStage)
			}
		}
	}
	executor.logger.Println("worker heartbeat end")
}

func (executor *Executor) executeMapF(assignTask *AssignTask, nReduce int) {
	executor.logger.Println("execute map task", assignTask.Id, "with input", assignTask.Input)
	path := assignTask.Input[0]
	content := readContent(path)
	kva := executor.mapF(path, content)
	//flush output
	ofiles := []*os.File{}
	encs := []*json.Encoder{}
	suffix := "." + executor.id + ".tmp"
	outputs := []string{}
	for i := 0; i < nReduce; i++ {
		oname := "mr-" + strconv.Itoa(assignTask.Id) + "-" + strconv.Itoa(i) + suffix
		outputs = append(outputs, strings.TrimSuffix(oname, suffix))
		ofile, _ := os.Create(oname)
		ofiles = append(ofiles, ofile)
	}
	for i := 0; i < nReduce; i++ {
		encs = append(encs, json.NewEncoder(ofiles[i]))
	}
	for _, kv := range kva {
		bucketID := toBucketID(kv.Key, nReduce)
		encs[bucketID].Encode(&kv)
	}
	for _, file := range ofiles {
		file.Close()
		os.Rename(file.Name(), strings.TrimSuffix(file.Name(), suffix))
	}
	executor.logger.Println("map task with id ", assignTask.Id, "execute success")
	executor.taskChan <- &CompletedTask{id: assignTask.Id, output: outputs}
}

func (executor *Executor) executorReduceF(assignTask *AssignTask) {
	executor.logger.Println("execute reduce task with id:", assignTask.Id, "with inputs", assignTask.Input)
	kvs := []KeyValue{}
	ifiles := []*os.File{}
	for _, input := range assignTask.Input {
		inputFile, e := os.Open(input)
		if e != nil {
			panic("open file failure with path " + input)
		}
		ifiles = append(ifiles, inputFile)
	}
	for _, file := range ifiles {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kvs))
	split := []string{}
	suffix := "." + executor.id + ".tmp"
	oname := "mr-out-" + strconv.Itoa(assignTask.Id) + suffix
	ofile, _ := os.Create(oname)
	defer func() {
		ofile.Close()
		os.Rename(oname, strings.TrimSuffix(oname, suffix))
	}()
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[i].Key == kvs[j].Key {
			j++
		}
		split = []string{}
		for k := i; k < j; k++ {
			split = append(split, kvs[k].Value)
		}
		output := executor.reduceF(kvs[i].Key, split)
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	executor.taskChan <- &CompletedTask{id: assignTask.Id, output: []string{oname}}
}

func readContent(path string) string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", path)
	}
	file.Close()
	return string(content)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
