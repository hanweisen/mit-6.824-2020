package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Stage string
type State string

const (
	MapStage        Stage = "MapStqage"
	ReduceStage     Stage = "ReduceStage"
	TerminatedStage Stage = "TerminatedStage"
	Init            State = "Init"
	Running         State = "Running"
	Success         State = "Success"
	Failure         State = "Failure"
)

type TaskBase struct {
	id           int
	state        State
	AssignWorker string
	submitTime   time.Time
	startTime    time.Time
	endTime      time.Time
	output       []string
	input        []string
	retryTimes   int
}
type Task interface {
	SetState(newState State)
	GetState() State
	GetId() int
	GetAssignWorker() string
	SetAssignWorker(worker string)
	GetStartTime() time.Time
	SetStartTime(time time.Time)
	AddRetryTimes()
	GetRetryTimes() int
}

func (base *TaskBase) SetState(newState State) {
	base.state = newState
}

func (base *TaskBase) GetState() State {
	return base.state
}

func (base *TaskBase) GetId() int {
	return base.id
}

func (base *TaskBase) GetAssignWorker() string {
	return base.AssignWorker
}

func (base *TaskBase) SetAssignWorker(worker string) {
	base.AssignWorker = worker
}

func (base *TaskBase) GetStartTime() time.Time {
	return base.startTime
}

func (base *TaskBase) SetStartTime(startTime time.Time) {
	base.startTime = startTime
}

func (base *TaskBase) GetRetryTimes() int {
	return base.retryTimes
}

func (base *TaskBase) AddRetryTimes() {
	base.retryTimes++
}

type MapTask struct {
	TaskBase
	output []string
	input  string
}

type ReduceTask struct {
	TaskBase
	output string
	input  []string
}

type WorkerStatus struct {
	workerId      string
	heartBeapTime time.Time
}

type Executing struct {
	waiting    []Task
	running    map[int]Task
	terminated []Task
}

type Master struct {
	// Your definitions here.
	//inputs string[]
	inputs        []string
	stage         Stage
	executing     *Executing
	workerStatus  map[string]*WorkerStatus
	nreduce       int
	maxRertyTimes int
	mut           sync.Mutex
	logger        *log.Logger
}

var timeOut, _ = time.ParseDuration("10s")

func (m *Master) checker() {
	m.mut.Lock()
	defer m.mut.Unlock()
	logger := m.logger
	logger.Printf("worker healthy check")
	//check worker alive
	now := time.Now()
	for _, worker := range m.workerStatus {
		if worker.heartBeapTime.Add(timeOut).Before(now) {
			logger.Println(worker.workerId, "is down")
			delete(m.workerStatus, worker.workerId)
			running := m.executing.running
			for _, task := range running {
				if task.GetAssignWorker() == worker.workerId {
					delete(running, task.GetId())
					task.AddRetryTimes()
					if task.GetRetryTimes() == m.maxRertyTimes {
						logger.Println("stage", m.stage, ",task", task.GetId(), "exceed retry times set failure")
						task.SetState(Failure)
						m.executing.terminated = append(m.executing.terminated, task)
					} else {
						logger.Println("stage", m.stage, ",put task", task.GetId(), "in waiting")
						task.SetState(Init)
						m.executing.waiting = append(m.executing.waiting, task)
					}
				}
			}
		}
	}
}

func (m *Master) updateWorkerStatus(request *Request, now time.Time) {
	if w, e := m.workerStatus[request.WorkerId]; e {
		w.heartBeapTime = now
	} else {
		m.workerStatus[request.WorkerId] = &WorkerStatus{workerId: request.WorkerId, heartBeapTime: now}
	}
}

func (m *Master) HeartBeat(request *Request, response *Response) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	logger := m.logger
	logger.Println("receive hart beat from:", request.WorkerId)
	response.ReduceCount = m.nreduce
	now := time.Now()
	//update worker status
	m.updateWorkerStatus(request, now)
	logger.Println("received report", len(request.TaskReports))
	m.updateTaskState(request)
	m.changeState()
	m.assignTask(request, response)

	logger.Println("current stage", m.stage)
	response.CurrentStage = m.stage
	return nil
}

func (m *Master) updateTaskState(request *Request) {
	for _, report := range request.TaskReports {
		finishedTask, exist := m.executing.running[report.TaskId]
		if !exist {
			panic("error can not find running task with id " + strconv.Itoa(report.TaskId))
		}
		finishedTask.SetState(report.State)
		delete(m.executing.running, report.TaskId)
		if m.stage == MapStage {
			mapTask := finishedTask.(*MapTask)
			mapTask.output = report.Output
		}
		m.executing.terminated = append(m.executing.terminated, finishedTask)
	}
}

func (m *Master) assignTask(request *Request, response *Response) {
	now := time.Now()
	for request.RemainSlot > 0 && len(m.executing.waiting) > 0 {
		request.RemainSlot--
		assignTask := m.executing.waiting[0]
		m.executing.waiting = m.executing.waiting[1:]
		m.executing.running[assignTask.GetId()] = assignTask
		assignTask.SetState(Running)
		assignTask.SetAssignWorker(request.WorkerId)
		assignTask.SetStartTime(now)
		switch m.stage {
		case MapStage:
			mapTask := assignTask.(*MapTask)
			response.AssignTasks = append(response.AssignTasks, &AssignTask{Id: mapTask.id, Input: []string{mapTask.input}})
		case ReduceStage:
			reduceTask := assignTask.(*ReduceTask)
			response.AssignTasks = append(response.AssignTasks, &AssignTask{Id: reduceTask.id, Input: reduceTask.input})
		default:
			panic("error stage")
		}
	}
}

func (m *Master) changeState() {
	logger := m.logger
	executing := m.executing
	if len(executing.running) != 0 || len(executing.waiting) != 0 {
		return
	}
	hasFailure := false
	for _, task := range executing.terminated {
		if task.GetState() == Failure {
			hasFailure = true
			break
		}
	}
	if hasFailure {
		m.stage = TerminatedStage
		return
	}

	switch m.stage {
	case MapStage:
		logger.Println("map tasks completed enter reduce stage")
		m.stage = ReduceStage
		//为reduce分配输入
		newWaiting := []Task{}
		newExecuting := &Executing{running: map[int]Task{}, terminated: []Task{}}
		for i := 0; i < m.nreduce; i++ {
			reduceTask := &ReduceTask{}
			reduceTask.id = i
			reduceTask.state = Init
			newWaiting = append(newWaiting, reduceTask)
		}
		newExecuting.waiting = newWaiting
		for _, task := range executing.terminated {
			mapTask := task.(*MapTask)
			logger.Println("map task", mapTask.id, mapTask.state)
			for _, input := range mapTask.output {
				id := getSplitId(input)
				reduceTask := newWaiting[id].(*ReduceTask)
				reduceTask.input = append(reduceTask.input, input)
			}
		}
		m.executing = newExecuting
	case ReduceStage:
		logger.Println("reduce tasks completed")
		m.stage = TerminatedStage
	}
}

func getSplitId(input string) int {
	splits := strings.Split(input, "-")
	id, _ := strconv.Atoi(splits[2])
	return id
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	logger := m.logger
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	logger.Println("start server success")
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.stage == TerminatedStage
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := &Master{
		inputs:        []string{},
		stage:         MapStage,
		executing:     &Executing{waiting: []Task{}, running: map[int]Task{}, terminated: []Task{}},
		workerStatus:  map[string]*WorkerStatus{},
		nreduce:       nReduce,
		maxRertyTimes: 10,
	}
	m.logger = log.New(os.Stdout, "master ", log.Lshortfile|log.LstdFlags)
	m.logger.Println("retryTimes:", m.maxRertyTimes)
	//收集输入文件
	collectInput(files, m)
	//初始化mapTask
	now := time.Now()
	for i, v := range m.inputs {
		mapTask := &MapTask{}
		mapTask.id = i
		mapTask.input = v
		mapTask.state = Init
		mapTask.submitTime = now
		m.executing.waiting = append(m.executing.waiting, mapTask)
	}
	m.server()
	m.logger.Println("master started success", "mapTaskSize:", len(m.executing.waiting), "reduceTaskSize:", nReduce)
	go func() {
		m.logger.Println("start checker thread")
		for m.Done() == false {
			m.checker()
			time.Sleep(time.Second * 1)
		}
	}()
	return m
}

func collectInput(files []string, m *Master) {
	for _, p := range files {
		filePattern := path.Base(p)
		basePath := path.Dir(p)
		if basePath == "" {
			basePath = "."
		}
		walkFn := func(pa string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() && pa != basePath {
				return filepath.SkipDir
			}
			if !info.IsDir() {
				matched, _ := path.Match(filePattern, info.Name())
				if matched {
					m.inputs = append(m.inputs, pa)
				}
			}
			return nil
		}
		filepath.Walk(basePath, walkFn)
	}
}
