package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

//worker向master发送的心跳信息
type Request struct {
	WorkerId string
	//剩余可以运行的任务数量
	RemainSlot int
	//运行中任务状态回报
	TaskReports []*TaskReport
}

type TaskReport struct {
	TaskId int
	State  State
	Output []string
}

type Response struct {
	CurrentStage Stage
	AssignTasks  []*AssignTask
	ReduceCount  int
}

type AssignTask struct {
	Id    int
	Input []string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
