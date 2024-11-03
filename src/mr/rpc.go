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

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Task    Task
	Phase   SchedulePhase
	NReduce int
	NMap    int
}

type ReportTaskRequest struct {
	Task     Task
	TaskType SchedulePhase
}

type ReportTaskReply struct {
	Complete bool
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
