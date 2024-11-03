package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type SchedulePhase int

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
	WaitPhase
)

const (
	Timeout = 10 * time.Second
)

type Task struct {
	FileName string
	Id       int

	WorkerId  int
	StartTime time.Time
	Status    TaskStatus
}

type Coordinator struct {
	mu      sync.RWMutex
	files   []string
	nReduce int
	nMap    int
	phase   SchedulePhase
	tasks   []Task
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	reply.Phase = c.phase

	found := false

	if c.phase == CompletePhase {
		// NOOP
		return nil
	}
	if c.phase == MapPhase {
		for i, task := range c.tasks {
			if task.Status == Idle || (task.Status == InProgress && time.Since(task.StartTime) > Timeout) {
				c.tasks[i].WorkerId = args.WorkerId
				c.tasks[i].StartTime = time.Now()
				c.tasks[i].Status = InProgress
				reply.Task = c.tasks[i]
				found = true
				break
			}
		}
	} else if c.phase == ReducePhase {
		for i, task := range c.tasks {
			if task.Status == Idle || (task.Status == InProgress && time.Since(task.StartTime) > Timeout) {
				c.tasks[i].WorkerId = args.WorkerId
				c.tasks[i].StartTime = time.Now()
				c.tasks[i].Status = InProgress
				reply.Task = c.tasks[i]
				found = true
				break
			}
		}
	}
	// can't found an available task, but the phase is not complete
	// wait for a while
	if !found {
		reply.Phase = WaitPhase
	}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskRequest, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == c.phase {
		task := &c.tasks[args.Task.Id]
		if task.Status == InProgress && task.WorkerId == args.Task.WorkerId {
			task.Status = Completed
			reply.Complete = true
			c.CheckTaskDone()
		} else {
			reply.Complete = false
		}
	} else {
		reply.Complete = false
	}
	return nil
}

// should be in lock
func (c *Coordinator) CheckTaskDone() {
	allTasksCompleted := true

	for _, task := range c.tasks {
		if task.Status != Completed {
			allTasksCompleted = false
			break
		}
	}

	if allTasksCompleted {
		if c.phase == MapPhase {
			c.phase = ReducePhase
			c.tasks = make([]Task, c.nReduce)
			for i := 0; i < c.nReduce; i++ {
				c.tasks[i] = Task{Id: i, Status: Idle}
			}
		} else if c.phase == ReducePhase {
			c.phase = CompletePhase
		}
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == CompletePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	length := len(files)
	c.phase = MapPhase
	c.files = files
	c.nReduce = nReduce
	c.nMap = length
	c.phase = MapPhase
	c.tasks = make([]Task, length)
	for i := 0; i < length; i++ {
		c.tasks[i] = Task{FileName: files[i], Id: i, Status: Idle}
	}

	c.server()
	return &c
}
