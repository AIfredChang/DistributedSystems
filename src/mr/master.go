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

type Task struct {
	taskType_      int
	isCompleted_   bool
	isDistributed_ bool
	index_         int
}

type Master struct {
	files       []string
	mTasks      []Task
	rTasks      []Task
	doneMaps    int64
	doneReduces int64
	mutex       sync.Mutex
	isFinished  bool
	nReduce     int
}

func (m *Master) RequestForTask(args *RequestTask, reply *RequestTaskResponse) error {
	if m.ReduceDone() {
		reply.TaskType_ = SLEEP
		return nil
	}

	if m.MapDone() {
		task := m.getTask(REDUCE)
		if task.taskType_ == SLEEP {
			reply.TaskType_ = SLEEP
			return nil
		}
		reply.TaskType_ = REDUCE
		reply.Id = task.index_
		go m.waitForTask(reply.Id, REDUCE)
	} else {
		task := m.getTask(MAP)
		if task.taskType_ == SLEEP {
			reply.TaskType_ = SLEEP
			return nil
		}
		reply.TaskType_ = MAP
		reply.Id = task.index_
		reply.fileNames = append(reply.fileNames, m.getFile(reply.Id))

		go m.waitForTask(reply.Id, MAP)
	}
	return nil
}

func (m *Master) waitForTask(index int, taskType int) {
	timer := time.NewTimer(time.Second * 10)
	<-timer.C
	m.mutex.Lock()
	if taskType == MAP {
		if m.mTasks[index].isCompleted_ == false {
			m.mTasks[index].isDistributed_ = false
		}
	} else {
		if m.rTasks[index].isCompleted_ == false {
			m.rTasks[index].isDistributed_ = false
		}
	}
	m.mutex.Unlock()

}

func (m *Master) SubmitTask(args *doneRequest, reply *doneResponse) error {
	m.mutex.Lock()
	if args.TaskType_ == MAP && m.mTasks[args.Index].isCompleted_ == false {
		m.mTasks[args.Index].isCompleted_ = true
		if m.doneMaps == -1 {
			m.doneMaps = 0
		}
		m.doneMaps++
	} else if args.TaskType_ == REDUCE && m.rTasks[args.Index].isCompleted_ == false {
		m.rTasks[args.Index].isCompleted_ = true
		if m.doneReduces == -1 {
			m.doneReduces = 0
		}
		m.doneReduces++
		if m.doneReduces == int64(m.nReduce) {
			m.isFinished = true
		}
	}
	m.mutex.Unlock()
	return nil
}

func (m *Master) getFile(index int) string {
	var file string
	m.mutex.Lock()
	file = m.files[index]
	m.mutex.Unlock()
	return file
}

func (m *Master) getTask(taskType int) Task {
	task := Task{}
	task.taskType_ = SLEEP
	m.mutex.Lock()
	if taskType == MAP {
		for i := 0; i < len(m.mTasks); i++ {
			if m.mTasks[i].isDistributed_ == false {
				m.mTasks[i].isDistributed_ = true
				task = m.mTasks[i]
				break
			}

		}

	} else if taskType == REDUCE {
		for i := 0; i < len(m.rTasks); i++ {
			if m.rTasks[i].isDistributed_ == false {
				m.rTasks[i].isDistributed_ = true
				task = m.rTasks[i]
				break
			}
		}

	}
	m.mutex.Unlock()
	return task
}

func (m *Master) ReduceDone() bool {
	isFinished := false
	m.mutex.Lock()

	if m.doneReduces >= int64(m.nReduce) {
		isFinished = true
	}
	m.mutex.Unlock()
	return isFinished

}

func (m *Master) MapDone() bool {
	isFinished := false
	m.mutex.Lock()

	if m.doneMaps >= int64(m.nReduce) {
		isFinished = true
	}
	m.mutex.Unlock()
	return isFinished

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
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
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.files = files
	m.isFinished = false
	m.doneMaps = -1
	m.doneReduces = -1
	m.mutex = sync.Mutex{}
	m.nReduce = nReduce

	for i := range files {
		mTask := Task{MAP, false, false, i}
		m.mTasks = append(m.mTasks, mTask)
	}

	for i := 0; i < nReduce; i++ {
		rTask := Task{REDUCE, false, false, i}
		m.rTasks = append(m.rTasks, rTask)
	}

	m.server()
	return &m
}
