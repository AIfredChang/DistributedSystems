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

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {

	if m.numFiles >= 0 {
		m.mu.Lock()
		reply.index = m.numFiles
		reply.fileName = m.files[m.numFiles]
		reply.nReduce = m.nReduce
		reply.mapTime = true
		m.numFiles--
		m.mu.Unlock()
	}

	return nil

}

func (m *Master) RequestForTask(args *RequestTask, reply *RequestTaskResponse) error {
	if m.ReduceDone() {
		reply.TaskType = SLEEP
		return nil
	}

	if m.MapDone() {
		task := m.getTask(REDUCE)
		if task.taskType_ == NONE {
			reply.TaskType_ = NONE
			return nil
		}
		reply.TaskType_ = REDUCE
		reply.Id_ = task.index_
		go m.waitForTask(reply.Id_, REDUCE)
	} else {
		task := m.getTask(MAP)
		if task.taskType_ == NONE {
			reply.taskType_ == NONE
			return nil
		}
		reply.TaskType_ = MAP
		reply.Id_ = task.index_
		reply.FileNames_ = append(reply.fileNames, m.getFile(reply.Id_))

		go m.waitForTask(reply.Id_, MAP)
	}

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

func (m *Master) getFile(index int) string {
	var file string
	m.mutex.Lock()
	file = m.files[index]
	m.mutex.Unlock()
	return file
}

func (m *Master) getTask(taskType int) Task {
	task := Task{}
	task.taskType_ = NONE
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
				task = m.reduceTasks[i]
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
		mTask := Task{MAP, false, false, index}
		m.mTasks = append(m.mTasks, mTask)
	}

	for i := 0; i < nReduce; i++ {
		rTask := Task{REDUCE, false, false, i}
		m.reduceTasks = append(m.reduceTaske, rTask)
	}

	m.server()
	return &m
}
