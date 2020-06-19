package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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

	}

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
