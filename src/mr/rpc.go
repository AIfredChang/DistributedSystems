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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestTask struct {
	source string
}

type RequestTaskResponse struct {
	fileNames []string
	InputNum  int64
	TaskType_ int
	Id        int
}

type doneRequest struct {
	TaskType_ int
	Index     int
}

type doneResponse struct {
	recieved bool
}

const (
	MAP    = 0
	REDUCE = 1
	SLEEP  = 2
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
