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

// example to show how to declare the arguments
// and reply for an RPC.
type JobResponse struct {
	Err error
}

type JobRequest struct {
	Job Cordjob
}

type FetchTaskArgs struct {
	Job         Cordjob
	done_map    bool
	done_reduce bool
	nreduce     int
}

type FetchTaskReply struct {
	Job         Cordjob
	Done_map    bool
	Done_reduce bool
	Nreduce     int
	Job_number  int
}

type Request struct {
	RequestBody string
}

type Response struct {
	ResponseBody string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
