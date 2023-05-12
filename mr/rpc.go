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

type RegisterArgs struct{}
type RegisterReply struct {
	NMap    int
	NReduce int
}

type ApplyForMapArgs struct{}
type ApplyForMapReply struct {
	MapID    int
	Filename string
}

type MapDoneArgs struct {
	MapId int
}
type MapDoneReply struct {
}

type ReduceDoneArgs struct {
	ReduceId int
}
type ReduceDoneReply struct{}

type ApplyForReduceArgs struct{}
type ApplyForReduceReply struct {
	ReduceId int
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
