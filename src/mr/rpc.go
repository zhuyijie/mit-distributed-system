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

type MRArgs struct {
	FinishedMapId    int `json:"finishedMapId,omitempty" default:"-1"`
	FinishedReduceId int `json:"finishedReduceId,omitempty" default:"-1"`
}

type MRReply struct {
	Done    bool `json:"done,omitempty" default:"false"`
	Wait    bool `json:"wait,omitempty" default:"false"`
	MapId   int  `json:"mapId,omitempty" default:"-1"`
	MapFile string

	ReduceId int `json:"reduceId,omitempty" default:"-1"`
	NReduce  int `json:"nReduce,omitempty" default:"-1"`
	NMap     int `json:"nMap,omitempty" default:"-1"`
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
