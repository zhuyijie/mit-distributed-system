package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	Mu sync.Mutex
	// Map tasks
	MapTasks        []string // [file1, file2, file3]
	NMap            int
	MapScheduleTime []int64 // [0, 0, 0]
	MapSubmitted    []bool  // [false, false, false]

	// Reduce tasks
	NReduce            int
	ReduceScheduleTime []int64
	ReduceSubmitted    []bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Handle(args *MRArgs, reply *MRReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if args.FinishedMapId != -1 {
		fmt.Printf("Map %d finished\n", args.FinishedMapId)
		c.MapSubmitted[args.FinishedMapId] = true
	}
	if args.FinishedReduceId != -1 {
		fmt.Printf("Reduce %d finished\n", args.FinishedReduceId)
		c.ReduceSubmitted[args.FinishedReduceId] = true
	}
	finishedMaps, finishedReduces := 0, 0
	reply.MapId = -1
	reply.ReduceId = -1
	for i := 0; i < c.NMap; i++ {
		if c.MapSubmitted[i] {
			finishedMaps++
		}
		if !c.MapSubmitted[i] && time.Now().Unix()-c.MapScheduleTime[i] > 10 {
			reply.Done = false
			reply.Wait = false
			reply.MapId = i
			reply.MapFile = c.MapTasks[i]
			reply.NReduce = c.NReduce
			reply.NMap = c.NMap
			c.MapScheduleTime[i] = time.Now().Unix()
			fmt.Printf("Map %d: %s\n", i, c.MapTasks[i])
			return nil
		}
	}
	if finishedMaps != c.NMap {
		reply.Wait = true
		return nil
	}
	for i := 0; i < c.NReduce; i++ {
		if c.ReduceSubmitted[i] {
			finishedReduces++
		}
		if !c.ReduceSubmitted[i] && time.Now().Unix()-c.ReduceScheduleTime[i] > 10 {
			reply.Done = false
			reply.Wait = false
			reply.ReduceId = i
			reply.NMap = c.NMap
			reply.NReduce = c.NReduce
			c.ReduceScheduleTime[i] = time.Now().Unix()
			fmt.Printf("Reduce %d\n", i)
			return nil
		}
	}
	if finishedMaps == c.NMap && finishedReduces == c.NReduce {
		reply.Done = true
	} else {
		reply.Wait = true
	}
	return nil
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
	// Your code here.
	for i := 0; i < c.NReduce; i++ {
		if !c.ReduceSubmitted[i] {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:        files,
		NMap:            len(files),
		MapScheduleTime: make([]int64, len(files)),
		MapSubmitted:    make([]bool, len(files)),

		NReduce:            nReduce,
		ReduceScheduleTime: make([]int64, nReduce),
		ReduceSubmitted:    make([]bool, nReduce),
	}

	// Your code here.

	c.server()
	return &c
}
