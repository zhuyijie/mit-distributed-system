package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := MRArgs{
		FinishedMapId:    -1,
		FinishedReduceId: -1,
	}
	os.Mkdir("mrtmp", 0755)
	for true {
		reply := MRReply{}
		call("Coordinator.Handle", &args, &reply)
		if reply.Done {
			fmt.Printf("done\n")
			break
		}
		if reply.MapId != -1 {
			fmt.Printf("Processing map %v %s\n", reply.MapId, reply.MapFile)
			file, err := os.Open(reply.MapFile)
			if err != nil {
				log.Fatalf("cannot open %v", reply.MapFile)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.MapFile)
			}
			file.Close()
			kva := mapf(reply.MapFile, string(content))
			tmpFiles := make([]*os.File, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				tmpFiles[i], err = os.CreateTemp("mrtmp", "mr-map-tmp-*")
				if err != nil {
					log.Fatalf("cannot create tmp file")
				}
			}
			for _, kv := range kva {
				reduceId := ihash(kv.Key) % reply.NReduce
				fmt.Fprintf(tmpFiles[reduceId], "%v %v\n", kv.Key, kv.Value)
			}
			for i := 0; i < reply.NReduce; i++ {
				tmpFiles[i].Close()
				os.Rename(tmpFiles[i].Name(), fmt.Sprintf("mrtmp/mr-%v-%v", reply.MapId, i))
			}
			args.FinishedReduceId = -1
			args.FinishedMapId = reply.MapId
			continue
		}
		if reply.ReduceId != -1 {
			fmt.Printf("Processing reduce %v\n", reply.ReduceId)
			tmpFiles := make([]*os.File, reply.NMap)
			for i := 0; i < reply.NMap; i++ {
				tmpFiles[i], _ = os.Open(fmt.Sprintf("mrtmp/mr-%v-%v", i, reply.ReduceId))
			}
			kva := []KeyValue{}
			for _, file := range tmpFiles {
				for {
					var kv KeyValue
					_, err := fmt.Fscanf(file, "%v %v\n", &kv.Key, &kv.Value)
					if err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Slice(kva, func(i, j int) bool {
				return kva[i].Key < kva[j].Key
			})
			ofile, _ := os.CreateTemp("mrtmp", "mr-reduce-tmp-*")
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()
			os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%v", reply.ReduceId))
			args.FinishedMapId = -1
			args.FinishedReduceId = reply.ReduceId
			continue
		}
		fmt.Printf("wait 2s\n")
		time.Sleep(2 * time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
