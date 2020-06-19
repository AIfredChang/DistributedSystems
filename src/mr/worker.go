package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := ExampleArgs{}

	// declare a reply structure.
	reply := ExampleReply{}

	call("Master.Example", &args, &reply)

	//used mrseq code
	file, err := os.Open(reply.fileName)

	if err != nil {
		log.Fatalf("cannot open %v", reply.fileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.fileName)
	}
	file.Close()

	//partitioning the data into intermediate files
	kva := mapf(reply.fileName, string(content))
	for i := 0; i < nReduce; r++ {
		intFileName := "mr" + "-" + strconv.Itoa(reply.index) + "-" + strconv.Itoa(i)

		intFile, err := os.Create(outputfilename)
		if err != nil {
			log.Fatalf("cannot create %v", intFileName)
		}
		enc := json.NewEncoder(intFile)
		for _, kv := range kva {
			hashValue := ihash(kv.Key) % uint32(nReduce)
			if hashValue == uint32(i) {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode")
				}
			}

		}

		intFile.Close()

	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
