package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	executing bool
	mutex     sync.Mutex
	id        int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	currentState := worker{false, sync.Mutex{}, -1}

	for {
		req := RequestTask{}
		res := RequestTaskResponse{}

		success := call("Master.RequestForTask", &req, &res)

		if success == false {
			fmt.Printf("can't request task \n")
			os.Exit(-1)
		}

		currentState.setId(res.Id)
		if res.TaskType_ == SLEEP {
			time.Sleep(time.Millisecond * 200)
			continue
		}
		if res.TaskType_ == MAP {
			Mapping(mapf, res.fileNames, currentState.getId())
			NotifyCompletion(MAP, currentState.getId())
		}
		if res.TaskType_ == REDUCE {
			Reducing(reducef, currentState.getId())
			NotifyCompletion(REDUCE, currentState.getId())
		}

	}

}

func (state *worker) getId() int {
	index := -1
	state.mutex.Lock()
	index = state.id
	state.mutex.Unlock()
	return index
}

func (state *worker) setId(index int) {
	state.mutex.Lock()
	state.id = index
	state.mutex.Unlock()
}

func NotifyCompletion(taskType int, index int) bool {
	req := doneRequest{}
	res := doneResponse{}
	req.TaskType_ = taskType
	req.Index = index
	success := call("Master.SubmitTask", &req, &res)
	return success

}

func Mapping(mapf func(string, string) []KeyValue, fileNames []string, index int) {
	for _, file := range fileNames {
		mapFile, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", mapFile)
		}
		content, err := ioutil.ReadAll(mapFile)
		if err != nil {
			log.Fatalf("cannot read %v", mapFile)
		}
		mapFile.Close()
		kva := mapf(mapFile.Name(), string(content))
		sort.Sort(ByKey(kva))
		createIntermediate(&kva, index)

	}
}

func Reducing(reducef func(string, []string) string, index int) {
	kva := loadIntermediate(index)
	sort.Sort(ByKey(kva))
	i := 0
	ofile, err := ioutil.TempFile("./", "prefix")
	if err != nil {
		log.Fatal(err)
	}

	for i < len(kva) {
		j := i + 1
		for j < len(kva) && (kva)[j].Key == (kva)[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, (kva)[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	oldname := ofile.Name()
	ofile.Close()
	name := fmt.Sprintf("mr-out-%v", index)
	os.Rename(oldname, name)

}

func createIntermediate(kva *[]KeyValue, index int) {
	var tempFiles []string
	for i := 0; i < 10; i++ {
		oname := fmt.Sprintf("mr", index, i)
		os.Remove(oname)
		file, err := ioutil.TempFile("./", "prefix"+oname)
		if err != nil {
			log.Fatal(err)
		}
		tempFiles = append(tempFiles, file.Name())
		file.Close()
	}
	i := 0
	for i < len(*kva) {
		j := i + 1
		for j < len(*kva) && (*kva)[j].Key == (*kva)[i].Key {
			j++
		}
		reducerIndex := ihash((*kva)[i].Key) % 10
		ofile, err := os.OpenFile(tempFiles[reducerIndex], os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			fmt.Printf("error : %v", err)
		}
		enc := json.NewEncoder(ofile)
		for k := i; k < j; k++ {
			err := enc.Encode((*kva)[k])
			if err != nil {
				fmt.Printf("encode error: %v", err)
				os.Exit(-1)
			}
		}
		ofile.Close()
		i = j
	}
	for rindex, oname := range tempFiles {
		name := "./" + fmt.Sprintf("mr-%v-%v", index, rindex)
		os.Rename(oname, name)
	}
}

func loadIntermediate(index int) []KeyValue {
	//need to figure out directory
	fs, err := ioutil.ReadDir("./")
	if err != nil {
		fmt.Printf("ERROR: v% , from reading dir %v", err, "./")
	}
	var kva []KeyValue
	for _, fileInfo := range fs {
		filename := "./" + fileInfo.Name()
		reducerIndex := -1
		mapIndex := -1
		matching, err := fmt.Sscanf(fileInfo.Name(), "mr-%v-%v", &mapIndex, &reducerIndex)
		if err == nil && matching == 2 && reducerIndex == index {
			file, err := os.Open(filename)
			if err != nil {
				fmt.Printf("open %v error %v \n", filename, err)
				os.Exit(-1)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
	}
	sort.Sort(ByKey(kva))
	return kva
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
