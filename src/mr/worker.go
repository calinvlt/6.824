package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	fileName, reduces, no := RequestFile()
	fmt.Println(reduces)
	content := ReadFileContent(fileName)

	intermediate := []KeyValue{}
	kva := mapf(fileName, string(content))

	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	reducerInputFiles := make(map[int][]KeyValue)
	for i := 0; i < reduces; i++ {
		reducerInputFiles[i] = nil
	}

	for _, v := range intermediate {
		i := ihash(v.Key) % reduces
		reducerInputFiles[i] = append(reducerInputFiles[i], v)
	}

	for i, v := range reducerInputFiles {
		fn := fmt.Sprintf("mr-%v-%v", no, i)
		ofile, _ := os.Create(fn)
		for _, kv := range v {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}

	CompleteFile(fileName)
}

func RequestFile() (string, int, int) {
	args := FileRequest{}
	resp := FileResponse{}

	ok := call("Coordinator.RequestFile", &args, &resp)
	if ok {
		//fmt.Printf("Received file %v\n", resp.FileName)
		if resp.FileName == "" {
			fmt.Printf("No received file %v\n", resp.FileName)
			return "", resp.Reduces, resp.No
		}
		return resp.FileName, resp.Reduces, resp.No
	} else {
		fmt.Printf("call failed!\n")
	}

	return resp.FileName, resp.Reduces, resp.No
}

func ReadFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	return string(content)
}

func CompleteFile(fileName string) {
	args := FileDoneRequest{}
	args.FileName = fileName
	resp := FileDoneResponse{}

	ok := call("Coordinator.CompleteFile", &args, &resp)
	if !ok {
		fmt.Printf("Complete file %v called failed\n", fileName)
	}
}

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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
