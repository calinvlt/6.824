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

// use ihash(key) % NReduce to choose the reduce; task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		time.Sleep(250 * time.Millisecond)

		resp := RequestWork()

		var mx sync.Mutex
		mx.Lock()
		mx.Unlock()

		switch resp.WorkType {
		case "map":
			content := ReadFileContent(resp.FileName)

			intermediate := []KeyValue{}
			kva := mapf(resp.FileName, string(content))

			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))

			reducerInputFiles := make(map[int][]KeyValue)
			for i := 0; i < resp.Reduces; i++ {
				reducerInputFiles[i] = nil
			}

			for _, v := range intermediate {
				i := ihash(v.Key) % resp.Reduces
				reducerInputFiles[i] = append(reducerInputFiles[i], v)
			}

			interFiles := make(map[int]string)
			for i, v := range reducerInputFiles {
				fn := fmt.Sprintf("mr-%v-%v", resp.FileNumber, i)
				interFiles[i] = fn
				ofile, _ := os.Create(fn)
				for _, kv := range v {
					enc := json.NewEncoder(ofile)
					enc.Encode(&kv)
				}
				ofile.Close()
			}

			fmt.Printf("Done map work %v \n", resp.FileName)
			args := WorkDoneRequest{FileName: resp.FileName, InterFiles: interFiles, WorkType: "map"}
			CompleteFile(args)

		case "reduce":
			kva := []KeyValue{}
			//fmt.Printf("Receive reduce work %v count %v\n", resp.Hash, len(resp.InterFiles))
			// read the files
			for _, fileName := range resp.InterFiles {
				//fmt.Printf("Loading file %v\n", fileName)
				file, _ := os.Open(fileName)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))

			//fmt.Printf("Processing hash %v\n", resp.Hash)
			// run reduce and save the final file
			oname := fmt.Sprintf("mr-out-%v\n", resp.Hash)
			ofile, _ := ioutil.TempFile(".", "*") //os.Create(oname)

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
			os.Rename(ofile.Name(), oname)

			fmt.Printf("Done hash work %v \n", resp.Hash)
			args := WorkDoneRequest{WorkType: "reduce", Hash: resp.Hash}
			CompleteFile(args)

		case "done":
			break
		default:
			continue
		}
	}
}

func RequestWork() WorkResponse {
	args := WorkRequest{}
	resp := WorkResponse{}

	ok := call("Coordinator.RequestWork", &args, &resp)
	if !ok {
		fmt.Printf("call failed!\n")
	}

	return resp
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

func CompleteFile(args WorkDoneRequest) {
	resp := WorkDoneResponse{}
	ok := call("Coordinator.CompleteWork", &args, &resp)
	if !ok {
		fmt.Printf("Complete file %v called failed\n", args.FileName)
	}
}

// send an RPC request to the coordinator, wait for the response usually returns true; returns false if something goes wrong.
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
