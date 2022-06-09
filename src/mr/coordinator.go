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

type Status struct {
	Done  bool
	Start time.Time
	Reduces int
	No int
}

type Coordinator struct {
	Files map[string]Status
	Mutex sync.Mutex
}

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Printf("Worker called %v\n", args.X)
	return nil
}

// request work
func (c *Coordinator) RequestFile(arg *FileRequest, resp *FileResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for k, v := range c.Files {
		if !v.Done {
			oldStatus := c.Files[k]
			oldStatus.Start = time.Now()
			c.Files[k] = oldStatus
			resp.FileName = k
			resp.Reduces = oldStatus.Reduces
			resp.No = oldStatus.No
			fmt.Printf("Sending to worker file %v\n", k)
			break
		}
	}

	return nil
}

// done
func (c *Coordinator) CompleteFile(arg *FileDoneRequest, resp *FileDoneResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for k := range c.Files {
		if k == arg.FileName {
			c.Files[k] = Status{Done: true}
			fmt.Printf("Worker completed file %v\n", k)
			break
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// initalizae the coordinator map
	c.Files = make(map[string]Status)
	for i, f := range files {
		c.Files[f] = Status{Reduces: nReduce, No: i}
	}

	c.server()
	return &c
}
