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
	Done       bool
	Start      time.Time
	Reduces    int
	FileNumber int
	InterFiles []string
}

type Coordinator struct {
	Files       map[string]Status
	ReduceFiles map[int]Status
	Mutex       sync.Mutex
}

// request work
func (c *Coordinator) RequestFile(arg *WorkRequest, resp *WorkResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for k, v := range c.Files {
		if !v.Done {
			oldStatus := c.Files[k]
			oldStatus.Start = time.Now()
			c.Files[k] = oldStatus
			resp.WorkType = "map"
			resp.FileName = k
			resp.Reduces = oldStatus.Reduces
			resp.FileNumber = oldStatus.FileNumber
			fmt.Printf("Sending to worker file %v\n", k)
			return nil
		}
	}

	for k, v := range c.ReduceFiles {
		if !v.Done {
			oldStatus := c.ReduceFiles[k]
			oldStatus.Start = time.Now()
			c.ReduceFiles[k] = oldStatus
			resp.WorkType = "reduce"
			resp.Hash = k
			resp.InterFiles = v.InterFiles
			fmt.Printf("Sending to worker reduce hash %v\n", k)
			return nil
		}
	}

	resp.WorkType = "done"
	return nil
}

// work complete
func (c *Coordinator) CompleteFile(arg *WorkDoneRequest, resp *WorkDoneResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	switch arg.WorkType {
	case "map":
		for k := range c.Files {
			if k == arg.FileName {
				c.Files[k] = Status{Done: true}
				//fmt.Printf("Worker completed file %v\n", k)

				for hash, fileName := range arg.InterFiles {
					//fmt.Printf("Adding intermediate file %v for hash %v\n", fileName, hash)
					if entry, ok := c.ReduceFiles[hash]; ok {
						entry.InterFiles = append(entry.InterFiles, fileName)
						c.ReduceFiles[hash] = entry
					} else {
						c.ReduceFiles[hash] = Status{InterFiles: []string{fileName}}
					}
				}

				break
			}
		}
	case "reduce":
		//fmt.Printf("Reduce work done %v\n", arg.Hash)
		c.ReduceFiles[arg.Hash] = Status{Done: true}
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

// main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for k := range c.Files {
		if !c.Files[k].Done {
			return false
		}
	}

	for k := range c.ReduceFiles {
		if !c.ReduceFiles[k].Done {
			return false
		}
	}

	return true
}

// create a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// initalizae the coordinator map
	c.Files = make(map[string]Status)
	c.ReduceFiles = make(map[int]Status)

	for i, f := range files {
		c.Files[f] = Status{Reduces: nReduce, FileNumber: i}
	}

	c.server()
	return &c
}
