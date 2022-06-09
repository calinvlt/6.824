package mr

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type WorkRequest struct{}
type WorkResponse struct {
	WorkType   string
	FileName   string
	Reduces    int
	FileNumber int
	Hash       int
	InterFiles []string
}

type WorkDoneRequest struct {
	WorkType   string
	FileName   string
	InterFiles map[int]string
	Hash       int
}
type WorkDoneResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
