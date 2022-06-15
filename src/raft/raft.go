package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// PERSISTENT STATE
	currentTerm int
	votedFor    int
	log         map[int]string

	// VOLATILE STATE
	commitIndex int
	lastApplied int

	// VOLATILE STATE ON LEADERS
	nextIndex  []int
	matchIndex []int

	// NEW STATE
	state           string // leader, follower, candidate
	lastHeartbeat   time.Time
	electionTimeout int64
}

func (rf *Raft) GetState() (int, bool) {
	//rf.printState("GetState")	
	return rf.currentTerm, rf.state == "leader"
}
func (rf *Raft) SetHeartBeat()  {
	rf.mu.Lock()
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()
}

func (rf *Raft) GetHeartBeat() (time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeartbeat
}
func (rf *Raft) SetServerState(state string)  {
	rf.mu.Lock()
	rf.state = state
	rf.mu.Unlock()
}
func (rf *Raft) GetServerState() (string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}


// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	return
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// example RequestVote RPC arguments structure field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = args.Term
		//rf.printState(fmt.Sprintf("VOTED FOR %v", args.CandidateId))
		return
	}

	if rf.votedFor == -1 {
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term		
		rf.mu.Unlock()
		rf.SetHeartBeat()

		reply.VoteGranted = true
		//rf.printState(fmt.Sprintf("VOTED FOR %v", args.CandidateId))
		return
	}

	//rf.printState(fmt.Sprintf("NOT VOTED FOR %v because votedfor %v", args.CandidateId, rf.votedFor))
}

// election ticker
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)

		if rf.state == "leader" {
			continue
		}
		
		
		if time.Since(rf.GetHeartBeat()).Milliseconds() > rf.electionTimeout {
			rf.printState(fmt.Sprintf("%v %v %v", time.Since(rf.lastHeartbeat).Milliseconds(), rf.electionTimeout, time.Since(rf.lastHeartbeat).Milliseconds()-rf.electionTimeout))
			rf.printState("trigger election")

			rf.SetServerState("candidate")
			rf.SetHeartBeat()
			rf.mu.Lock()			
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.mu.Unlock()			

			var wg sync.WaitGroup
			wg.Add(len(rf.peers) - 1)

			numberOfVotes := 1
			for i := range rf.peers {
				if i != rf.me {
					args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
					x := i
					go func(peerId int) {
						reply := RequestVoteReply{}
						ok := rf.peers[peerId].Call("Raft.RequestVote", &args, &reply)
						if ok && reply.VoteGranted {
							rf.printState(fmt.Sprintf("VOTED BY: %v", peerId))
							numberOfVotes++
						}
						wg.Done()
					}(x)
				}
			}
			wg.Wait()
			if numberOfVotes >= (len(rf.peers)/2 + 1) {
				rf.SetServerState("leader")
				rf.printState(fmt.Sprintf("LEADER elected votes:%v peers:%v", numberOfVotes, len(rf.peers)))

				for j := range rf.peers {
					if j != rf.me {
						d := j
						args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
						go func(peerId int) {
							reply := AppendEntriesReply{}
							rf.peers[peerId].Call("Raft.AppendEntries", &args, &reply)
							if reply.Term > rf.currentTerm {
								rf.becomeFollower(reply.Term)
							}
						}(d)
					}
				}
			}
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AppendEntries
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []string
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		return
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.GetServerState() != "follower" {
			rf.becomeFollower(args.Term)
		}
		rf.mu.Lock()		
		rf.votedFor = -1
		rf.mu.Unlock()
		rf.SetHeartBeat()
		reply.Success = true
	}
	reply.Term = rf.currentTerm
}

// leader ticker
func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond)

		if rf.GetServerState() == "leader" {
			for i := range rf.peers {
				if i != rf.me {
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
					d := i
					go func(peerId int) {
						reply := AppendEntriesReply{}
						//rf.printState(fmt.Sprintf("Send heartbeat to: %v", peerId))
						rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
						if reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)
						}
					}(d)
				}
			}
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func (rf *Raft) printState(msg string) {
	fmt.Printf("%v me:%v t:%v vf:%v state:%v msg:%v\n", time.Now().UnixMilli(), rf.me, rf.currentTerm, rf.votedFor, rf.state, msg)
}

func (rf *Raft) becomeFollower(term int) {
	rf.mu.Lock()	
	rf.currentTerm = term	
	rf.votedFor = -1
	rf.mu.Unlock()
	rf.SetServerState("follower")
	rf.SetHeartBeat()
	rf.printState("becoming follower")
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.becomeFollower(1)

	// election timeout
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = int64(300 + rand.Intn(300))
	rf.printState(fmt.Sprintf("timeout:%v", rf.electionTimeout))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start ticker goroutine to start elections
	go rf.leaderTicker()

	return rf
}
