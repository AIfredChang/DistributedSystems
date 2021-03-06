package raft

//
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
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm           int
	votedFor              int
	commitIndex           int
	lastApplied           int
	nextIndex             []int
	matchIndex            []int
	leaderId              int
	role                  int
	lastHeartBeatReceived time.Time
	applyCh               chan ApplyMsg
	Logs                  []LogEntry
	Applycond             *sync.Cond
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
	Granted int
}

const (
	LEADER int = iota
	CANDIDATE
	FOLLOWER
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	{
		rf.mu.Lock()
		term = int(rf.currentTerm)

		if rf.leaderId == LEADER {
			isleader = true
		} else {
			isleader = false
		}
		rf.mu.Unlock()
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Src  int
	Term int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
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

func (rf *Raft) becomeFollowerWithLock() {
	if rf.role == LEADER {
		rf.role = FOLLOWER
		go rf.electionTimer()
	} else if rf.role == CANDIDATE {
		rf.role = FOLLOWER
	}
}

func (rf *Raft) updateTerm(term int) {
	rf.mu.Lock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.leaderId = -1
		rf.becomeFollowerWithLock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) VotedFor() int {
	voted := -1
	rf.mu.Lock()
	voted = rf.votedFor
	rf.mu.Unlock()
	return voted
}

func (rf *Raft) setVotedFor(index int) {
	rf.mu.Lock()
	rf.votedFor = index
	rf.mu.Unlock()
}

func (rf *Raft) GetLastLogEntryWithLock() LogEntry {
	entry := LogEntry{}
	if len(rf.Logs) == 0 {
		entry.Term = rf.currentTerm
		entry.Index = 0
	} else {
		entry = rf.Logs[len(rf.Logs)-1]

	}
	return entry
}

func (rf *Raft) GetLastLogEntry() LogEntry {
	entry := LogEntry{}
	rf.mu.Lock()
	entry = rf.GetLastLogEntryWithLock()
	rf.mu.Unlock()
	return entry
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.updateTerm(args.Term)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartBeatRecieved = time.Now()
	}
	rf.mu.Unlock()

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) handleRequestVoteResponse(request RequestVoteArgs, reply RequestVoteReply) bool {
	rf.updateTerm(reply.Term)
	rf.mu.Lock()
	if rf.currentTerm != request.Term {
		rf.mu.Unlock()
		return false
	}
	granted := reply.VoteGranted
	rf.mu.Unlock()
	return granted

}

func (rf *Raft) setLastHeartBeatRecieved(recievedTime time.Time) {
	rf.mu.Lock()
	rf.lastHeartBeatRecieved = recievedTime
	rf.mu.Unlock()
}

func (rf *raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	term := args.Term
	rf.updateTerm(term)
	rf.mu.Lock()
	if term == rf.currentTerm {
		if rf.role == CANDIDATE {
			rf.becomeFollowerWithLock()
		}
		rf.leaderId = args.Src
		rf.lastHeartBeatRecieved = time.Now()
		rf.currentTerm = term
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntriesResponse(request AppendEntriesRequest, reply AppendEntriesResponse) {
	rf.updateTerm(reply.Term)
	rf.mu.Lock()
	if request.term != rf.currentTerm {
		rf.mu.Unlock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) heartBeatTimer() {
	for rf.Role() == LEADER {
		for i := 0; i < len(rf.peers); i++ {
			wg.Add(1)

			go func(index int, request AppendEntriesRequest, reply AppendEntriesResponse) {
				ok := rf.sendAppendEntries(index, &request, &reply)
				if ok {
					rf.handleAppendEntriesResponse(request, reply)
				}
				wg.Done()
			}(i, request, reply)
		}
		time.Sleep(time.Millisecond * 150)

	}
}

func (rf *Raft) becomeLeader() {
	lastLogIndex := 0
	rf.mu.Lock()
	rf.role = LEADER
	lastLogIndex = rf.GetLastLogEntryWithLock().Index
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	for i := rf.commitIndex + 1; i <= rf.GetLastLogEntryWithLock().Index; i++ {
		rf.Logs[i-1].Granted = 1
	}
	rf.mu.Unlock()
	go rf.heartBeatTimer()
	DPrintf("%v become leader \n", rf.me)
}

func (rf *Raft) handleElectionTimeout() {
	finished := 0
	granted := 0
	reply_ := RequestVoteArgs{}
	request_ := RequestVoteArgs{}
	cond := sync.NewCond(&rf.mu)
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = CANDIDATE
	rf.lastHeartBeatReceived = time.Now()
	granted++
	finished++
	request_.CandidateId = rf.me
	request_.Term = rf.currentTerm
	entry := rf.GetLastLogEntryWithLock()
	request_.LastLogIndex = entry.Index
	request_.LastLogTerm = entry.Term
	rf.mu.Unlock()
	go rf.electionTimer()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int, request RequestVoteArgs, reply RequestVoteReply) {
			ok := rf.sendRequestVote(index, &request, &reply)

			if ok {
				ok = rf.handleRequestVoteResponse(request, reply)

			}
			rf.mu.Lock()
			finished++
			if ok {
				granted++
			}
			cond.Signal()
			rf.mu.Unlock()

		}(i, request_, reply_)

	}

	rf.mu.Lock()
	for finished != len(rf.peers) && granted < len(r.peers)/2+1 {
		cond.Wait()
	}
	rf.mu.Unlock()
	term, _ := rf.GetState()
	if request_.Term == term && granted >= len(rf.peers)/2+1 {
		rf.becomeLeader()
	} else {
		DPrintf("%v fail in request vote in term %v \n,rf.me,request_.Term")
	}

}

func (rf *Raft) LastHeartBeatRecieved() time.Time {
	var time time.Time
	rf.mu.Lock()
	time = rf.lastHeartBeatRecieved
	rf.mu.Unlock()
	return time
}

func randTimeout(lower int, upper int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := upper - lower
	return lower + r.Intn(diff)

}

func (rf *raft) electionTimer() {
	for rf.Role() != LEADER {
		interval := randTimeout(300, 600)
		time.Sleep(time.Millisecond * time.Duration(interval))
		role := rf.Role()
		if role == FOLLOWER {
			diff := time.Since(rf.LastHeartBeatRecieved)
			if diff < time.Duration(interval)*time.Millisecond {
				continue
			} else {
				rf.handleElectionTimeout()
				return
			}
		} else if role == CANDIDATE {
			return
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.leaderId = -1

	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, -1)
		rf.matchIndex = append(rf.matchIndex, -1)
	}

	rf.role = FOLLOWER
	go rf.electionTimer()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
