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
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"MIT6.5840/labgob"
	"MIT6.5840/labrpc"
)

const (
	STATE_FOLLOW = iota
	STATE_CANDIDATE
	STATE_LEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

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

	state       int
	currentTerm int
	votedFor    int
	cntVote     int
	heartBeat   chan bool
	winElection chan bool
	voteAsk     int
	//time
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == STATE_LEADER {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestAppendArgs struct {
	Term     int
	LeaderId int
}
type RequestAppendReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//被选举人调用选举人的RequestVote，rf是指的选举人？
	rf.mu.Lock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOW
		rf.votedFor = -1
		//rf.heartBeat <- true
		fmt.Printf("server %d turn to follower in term%d state %d\n", rf.me, rf.currentTerm, rf.state)
	}
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		fmt.Printf("term %d :server %d vote for %d\n", args.Term, rf.me, args.CandidateId)
	}
	rf.mu.Unlock()

}
func (rf *Raft) AppendEntries(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		//fmt.Printf("leader%d term%d less than my%d term%d\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Success = true
	//fmt.Printf("term %d :server %d appendentries for %d\n", args.Term, args.LeaderId, rf.me)
	rf.mu.Unlock()
	rf.setHeartBeat()

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
func (rf *Raft) setHeartBeat() {
	//fmt.Printf("%d sethearbeat\n", rf.me)
	rf.mu.Lock()
	rf.votedFor = -1
	rf.cntVote = 0
	rf.heartBeat <- true
	rf.mu.Unlock()
}

func (rf *Raft) setWinElection() {
	rf.votedFor = -1
	rf.cntVote = 0
	fmt.Printf("%d *********win election \n", rf.me)
	rf.winElection <- true
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VoteGranted {
			rf.mu.Lock()
			rf.cntVote += 1
			if rf.cntVote > (len(rf.peers) / 2) {
				rf.setWinElection()
			}
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		rf.voteAsk++
		if rf.voteAsk == len(rf.peers) {
			rf.cntVote = 0
			rf.votedFor = -1
			rf.voteAsk = 0
		}
		rf.mu.Unlock()
	}
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if !reply.Success {
			rf.updateState(STATE_FOLLOW)
		}
	}
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

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) updateState(state int) {
	rf.mu.Lock()
	rf.state = state
	rf.mu.Unlock()

}
func timeOut() time.Duration {
	ms := 300 + (rand.Int63() % 500)
	return time.Duration(ms) * time.Millisecond
}
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	args := RequestAppendArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		reply := RequestAppendReply{}
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}
func (rf *Raft) startElection() {
	rf.updateState(STATE_CANDIDATE)
	rf.mu.Lock()
	rf.currentTerm += 1
	fmt.Printf("server %d start election in term %d\n", rf.me, rf.currentTerm)
	if rf.votedFor == -1 {
		rf.cntVote += 1
		rf.votedFor = rf.me
		rf.voteAsk += 1
		fmt.Printf("term %d :server %d vote for %d\n", rf.currentTerm, rf.me, rf.votedFor)
	}

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		reply := RequestVoteReply{}
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, &reply)
	}

}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		// Your code here (2A)
		// Check if a leader election should be started.
		switch state {
		//follow状态的行为：超时进入candidate
		case STATE_FOLLOW:
			select {
			case <-rf.heartBeat:
			case <-time.After(timeOut()):
				rf.startElection()
			}
		case STATE_CANDIDATE:
			select {
			case <-rf.heartBeat:
				rf.updateState(STATE_FOLLOW)
			case <-time.After(timeOut()):
				rf.startElection()
			case <-rf.winElection:
				rf.updateState(STATE_LEADER)
				rf.broadcastAppendEntries()
			}
		//leader每隔一段时间就像follow发送心跳包
		case STATE_LEADER:
			//fmt.Printf("appendentries%d\n", rf.me)
			select {
			case <-time.After(100 * time.Millisecond):
				rf.broadcastAppendEntries()
			case <-rf.heartBeat:
				rf.updateState(STATE_FOLLOW)
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
	}
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
	rf.state = STATE_FOLLOW
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.cntVote = 0
	rf.heartBeat = make(chan bool)
	rf.winElection = make(chan bool)
	rf.voteAsk = 0
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
