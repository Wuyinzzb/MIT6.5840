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

	//"fmt"

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state          int
	currentTerm    uint64
	heartBeat      chan bool
	winElection    chan bool
	appendflag     Appendflag
	voteflag       VoteFlag
	log            Log
	commitIndex    uint64
	applyCh        chan ApplyMsg
	commitRaft     []bool
	peerTrackers   []PeerTracker
	commandsrecord []CommandRecord
	//time
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term uint64
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
	return int(term), isleader
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
// 如果该raft是leader节点，他会把接受到的命令在appendEntries里发送给follower

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	// if rf.state == STATE_LEADER {
	// 	fmt.Printf("command is %v\n", command)
	// }
	//不能简单的拒绝重复的命令，因为当config发送重复命令时还需要raft的返回，拒之门外就没有返回了
	//if rf.state == STATE_LEADER && command != rf.log.entries[rf.log.lastIndex()].Data
	if rf.state == STATE_LEADER {
		isLeader = true
		term = int(rf.currentTerm)
		index = int(rf.log.lastIndex()) + 1
		//如果是重复命令，直接返回
		if command == rf.log.entries[rf.log.lastIndex()].Data {
			rf.mu.Unlock()
			return index - 1, term, isLeader
		}
		currentEntry := Entry{Term: rf.currentTerm, Data: command}
		rf.log.entries = append(rf.log.entries, currentEntry)
		fmt.Printf("raft %d add a command %v in index%d\n", rf.me, command, rf.log.lastIndex())
		rf.mu.Unlock()
		//假如两条命令紧接着进入broadcast，上一条信息还没处理完就会接受到下一条
		rf.broadcastAppendEntries(true)
	} else {
		rf.mu.Unlock()
	}
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
func (rf *Raft) leadertofollow() {
	rf.mu.Lock()
	rf.state = STATE_FOLLOW
	rf.heartBeat <- true
	rf.mu.Unlock()
}
func (rf *Raft) updateState(state int) {
	rf.mu.Lock()
	rf.state = state
	rf.mu.Unlock()
}
func timeOut() time.Duration {
	ms := 500 + (rand.Int63() % 500)
	return time.Duration(ms) * time.Millisecond
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
				//fmt.Println("candidate broadcast appendentries")
				rf.broadcastAppendEntries(false)
			}
		//leader每隔一段时间就像follow发送心跳包
		case STATE_LEADER:
			//fmt.Printf("appendentries%d\n", rf.me)
			select {
			case <-time.After(100 * time.Millisecond):
				//fmt.Println("leader broadcast appendentries")
				rf.broadcastAppendEntries(false)
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

	rf.heartBeat = make(chan bool)
	rf.winElection = make(chan bool)

	rf.commitIndex = 1
	rf.log = makelog()
	rf.appendflag = makeappend()
	rf.voteflag = makevote()
	rf.applyCh = applyCh
	rf.commitRaft = make([]bool, len(rf.peers))
	rf.peerTrackers = make([]PeerTracker, len(rf.peers))
	rf.commandsrecord = []CommandRecord{}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.committer()

	return rf
}
