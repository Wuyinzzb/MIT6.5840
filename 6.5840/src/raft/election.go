package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         uint64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint64
	VoteGranted bool
}

type VoteFlag struct {
	votedFor int
	cntVote  int
	voteAsk  int
	maxterm  uint64
}

func makevote() VoteFlag {
	flag := VoteFlag{
		votedFor: -1,
		cntVote:  0,
		voteAsk:  0,
		maxterm:  1,
	}
	return flag
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//被选举人调用选举人的RequestVote，rf是指的选举人？
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	term := int(rf.log.entries[rf.log.lastIndex()].Term)
	index := int(rf.log.lastIndex())
	if args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= index) {
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.state = STATE_FOLLOW
			rf.voteflag.votedFor = -1
			//rf.heartBeat <- true
			//fmt.Printf("server %d turn to follower in term%d state %d\n", rf.me, rf.currentTerm, rf.state)
		}
		if rf.voteflag.votedFor == -1 || rf.voteflag.votedFor == args.CandidateId {
			rf.voteflag.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			//fmt.Printf("term %d :server %d vote for %d\n", args.Term, rf.me, args.CandidateId)
		}
	}

}
func (rf *Raft) setWinElection() {
	rf.voteflag.votedFor = -1
	rf.voteflag.cntVote = 0
	//fmt.Printf("%d *********win election \n", rf.me)
	rf.winElection <- true
	rf.resetTrackedIndexes()
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.VoteGranted {
			rf.mu.Lock()
			rf.voteflag.cntVote += 1
			rf.mu.Unlock()
		} else if reply.Term > rf.currentTerm { //如果投票失败应该考虑到我的任期太小了，不应该等待下一次任期再加，直接让我的任期等于回复的任期
			if rf.voteflag.maxterm < reply.Term {
				rf.voteflag.maxterm = reply.Term
			}
		}
		rf.mu.Lock()
		rf.voteflag.voteAsk++
		//所有人都投票了，统计有没有过半
		if rf.voteflag.voteAsk == len(rf.peers) {
			if rf.voteflag.cntVote > (len(rf.peers) / 2) {
				if rf.currentTerm < rf.voteflag.maxterm { //就算过半的人投票，如果任期小于任何一个节点，也不应该当选
					rf.currentTerm = rf.voteflag.maxterm
				} else {
					rf.setWinElection()
				}
			}
			rf.voteflag.cntVote = 0
			rf.voteflag.votedFor = -1
			rf.voteflag.voteAsk = 0
		}
		rf.mu.Unlock()
	}
	return ok
}
func (rf *Raft) startElection() {
	rf.updateState(STATE_CANDIDATE)
	rf.mu.Lock()
	rf.currentTerm += 1
	//fmt.Printf("server %d start election in term %d\n", rf.me, rf.currentTerm)
	if rf.voteflag.votedFor == -1 {
		rf.voteflag.cntVote += 1
		rf.voteflag.votedFor = rf.me
		rf.voteflag.voteAsk += 1
		//fmt.Printf("term %d :server %d vote for %d\n", rf.currentTerm, rf.me, rf.votedFor)
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: int(rf.log.lastIndex()),
		LastLogTerm:  int(rf.log.entries[rf.log.lastIndex()].Term),
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
