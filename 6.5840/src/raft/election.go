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
		//fmt.Printf("server %d turn to follower in term%d state %d\n", rf.me, rf.currentTerm, rf.state)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		//fmt.Printf("term %d :server %d vote for %d\n", args.Term, rf.me, args.CandidateId)
	}
	rf.mu.Unlock()

}
func (rf *Raft) setWinElection() {
	rf.votedFor = -1
	rf.cntVote = 0
	//fmt.Printf("%d *********win election \n", rf.me)
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
func (rf *Raft) startElection() {
	rf.updateState(STATE_CANDIDATE)
	rf.mu.Lock()
	rf.currentTerm += 1
	//fmt.Printf("server %d start election in term %d\n", rf.me, rf.currentTerm)
	if rf.votedFor == -1 {
		rf.cntVote += 1
		rf.votedFor = rf.me
		rf.voteAsk += 1
		//fmt.Printf("term %d :server %d vote for %d\n", rf.currentTerm, rf.me, rf.votedFor)
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
