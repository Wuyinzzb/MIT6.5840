package raft

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
type CommitArgs struct {
}
type CommitReply struct {
}

func (rf *Raft) committer() {
	for !rf.killed() {
		<-rf.appendflag.commitLog
		//<-rf.appendflag.clientReply
		//fmt.Printf("message reply client by applmsg%d\n", rf.me)
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log.entries[rf.log.lastIndex()].Data, CommandIndex: int(rf.log.lastIndex())}
	}
}
func (rf *Raft) broadcommit() {
	for i := range rf.peers {
		args := CommitArgs{}
		reply := CommitReply{}
		if rf.commitRaft[i] {
			//fmt.Printf("raft %d is braod sent to commit\n", i)
			go rf.sendCommit(i, &args, &reply)
		}
		rf.mu.Lock()
		rf.commitRaft[i] = false
		rf.mu.Unlock()
	}
}
func (rf *Raft) CommitAsk(args *CommitArgs, reply *CommitReply) {
	rf.appendflag.commitLog <- true
	//fmt.Printf("****raft%d commit\n", rf.me)
}
func (rf *Raft) sendCommit(server int, args *CommitArgs, reply *CommitReply) bool {
	ok := rf.peers[server].Call("Raft.CommitAsk", args, reply)
	return ok
}
