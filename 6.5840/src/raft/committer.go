package raft

import (
	"fmt"
	"time"
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
type CommitArgs struct {
	Index uint64
}
type CommitReply struct {
}

func (rf *Raft) committer() {
	for !rf.killed() {
		select {
		case index := <-rf.appendflag.commitLog:
			//<-rf.appendflag.clientReply
			//fmt.Printf("message reply client by applmsg%d\n", rf.me)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log.entries[index].Data, CommandIndex: int(index)}
			fmt.Printf("****raft%d commit index %d command %v\n", rf.me, index, rf.log.entries[index].Data)
		case <-time.After(100 * time.Millisecond):
		}
	}

}

func (rf *Raft) broadcommit(record CommandRecord) {
	for i := range rf.peers {
		rf.mu.Lock()
		args := CommitArgs{
			Index: record.index,
		}
		reply := CommitReply{}
		rf.mu.Unlock()
		if record.commitRafts[i] {
			//fmt.Printf("raft %d is braod sent to commit\n", i)
			go rf.sendCommit(i, &args, &reply)
		}
	}
	// 如果一个命令先后进入broad，在删除了节点之后，第二次的访问将会访问到下一个命令导致错误
}
func (rf *Raft) CommitAsk(args *CommitArgs, reply *CommitReply) {
	rf.appendflag.commitLog <- args.Index
	rf.mu.Lock()
	rf.commitIndex++
	rf.mu.Unlock()
}
func (rf *Raft) sendCommit(server int, args *CommitArgs, reply *CommitReply) bool {
	ok := rf.peers[server].Call("Raft.CommitAsk", args, reply)
	return ok
}
