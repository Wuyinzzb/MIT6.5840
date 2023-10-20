package raft

import "fmt"

type RequestAppendArgs struct {
	Term         uint64
	LeaderId     int
	PreLogIndex  uint64
	PreLogTerm   uint64
	LeaderCommit uint64
	Entry        Entry
}
type Appendflag struct {
	clientReply chan bool
	commitLog   chan bool
	flag        bool
	isclinet    bool
}
type RequestAppendReply struct {
	Term         uint64
	Success      bool
	Appendentry  bool
	FollowCommit uint64
}

func makeappend() Appendflag {
	flag := Appendflag{
		clientReply: make(chan bool),
		commitLog:   make(chan bool),
		flag:        false,
		isclinet:    false,
	}
	return flag
}
func (rf *Raft) AppendEntries(args *RequestAppendArgs, reply *RequestAppendReply) {
	rf.mu.Lock()
	//fmt.Printf("raft %d accept a append\n", rf.me)
	//1，如果leader任期该节点，拒绝，return false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//fmt.Printf("leader%d term%d less than my%d term%d\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	//首先rf.log.entries得在对应的位置上有数据
	if rf.log.lastIndex() < args.PreLogIndex {
		reply.Success = false
		rf.mu.Unlock()
		rf.setHeartBeat()
		return
	}
	//2.如果index位置的term不一致，return false
	if args.PreLogTerm != rf.log.entries[args.PreLogIndex].Term {
		reply.Success = false
		rf.mu.Unlock()
		rf.setHeartBeat()
		return
	}
	//fmt.Printf("raft %d add a command %v in index%d\n", rf.me, args.Entry.Data, args.PreLogIndex)
	//3.如果一个已有的词条和新词条冲突，删除已有词条和其后的所有词条
	//如果nextindex的位置上有数据，直接删除该位置之后的所有词条
	if rf.log.lastIndex() > args.PreLogIndex {
		rf.log.entries = rf.log.entries[:args.PreLogIndex+1]
	}
	// if rf.log.entries[args.PreLogIndex+1].Term != 0 && rf.log.entries[args.PreLogIndex+1] != args.Entry{
	//

	// }

	//4.如果日志相同return true
	//如果相等就是普通的心跳，双方的最后一个日志相同，此时只需要更新currenTerm
	//如果不相等那到了这一步应该是要追加客户端日志
	//args.prelogindex要求是log的最后一个数据，这样才能把args.entry加到它后面
	if rf.log.entries[args.PreLogIndex] != args.Entry && args.PreLogIndex == rf.log.lastIndex() {
		rf.currentTerm = args.Term
		rf.log.entries = append(rf.log.entries, args.Entry)
		reply.Appendentry = true
		fmt.Printf("raft %d add a command %v in index%d\n", rf.me, args.Entry.Data, rf.log.lastIndex())
	} else {
		//在空位置追加新元素
		rf.currentTerm = args.Term
	}
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.log.lastIndex() {
			rf.commitIndex = rf.log.lastIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

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
	rf.voteflag.votedFor = -1
	rf.voteflag.cntVote = 0
	rf.heartBeat <- true
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendArgs, reply *RequestAppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		alreadycommit := false
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				go rf.leadertofollow()
				//fmt.Println("*****************")
			} else {
				rf.mu.Lock()
				rf.peerTrackers[server].nextLogIndex -= 1
				rf.mu.Unlock()
				//fmt.Println("position index term is not equal!!")
			}
		}
		//如果是客户端连接请求
		rf.mu.Lock()
		if rf.appendflag.isclinet {
			if reply.Success {
				//rf.appendflag.cntLog += 1
				//命令记录+1，表示有一个新的节点复制了对应的日志
				rf.setCount(rf.peerTrackers[server].nextLogIndex)
				fmt.Printf("index %d count %d\n", rf.peerTrackers[server].nextLogIndex, rf.getCount(rf.peerTrackers[server].nextLogIndex))
				rf.commitRaft[server] = true
				//fmt.Printf("raft %d is true\n", server)
				if rf.appendflag.flag {
					args := CommitArgs{}
					reply := CommitReply{}
					rf.mu.Unlock()
					go rf.sendCommit(server, &args, &reply)
					alreadycommit = true
					rf.mu.Lock()
					//fmt.Printf("raft%d is alone to send commit\n", server)
				}
			}
			//rf.appendflag.logAsk += 1
			//如果半数服务器同意了请求
			if rf.getCount(rf.peerTrackers[server].nextLogIndex) > uint64(len(rf.peers)/2) && !rf.appendflag.flag {
				//fmt.Println("success reply client")
				//fmt.Printf("len(rf.peers) is %d\n", len(rf.peers))
				rf.appendflag.flag = true //每一次最多只进来一次if
				rf.mu.Unlock()
				go rf.broadcommit()
				rf.mu.Lock()
			}

		}
		rf.mu.Unlock()
		if reply.Appendentry {
			rf.mu.Lock()
			if rf.commitIndex > rf.peerTrackers[server].nextLogIndex && !alreadycommit {
				args := CommitArgs{}
				reply := CommitReply{}
				rf.mu.Unlock()
				go rf.sendCommit(server, &args, &reply)
				rf.mu.Lock()
				fmt.Printf("commit is %d\n", rf.commitIndex)
			}
			rf.peerTrackers[server].nextLogIndex += 1
			//fmt.Printf("raft %d nextlogindex %d\n", server, rf.peerTrackers[server].nextLogIndex)
			rf.mu.Unlock()
		}
	}

	return ok
}

// 如果是客户端有信息进来，此时leader
func (rf *Raft) broadcastAppendEntries(flag bool) {
	rf.mu.Lock()
	//fmt.Printf("raft%d is leader, \n", rf.me)
	rf.appendflag.isclinet = flag
	if flag {
		rf.appendflag.flag = false
		//fmt.Printf("clear***************\n")
		//args.Entry = rf.log.currentEntry
		//rf.log.entries = append(rf.log.entries, rf.log.currentEntry)
		//为每一个命令创建一个记录commit数值的记录，当这个记录能提交时再删除
		//不存在前一个命令不能提交下一个命令能提交的状况
		record := CommandRecord{index: rf.log.lastIndex(), count: 1}
		fmt.Printf("command is %v, index is %d\n", rf.log.entries[rf.log.lastIndex()].Data, rf.log.lastIndex())
		rf.commandsrecord = append(rf.commandsrecord, record)
		rf.commitRaft[rf.me] = true
		//rf.appendflag.clientReply <- true
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		reply := RequestAppendReply{}
		if i == rf.me {
			continue
		}
		//start的broadcast和leader的broadcast可能同时进行，会导致连续两次发送一样的内容
		//从而导致日志重复
		args := rf.makeAppendEntriesArgs(i)
		//起始每次发送之后就默认成功了nextindex++，就算失败了后续心跳也会慢慢更正(X)
		go rf.sendAppendEntries(i, &args, &reply)
		// rf.mu.Lock()
		// if rf.log.lastIndex() <
		// rf.mu.Unlock()
	}
}

// func (rf *Raft) clearappend() {
// 	rf.appendflag.cntLog = 0
// 	rf.appendflag.logAsk = 0
// 	// if !rf.appendflag.flag { //都结束了还没进去半数请求
// 	// 	fmt.Println("the worst things happen!!!!!!!!!!!!!!!!")
// 	// }
// 	rf.appendflag.flag = false
// 	//fmt.Println("init appendflag")
// }

// 对于每一个raft的args，pre和entry是不一样的
func (rf *Raft) makeAppendEntriesArgs(to int) RequestAppendArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	preindex := rf.peerTrackers[to].nextLogIndex - 1
	preterm := rf.log.entries[preindex].Term
	entry := rf.log.entries[preindex]
	//如果next所在的位置有数据
	if rf.peerTrackers[to].nextLogIndex <= rf.log.lastIndex() {
		entry = rf.log.entries[rf.peerTrackers[to].nextLogIndex]
		//fmt.Printf("raft %d update %v\n", to, entry.Data)
	}
	// if rf.appendflag.isclinet {
	// 	entry = rf.log.currentEntry
	// }
	args := RequestAppendArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  preindex,
		PreLogTerm:   preterm,
		Entry:        entry,
		LeaderCommit: rf.commitIndex,
	}

	return args
}
