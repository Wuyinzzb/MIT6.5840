package raft

type CommandRecord struct {
	index       uint64
	count       uint64
	commitRafts []bool
}

// 根据index找到对应的记录的位置
// func (rf *Raft) getCount(pos uint64) uint64 {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	for _, record := range rf.commandsrecord {
// 		if record.index == pos {
// 			return record.count
// 		}
// 	}
// 	return 0
// }

func (rf *Raft) setCount(server int, pos uint64) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.commandsrecord {
		if rf.commandsrecord[i].index == pos {
			rf.commandsrecord[i].count += 1
			rf.commandsrecord[i].commitRafts[server] = true
			return true
		}
	}
	return false
}

func (rf *Raft) deleteHead() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.commandsrecord) > 0 {
		rf.commandsrecord = rf.commandsrecord[1:]
	}
}
