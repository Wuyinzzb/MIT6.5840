package raft

type CommandRecord struct {
	index uint64
	count uint64
}

// 根据index找到对应的记录的位置
func (rf *Raft) getCount(pos uint64) uint64 {
	for _, record := range rf.commandsrecord {
		if record.index == pos {
			return record.count
		}
	}
	return 0
}

func (rf *Raft) setCount(pos uint64) {
	for i := range rf.commandsrecord {
		if rf.commandsrecord[i].index == pos {
			rf.commandsrecord[i].count += 1
		}
	}
}

func (rf *Raft) deleteHead() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.commandsrecord) > 0 {
		rf.commandsrecord = rf.commandsrecord[1:]
	}
}
