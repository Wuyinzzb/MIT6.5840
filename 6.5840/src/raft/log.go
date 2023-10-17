package raft

type Entry struct {
	Term uint64
	Data interface{}
}

type Log struct {
	entries      []Entry
	currentEntry Entry
}

func makelog() Log {
	log := Log{
		entries: []Entry{
			{Term: 0},
		},
		currentEntry: Entry{Term: 0},
	}
	return log
}

func (log *Log) lastIndex() uint64 {
	//fmt.Printf("len(log.entries)%d\n", len(log.entries))
	return uint64(len(log.entries) - 1)
}
