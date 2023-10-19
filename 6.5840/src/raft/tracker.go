package raft

type PeerTracker struct {
	nextLogIndex uint64
}

func (rf *Raft) resetTrackedIndexes() {
	for i := range rf.peerTrackers {
		rf.peerTrackers[i].nextLogIndex = rf.log.lastIndex() + 1
		// warning: cannot set the initial match index to the snapshot index since there might be new peers or way too lag-behind peers.
		//rf.peerTrackers[i].nextLogIndex = 0
	}
}
