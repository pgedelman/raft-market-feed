package raft

import "sync"

type AppendEntriesArgs struct {
	LeaderID     NodeID
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	CandidateID NodeID
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type RPCEnvelope struct {
	Term      int
	Payload   any
	ReplyChan chan any
}

type NetworkRegistry struct {
	mu    sync.RWMutex
	nodes map[NodeID]*Node
}

func (registry *NetworkRegistry) GetPeers(nid NodeID) []*Node {
	registry.mu.RLock()
	peers := make([]*Node, 0, len(registry.nodes))
	for id, peer := range registry.nodes {
		if nid != id {
			peers = append(peers, peer)
		}
	}
	registry.mu.RUnlock()
	return peers
}
