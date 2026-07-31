package raft

import "sync"

type RequestVoteArgs struct {
	Term        int
	CandidateID NodeID
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type RequestVoteEnvelope struct{
	Args RequestVoteArgs
	ReplyChan chan RequestVoteReply
}

type Transport struct {
	mu    sync.RWMutex
	nodes map[NodeID]Node
}
