package raft

import "sync"

type AppendEntriesArgs struct {
	Term int
	LeaderID int
	PrevLogTerm int
	PrevLogIndex int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

type AppendEntriesEnvelope struct {
	Args AppendEntriesArgs
	ReplyChan chan AppendEntriesReply
}

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
