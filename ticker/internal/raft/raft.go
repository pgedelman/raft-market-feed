package raft

import (
	"math/rand/v2"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	Dead
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

type StateChange struct {
	NodeID PeerID
	Role   Role
	Term   int
	Leader *PeerID
	At     time.Time
}

type PeerID int

type Node struct {
	mu sync.Mutex

	id PeerID

	currentTerm int
	votedFor    *PeerID
	votesChan   chan int

	logs     []LogEntry
	logsChan chan LogEntry

	role          Role
	leaderID      *PeerID
	stateChangeCh chan StateChange

	electionTimer  *time.Timer
	heartbeatEvery time.Duration
	electionReset  chan struct{}
}

func NewNode(id PeerID, peers map[int]chan string) *Node {
	return &Node{
		id:             id,
		role:           Follower,
		logsChan:       make(chan LogEntry),
		stateChangeCh:  make(chan StateChange, 16),
		heartbeatEvery: time.Duration(50+rand.IntN(51)) * time.Millisecond,
	}
}
