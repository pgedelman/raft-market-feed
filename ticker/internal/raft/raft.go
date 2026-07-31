package raft

import (
	"fmt"
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
	Role   Role
	Term   int
	Leader *NodeID
	At     time.Time
}

type NodeID int

type Node struct {
	mu sync.Mutex

	id NodeID

	currentTerm int
	votedFor    *NodeID

	logs []LogEntry

	role     Role
	leaderID *NodeID

	electionTimer  *time.Timer
	heartbeatEvery time.Duration

	rpcChan chan any
}

func NewNode(id NodeID, peers map[int]chan string) *Node {
	return &Node{
		id:             id,
		role:           Follower,
		heartbeatEvery: time.Duration(50+rand.IntN(51)) * time.Millisecond,
		rpcChan:        make(chan any),
	}
}

func (node *Node) Start() {
	for msg := range node.rpcChan {
		switch envelope := msg.(type) {
		case RequestVoteEnvelope:
			reply := RequestVoteReply{Term: node.CurrentTerm(), VoteGranted: false}

			if envelope.Args.Term >= node.currentTerm {
				reply.VoteGranted = true
				fmt.Printf("[%s] Voted YES for %s\n", node.id, envelope.Args.CandidateID)
			} else {
				fmt.Printf("[%s] Voted NO for %s\n", node.id, envelope.Args.CandidateID)
			}
			envelope.ReplyChan <- reply
		case RequestVoteReply:
			
		}
	}
}

func (node *Node) runElectionTimer() {
	for {
		select {
		case <-node.electionTimer.C: // Happens when timer goes off
			node.becomeCandidate()
		case <-node.electionReset: // When timer needs to be reset (received a heartbeat)
			if !node.electionTimer.Stop() {
				select {
				case <-node.electionTimer.C:
				default:
				}
			}

			node.electionTimer.Reset(node.heartbeatEvery)
		}
	}
}

func (node *Node) becomeCandidate() {
	node.mu.Lock()

	node.role = Candidate
	node.currentTerm += 1
	node.votedFor = &node.id

	node.mu.Unlock()

	//node.getVotes()
}

func (node *Node) CurrentTerm() int {
	return node.currentTerm
}
