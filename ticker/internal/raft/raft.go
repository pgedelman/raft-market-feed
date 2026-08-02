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

	id       NodeID
	registry NetworkRegistry

	currentTerm  int
	votedFor     *NodeID
	currentVotes int

	logs        []LogEntry
	logIndex    int
	commitIndex int
	lastApplied int

	role     Role
	leaderID *NodeID

	electionTimer  *time.Timer
	heartbeatEvery time.Duration

	rpcChan chan RPCEnvelope
}

func NewNode(id NodeID, peers map[int]chan string) *Node {
	return &Node{
		id:             id,
		role:           Follower,
		heartbeatEvery: time.Duration(50+rand.IntN(51)) * time.Millisecond,
		rpcChan:        make(chan RPCEnvelope),
	}
}

func (node *Node) Start() {
	go node.runElectionTimer()

	for rpc := range node.rpcChan {
		switch msg := rpc.Payload.(type) {
		case AppendEntriesArgs:
			if !node.electionTimer.Stop() { // Reset timer (received a heartbeat)
				select {
				case <-node.electionTimer.C:
				default:
				}
			}
			reply := AppendEntriesReply{Term: node.currentTerm, Success: false}

			if rpc.Term >= node.currentTerm { // Peer has a newer leader
				node.becomeFollower(msg.LeaderID, rpc.Term)
				reply.Success = true
			}

			rpc.ReplyChan <- reply
			node.electionTimer.Reset(node.heartbeatEvery) //  Reset heartbeat timer
		case RequestVoteArgs:
			reply := RequestVoteReply{Term: node.currentTerm, VoteGranted: false}

			if rpc.Term >= node.currentTerm { // Confirms node is a follower
				reply.VoteGranted = true
				fmt.Printf("[%d] Voted YES for %d\n", node.id, msg.CandidateID)
			} else {
				fmt.Printf("[%d] Voted NO for %d\n", node.id, msg.CandidateID)
			}
			rpc.ReplyChan <- reply
		}
	}
}

func (node *Node) runElectionTimer() {
	for range node.electionTimer.C {
		node.becomeCandidate()
		go node.requestVotes()
	}
}

func (node *Node) runVoteHandler(voteChan chan any, term int, clusterSize int) {
	votes := 1 // Votes for itself
	for msg := range voteChan {
		reply, ok := msg.(RequestVoteReply)
		if !ok {
			continue
		}

		node.mu.Lock()
		stale := node.currentTerm != term || node.role == Candidate
		stepDown := reply.Term > node.currentTerm
		node.mu.Unlock()

		if stepDown {
			node.becomeFollower(node.id, reply.Term)
			return
		}
		if stale {
			return // A newer election has already started
		}

		if reply.VoteGranted {
			votes++
			if votes > clusterSize/2 {
				// NOTE DEFINED         node.becomeLeader() // Majority votes becomes leader
				return
			}
		}
	}
}

func (node *Node) becomeFollower(newLeaderID NodeID, newTerm int) {
	node.mu.Lock()

	node.leaderID = &newLeaderID
	node.role = Follower
	node.currentTerm = newTerm

	node.mu.Unlock()
}
func (node *Node) becomeCandidate() {
	node.mu.Lock()

	node.role = Candidate
	node.currentTerm += 1
	node.votedFor = &node.id

	node.mu.Unlock()
}

func (node *Node) requestVotes() {
	peers := node.registry.GetPeers(node.id) // Get other nodes in cluster
	replyChan := make(chan any)

	for _, peer := range peers {
		go func() {
			peer.rpcChan <- RPCEnvelope{
				Term:      node.currentTerm,
				Payload:   RequestVoteArgs{CandidateID: node.id},
				ReplyChan: replyChan,
			}
		}()
	}

	go node.runVoteHandler(replyChan, node.currentTerm, len(peers)+1)
}
