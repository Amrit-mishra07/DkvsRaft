package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "dkvsraft/proto"
)

const (
	HeartbeatInterval = 50 * time.Millisecond
)

type Node struct {
	pb.UnimplementedRaftServiceServer

	mu sync.Mutex

	id          int32
	currentTerm int64
	votedFor    int32
	log         []*pb.LogEntry

	commitIndex int64
	lastApplied int64
	state       NodeState

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	peers       []string
	peerClients map[string]pb.RaftServiceClient

	ctx    context.Context
	cancel context.CancelFunc
}

func NewNode(id int32, peers []string) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		id:             id,
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]*pb.LogEntry, 0),
		state:          Follower,
		peers:          peers,
		peerClients:    make(map[string]pb.RaftServiceClient),
		ctx:            ctx,
		cancel:         cancel,
	}

	n.log = append(n.log, &pb.LogEntry{Term: 0, Command: ""})

	n.electionTimer = time.NewTimer(n.randomElectionTimeout())
	n.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	n.heartbeatTimer.Stop() // Only leaders tick heartbeats

	return n
}

func (n *Node) Start() {
	n.connectToPeers()
	go n.runLoop()
}

func (n *Node) Stop() {
	n.cancel()
}

func (n *Node) connectToPeers() {
	for _, peer := range n.peers {
		// In a production system we'd handle connection backoff/retries differently
		conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[Node %d] Failed to connect to peer %s: %v", n.id, peer, err)
			continue
		}
		n.peerClients[peer] = pb.NewRaftServiceClient(conn)
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	// 150ms to 300ms
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) resetElectionTimer() {
	n.electionTimer.Stop()
	n.electionTimer.Reset(n.randomElectionTimeout())
}

func (n *Node) runLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.state != Leader {
				n.startElection()
			}
			n.mu.Unlock()
		case <-n.heartbeatTimer.C:
			n.mu.Lock()
			if n.state == Leader {
				n.sendHeartbeats()
				n.heartbeatTimer.Reset(HeartbeatInterval)
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.resetElectionTimer()
	log.Printf("[Node %d] Starting election for term %d", n.id, n.currentTerm)

	votesReceived := 1 // Vote for self
	votesNeeded := (len(n.peers) + 1) / 2 + 1

	lastLogIndex := int64(len(n.log) - 1)
	lastLogTerm := n.log[lastLogIndex].Term

	args := &pb.RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateId:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	var wg sync.WaitGroup
	var votesMu sync.Mutex

	for _, peer := range n.peers {
		client, ok := n.peerClients[peer]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(p string, c pb.RaftServiceClient) {
			defer wg.Done()
			
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			
			reply, err := c.RequestVote(ctx, args)
			if err != nil {
				log.Printf("[Node %d] RequestVote to %s failed: %v", n.id, p, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || n.currentTerm != args.Term {
				return // State changed while waiting
			}

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				votesMu.Lock()
				votesReceived++
				if votesReceived >= votesNeeded && n.state == Candidate {
					n.becomeLeader()
				}
				votesMu.Unlock()
			}
		}(peer, client)
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	log.Printf("[Node %d] Became LEADER for term %d!", n.id, n.currentTerm)
	n.electionTimer.Stop()
	n.sendHeartbeats()
	n.heartbeatTimer.Reset(HeartbeatInterval)
}

func (n *Node) becomeFollower(term int64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.resetElectionTimer()
	log.Printf("[Node %d] Became Follower for term %d", n.id, term)
}

func (n *Node) sendHeartbeats() {
	lastLogIndex := int64(len(n.log) - 1)
	lastLogTerm := n.log[lastLogIndex].Term

	args := &pb.AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  lastLogTerm,
		Entries:      nil, // Empty for heartbeat
		LeaderCommit: n.commitIndex,
	}

	for _, peer := range n.peers {
		client, ok := n.peerClients[peer]
		if !ok {
			continue
		}
		go func(p string, c pb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			reply, err := c.AppendEntries(ctx, args)
			if err != nil {
				return
			}
			
			n.mu.Lock()
			defer n.mu.Unlock()
			
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
			}
		}(peer, client)
	}
}

// RequestVote RPC handler
func (n *Node) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &pb.RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if args.Term < n.currentTerm {
		return reply, nil
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	lastLogIndex := int64(len(n.log) - 1)
	lastLogTerm := n.log[lastLogIndex].Term

	logIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logIsUpToDate = true
	}

	if (n.votedFor == -1 || n.votedFor == args.CandidateId) && logIsUpToDate {
		n.votedFor = args.CandidateId
		reply.VoteGranted = true
		n.resetElectionTimer()
	}

	reply.Term = n.currentTerm
	return reply, nil
}

// AppendEntries RPC handler
func (n *Node) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &pb.AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	if args.Term < n.currentTerm {
		return reply, nil
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	// Any AppendEntries from current leader confirms they are alive
	n.resetElectionTimer()
	if n.state == Candidate {
		n.becomeFollower(args.Term)
	}

	reply.Success = true // Simplified for phase 1
	return reply, nil
}
