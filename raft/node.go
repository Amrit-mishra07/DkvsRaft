package raft

import (
	"context"
	"log"
	"math/rand"
	"strings"
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
	leaderId    int32 // Tracks the current leader ID (-1 if unknown)

	// Volatile state on leaders
	nextIndex  map[string]int64
	matchIndex map[string]int64

	// Application State Machine
	kvStore map[string]string
	applyCh chan struct{}

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
		leaderId:       -1,
		nextIndex:      make(map[string]int64),
		matchIndex:     make(map[string]int64),
		kvStore:        make(map[string]string),
		applyCh:        make(chan struct{}, 1),
		peers:          peers,
		peerClients:    make(map[string]pb.RaftServiceClient),
		ctx:            ctx,
		cancel:         cancel,
	}

	n.log = append(n.log, &pb.LogEntry{Term: 0, Command: ""})

	n.electionTimer = time.NewTimer(n.randomElectionTimeout())
	n.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	n.heartbeatTimer.Stop()

	return n
}

func (n *Node) Start() {
	n.connectToPeers()
	go n.runLoop()
	go n.applyLoop()
}

func (n *Node) Stop() {
	n.cancel()
}

func (n *Node) connectToPeers() {
	for _, peer := range n.peers {
		conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[Node %d] Failed to connect to peer %s: %v", n.id, peer, err)
			continue
		}
		n.peerClients[peer] = pb.NewRaftServiceClient(conn)
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
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
				n.sendAppendEntries()
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
	n.leaderId = -1 // Unknown leader during election
	n.resetElectionTimer()
	log.Printf("[Node %d] Starting election for term %d", n.id, n.currentTerm)

	votesReceived := 1
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
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || n.currentTerm != args.Term {
				return
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
	n.leaderId = n.id
	log.Printf("[Node %d] Became LEADER for term %d!", n.id, n.currentTerm)
	
	lastLogIndex := int64(len(n.log) - 1)
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastLogIndex + 1
		n.matchIndex[peer] = 0
	}

	n.electionTimer.Stop()
	n.sendAppendEntries()
	n.heartbeatTimer.Reset(HeartbeatInterval)
}

func (n *Node) becomeFollower(term int64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.resetElectionTimer()
	log.Printf("[Node %d] Became Follower for term %d", n.id, term)
}

// GetLeaderId returns the currently known leader, or -1 if unknown
func (n *Node) GetLeaderId() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderId
}

func (n *Node) Submit(command string) (bool, int64, int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return false, -1, -1
	}

	term := n.currentTerm
	index := int64(len(n.log))
	n.log = append(n.log, &pb.LogEntry{Term: term, Command: command})

	log.Printf("[Node %d] Accepted command '%s', appending to log index %d", n.id, command, index)

	go func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.state == Leader {
			n.sendAppendEntries()
		}
	}()

	return true, index, term
}

func (n *Node) sendAppendEntries() {
	for _, peer := range n.peers {
		client, ok := n.peerClients[peer]
		if !ok {
			continue
		}

		nextIdx := n.nextIndex[peer]
		prevLogIndex := nextIdx - 1
		prevLogTerm := n.log[prevLogIndex].Term
		
		var entries []*pb.LogEntry
		if nextIdx < int64(len(n.log)) {
			entries = n.log[nextIdx:]
		}

		args := &pb.AppendEntriesArgs{
			Term:         n.currentTerm,
			LeaderId:     n.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}

		go func(p string, c pb.RaftServiceClient, reqArgs *pb.AppendEntriesArgs) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			reply, err := c.AppendEntries(ctx, reqArgs)
			if err != nil {
				return
			}
			
			n.mu.Lock()
			defer n.mu.Unlock()
			
			if n.state != Leader || n.currentTerm != reqArgs.Term {
				return
			}

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				if len(reqArgs.Entries) > 0 {
					n.nextIndex[p] = reqArgs.PrevLogIndex + int64(len(reqArgs.Entries)) + 1
					n.matchIndex[p] = n.nextIndex[p] - 1
					n.updateCommitIndex()
				}
			} else {
				n.nextIndex[p]--
				if n.nextIndex[p] < 1 {
					n.nextIndex[p] = 1
				}
			}
		}(peer, client, args)
	}
}

func (n *Node) updateCommitIndex() {
	for n.commitIndex < int64(len(n.log)-1) {
		newCommitIndex := n.commitIndex + 1
		matches := 1 
		
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= newCommitIndex {
				matches++
			}
		}

		if matches > len(n.peers)/2 && n.log[newCommitIndex].Term == n.currentTerm {
			n.commitIndex = newCommitIndex
			log.Printf("[Node %d] Leader advanced commitIndex to %d", n.id, n.commitIndex)
			n.triggerApply()
		} else {
			break
		}
	}
}

func (n *Node) triggerApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.applyCh:
			n.mu.Lock()
			for n.lastApplied < n.commitIndex {
				n.lastApplied++
				entry := n.log[n.lastApplied]
				n.applyCommand(entry.Command)
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) applyCommand(command string) {
	parts := strings.SplitN(command, " ", 3)
	if len(parts) == 0 {
		return
	}
	
	action := strings.ToUpper(parts[0])
	switch action {
	case "SET":
		if len(parts) >= 3 {
			key, value := parts[1], parts[2]
			n.kvStore[key] = value
			log.Printf("[Node %d] StateMachine Applied: SET %s = %s", n.id, key, value)
		}
	case "DEL":
		if len(parts) >= 2 {
			key := parts[1]
			delete(n.kvStore, key)
			log.Printf("[Node %d] StateMachine Applied: DEL %s", n.id, key)
		}
	}
}

// VerifyLeadership forces the leader to contact a majority to ensure it isn't isolated.
func (n *Node) VerifyLeadership() bool {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return false
	}
	
	term := n.currentTerm
	leaderId := n.id
	commitIdx := n.commitIndex
	n.mu.Unlock()

	args := &pb.AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: 0, 
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: commitIdx,
	}

	var wg sync.WaitGroup
	var acks int32 = 1 // Self
	var needs = (len(n.peers) + 1) / 2

	for _, peer := range n.peers {
		client, ok := n.peerClients[peer]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(c pb.RaftServiceClient) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			reply, err := c.AppendEntries(ctx, args)
			if err == nil && reply.Success {
				n.mu.Lock()
				acks++
				n.mu.Unlock()
			}
		}(client)
	}

	wg.Wait()
	
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader && acks >= int32(needs)
}

func (n *Node) Get(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	val, ok := n.kvStore[key]
	return val, ok
}

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

	// Update our knowledge of the leader
	n.leaderId = args.LeaderId

	n.resetElectionTimer()
	if n.state == Candidate {
		n.becomeFollower(args.Term)
	}

	if args.PrevLogIndex >= int64(len(n.log)) {
		return reply, nil
	}
	if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return reply, nil
	}

	insertIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0

	for {
		if insertIndex >= int64(len(n.log)) || newEntriesIndex >= len(args.Entries) {
			break
		}
		if n.log[insertIndex].Term != args.Entries[newEntriesIndex].Term {
			n.log = n.log[:insertIndex]
			break
		}
		insertIndex++
		newEntriesIndex++
	}

	if newEntriesIndex < len(args.Entries) {
		n.log = append(n.log, args.Entries[newEntriesIndex:]...)
		log.Printf("[Node %d] Appended %d entries, log length is now %d", n.id, len(args.Entries)-newEntriesIndex, len(n.log))
	}

	if args.LeaderCommit > n.commitIndex {
		lastLogIndex := int64(len(n.log) - 1)
		if args.LeaderCommit < lastLogIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastLogIndex
		}
		log.Printf("[Node %d] Follower advanced commitIndex to %d", n.id, n.commitIndex)
		n.triggerApply()
	}

	reply.Success = true
	return reply, nil
}
