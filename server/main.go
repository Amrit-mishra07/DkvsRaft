package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"

	pb "dkvsraft/proto"
	"dkvsraft/raft"
)

func main() {
	var id int
	var port int
	var peersStr string
	
	flag.IntVar(&id, "id", 0, "Node ID")
	flag.IntVar(&port, "port", 50051, "Port to listen on")
	flag.StringVar(&peersStr, "peers", "", "Comma-separated list of peer addresses")
	flag.Parse()

	if envID := os.Getenv("NODE_ID"); envID != "" {
		fmt.Sscanf(envID, "%d", &id)
	}
	if envPeers := os.Getenv("PEERS"); envPeers != "" {
		peersStr = envPeers
	}

	if id == 0 {
		log.Fatalf("Node ID must be provided and greater than 0")
	}

	peers := []string{}
	if peersStr != "" {
		peers = strings.Split(peersStr, ",")
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	node := raft.NewNode(int32(id), peers)
	pb.RegisterRaftServiceServer(grpcServer, node)

	node.Start()
	defer node.Stop()

	log.Printf("Starting Raft Node %d on port %d with peers %v...", id, port, peers)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
