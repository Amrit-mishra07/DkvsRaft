package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"google.golang.org/grpc"

	pb "dkvsraft/proto"
	"dkvsraft/raft"
)

func main() {
	var id int
	var port int
	var httpPort int
	var peersStr string
	
	flag.IntVar(&id, "id", 0, "Node ID")
	flag.IntVar(&port, "port", 50051, "gRPC Port")
	flag.IntVar(&httpPort, "httpport", 8080, "HTTP API Port")
	flag.StringVar(&peersStr, "peers", "", "Comma-separated peer addresses")
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

	// Simple HTTP API for testing log replication
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST supported", http.StatusMethodNotAllowed)
			return
		}
		
		body, _ := io.ReadAll(r.Body)
		command := string(body)
		
		isLeader, index, term := node.Submit(command)
		
		w.Header().Set("Content-Type", "application/json")
		if isLeader {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"message": "Command accepted by leader",
				"index":   index,
				"term":    term,
			})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Not the leader",
			})
		}
	})

	go func() {
		log.Printf("Starting HTTP API on port %d...", httpPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	log.Printf("Starting Raft Node %d gRPC on port %d with peers %v...", id, port, peers)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
