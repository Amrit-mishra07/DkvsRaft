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

	// Redirect helper
	redirectIfNeeded := func(w http.ResponseWriter, r *http.Request) bool {
		leaderId := node.GetLeaderId()
		if leaderId != -1 && leaderId != int32(id) {
			// Hardcoded mapping: Node N uses HTTP port 8080 + N
			targetUrl := fmt.Sprintf("http://localhost:%d%s", 8080+leaderId, r.URL.Path)
			if r.URL.RawQuery != "" {
				targetUrl += "?" + r.URL.RawQuery
			}
			http.Redirect(w, r, targetUrl, http.StatusTemporaryRedirect)
			return true
		}
		if leaderId == -1 {
			http.Error(w, "Cluster currently has no elected leader", http.StatusServiceUnavailable)
			return true
		}
		return false
	}

	// POST /submit
	http.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST supported", http.StatusMethodNotAllowed)
			return
		}

		if redirectIfNeeded(w, r) {
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
			// Fallback if leadership was lost exactly during processing
			http.Error(w, "Lost leadership during processing", http.StatusServiceUnavailable)
		}
	})

	// GET /get?key=xyz
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET supported", http.StatusMethodNotAllowed)
			return
		}

		if redirectIfNeeded(w, r) {
			return
		}
		
		// Enforce Linearizable Reads: Leader must verify it still has quorum
		if !node.VerifyLeadership() {
			http.Error(w, "Leader partition detected, cannot serve read", http.StatusServiceUnavailable)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing key parameter", http.StatusBadRequest)
			return
		}

		val, found := node.Get(key)
		w.Header().Set("Content-Type", "application/json")
		if found {
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"key":     key,
				"value":   val,
			})
		} else {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"message": "Key not found",
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
