#!/bin/bash
# Simple script to demonstrate cluster resilience

echo "1. Writing key 'status=active' to Node 1..."
curl -s -L -X POST -d "SET status active" http://localhost:8081/submit
echo ""

echo -e "\n2. Reading key 'status' from Node 3..."
curl -s -L "http://localhost:8083/get?key=status"
echo ""

echo -e "\n\n3. Killing Node 1 (simulating crash)..."
sudo docker stop dkvsraft-node1-1

echo "4. Waiting 2 seconds for new leader election..."
sleep 2

echo "5. Writing key 'recovery=success' to Node 2..."
curl -s -L -X POST -d "SET recovery success" http://localhost:8082/submit
echo ""

echo -e "\n6. Reading new key from Node 3..."
curl -s -L "http://localhost:8083/get?key=recovery"
echo ""

echo -e "\n\n7. Bringing Node 1 back online..."
sudo docker start dkvsraft-node1-1
echo "Cluster fully recovered!"
