#!/bin/bash

# Start the first node (seed node)
echo "Starting seed node on port 3000..."
ruby cluster_test.rb 3000 seed_node &

# Wait a bit for the seed node to start
sleep 2

# Start additional nodes
echo "Starting node 1 on port 3001..."
ruby cluster_test.rb 3001 node1 localhost:3000 &

echo "Starting node 2 on port 3002..."
ruby cluster_test.rb 3002 node2 localhost:3000 &

echo "Starting node 3 on port 3003..."
ruby cluster_test.rb 3003 node3 localhost:3000 &

# Wait for all processes
wait
