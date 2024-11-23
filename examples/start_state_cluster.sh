#!/bin/bash

# Cleanup any existing processes
./cleanup.sh

# Start state services
echo "Starting state service 1 (seed) on port 3000..."
ruby user_service.rb 3000 state_service &

sleep 2

echo "Starting state service 2 on port 3001..."
ruby user_service.rb 3001 state_service localhost:3000 &

echo "Starting state service 3 on port 3002..."
ruby user_service.rb 3002 state_service localhost:3000 &

sleep 2

echo "Starting test client..."
ruby test_state_sync.rb

# Wait for all processes
wait
