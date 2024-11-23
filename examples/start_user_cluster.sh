#!/bin/bash

# Cleanup any existing processes
./cleanup.sh

# Start user services
echo "Starting user service 1 (seed) on port 3000..."
ruby user_service.rb 3000 &

sleep 2

echo "Starting user service 2 on port 3001..."
ruby user_service.rb 3001 localhost:3000 &

echo "Starting user service 3 on port 3002..."
ruby user_service.rb 3002 localhost:3000 &

sleep 2

echo "Starting test client..."
ruby test_user_service.rb

# Wait for all processes
wait
