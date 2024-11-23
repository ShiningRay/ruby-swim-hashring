#!/bin/bash

# Function to kill processes using specific ports
kill_port() {
    local port=$1
    local pid=$(lsof -ti :$port)
    if [ ! -z "$pid" ]; then
        echo "Killing process using port $port (PID: $pid)"
        kill -9 $pid
    fi
}

# Kill processes on our service ports
kill_port 3000
kill_port 3001
kill_port 3002
kill_port 3100

echo "Cleanup completed"
sleep 1
