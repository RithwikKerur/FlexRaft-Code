#!/bin/bash
# --- 0. Define Cleanup (Trap) ---

cleanup() {

echo ""

echo "Stopping all remaining servers..."

# Kill all child processes of this script

kill $(jobs -p) 2>/dev/null

wait

echo "All processes stopped."

}

trap cleanup SIGINT



# --- 1. Clean Old Data ---

echo "Cleaning up old databases and logs..."

rm -rf /Users/rithwikkerur/Documents/UCSB/data/testdb{0..4}

rm -rf /Users/rithwikkerur/Documents/UCSB/data/raft_log{0..4}



# --- 2. Build the Project ---

echo "Building project..."

CMAKE=cmake make build || { echo "Build failed! Exiting."; exit 1; }



# --- 3. Start Servers and Capture PIDs ---

echo "Starting 5 servers..."

declare -a pids # Array to store Process IDs



for i in {0..4}

do

# Start server in background

build/bench/bench_server --conf=example.conf --id=$i > raft_log$i 2>&1 &


# $! contains the PID of the last background command

pids[$i]=$!

echo "Started Server $i (PID: ${pids[$i]})"

sleep 1

done







# --- 4. Wait 2 Seconds ---

echo "Waiting 2 seconds..."

sleep 2





# --- 6. Start Client ---

echo "Starting Client..."

build/bench/bench_client --conf=example.conf --id=0 --size=4k --write_num=10000