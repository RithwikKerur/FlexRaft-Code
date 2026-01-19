#!/bin/bash

# --- 0. Define Cleanup (Trap) ---
cleanup() {
    echo ""
    echo "Stopping all remaining servers..."
    # Kill all child processes (servers)
    kill $(jobs -p) 2>/dev/null
    wait
    echo "All processes stopped."
}
# Trap Ctrl+C (SIGINT) so you can still abort early if needed
trap cleanup SIGINT

# --- 1. Clean Old Data ---
echo "Cleaning up old databases and logs..."
rm -rf /Users/rithwikkerur/Documents/UCSB/data/testdb{0..4}
rm -rf /Users/rithwikkerur/Documents/UCSB/data/raft_log{0..4}

# --- 2. Build the Project ---
echo "Building project..."
CMAKE=cmake make build || { echo "Build failed! Exiting."; exit 1; }

# --- 3. Start Initial Servers (0-3) ---
LATE_NODE_ID=4
DELAY_SECONDS=1

echo "Starting 4 initial servers..."
declare -a pids

for i in {0..4}
do
    # Skip the late node
    if [ "$i" -eq "$LATE_NODE_ID" ]; then
        continue
    fi

    # Start server in background
    build/bench/bench_server --conf=example.conf --id=$i > raft_log$i 2>&1 &
    
    # Capture PID
    pids[$i]=$!
    echo "Started Server $i (PID: ${pids[$i]})"
    sleep 1
done

echo "Waiting 2 seconds for cluster to stabilize..."
sleep 2

# --- 4. Start Client (IN BACKGROUND) ---
echo "Starting Client (Backgrounded)..."
# We add '&' here so the script doesn't pause!
build/bench/bench_client --conf=example.conf --id=0 --size=4k --write_num=1000 &
CLIENT_PID=$!

# --- 5. Delay before Late Server ---
echo "Client running... waiting $DELAY_SECONDS seconds to start late server..."
sleep $DELAY_SECONDS

# --- 6. Start Late Server -> Freeze -> Resume ---
echo "Starting Late Server $LATE_NODE_ID..."
build/bench/bench_server --conf=example.conf --id=$LATE_NODE_ID > raft_log$LATE_NODE_ID 2>&1 &
pids[$LATE_NODE_ID]=$!
echo "Started Server $LATE_NODE_ID (PID: ${pids[$LATE_NODE_ID]})"

# A. Let it run briefly to initialize and join
echo "Letting Server $LATE_NODE_ID run for 1 second..."
sleep 1

# B. Freeze the process (Simulate network/process hang)
echo "Freezing Server $LATE_NODE_ID (SIGSTOP)..."
kill -STOP ${pids[$LATE_NODE_ID]}

# C. Wait 500ms (0.5 seconds)
sleep 0.5

# D. Unfreeze the process (Resume execution)
echo "Resuming Server $LATE_NODE_ID (SIGCONT)..."
kill -CONT ${pids[$LATE_NODE_ID]}

# --- 7. Wait for Client to Finish ---
echo "Waiting for client (PID $CLIENT_PID) to finish its work..."
wait $CLIENT_PID

# --- 8. Shutdown ---
echo "Benchmark finished. Cleaning up..."
cleanup