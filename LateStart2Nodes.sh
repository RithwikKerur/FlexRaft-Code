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

# --- 3. Start Initial Servers (0, 1, 2) ---
# CONFIGURATION: Define which two nodes start late
LATE_NODE_A=3
LATE_NODE_B=4
DELAY_SECONDS=3

echo "Starting 3 initial servers (skipping $LATE_NODE_A and $LATE_NODE_B)..."
declare -a pids

for i in {0..4}
do
    # Check if current index matches EITHER late node
    if [ "$i" -eq "$LATE_NODE_A" ] || [ "$i" -eq "$LATE_NODE_B" ]; then
        continue
    fi

    build/bench/bench_server --conf=example.conf --id=$i > raft_log$i 2>&1 &
    
    pids[$i]=$!
    echo "Started Server $i (PID: ${pids[$i]})"
    sleep 1
done

echo "Waiting 2 seconds for cluster to stabilize..."
sleep 2

# --- 4. Start Client (IN BACKGROUND) ---
echo "Starting Client (Backgrounded)..."
build/bench/bench_client --conf=example.conf --id=0 --size=4k --write_num=1000 &
CLIENT_PID=$!

# --- 5. Delay before Late Servers ---
echo "Client running... waiting $DELAY_SECONDS seconds to start late servers..."
sleep $DELAY_SECONDS

# --- 6. Start Late Servers (3 and 4) ---
echo "Starting Late Server $LATE_NODE_A..."
build/bench/bench_server --conf=example.conf --id=$LATE_NODE_A > raft_log$LATE_NODE_A 2>&1 &
pids[$LATE_NODE_A]=$!
echo "Started Server $LATE_NODE_A (PID: ${pids[$LATE_NODE_A]})"

echo "Starting Late Server $LATE_NODE_B..."
build/bench/bench_server --conf=example.conf --id=$LATE_NODE_B > raft_log$LATE_NODE_B 2>&1 &
pids[$LATE_NODE_B]=$!
echo "Started Server $LATE_NODE_B (PID: ${pids[$LATE_NODE_B]})"

# --- 7. Wait for Client to Finish ---
echo "Waiting for client (PID $CLIENT_PID) to finish its work..."
wait $CLIENT_PID

# --- 8. Shutdown ---
echo "Benchmark finished. Cleaning up..."
cleanup