#!/bin/bash

# --- 0. Define Helper Functions ---

# Function to kill all running background jobs (servers)
stop_all_servers() {
    echo "Stopping all servers..."
    # Kill all child processes (the servers started with &)
    kill $(jobs -p) 2>/dev/null
    wait
    echo "All processes stopped."
}

# Trap Ctrl+C so you can abort the entire script
trap "stop_all_servers; exit" SIGINT

# --- 1. Build Once ---
echo "Building project..."
CMAKE=cmake make build || { echo "Build failed! Exiting."; exit 1; }

# --- 2. Main Benchmark Loop (1 min to 5 mins) ---
for MINUTE in {1..5}
do
    DURATION=$((MINUTE * 60))
    echo ""
    echo "========================================================"
    echo "STARTING RUN #$MINUTE: Duration = $DURATION seconds"
    echo "========================================================"

    # A. Clean Old Data (Fresh Start for this run)
    echo "Cleaning up old databases and logs..."
    rm -rf /Users/rithwikkerur/Documents/UCSB/data/testdb{0..4}
    rm -rf /Users/rithwikkerur/Documents/UCSB/data/raft_log{0..4}

    # B. Start 5 Servers
    echo "Starting 5 servers..."
    for i in {0..4}
    do
        build/bench/bench_server --conf=example.conf --id=$i > raft_log$i 2>&1 &
        echo "Started Server $i (PID: $!)"
    done

    # C. Wait for cluster to stabilize
    sleep 2

    # D. Start Client (Backgrounded so we can sleep)
    # Note: If write_num=1000 finishes fast, the servers will just sit idle 
    # for the rest of the duration. Increase write_num if you want constant load.
    echo "Starting Client..."
    build/bench/bench_client --conf=example.conf --id=0 --size=4k --write_num=100000 &

    # E. Let the script run for the specified duration
    echo "Letting cluster run for $DURATION seconds..."
    sleep $DURATION

    # F. Kill Everything
    stop_all_servers

    # G. Run the Storage Test
    echo ">>> Running Storage Test (Post-${MINUTE}min run)..."
    ./build/bench/testStorage
    echo ">>> Storage Test Finished."

    # Optional: Short pause before next iteration
    sleep 2
done

echo "All 5 benchmark runs completed."