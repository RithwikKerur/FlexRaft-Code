#!/bin/bash

# --- 0. Define Cleanup (Trap) ---
stop_all_jobs() {
    echo "Stopping all background processes..."
    # Kills both the servers AND the persistent client
    kill $(jobs -p) 2>/dev/null
    wait
    echo "All processes stopped."
}
trap "stop_all_jobs; exit" SIGINT

# --- 1. Build the Project ---
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

    # --- A. Clean Old Data ---
    echo "Cleaning up old databases and logs..."
    rm -rf /Users/rithwikkerur/Documents/UCSB/data/testdb{0..4}
    rm -rf /Users/rithwikkerur/Documents/UCSB/data/raft_log{0..4}

    # --- B. Start Initial Servers (0, 1, 2) ---
    LATE_NODE_A=3
    LATE_NODE_B=4
    DELAY_SECONDS=1

    echo "Starting 3 initial servers (skipping $LATE_NODE_A and $LATE_NODE_B)..."
    
    for i in {0..4}
    do
        # Check if current index matches EITHER late node
        if [ "$i" -eq "$LATE_NODE_A" ] || [ "$i" -eq "$LATE_NODE_B" ]; then
            continue
        fi

        build/bench/bench_server --conf=example.conf --id=$i > raft_log$i 2>&1 &
        echo "Started Server $i (PID: $!)"
    done

    echo "Waiting 2 seconds for cluster to stabilize..."
    sleep 2

    # --- C. Start Client (LONG RUNNING) ---
    # NOTE: write_num is set to 100 Million so it runs for the full duration
    echo "Starting Client (Backgrounded, Massive Write Count)..."
    build/bench/bench_client \
        --conf=example.conf \
        --id=0 \
        --size=4k \
        --write_num=100000000 & 
    
    # --- D. Delay before Late Servers ---
    echo "Client running... waiting $DELAY_SECONDS seconds to start late servers..."
    sleep $DELAY_SECONDS

    # --- E. Start Late Servers (3 and 4) ---
    echo "Starting Late Server $LATE_NODE_A..."
    build/bench/bench_server --conf=example.conf --id=$LATE_NODE_A > raft_log$LATE_NODE_A 2>&1 &
    echo "Started Server $LATE_NODE_A (PID: $!)"

    echo "Starting Late Server $LATE_NODE_B..."
    build/bench/bench_server --conf=example.conf --id=$LATE_NODE_B > raft_log$LATE_NODE_B 2>&1 &
    echo "Started Server $LATE_NODE_B (PID: $!)"

    # --- F. Run for the Specified Duration ---
    # We subtract the startup delays (~3s) to be precise, or just sleep full duration
    echo "Letting cluster run for remaining $DURATION seconds..."
    sleep $DURATION

    # --- G. Kill Everything ---
    stop_all_jobs

    # --- H. Run the Storage Test ---
    echo ">>> Running Storage Test (Post-${MINUTE}min run)..." >&2
    ./build/bench/testStorage 1>&2
    echo ">>> Storage Test Finished." >&2

    # Pause briefly before the next loop (which will wipe data)
    sleep 3
done

echo "All 5 benchmark runs completed."