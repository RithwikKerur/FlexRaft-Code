import subprocess
import time
import os
import sys
import shutil
import signal

# --- Configuration ---
WRITE_COUNTS = [1000, 2000, 4000, 8000]  # The specific tests to run
LATE_NODE_A = 3
LATE_NODE_B = 4
DELAY_SECONDS = 2.0
BASE_DIR = "/Users/rithwikkerur/Documents/UCSB/data"
CONF_FILE = "example.conf"
BUILD_DIR = "build/bench"
SERVER_BIN = os.path.join(BUILD_DIR, "bench_server")
CLIENT_BIN = os.path.join(BUILD_DIR, "bench_client")
TEST_STORAGE_BIN = os.path.join(BUILD_DIR, "testStorage")

# Track running processes to kill them later
running_procs = []
open_files = []

def cleanup(signum=None, frame=None):
    """Kills all tracked subprocesses."""
    print("\nStopping all background processes...")
    for proc in running_procs:
        if proc.poll() is None:  # If process is still running
            try:
                proc.terminate()
                proc.wait(timeout=1)
            except subprocess.TimeoutExpired:
                proc.kill()
    
    for f in open_files:
        if not f.closed:
            f.close()
    
    running_procs.clear()
    open_files.clear()
    print("All processes stopped.")
    
    if signum is not None:
        sys.exit(1)

signal.signal(signal.SIGINT, cleanup)

def clean_old_data():
    """Removes old raft logs (files) and database (directories)."""
    print("Cleaning up old databases and logs...")
    for i in range(5):
        # DB Cleanup
        db_path = os.path.join(BASE_DIR, f"testdb{i}")
        if os.path.exists(db_path):
            if os.path.isdir(db_path):
                shutil.rmtree(db_path)
            else:
                os.remove(db_path)

        # Log Cleanup
        log_path = f"raft_log{i}"
        candidates = [log_path, os.path.join(BASE_DIR, log_path)]
        for p in candidates:
            if os.path.exists(p):
                if os.path.isfile(p):
                    os.remove(p)
                else:
                    shutil.rmtree(p)
    
    if os.path.exists("client_run.log"):
        os.remove("client_run.log")

def wait_for_client_init(client_proc):
    """Polls log until client is ready."""
    print("Building Bench... Waiting for client to be ready...")
    while True:
        if os.path.exists("client_run.log"):
            with open("client_run.log", "r", errors='ignore') as f:
                if "[Execution Process]" in f.read():
                    print(">>> Client Ready! ([Execution Process] detected)")
                    return

        if client_proc.poll() is not None:
            print("Error: Client process died unexpectedly!")
            cleanup()
            sys.exit(1)
        time.sleep(0.1)

def main():
    # --- 1. Build Project ---
    print("Building project...")
    ret = subprocess.call("CMAKE=cmake make build", shell=True)
    if ret != 0:
        print("Build failed! Exiting.")
        sys.exit(1)

    # --- 2. Main Loop over Write Counts ---
    for count in WRITE_COUNTS:
        print(f"\n{'='*56}")
        print(f"STARTING RUN: Write Count = {count} entries")
        print(f"{'='*56}")

        clean_old_data()

        # --- Start Initial Servers (Skip Late Nodes) ---
        print(f"Starting 3 initial servers...")
        for i in range(5):
            if i == LATE_NODE_A or i == LATE_NODE_B:
                continue
            
            log_file = open(f"raft_log{i}", "w")
            open_files.append(log_file)
            proc = subprocess.Popen(
                [SERVER_BIN, f"--conf={CONF_FILE}", f"--id={i}"],
                stdout=log_file, stderr=subprocess.STDOUT
            )
            running_procs.append(proc)
            print(f"Started Server {i} (PID: {proc.pid})")

        print("Waiting 2 seconds for cluster to stabilize...")
        time.sleep(2)

        # --- Start Client (With specific write_num) ---
        print(f"Starting Client to write {count} entries...")
        client_log = open("client_run.log", "w")
        open_files.append(client_log)
        
        client_proc = subprocess.Popen(
            [CLIENT_BIN, f"--conf={CONF_FILE}", "--id=0", "--size=4k", f"--write_num={count}"],
            stdout=client_log, stderr=subprocess.STDOUT
        )
        running_procs.append(client_proc)
        
        wait_for_client_init(client_proc)

        # --- Delay before Late Servers ---
        # Note: If count is very small, client might finish during this sleep.
        # That is okay; we check if client is alive later.
        print(f"Client running... waiting {DELAY_SECONDS} seconds to start late servers...")
        time.sleep(DELAY_SECONDS)

        # --- Start Late Servers ---
        for node_id in [LATE_NODE_A, LATE_NODE_B]:
            print(f"Starting Late Server {node_id}...")
            log_file = open(f"raft_log{node_id}", "w")
            open_files.append(log_file)
            proc = subprocess.Popen(
                [SERVER_BIN, f"--conf={CONF_FILE}", f"--id={node_id}"],
                stdout=log_file, stderr=subprocess.STDOUT
            )
            running_procs.append(proc)
            print(f"Started Server {node_id} (PID: {proc.pid})")

        # --- WAIT for Client to Finish ---
        print(f"Waiting for client to finish writing {count} entries...")
        exit_code = client_proc.wait() # This blocks until client exits
        print(f"Client finished with exit code {exit_code}")

        # --- Kill Servers ---
        cleanup()

        # --- Run Storage Test ---
        print(f">>> Running Storage Test (Post-{count} entries)...", file=sys.stderr)
        subprocess.call([TEST_STORAGE_BIN], stdout=sys.stderr, stderr=sys.stderr)
        print(">>> Storage Test Finished.", file=sys.stderr)

        time.sleep(45)

    print("All benchmark runs completed.")

if __name__ == "__main__":
    main()