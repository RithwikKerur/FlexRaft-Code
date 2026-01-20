import subprocess
import time
import os
import sys
import shutil
import signal

# --- Configuration ---
NUM_NODES = 5          
WRITE_COUNTS = [1000, 2000, 4000, 8000]  
LATE_NODE_A = NUM_NODES - 2  # Automatically pick 2nd to last node
LATE_NODE_B = NUM_NODES - 1  # Automatically pick last node
DELAY_SECONDS = 2
BASE_DIR = "./experiments"
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
        if proc.poll() is None:
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

def force_kill_all():
    """Blindly kills any process named 'bench_server' to prevent 'Port in use' errors."""
    print("Pre-flight check: Killing any lingering server instances...")
    try:
        subprocess.call(["pkill", "-9", "-f", "bench_server"])
        subprocess.call(["pkill", "-9", "-f", "bench_client"])
        time.sleep(1)
    except Exception as e:
        print(f"Warning: automatic cleanup failed ({e}). Check manually if ports are blocked.")

def generate_config(num_nodes):
    """Generates the example.conf file dynamically based on NUM_NODES."""
    print(f"Generating {CONF_FILE} for {num_nodes} nodes...")
    
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    with open(CONF_FILE, "w") as f:
        for i in range(num_nodes):
            # Calculate ports: 50001/50002, 50003/50004, etc.
            raft_port = 50001 + (i * 2)
            service_port = 50002 + (i * 2)
            
            # Paths
            log_path = os.path.join(BASE_DIR, f"raft_log{i}")
            db_path = os.path.join(BASE_DIR, f"testdb{i}")
            
            # Line format: ID IP:RaftPort IP:ServicePort LogPath DBPath
            line = f"{i} 127.0.0.1:{raft_port} 127.0.0.1:{service_port} {log_path} {db_path}\n"
            f.write(line)
    
    print(f"Successfully generated {CONF_FILE}")

def clean_old_data():
    """Removes old raft logs (files) and database (directories)."""
    print("Cleaning up old databases and logs...")
    for i in range(NUM_NODES):
        db_path = os.path.join(BASE_DIR, f"testdb{i}")
        if os.path.exists(db_path):
            if os.path.isdir(db_path):
                shutil.rmtree(db_path)
            else:
                os.remove(db_path)

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
    # --- 0. Pre-flight Cleanup ---
    force_kill_all()

    # --- 1. Generate Config ---
    generate_config(NUM_NODES)

    # --- 2. Build Project ---
    print("Building project...")
    ret = subprocess.call("CMAKE=cmake make build", shell=True)
    if ret != 0:
        print("Build failed! Exiting.")
        sys.exit(1)

    # --- 3. Main Loop over Write Counts ---
    for count in WRITE_COUNTS:
        print(f"\n{'='*56}")
        print(f"STARTING RUN: Write Count = {count} entries")
        print(f"{'='*56}")

        clean_old_data()

        # --- Start Initial Servers (Skip Late Nodes) ---
        print(f"Starting {NUM_NODES - 2} initial servers...")
        for i in range(NUM_NODES):
            if i == LATE_NODE_A or i == LATE_NODE_B:
                continue
            
            log_file = open(f"raft_log{i}", "w")
            open_files.append(log_file)
            
            # We assume your C++ binary reads paths from conf, but passing --data_path
            # is safer if your binary supports it.
            proc = subprocess.Popen(
                [SERVER_BIN, f"--conf={CONF_FILE}", f"--id={i}"],
                stdout=log_file, stderr=subprocess.STDOUT
            )
            running_procs.append(proc)
            print(f"Started Server {i} (PID: {proc.pid})")

        print("Waiting 2 seconds for cluster to stabilize...")
        time.sleep(2)

        # --- Start Client ---
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
        exit_code = client_proc.wait() 
        print(f"Client finished with exit code {exit_code}")

        # --- FIX: Give late nodes time to catch up! ---
        print("Waiting 15 seconds for late nodes to replicate data...")
        time.sleep(15) 

        # --- Kill Servers ---
        cleanup()

        # --- Run Storage Test ---
        print(f">>> Running Storage Test (Post-{count} entries)...", file=sys.stderr)
        subprocess.call([TEST_STORAGE_BIN, str(NUM_NODES)], stdout=sys.stderr, stderr=sys.stderr)        
        print(">>> Storage Test Finished.", file=sys.stderr)

        time.sleep(45)

    print("All benchmark runs completed.")

if __name__ == "__main__":
    main()