import subprocess
import time
import os
import sys
import shutil
import signal

# --- Configuration ---
NUM_NODES = 11          
WRITE_COUNTS = [4000]  
LATE_NODE_A = NUM_NODES - 2
LATE_NODE_B = NUM_NODES - 1 
DELAY_SECONDS = 2
BASE_DIR = "./experiments"
CONF_FILE = "example.conf"
BUILD_DIR = "build/bench"
SERVER_BIN = os.path.join(BUILD_DIR, "bench_server")
CLIENT_BIN = os.path.join(BUILD_DIR, "bench_client")
TEST_STORAGE_BIN = os.path.join(BUILD_DIR, "testStorage")
debug = True

# --- Globals for Tracking ---
running_procs = [] # For blind kill on Ctrl+C
open_files = []
active_servers = [] # Metadata to map PIDs to IDs

def get_exit_message(code):
    """Converts a return code into a human-readable system message."""
    if code == 0:
        return "Success (0)"
    
    # Negative values indicate termination by signal
    if code < 0:
        try:
            sig_name = signal.Signals(-code).name
            return f"{sig_name} ({code})"
        except ValueError:
            return f"Signal {code}"
            
    return f"Error Code ({code})"

def cleanup(signum=None, frame=None):
    """Kills processes and prints a clean exit code table."""
    print("\n" + "="*50)
    print("STOPPING SERVERS & CHECKING EXIT CODES")
    print("="*50)

    # 1. Terminate all tracked servers
    for server in active_servers:
        proc = server['proc']
        if proc.poll() is None:
            try:
                proc.terminate()
                # Give it a moment to shut down gracefully so we get a code
                proc.wait(timeout=1) 
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    # 2. Kill any other background processes (like the client)
    for proc in running_procs:
        if proc.poll() is None:
            proc.kill()

    # 3. Print the Summary Table
    print(f"{'Node ID':<10} | {'PID':<8} | {'Status Message'}")
    print("-" * 50)
    
    for server in active_servers:
        node_id = server['id']
        proc = server['proc']
        code = proc.returncode
        msg = get_exit_message(code)
        
        print(f"{node_id:<10} | {proc.pid:<8} | {msg}")

    # 4. Clean up file handles
    for f in open_files:
        if not f.closed:
            f.close()
    
    # Reset tracking lists
    running_procs.clear()
    active_servers.clear()
    open_files.clear()
    print("\nCleanup complete.")
    
    if signum is not None:
        sys.exit(1)

signal.signal(signal.SIGINT, cleanup)

def force_kill_all():
    print("Pre-flight check: Killing lingering instances...")
    subprocess.call(["pkill", "-9", "-f", "bench_server"], stderr=subprocess.DEVNULL)
    subprocess.call(["pkill", "-9", "-f", "bench_client"], stderr=subprocess.DEVNULL)
    subprocess.call(["pkill", "-9", "gdb"], stderr=subprocess.DEVNULL)  # Kill GDB wrappers
    time.sleep(2)  # Give kernel time to release sockets from TIME_WAIT

def generate_config(num_nodes):
    print(f"Generating {CONF_FILE} for {num_nodes} nodes...")
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
    with open(CONF_FILE, "w") as f:
        for i in range(num_nodes):
            raft_port = 50001 + (i * 2)
            service_port = 50002 + (i * 2)
            log_path = os.path.join(BASE_DIR, f"raft_log{i}")
            db_path = os.path.join(BASE_DIR, f"testdb{i}")
            line = f"{i} 127.0.0.1:{raft_port} 127.0.0.1:{service_port} {log_path} {db_path}\n"
            f.write(line)

def clean_old_data():
    print("Cleaning up old databases and logs...")
    subprocess.call(f"rm -rf {BASE_DIR}/* raft_log*", shell=True)
    if os.path.exists("client_run.log"): os.remove("client_run.log")

def wait_for_client_init(client_proc):
    print("Waiting for client init...")
    while True:
        if os.path.exists("client_run.log"):
            with open("client_run.log", "r", errors='ignore') as f:
                if "[Execution Process]" in f.read():
                    print(">>> Client Ready!")
                    return
        if client_proc.poll() is not None:
            print("Error: Client died unexpectedly.")
            cleanup()
            sys.exit(1)
        time.sleep(0.1)

def start_server_debug(node_id):
    """Starts a server wrapped in GDB to catch segfaults."""
    log_filename = f"raft_log{node_id}"
    log_file = open(log_filename, "w")
    open_files.append(log_file)
    
    # --- GDB WRAPPER COMMAND ---
    cmd = [
        "gdb", "--batch",              # Run in batch mode (no interactive shell)
        "-ex", "run",                  # Start the program immediately
        "-ex", "set style enabled on", # (Optional) Keep colors in logs if supported
        "-ex", "echo \n*** CRASH DETECTED - BACKTRACE: ***\n",
        "-ex", "thread apply all bt",  # Print backtrace for ALL threads (crucial for C++)
        "--args",                      # All arguments after this belong to the binary
        SERVER_BIN, 
        f"--conf={CONF_FILE}", 
        f"--id={node_id}"
    ]
    
    # Launch GDB instead of the server directly
    proc = subprocess.Popen(
        cmd,
        stdout=log_file, stderr=subprocess.STDOUT
    )
    
    running_procs.append(proc)
    active_servers.append({'id': node_id, 'proc': proc})
    print(f"Started Server {node_id} (GDB PID: {proc.pid})")

def start_server(node_id):
    """Starts a server and records its metadata."""
    log_file = open(f"raft_log{node_id}", "w")
    open_files.append(log_file)
    
    proc = subprocess.Popen(
        [SERVER_BIN, f"--conf={CONF_FILE}", f"--id={node_id}"],
        stdout=log_file, stderr=subprocess.STDOUT
    )
    
    running_procs.append(proc)
    active_servers.append({'id': node_id, 'proc': proc})
    print(f"Started Server {node_id} (PID: {proc.pid})")

def main():
    force_kill_all()
    generate_config(NUM_NODES)
    
    # Build check (optional)
    if subprocess.call("CMAKE=cmake make build", shell=True) != 0: sys.exit(1)

    for count in WRITE_COUNTS:
        print(f"\n--- STARTING RUN: {count} writes ---")
        clean_old_data()

        # Start Initial
        for i in range(NUM_NODES):
            if i == LATE_NODE_A or i == LATE_NODE_B: 
                continue
            if debug:
                start_server_debug(i)
            else:
                start_server(i)

        time.sleep(5)

        # Start Client
        client_log = open("client_run.log", "w")
        open_files.append(client_log)
        client_proc = subprocess.Popen(
            [CLIENT_BIN, f"--conf={CONF_FILE}", "--id=0", "--size=4k", f"--write_num={count}"],
            stdout=client_log, stderr=subprocess.STDOUT
        )
        running_procs.append(client_proc)
        wait_for_client_init(client_proc)

        time.sleep(DELAY_SECONDS)

        # Start Late
        for node_id in [LATE_NODE_A, LATE_NODE_B]:
            if(debug):
                start_server_debug(node_id)
            else:
                start_server(node_id)

        # Wait for Client
        client_proc.wait()
        
        print("Waiting 25s for replication...")
        time.sleep(25)

        # Stop Servers and Print Exit Codes
        cleanup()

        # Test Storage
        print(">>> Running Storage Test...")
        subprocess.call([TEST_STORAGE_BIN, str(NUM_NODES)], stdout=sys.stderr)

        time.sleep(5)

if __name__ == "__main__":
    main()