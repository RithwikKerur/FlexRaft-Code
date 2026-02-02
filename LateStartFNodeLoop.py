import subprocess
import time
import os
import sys
import shutil
import signal
import socket

# --- Configuration ---
NUM_NODES = 11         # CHANGE THIS: Total size of cluster (e.g., 5, 7, 9)
NUM_LATE_NODES = (NUM_NODES - 1) // 2 # Automatically sets F to max allowable (F < N/2)
DEBUG_GDB = True      # Set to True to run servers in GDB (prints backtrace on crash)

print(f"Configuration: N={NUM_NODES}, Late Nodes (F)={NUM_LATE_NODES}")

# Automatically pick the last F nodes to be late
LATE_NODE_IDS = list(range(NUM_NODES - NUM_LATE_NODES, NUM_NODES))

WRITE_COUNTS = [8000]
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

def check_port_status(ports=None, verbose=True):
    """Check which ports are in use and return list of blocked ports.

    Uses SO_REUSEADDR to match RCF's behavior - TIME_WAIT sockets won't block.
    """
    if ports is None:
        ports = range(50001, 50023)

    blocked = []
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.1)
        # Use SO_REUSEADDR to match what RCF does - this allows binding even if
        # a socket is in TIME_WAIT state from a recently closed connection
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('127.0.0.1', port))
            sock.close()
        except OSError as e:
            blocked.append((port, str(e)))
            if verbose:
                # Try to find what's using the port
                try:
                    result = subprocess.run(
                        ["lsof", "-i", f":{port}"],
                        capture_output=True, text=True, timeout=2
                    )
                    proc_info = result.stdout.strip().split('\n')[1:] if result.stdout else ["Unknown"]
                    if proc_info and proc_info[0] != "Unknown":
                        print(f"  [DEBUG] Port {port} BLOCKED by process: {proc_info[0]}")
                    else:
                        # Check if it's in TIME_WAIT using ss
                        ss_result = subprocess.run(
                            ["ss", "-tan", f"sport = :{port}"],
                            capture_output=True, text=True, timeout=2
                        )
                        if "TIME-WAIT" in ss_result.stdout:
                            print(f"  [DEBUG] Port {port} in TIME_WAIT (should be reclaimable with SO_REUSEADDR)")
                        else:
                            print(f"  [DEBUG] Port {port} BLOCKED: {e}")
                except:
                    print(f"  [DEBUG] Port {port} BLOCKED: {e}")
        finally:
            try:
                sock.close()
            except:
                pass

    if verbose:
        if blocked:
            print(f"[DEBUG] {len(blocked)} ports blocked: {[p[0] for p in blocked]}")
        else:
            print("[DEBUG] All ports are FREE")

    return blocked

def force_kill_all():
    """Kills all server instances and ensures ports are free."""
    print("Pre-flight check: Killing any lingering server instances...")

    # Check ports BEFORE killing
    print("[DEBUG] Port status BEFORE cleanup:")
    blocked_before = check_port_status()

    try:
        # Kill by name patterns
        subprocess.call(["pkill", "-9", "-f", "bench_server"], stderr=subprocess.DEVNULL)
        subprocess.call(["pkill", "-9", "-f", "bench_client"], stderr=subprocess.DEVNULL)
        subprocess.call(["pkill", "-9", "gdb"], stderr=subprocess.DEVNULL)

        # Also kill by port - more reliable
        for port in range(50001, 50023):
            subprocess.call(["fuser", "-k", f"{port}/tcp"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        time.sleep(2)  # Short wait

        # Check ports AFTER killing
        print("[DEBUG] Port status AFTER cleanup (attempt 1):")
        blocked_after = check_port_status()

        if blocked_after:
            print(f"[DEBUG] Still {len(blocked_after)} ports blocked, waiting 5 more seconds...")
            time.sleep(5)
            print("[DEBUG] Port status AFTER extended wait:")
            blocked_final = check_port_status()

            if blocked_final:
                print(f"[ERROR] Cannot free ports: {[p[0] for p in blocked_final]}")
                print("[DEBUG] Attempting aggressive cleanup with fuser...")
                for port, _ in blocked_final:
                    subprocess.call(["fuser", "-k", "-9", f"{port}/tcp"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(2)
                print("[DEBUG] Port status AFTER aggressive cleanup:")
                check_port_status()

    except Exception as e:
        print(f"Warning: cleanup failed ({e})")

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

def build_server_cmd(server_bin, conf_file, node_id):
    """Builds the command to run a server, optionally wrapped in GDB."""
    base_cmd = [server_bin, f"--conf={conf_file}", f"--id={node_id}"]
    if DEBUG_GDB:
        # Run in GDB with auto-run and backtrace on crash
        return ["gdb", "-batch", "-ex", "run", "-ex", "bt", "--args"] + base_cmd
    return base_cmd

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
        print(f"Starting {NUM_NODES - NUM_LATE_NODES} initial servers (Nodes not in {LATE_NODE_IDS})...")

        # Debug: Check ports before starting servers
        print("[DEBUG] Port status BEFORE starting initial servers:")
        blocked = check_port_status()
        if blocked:
            print(f"[WARNING] {len(blocked)} ports still blocked! Server startup may fail.")

        for i in range(NUM_NODES):
            if i in LATE_NODE_IDS:
                continue

            raft_port = 50001 + (i * 2)
            svc_port = 50002 + (i * 2)

            # Check THIS server's ports right before starting
            blocked = check_port_status([raft_port, svc_port], verbose=False)
            if blocked:
                print(f"[ERROR] Server {i} ports {[p[0] for p in blocked]} blocked!")
                check_port_status([raft_port, svc_port], verbose=True)
                time.sleep(2)

            log_file = open(f"raft_log{i}", "w")
            open_files.append(log_file)

            cmd = build_server_cmd(SERVER_BIN, CONF_FILE, i)
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
            running_procs.append(proc)
            print(f"Started Server {i} (PID: {proc.pid}) on ports {raft_port},{svc_port}{' [GDB]' if DEBUG_GDB else ''}")

            # Small delay between server starts to avoid race conditions
            time.sleep(0.3)

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
        # Debug: Check ports for late servers before starting
        late_ports = []
        for node_id in LATE_NODE_IDS:
            raft_port = 50001 + (node_id * 2)
            service_port = 50002 + (node_id * 2)
            late_ports.extend([raft_port, service_port])
        print(f"[DEBUG] Port status for late servers (ports {late_ports}):")
        check_port_status(late_ports)

        # Extra debug: Show ALL socket states for these ports
        print("[DEBUG] Full socket state for late server ports:")
        for port in late_ports:
            result = subprocess.run(
                f"ss -tan state all 'sport = :{port} or dport = :{port}' 2>/dev/null | head -5",
                shell=True, capture_output=True, text=True
            )
            if result.stdout.strip():
                print(f"  Port {port}:\n{result.stdout}")
            else:
                print(f"  Port {port}: No sockets")

        for node_id in LATE_NODE_IDS:
            raft_port = 50001 + (node_id * 2)
            svc_port = 50002 + (node_id * 2)

            # Check THIS server's ports right before starting
            print(f"[DEBUG] Checking ports {raft_port},{svc_port} for Server {node_id}...")
            blocked = check_port_status([raft_port, svc_port], verbose=True)
            if blocked:
                print(f"[WARNING] Server {node_id} ports may be blocked, but attempting to start anyway...")
                print(f"         (RCF with SO_REUSEADDR can often bind even with TIME_WAIT sockets)")
                time.sleep(1)  # Brief pause

            print(f"Starting Late Server {node_id}...")
            log_file = open(f"raft_log{node_id}", "w")
            open_files.append(log_file)

            cmd = build_server_cmd(SERVER_BIN, CONF_FILE, node_id)
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
            running_procs.append(proc)
            print(f"Started Late Server {node_id} (PID: {proc.pid}) on ports {raft_port},{svc_port}{' [GDB]' if DEBUG_GDB else ''}")

            # Give server a moment to bind its ports before starting next one
            time.sleep(0.5)

        # --- WAIT for Client to Finish ---
        print(f"Waiting for client to finish writing {count} entries...")
        exit_code = client_proc.wait() 
        print(f"Client finished with exit code {exit_code}")

        # --- FIX: Give late nodes time to catch up! ---
        print("Waiting 15 seconds for late nodes to replicate data...")
        time.sleep(5) 

        # --- Kill Servers ---
        cleanup()

        # --- Run Storage Test ---
        print(f">>> Running Storage Test (Post-{count} entries)...", file=sys.stderr)
        subprocess.call([TEST_STORAGE_BIN, str(NUM_NODES)], stdout=sys.stderr, stderr=sys.stderr)
        print(">>> Storage Test Finished.", file=sys.stderr)

        time.sleep(25)

    print("All benchmark runs completed.")

if __name__ == "__main__":
    main()