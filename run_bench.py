import subprocess
import time
import re
import sys
import atexit

# --- CONFIGURATION ---
NUM_SERVERS = 5
BASE_DIR = "/Users/rithwikkerur/Documents/UCSB/data"
BUILD_CMD = "CMAKE=cmake make build"
SERVER_BIN = "build/bench/bench_server"
CLIENT_BIN = "build/bench/bench_client"
CONF_FILE = "example.conf"

server_procs = {}

def cleanup():
    print("\n[Script] Cleaning up...")
    for s_id, proc in server_procs.items():
        if proc.poll() is None:
            proc.terminate()
            proc.wait()
    print("[Script] All servers stopped.")

def run_command(cmd):
    ret = subprocess.call(cmd, shell=True)
    if ret != 0:
        print(f"[Script] Error: Command failed: {cmd}")
        sys.exit(1)

atexit.register(cleanup)

def main():
    # 1. Clean & Build
    run_command(f"rm -rf {BASE_DIR}/testdb* {BASE_DIR}/raft_log*")
    run_command(BUILD_CMD)
    #run_command("rm output.txt")

    # 2. Start Servers
    print(f"[Script] Starting {NUM_SERVERS} servers...")
    for i in range(NUM_SERVERS):
        log_file = open(f"raft_log{i}", "w")
        cmd = [SERVER_BIN, f"--conf={CONF_FILE}", f"--id={i}"]
        server_procs[i] = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)

    print("[Script] Waiting 3 seconds for cluster to form...")
    time.sleep(3)

    # 3. Start Client
    print("[Script] Starting Client...")
    client_cmd = [CLIENT_BIN, f"--conf={CONF_FILE}", "--id=0", "--size=4k", "--write_num=1000"]
    client_proc = subprocess.Popen(client_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    leader_regex = re.compile(r"to S(\d+)")
    leader_crashed = False

    try:
        while True:
            line = client_proc.stdout.readline()
            if not line and client_proc.poll() is not None:
                break
            
            if line:
                print(line.strip())

                # DETECT LEADER
                if not leader_crashed:
                    match = leader_regex.search(line)
                    if match:
                        leader_id = int(match.group(1))
                        print(f"\n[Script] >>> FOUND LEADER S{leader_id}. Waiting 3 seconds to crash... <<<\n")
                        
                        # --- THE FIX: READ LOGS WHILE WAITING ---
                        # We loop for 3 seconds, reading logs so the client doesn't freeze.
                        end_time = time.time() + 1
                        while time.time() < end_time:
                            # Try to read more lines while waiting
                            # Note: This might block slightly, but keeps flow moving
                            drain_line = client_proc.stdout.readline()
                            if drain_line:
                                print(drain_line.strip())
                        # ----------------------------------------

                        print(f"\n[Script] >>> CRASHING S{leader_id} NOW! <<<\n")
                        if leader_id in server_procs:
                            server_procs[leader_id].terminate()
                        leader_crashed = True

    except KeyboardInterrupt:
        print("\n[Script] Interrupted.")

    client_proc.wait()
    print("[Script] Benchmark finished.")

if __name__ == "__main__":
    main()