import subprocess
import time
import os
import sys
import shutil
import signal

# --- Configuration ---
YCSB_TYPES    = ["YCSB_A", "YCSB_B", "YCSB_C"]
CLIENT_NUMS   = [1]
OP_COUNT      = 1000
VALUE_SIZE    = "512"

BASE_DIR      = "/Users/rithwikkerur/Documents/UCSB/data"
CONF_FILE     = "example.conf"
BUILD_DIR     = "build/bench"
SERVER_BIN    = os.path.join(BUILD_DIR, "ycsb_server")
CLIENT_BIN    = os.path.join(BUILD_DIR, "ycsb_client")

RESULTS_FILE  = "ycsb_results.txt"
CLIENT_LOG    = "ycsb_client.log"

running_procs = []
open_files    = []

def cleanup(signum=None, frame=None):
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

def clean_old_data():
    print("Cleaning up old databases and logs...")
    for i in range(5):
        db_path = os.path.join(BASE_DIR, f"testdb{i}")
        if os.path.exists(db_path):
            shutil.rmtree(db_path) if os.path.isdir(db_path) else os.remove(db_path)
        for log_path in [f"raft_log{i}", os.path.join(BASE_DIR, f"raft_log{i}")]:
            if os.path.exists(log_path):
                shutil.rmtree(log_path) if os.path.isdir(log_path) else os.remove(log_path)
    if os.path.exists(CLIENT_LOG):
        os.remove(CLIENT_LOG)

def wait_for_client_start(client_proc):
    print("Waiting for YCSB warmup to finish...")
    while True:
        if os.path.exists(CLIENT_LOG):
            with open(CLIENT_LOG, "r", errors="ignore") as f:
                if "Warmup is done" in f.read():
                    print(">>> Warmup done, benchmark executing!")
                    return
        if client_proc.poll() is not None:
            print(f"Error: Client process died unexpectedly (exit code {client_proc.returncode})!")
            cleanup()
            sys.exit(1)
        time.sleep(0.1)

def main():
    print("Building project...")
    if subprocess.call("CMAKE=cmake make build", shell=True) != 0:
        print("Build failed! Exiting.")
        sys.exit(1)

    results = open(RESULTS_FILE, "w")
    results.write(f"YCSB Benchmark Results\n")
    results.write(f"Value Size: {VALUE_SIZE}  Op Count: {OP_COUNT}\n")
    results.write("=" * 60 + "\n\n")

    for ycsb_type in YCSB_TYPES:
        for client_num in CLIENT_NUMS:
            print(f"\n{'='*56}")
            print(f"STARTING RUN: type={ycsb_type}  clients={client_num}  ops={OP_COUNT}")
            print(f"{'='*56}")

            clean_old_data()

            print("Starting 4 servers...")
            for i in range(4):
                log_file = open(f"raft_log{i}", "w")
                open_files.append(log_file)
                proc = subprocess.Popen(
                    [SERVER_BIN, f"--conf={CONF_FILE}", f"--id={i}"],
                    stdout=log_file, stderr=subprocess.STDOUT
                )
                running_procs.append(proc)
                print(f"  Started Server {i} (PID: {proc.pid})")

            print("Waiting 5 seconds for cluster to stabilize...")
            time.sleep(5)

            print(f"Starting YCSB client (type={ycsb_type}, clients={client_num})...")
            client_log = open(CLIENT_LOG, "w")
            open_files.append(client_log)
            client_proc = subprocess.Popen(
                [CLIENT_BIN,
                 f"--conf={CONF_FILE}",
                 f"--client_num={client_num}",
                 f"--size={VALUE_SIZE}",
                 f"--op_count={OP_COUNT}",
                 f"--type={ycsb_type}"],
                stdout=client_log, stderr=sys.stderr
            )
            running_procs.append(client_proc)

            wait_for_client_start(client_proc)

            print("Waiting for client to finish...")
            exit_code = client_proc.wait()
            print(f"Client finished (exit code {exit_code})")

            # Extract results from log
            results.write(f"[{ycsb_type}] clients={client_num}\n")
            if os.path.exists(CLIENT_LOG):
                with open(CLIENT_LOG, "r", errors="ignore") as f:
                    for line in f:
                        if "Throughput" in line or "Results" in line or "Error" in line:
                            results.write(f"  {line.rstrip()}\n")
            results.write("\n")
            results.flush()

            cleanup()
            time.sleep(3)

    results.close()
    print(f"\nAll runs complete. Results written to {RESULTS_FILE}")

if __name__ == "__main__":
    main()
