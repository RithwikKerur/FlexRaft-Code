# eAID: Adaptive Erasure Coding for Consensus

**eAID** is an information dispersal scheme integrated with a Raft backed KV store that picks its erasure-coding scheme *per log entry*, and then
*shrinks what it stored* once it knows who actually answered. Instead of committing to one
coding parameter up front, the leader:

1. **Disseminates optimistically** — each follower gets a shard sized by the leader's current
   belief about how many nodes are responsive (starting at 2 fragments per node), and the entry
   commits at a **3N/4** acknowledgement threshold rather than a bare majority.
2. **Re-encodes on timeout** — if the coding round does not gather enough acks before
   `ec_timeout`, the leader recomputes the fragment count from the responses it *did* get
   (`t = responses − F`, `fragments = max(2, ⌈F/t⌉)`) and sends only the delta.
3. **Prunes after commit** — once an entry is durable, shards are trimmed to
   `fragment_size × (N / responders)`, so the steady-state storage cost tracks the cluster's
   real liveness instead of its worst case.

The result is that latency degrades gracefully as nodes slow down or drop out.

This repository is built on the **FlexRaft** codebase from the *ICPP'23* paper
**Minimizing Network and Storage Costs for Consensus with Flexible Erasure Coding**, and retains
its Raft core, RocksDB state machine, RCF transport and benchmarking harness.

---

## Repository layout

| Path | Contents |
| --- | --- |
| [raft/](raft/) | Raft core: consensus state machine, log manager, ISA-L encoder, RCF RPC transport |
| [kv/](kv/) | RocksDB-backed key-value state machine layered on Raft |
| [bench/](bench/) | `bench_server` / `bench_client`, YCSB driver, storage and coding microbenchmarks |
| [scripts/](scripts/) | Dependency install, multi-node deployment, protocol simulator |
| [exp/](exp/) | Original FlexRaft ICPP'23 experiment scripts — see [exp/README.md](exp/README.md) |
| [scripts/deploy.sh](scripts/deploy.sh) | One-command deploy + benchmark across a remote cluster |
| [scripts/eAID_network_sim.py](scripts/eAID_network_sim.py) | Discrete-event simulator comparing eAID against three baselines |


## Building

### Prerequisites

* **Build system:** *cmake* >= 3.8
* **Compiler:** *g++* >= 10.2.1
* **Platform:** tested on Ubuntu 20.04+ and CentOS-7 (use devtoolset-10 on CentOS-7)

### Dependencies

The core dependencies are [RocksDB](https://github.com/facebook/rocksdb) and
[Intel isa-l](https://github.com/intel/isa-l). On a machine with network access and root:

```bash
python3 scripts/install_dependencies.py
```

On Debian/Ubuntu the equivalent packages can be installed directly:

```bash
sudo apt-get install -y \
    uuid-dev zlib1g-dev libbz2-dev liblz4-dev libsnappy-dev libzstd-dev \
    libgflags-dev cmake librocksdb-dev libgtest-dev libisal-dev build-essential
```

> **Note:** the coding microbenchmarks in [bench/CMakeLists.txt](bench/CMakeLists.txt) (`gf16_bench`,
> `gf16_incremental_bench`) hard-fail cmake configuration if Jerasure is missing, so a full build
> also needs `sudo apt-get install -y libjerasure-dev libgf-complete-dev`. Drop those targets from
> `bench/CMakeLists.txt` if you do not want the dependency. `scripts/deploy.sh setup` installs
> them on the remote hosts for you.

### Build

```bash
CMAKE=cmake make build     # Debug build
CMAKE=cmake make release   # Release build (used by deploy.sh)
CMAKE=cmake make log       # Debug build with verbose Raft logging (-DLOG=on)
```

Binaries land in `build/bench/`. `make clean` removes the build tree; `make format` runs
clang-format over `raft/`, `kv/` and `bench/`.

---

## Running a cluster by hand

### Cluster configuration file

One line per server:

```bash
<node id> <ip:raft_port> <ip:kv_port> <log path> <db path>
```

* **node id** — integer identifier of a Raft server
* **ip:raft_port** — address used for Raft-internal communication
* **ip:kv_port** — address used for KV service communication
* **log path** — directory for the Raft log
* **db path** — RocksDB directory used as the state machine

[example.conf](example.conf) describes a 5-node cluster on loopback:

```bash
0 127.0.0.1:50001 127.0.0.1:50002 /home/rkerur/FlexRaft-Code/data/raft_log0 /home/rkerur/FlexRaft-Code/data/testdb0
1 127.0.0.1:50003 127.0.0.1:50004 /home/rkerur/FlexRaft-Code/data/raft_log1 /home/rkerur/FlexRaft-Code/data/testdb1
2 127.0.0.1:50005 127.0.0.1:50006 /home/rkerur/FlexRaft-Code/data/raft_log2 /home/rkerur/FlexRaft-Code/data/testdb2
3 127.0.0.1:50007 127.0.0.1:50008 /home/rkerur/FlexRaft-Code/data/raft_log3 /home/rkerur/FlexRaft-Code/data/testdb3
4 127.0.0.1:50009 127.0.0.1:50010 /home/rkerur/FlexRaft-Code/data/raft_log4 /home/rkerur/FlexRaft-Code/data/testdb4
```

Adjust the paths to your checkout before using it.

### Start the servers

Run once per node id, on the corresponding host:

```bash
build/bench/bench_server --conf=example.conf --id=0
```

### Start a client

From a machine that is not running a server:

```bash
build/bench/bench_client --conf=example.conf --id=0 --size=4k --write_num=1000
```

* `--conf` — path to the configuration file
* `--id` — client identifier (only used to keep concurrent clients distinct)
* `--size` — payload size per key-value pair, e.g. `4k`, `2M`
* `--write_num` — number of *Put* requests to issue

The client prints a results line on completion:

```bash
[Results][Succ Cnt=10000][Average Latency = 60431 us][Average Commit Latency = 36181 us][Average Apply Latency = 3473]
```

`Average Commit Latency` is the consensus round; `Average Apply Latency` is the state machine.

---

## Multi-node deployment — `scripts/deploy.sh`

[scripts/deploy.sh](scripts/deploy.sh) drives a real cluster over SSH: it provisions the hosts,
syncs the checkout, builds, generates `cluster.conf`, runs a client sweep and collects results.
It assumes **4 cluster droplets plus 1 dedicated client droplet**.

### Configure it first

Edit the configuration block at the top of the script:

| Variable | Meaning |
| --- | --- |
| `DROPLET_IPS` | Addresses of the cluster nodes, in order — array index becomes the Raft node id |
| `CLIENT_IP` | Address of the separate benchmark client host |
| `SSH_USER` | SSH login (default `root`) |
| `SSH_KEY` | Private key registered with the hosts (default `~/.ssh/id_rsa`) |
| `REMOTE_DIR` | Where the checkout is synced on each host (default `/root/FlexRaft-Code`) |
| `DATA_DIR` | Where Raft logs, RocksDB data and server stdout land (default `/root/data`) |
| `RAFT_PORT` / `KV_PORT` | Ports written into the generated `cluster.conf` |
| `WRITE_SIZE` / `WRITE_NUM` | `bench_client` payload size and request count per client |

The number of nodes is inferred from `DROPLET_IPS`, so adding or removing entries resizes the
cluster. The hosts need to reach each other on `RAFT_PORT` and `KV_PORT`, and passwordless SSH
(`BatchMode=yes`) must work to every one of them.

`setup` runs a preflight check first and aborts with a clear message if `rsync`, `ssh` or `scp`
is missing locally, if `SSH_KEY` does not exist, or if `LOCAL_DIR` is not the repository root —
these would otherwise fail silently inside a backgrounded subshell whose output goes to a log
file. Paths resolve relative to the repository root regardless of where you invoke the script
from, and the sync skips local-only artifacts (`.git/`, `build/`, `data/`, `results/`, `.venv/`,
logs) so only source is pushed.

### Commands

```bash
./scripts/deploy.sh <command>
```

| Command | Effect |
| --- | --- |
| `setup` | apt-installs dependencies, rsyncs the checkout, and runs `make release` on every cluster host *and* the client host, all in parallel |
| `start` | Generates `cluster.conf`, pushes it everywhere, launches `bench_server` under `nohup` on each node, waits 3s, then prints status |
| `bench` | Full sweep: for 1–7 concurrent clients, wipes data, restarts the cluster, waits 10s to stabilize, runs that many `bench_client` processes on the client host, and collects results |
| `stop` | `pkill -f bench_server` on every cluster host |
| `clean` | `stop`, then wipes the per-node data directories and removes the local `cluster.conf` |
| `status` | Prints a RUNNING/STOPPED table with pids for each node |
| `all` | `setup` → `start` → `bench` |

### Typical workflow

```bash
# one time, or after changing dependencies
./scripts/deploy.sh setup

# after every code change: re-sync + rebuild, then sweep
./scripts/deploy.sh setup
./scripts/deploy.sh bench

# inspect and tear down
./scripts/deploy.sh status
./scripts/deploy.sh clean
```

Note that `bench` manages the cluster lifecycle itself — it restarts servers before each client
count — so `start` is only needed for interactive poking at a live cluster.

### Outputs

Everything is written under `deploy_logs/` at the repository root:

| File | Contents |
| --- | --- |
| `deployResults.txt` | The summary: one `[clients=N]` block per run with each client's `[Results]` line |
| `setup_node<i>.log` | Per-node package install, rsync and build output from `setup` |
| `setup_client.log` | Same, for the client host |
| `start_node<i>.log` | Server launch output |
| `bench_c<N>_id<M>.log` | Raw `bench_client` output, per run and per client |
| `node<i>.log` | Output of ad-hoc parallel SSH commands |

Server stdout stays on the remote hosts at `$DATA_DIR/server<i>.log` — useful when a node fails
to join, since the launch script itself only reports the pid.

---

## Protocol simulator — `scripts/eAID_network_sim.py`

[scripts/eAID_network_sim.py](scripts/eAID_network_sim.py) is a self-contained discrete-event
simulator used to compare eAID's commit latency and bandwidth against three baselines, without
needing hardware. It models an `N = 2F+1` cluster where each node has a per-entry
acknowledgement delay drawn from a fixed seed, so every protocol sees the *same* network.

### What it compares

| Simulator class | Label in output | Behaviour |
| --- | --- | --- |
| `eAID` | `eAID` | Adaptive fragment count, 3N/4 commit threshold, re-encode on timeout, post-commit shard pruning |
| `CRaftLeader` | `Full Replication Fallback` | Erasure-code first, resend the full log entry when the coded round times out |
| `HRaft` | `Endangered Fragment Resharing` | Redistributes fragments that become under-replicated |
| `FlexRaftLeader` | `Proactive Encoding` (CSV) / `FlexRaft-ID` (plot) | Picks `k` from the leader's perception of live nodes, re-encodes when that perception is wrong |

`SmartRafture` is also implemented but is commented out of the comparison.

### Requirements

Python 3 plus `matplotlib`, `pandas` and `numpy`:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install matplotlib pandas numpy
```

### Run it

```bash
python3 scripts/eAID_network_sim.py
```

The script writes its output files into the **current working directory**, so run it from a
scratch directory if you do not want them in the repository root:

```bash
mkdir -p results/sim && cd results/sim
python3 ../../scripts/eAID_network_sim.py
```

On a headless machine, set a non-interactive backend so the final `plt.show()` does not block:

```bash
MPLBACKEND=Agg python3 scripts/eAID_network_sim.py
```
