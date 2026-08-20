#!/bin/bash
# deploy.sh — Deploy eAID on 4 DigitalOcean droplets (run from anywhere; paths
#             resolve relative to the repository root, not to this script)
#
# Usage:
#   ./scripts/deploy.sh setup    — install deps, sync code, build on all droplets
#   ./scripts/deploy.sh start    — generate cluster.conf and start servers
#   ./scripts/deploy.sh bench    — run bench_client locally against the cluster
#   ./scripts/deploy.sh stop     — kill servers on all droplets
#   ./scripts/deploy.sh clean    — stop + wipe data directories
#   ./scripts/deploy.sh status   — show running server processes on each droplet
#   ./scripts/deploy.sh all      — setup + start + bench (full run)

set -euo pipefail

# ============================================================
# CONFIGURATION — edit before first run
# ============================================================

# IP addresses of your four DigitalOcean droplets (in order, node 0–3)
DROPLET_IPS=(
    "146.190.141.141" 
    "134.199.231.2" 
    "144.126.219.95"
    "64.23.131.208"
)

# 5th droplet used as the benchmark client node (same region as cluster)
CLIENT_IP="64.227.96.242"

SSH_USER="root"
SSH_KEY="$HOME/.ssh/id_rsa"   # path to private key registered with DigitalOcean

REMOTE_DIR="/root/FlexRaft-Code"
DATA_DIR="/root/data"

RAFT_PORT=50001
KV_PORT=50002

# bench_client parameters
WRITE_SIZE="4k"
WRITE_NUM=1000

# ============================================================
# INTERNALS — nothing below typically needs changing
# ============================================================

NUM_NODES=${#DROPLET_IPS[@]}
# Repository root — this script lives in scripts/, everything it syncs lives one level up
LOCAL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_CONF="$LOCAL_DIR/cluster.conf"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes"
LOG_DIR="$LOCAL_DIR/deploy_logs"
mkdir -p "$LOG_DIR"

# Packages needed to build the codebase. libjerasure-dev/libgf-complete-dev are required by the
# gf16_* targets in bench/CMakeLists.txt, which fail cmake configuration when they are missing.
APT_PACKAGES="uuid-dev zlib1g-dev libbz2-dev liblz4-dev \
    libsnappy-dev libzstd-dev libgflags-dev \
    cmake librocksdb-dev libgtest-dev libisal-dev \
    libjerasure-dev libgf-complete-dev build-essential"

# Local-only artifacts that must never be pushed to the hosts
RSYNC_EXCLUDES=(
    --exclude='.git'
    --exclude='build/'
    --exclude='*.o'
    --exclude='.venv/'
    --exclude='data/'
    --exclude='results/'
    --exclude='deploy_logs/'
    --exclude='raft_log*'
    --exclude='*.log'
    --exclude='cluster.conf'
)

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[+]${NC} $*"; }
warn()    { echo -e "${YELLOW}[!]${NC} $*"; }
err()     { echo -e "${RED}[x]${NC} $*" >&2; }

# ---- helpers ------------------------------------------------

ssh_cmd() {
    local node=$1; shift
    ssh $SSH_OPTS "$SSH_USER@${DROPLET_IPS[$node]}" "$@"
}

# Run a command on all nodes in parallel, wait for all
ssh_all() {
    local pids=()
    for i in $(seq 0 $((NUM_NODES - 1))); do
        ssh_cmd "$i" "$@" > "$LOG_DIR/node${i}.log" 2>&1 &
        pids+=($!)
    done
    local failed=0
    for i in $(seq 0 $((NUM_NODES - 1))); do
        if ! wait "${pids[$i]}"; then
            err "Node $i failed — see $LOG_DIR/node${i}.log"
            failed=1
        fi
    done
    return $failed
}

# Fail fast, and loudly, before any work is backgrounded into a log file
check_prereqs() {
    local missing=0
    for tool in rsync ssh scp; do
        if ! command -v "$tool" > /dev/null 2>&1; then
            err "'$tool' is not installed locally (apt-get install -y rsync openssh-client)"
            missing=1
        fi
    done
    [[ $missing -eq 0 ]] || exit 1

    if [[ ! -f "$SSH_KEY" ]]; then
        err "SSH key not found: $SSH_KEY — set SSH_KEY at the top of this script."
        exit 1
    fi
    if [[ ! -f "$LOCAL_DIR/Makefile" ]]; then
        err "No Makefile under $LOCAL_DIR — LOCAL_DIR must point at the repository root."
        exit 1
    fi
}

check_ips() {
    for ip in "${DROPLET_IPS[@]}"; do
        if [[ "$ip" == "YOUR_DROPLET"* ]]; then
            err "Edit DROPLET_IPS in deploy.sh before running."
            exit 1
        fi
    done
}

generate_conf() {
    info "Generating cluster.conf"
    : > "$CLUSTER_CONF"
    for i in $(seq 0 $((NUM_NODES - 1))); do
        local ip="${DROPLET_IPS[$i]}"
        echo "$i ${ip}:${RAFT_PORT} ${ip}:${KV_PORT} ${DATA_DIR}/raft_log${i} ${DATA_DIR}/testdb${i}" \
            >> "$CLUSTER_CONF"
    done
    cat "$CLUSTER_CONF"
}

# ---- commands -----------------------------------------------

cmd_setup() {
    check_ips
    check_prereqs
    info "Setting up $NUM_NODES droplets in parallel..."

    local pids=()
    for i in $(seq 0 $((NUM_NODES - 1))); do
        {
            local ip="${DROPLET_IPS[$i]}"
            echo "  [node $i] installing system packages..."
            ssh_cmd "$i" "apt-get update -qq && \
                apt-get install -y -qq $APT_PACKAGES 2>&1 | tail -5"

            echo "  [node $i] syncing codebase..."
            rsync -az --delete \
                -e "ssh $SSH_OPTS" \
                "${RSYNC_EXCLUDES[@]}" \
                "$LOCAL_DIR/" \
                "$SSH_USER@$ip:$REMOTE_DIR/"

            echo "  [node $i] building (release)..."
            ssh_cmd "$i" "cd $REMOTE_DIR && CMAKE=cmake make release 2>&1 | tail -5"

            echo "  [node $i] done."
        } > "$LOG_DIR/setup_node${i}.log" 2>&1 &
        pids+=($!)
    done

    local failed=0
    for i in $(seq 0 $((NUM_NODES - 1))); do
        if wait "${pids[$i]}"; then
            info "Node $i setup complete"
        else
            err "Node $i setup FAILED — see $LOG_DIR/setup_node${i}.log"
            failed=1
        fi
    done

    if [[ $failed -ne 0 ]]; then
        err "One or more nodes failed during setup."
        exit 1
    fi
    info "All nodes ready."

    info "Setting up client node ($CLIENT_IP)..."
    {
        ssh $SSH_OPTS "$SSH_USER@$CLIENT_IP" "apt-get update -qq && \
            apt-get install -y -qq $APT_PACKAGES 2>&1 | tail -5"
        rsync -az --delete \
            -e "ssh $SSH_OPTS" \
            "${RSYNC_EXCLUDES[@]}" \
            "$LOCAL_DIR/" \
            "$SSH_USER@$CLIENT_IP:$REMOTE_DIR/"
        ssh $SSH_OPTS "$SSH_USER@$CLIENT_IP" "cd $REMOTE_DIR && CMAKE=cmake make release 2>&1 | tail -5"
    } > "$LOG_DIR/setup_client.log" 2>&1 && info "Client node ready." || err "Client node setup FAILED — see $LOG_DIR/setup_client.log"
}

cmd_start() {
    check_ips
    generate_conf

    info "Distributing cluster.conf to all nodes..."
    local pids=()
    for i in $(seq 0 $((NUM_NODES - 1))); do
        scp $SSH_OPTS "$CLUSTER_CONF" \
            "$SSH_USER@${DROPLET_IPS[$i]}:$REMOTE_DIR/cluster.conf" \
            > /dev/null &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p"; done

    info "Starting servers..."
    pids=()
    for i in $(seq 0 $((NUM_NODES - 1))); do
        local script="$LOG_DIR/start_node${i}.sh"
        cat > "$script" << EOF
#!/bin/bash
ulimit -n 65536
mkdir -p ${DATA_DIR}/raft_log${i} ${DATA_DIR}/testdb${i}
pkill -f bench_server 2>/dev/null || true
sleep 0.5
cd ${REMOTE_DIR}
nohup build/bench/bench_server --conf=cluster.conf --id=${i} </dev/null >${DATA_DIR}/server${i}.log 2>&1 &
echo \$!
EOF
        scp $SSH_OPTS "$script" "$SSH_USER@${DROPLET_IPS[$i]}:/tmp/start_node${i}.sh" > /dev/null
        ssh_cmd "$i" "bash /tmp/start_node${i}.sh" > "$LOG_DIR/start_node${i}.log" 2>&1 &
        pids+=($!)
    done
    for i in $(seq 0 $((NUM_NODES - 1))); do
        if ! wait "${pids[$i]}"; then
            err "Node $i start failed — log:"
            cat "$LOG_DIR/start_node${i}.log" >&2
        fi
    done

    info "Waiting 3 seconds for cluster to form..."
    sleep 3
    cmd_status
}

cmd_bench() {
    check_ips
    generate_conf

    local summary="$LOG_DIR/deployResults.txt"
    : > "$summary"

    info "Pushing cluster.conf to all nodes and client..."
    local pids=()
    for i in $(seq 0 $((NUM_NODES - 1))); do
        scp $SSH_OPTS "$CLUSTER_CONF" \
            "$SSH_USER@${DROPLET_IPS[$i]}:$REMOTE_DIR/cluster.conf" > /dev/null &
        pids+=($!)
    done
    scp $SSH_OPTS "$CLUSTER_CONF" "$SSH_USER@$CLIENT_IP:$REMOTE_DIR/cluster.conf" > /dev/null &
    pids+=($!)
    for p in "${pids[@]}"; do wait "$p"; done

    for clients in 1 2 3 4 5 6 7; do
        info "============================================"
        info "RUN: $clients concurrent client(s)"
        info "============================================"

        # 1. Clean data on all nodes
        info "Cleaning data on all nodes..."
        local cpids=()
        for i in $(seq 0 $((NUM_NODES - 1))); do
            ssh_cmd "$i" "rm -rf ${DATA_DIR}/raft_log${i} ${DATA_DIR}/testdb${i} ${DATA_DIR}/server${i}.log && mkdir -p ${DATA_DIR}" &
            cpids+=($!)
        done
        for p in "${cpids[@]}"; do wait "$p" || true; done

        # 2. Start fresh servers on all nodes
        info "Starting fresh servers..."
        local spids=()
        for i in $(seq 0 $((NUM_NODES - 1))); do
            local script="$LOG_DIR/start_node${i}.sh"
            cat > "$script" << EOF
#!/bin/bash
ulimit -n 65536
mkdir -p ${DATA_DIR}
pkill -f bench_server 2>/dev/null || true
sleep 0.5
cd ${REMOTE_DIR}
nohup build/bench/bench_server --conf=cluster.conf --id=${i} </dev/null >${DATA_DIR}/server${i}.log 2>&1 &
echo \$!
EOF
            scp $SSH_OPTS "$script" "$SSH_USER@${DROPLET_IPS[$i]}:/tmp/start_node${i}.sh" > /dev/null
            ssh_cmd "$i" "bash /tmp/start_node${i}.sh" > "$LOG_DIR/start_node${i}.log" 2>&1 &
            spids+=($!)
        done
        for p in "${spids[@]}"; do wait "$p" || true; done

        # 3. Wait for cluster to stabilize
        info "Waiting 10 seconds for cluster to stabilize..."
        sleep 10

        # 4. Run N clients concurrently on client node
        local script="$LOG_DIR/bench_c${clients}.sh"
        cat > "$script" << EOF
#!/bin/bash
pids=()
for id in \$(seq 0 $((clients - 1))); do
    $REMOTE_DIR/build/bench/bench_client \
        --conf=$REMOTE_DIR/cluster.conf \
        --id=\$id \
        --size=$WRITE_SIZE \
        --write_num=$WRITE_NUM > /tmp/bench_c${clients}_id\${id}.log 2>&1 &
    pids+=(\$!)
done
for p in "\${pids[@]}"; do wait "\$p" || true; done
EOF
        info "Running $clients client(s) on $CLIENT_IP..."
        scp $SSH_OPTS "$script" "$SSH_USER@$CLIENT_IP:/tmp/bench_c${clients}.sh" > /dev/null
        ssh $SSH_OPTS "$SSH_USER@$CLIENT_IP" "bash /tmp/bench_c${clients}.sh"

        # 5. Collect results
        echo "[clients=$clients]" >> "$summary"
        for id in $(seq 0 $((clients - 1))); do
            scp $SSH_OPTS \
                "$SSH_USER@$CLIENT_IP:/tmp/bench_c${clients}_id${id}.log" \
                "$LOG_DIR/bench_c${clients}_id${id}.log" > /dev/null 2>&1 || true
            grep -E "\[Results\]|\[Client" "$LOG_DIR/bench_c${clients}_id${id}.log" \
                | sed "s/^/  [id=$id] /" >> "$summary" || true
        done
        echo "" >> "$summary"

        # 6. Stop servers before next run
        info "Stopping servers..."
        local kpids=()
        for i in $(seq 0 $((NUM_NODES - 1))); do
            ssh_cmd "$i" "pkill -f bench_server 2>/dev/null || true" &
            kpids+=($!)
        done
        for p in "${kpids[@]}"; do wait "$p" || true; done
        sleep 2
    done

    info "All runs complete. Summary -> $summary"
    cat "$summary"
}

cmd_stop() {
    check_ips
    info "Stopping servers on all nodes..."
    local pids=()
    for i in $(seq 0 $((NUM_NODES - 1))); do
        ssh_cmd "$i" "pkill -f bench_server 2>/dev/null || true" &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || true; done
    info "Done."
}

cmd_clean() {
    cmd_stop
    warn "Wiping data directories on all nodes..."
    local pids=()
    for i in $(seq 0 $((NUM_NODES - 1))); do
        ssh_cmd "$i" "rm -rf ${DATA_DIR}/raft_log${i} ${DATA_DIR}/testdb${i} ${DATA_DIR}/server${i}.log" &
        pids+=($!)
    done
    for p in "${pids[@]}"; do wait "$p" || true; done
    rm -f "$CLUSTER_CONF"
    info "Cluster data wiped."
}

cmd_status() {
    check_ips
    echo ""
    printf "%-6s %-20s %s\n" "Node" "IP" "Status"
    printf "%-6s %-20s %s\n" "----" "------------------" "------"
    for i in $(seq 0 $((NUM_NODES - 1))); do
        local ip="${DROPLET_IPS[$i]}"
        local result
        if result=$(ssh_cmd "$i" "pgrep -a bench_server 2>/dev/null" 2>/dev/null); then
            printf "%-6s %-20s ${GREEN}RUNNING${NC} (pid $(echo "$result" | awk '{print $1}'))\n" "$i" "$ip"
        else
            printf "%-6s %-20s ${RED}STOPPED${NC}\n" "$i" "$ip"
        fi
    done
    echo ""
}

cmd_all() {
    cmd_setup
    cmd_start
    cmd_bench
}

usage() {
    echo "Usage: $0 {setup|start|bench|stop|clean|status|all}"
    echo ""
    echo "  setup   — install deps, sync code, build on all droplets"
    echo "  start   — generate cluster.conf and start servers"
    echo "  bench   — run bench_client locally against the running cluster"
    echo "  stop    — kill servers on all droplets"
    echo "  clean   — stop servers and wipe data directories"
    echo "  status  — show server process state on each droplet"
    echo "  all     — setup + start + bench"
}

# ---- dispatch -----------------------------------------------

case "${1:-}" in
    setup)  cmd_setup  ;;
    start)  cmd_start  ;;
    bench)  cmd_bench  ;;
    stop)   cmd_stop   ;;
    clean)  cmd_clean  ;;
    status) cmd_status ;;
    all)    cmd_all    ;;
    *)      usage; exit 1 ;;
esac
