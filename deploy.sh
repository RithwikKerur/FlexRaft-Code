#!/bin/bash
# deploy.sh — Deploy FlexRaft on 4 DigitalOcean droplets
#
# Usage:
#   ./deploy.sh setup    — install deps, sync code, build on all droplets
#   ./deploy.sh start    — generate cluster.conf and start servers
#   ./deploy.sh bench    — run bench_client locally against the cluster
#   ./deploy.sh stop     — kill servers on all droplets
#   ./deploy.sh clean    — stop + wipe data directories
#   ./deploy.sh status   — show running server processes on each droplet
#   ./deploy.sh all      — setup + start + bench (full run)

set -euo pipefail

# ============================================================
# CONFIGURATION — edit before first run
# ============================================================

# IP addresses of your four DigitalOcean droplets (in order, node 0–3)
DROPLET_IPS=(
    "137.184.75.143" #fftech-1
    "162.243.170.120" #fftech-2
    "24.199.86.16"
    "206.189.207.249"
)

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
LOCAL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_CONF="$LOCAL_DIR/cluster.conf"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=15 -o BatchMode=yes"
LOG_DIR="$LOCAL_DIR/deploy_logs"
mkdir -p "$LOG_DIR"

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
    info "Setting up $NUM_NODES droplets in parallel..."

    local pids=()
    for i in $(seq 0 $((NUM_NODES - 1))); do
        {
            local ip="${DROPLET_IPS[$i]}"
            echo "  [node $i] installing system packages..."
            ssh_cmd "$i" "apt-get update -qq && \
                apt-get install -y -qq \
                    uuid-dev zlib1g-dev libbz2-dev liblz4-dev \
                    libsnappy-dev libzstd-dev libgflags-dev \
                    cmake librocksdb-dev libgtest-dev libisal-dev \
                    build-essential 2>&1 | tail -5"

            echo "  [node $i] syncing codebase..."
            rsync -az --delete \
                -e "ssh $SSH_OPTS" \
                --exclude='.git' \
                --exclude='build/' \
                --exclude='*.o' \
                --exclude='deploy_logs/' \
                --exclude='cluster.conf' \
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
    if [[ ! -f "$CLUSTER_CONF" ]]; then
        err "cluster.conf not found locally — run './deploy.sh start' first."
        exit 1
    fi

    local results="$LOG_DIR/deployResults.txt"
    info "Running bench_client (size=$WRITE_SIZE, writes=$WRITE_NUM)... (output -> $results)"
    "$LOCAL_DIR/build/bench/bench_client" \
        --conf="$CLUSTER_CONF" \
        --id=0 \
        --size="$WRITE_SIZE" \
        --write_num="$WRITE_NUM" > "$results" 2>&1
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
