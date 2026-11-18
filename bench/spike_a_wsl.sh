#!/usr/bin/env bash
# Spike A, first pass on one machine (WSL2): Hotstash against Valkey with the
# server pinned to N cores and hload pinned to the others. Both write a log
# with fsync every second. Each cell runs REPS times on a fresh server.
#
# Usage: bench/spike_a_wsl.sh VALKEY_SERVER HOTSTASH_SERVER HLOAD OUT_FILE
set -euo pipefail

VALKEY=$1
HOTSTASH=$2
HLOAD=$3
OUT=$4
SECS=${SECS:-10}
REPS=${REPS:-3}
CLIENTS=${CLIENTS:-64}
TOTAL_CPUS=$(nproc)
WORK=$(mktemp -d)
trap 'kill "$PID" 2>/dev/null || true; rm -rf "$WORK"' EXIT
PID=

wait_port() {
    for _ in $(seq 100); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then return 0; fi
        sleep 0.05
    done
    echo "server on port $1 did not start" >&2
    return 1
}

start() { # server cores
    rm -rf "$WORK/data" && mkdir -p "$WORK/data"
    local cpus="0-$(($2 - 1))"
    case $1 in
        valkey)
            taskset -c "$cpus" "$VALKEY" --port 6379 --bind 127.0.0.1 --save "" \
                --appendonly yes --appendfsync everysec --dir "$WORK/data" \
                --io-threads "$2" --logfile "$WORK/valkey.log" >/dev/null 2>&1 &
            PID=$!
            wait_port 6379 ;;
        hotstash)
            taskset -c "$cpus" "$HOTSTASH" --port 6380 --bind 127.0.0.1 --workers "$2" \
                --data "$WORK/data/data.hc" --appendfsync everysec >"$WORK/hotstash.log" 2>&1 &
            PID=$!
            wait_port 6380 ;;
    esac
}

stop() {
    kill "$PID" && wait "$PID" 2>/dev/null || true
    PID=
}

port() { [ "$1" = valkey ] && echo 6379 || echo 6380; }

echo "# $(date -Is) secs=$SECS reps=$REPS clients=$CLIENTS cpus=$TOTAL_CPUS" | tee -a "$OUT"
for cores in ${CORES:-1 2 4}; do
    client_cpus="$cores-$((TOTAL_CPUS - 1))"
    for zipf in 0 0.99; do
        for pipeline in 1 16; do
            for rep in $(seq "$REPS"); do
                for server in valkey hotstash; do
                    start "$server" "$cores"
                    line=$(taskset -c "$client_cpus" "$HLOAD" baseline \
                        --addr "127.0.0.1:$(port "$server")" --secs "$SECS" \
                        --clients "$CLIENTS" --pipeline "$pipeline" --zipf "$zipf" \
                        --seed "$rep" | grep 'ops/s' | tail -1)
                    stop
                    echo "cores=$cores zipf=$zipf pipeline=$pipeline rep=$rep server=$server $line" | tee -a "$OUT"
                done
            done
        done
    done
done
