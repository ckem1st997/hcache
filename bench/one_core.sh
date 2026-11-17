#!/usr/bin/env bash
# Spike A, 1-core investigation: HCache against Valkey on one pinned vCPU,
# with and without a log, and with only reads or only writes. Pipeline 16,
# where the server's own CPU cost decides the result.
#
# Usage: bench/one_core.sh VALKEY_SERVER HCACHE_SERVER HLOAD [extra hcache flags...]
set -euo pipefail

VALKEY=$1
HCACHE=$2
HLOAD=$3
shift 3
EXTRA=("$@")
SECS=${SECS:-5}
WORK=$(mktemp -d)
PID=
trap 'kill "$PID" 2>/dev/null || true; rm -rf "$WORK"' EXIT

wait_port() {
    for _ in $(seq 100); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then return 0; fi
        sleep 0.05
    done
    echo "server on port $1 did not start" >&2
    return 1
}

run() { # server log write_pct
    rm -rf "$WORK/data" && mkdir -p "$WORK/data"
    local port
    case $1 in
        valkey)
            port=6379
            local aof=(--appendonly no)
            [ "$2" = log ] && aof=(--appendonly yes --appendfsync everysec --dir "$WORK/data")
            taskset -c 0 "$VALKEY" --port $port --bind 127.0.0.1 --save "" "${aof[@]}" \
                --io-threads 1 --logfile "$WORK/valkey.log" >/dev/null 2>&1 &
            ;;
        hcache)
            port=6380
            local data=()
            [ "$2" = log ] && data=(--data "$WORK/data/data.hc")
            taskset -c 0 "$HCACHE" --port $port --bind 127.0.0.1 --workers 1 "${data[@]}" "${EXTRA[@]}" \
                >"$WORK/hcache.log" 2>&1 &
            ;;
    esac
    PID=$!
    wait_port $port
    local line
    line=$(taskset -c 1-7 "$HLOAD" baseline --addr "127.0.0.1:$port" --secs "$SECS" \
        --clients 64 --pipeline 16 --write-pct "$3" 2>/dev/null | grep 'ops/s' | tail -1 | sed -E 's/ +/ /g')
    kill "$PID" && wait "$PID" 2>/dev/null || true
    PID=
    echo "$1 $2 write=$3: $line"
}

for log in nolog log; do
    for w in 0 10 100; do
        run valkey $log $w
        run hcache $log $w
    done
done
