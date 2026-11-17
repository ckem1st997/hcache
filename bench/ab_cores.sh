#!/usr/bin/env bash
# Interleaved A/B on N pinned vCPUs: each round runs every server once, so
# drift of the host hits all of them alike. The load generator gets the
# vCPUs left over. A server is "valkey", "redis", or the name of an HCache
# binary in $VARIANTS. Valkey and Redis get --io-threads N, HCache
# --workers N; none of them writes to disk.
# Usage: VALKEY=... REDIS=... VARIANTS=dir bench/ab_cores.sh CORES KEYS ROUNDS server...
CORES=$1; KEYS=$2; ROUNDS=$3; shift 3
SERVERS=("$@")
cd "$(dirname "$0")/.."
VALKEY=${VALKEY:-valkey-server}
REDIS=${REDIS:-redis-server}
VARIANTS=${VARIANTS:-variants}
H=target/release/hload
PIN="0-$((CORES - 1))"
LOAD="$CORES-$(($(nproc) - 1))"
one() { # name
    case "$1" in
        valkey) taskset -c "$PIN" "$VALKEY" --port 6390 --save '' --appendonly no --io-threads "$CORES" >/dev/null 2>&1 & ;;
        redis) taskset -c "$PIN" "$REDIS" --port 6390 --save '' --appendonly no --io-threads "$CORES" >/dev/null 2>&1 & ;;
        *) taskset -c "$PIN" "$VARIANTS/$1" --port 6390 --workers "$CORES" >/dev/null 2>&1 & ;;
    esac
    local p=$!; sleep 0.5
    local r
    r=$(taskset -c "$LOAD" $H baseline --addr 127.0.0.1:6390 --secs 4 --clients 64 --pipeline "${PIPELINE:-16}" --keys "$KEYS" 2>/dev/null | grep ops/s | awk '{print $2}')
    kill $p; wait $p 2>/dev/null
    echo "$1 $r"
}
for _ in $(seq "$ROUNDS"); do
    for s in "${SERVERS[@]}"; do one "$s"; done
done | awk -v base="${SERVERS[0]}" '{n[$1]++; s[$1]+=$2; v[$1, n[$1]]=$2} END {
    for (k in n) { m=s[k]/n[k]; d=0; for (i=1;i<=n[k];i++) d+=(v[k,i]-m)^2; printf "%-8s mean %9.0f  sd %7.0f  n %d  vs %s %.3f\n", k, m, sqrt(d/(n[k]-1)), n[k], base, m/(s[base]/n[base]) } }' | sort
