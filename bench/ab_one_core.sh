#!/usr/bin/env bash
# Interleaved A/B on one pinned vCPU: each round runs every server once,
# so drift of the host hits all of them alike. Variants are Hotstash binaries
# in /root/variants (built by hand from the tree to compare).
# Usage: bench/ab_one_core.sh KEYS ROUNDS [variant...]
KEYS=$1; ROUNDS=$2; shift 2
VARIANTS=("$@")
cd /root/spike
V=/root/src/valkey-9.1.3/src/valkey-server
H=target/release/hload
one() { # name
    if [ "$1" = valkey ]; then
        taskset -c 0 $V --port 6379 --save '' --appendonly no --io-threads 1 >/dev/null 2>&1 & port=6379
    else
        taskset -c 0 "/root/variants/$1" --port 6380 --workers 1 >/dev/null 2>&1 & port=6380
    fi
    local p=$!; sleep 0.4
    local r
    r=$(taskset -c 1-7 $H baseline --addr 127.0.0.1:$port --secs 4 --clients 64 --pipeline 16 --keys "$KEYS" 2>/dev/null | grep ops/s | awk '{print $2}')
    kill $p; wait $p 2>/dev/null
    echo "$1 $r"
}
for r in $(seq "$ROUNDS"); do
    for v in valkey "${VARIANTS[@]}"; do one "$v"; done
done | awk '{n[$1]++; s[$1]+=$2; v[$1, n[$1]]=$2} END {
    for (k in n) { m=s[k]/n[k]; d=0; for (i=1;i<=n[k];i++) d+=(v[k,i]-m)^2; printf "%-8s mean %8.0f  sd %6.0f  n %d  vs valkey %.3f\n", k, m, sqrt(d/(n[k]-1)), n[k], m/(s["valkey"]/n["valkey"]) } }' | sort
