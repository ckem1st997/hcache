#!/usr/bin/env bash
# Which vCPUs are slow for the server?
cd /root/spike
V=/root/src/valkey-9.1.3/src/valkey-server
one() { # server_cpus io_threads client_cpus
    taskset -c "$1" "$V" --port 6379 --save '' --appendonly no --io-threads "$2" >/dev/null 2>&1 &
    p=$!
    sleep 0.5
    printf 'server %s clients %s: ' "$1" "$3"
    taskset -c "$3" target/release/hload baseline --addr 127.0.0.1:6379 --secs 5 --clients 64 2>/dev/null | grep ops/s | sed -E 's/ +/ /g'
    kill "$p"; wait "$p" 2>/dev/null
}
one 0 1 4-7
one 1 1 4-7
one 2 1 4-7
one 3 1 4-7
one 7 1 0-3
one 2-3 2 4-7
one 6-7 2 0-3
one 4-7 4 0-3
