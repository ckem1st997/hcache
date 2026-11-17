#!/usr/bin/env bash
# Memory after loading 1M string keys (64-byte values), per server.
cd /root/spike
V=/root/src/valkey-9.1.3/src/valkey-server
CLI=/root/src/valkey-9.1.3/src/valkey-cli
for s in valkey base final; do
    if [ $s = valkey ]; then
        $V --port 6390 --save '' --appendonly no >/dev/null 2>&1 &
    else
        /root/variants/$s --port 6390 --workers 1 >/dev/null 2>&1 &
    fi
    p=$!; sleep 0.5
    target/release/hload baseline --addr 127.0.0.1:6390 --secs 1 --clients 4 --keys 1000000 >/dev/null 2>&1
    keys=$($CLI -p 6390 DBSIZE)
    used=$($CLI -p 6390 INFO memory | tr -d '\r' | awk -F: '/^used_memory:/ {print $2}')
    rss=$(awk '/VmRSS/ {print $2*1024}' /proc/$p/status)
    echo "$s keys=$keys used_memory=$used rss=$rss bytes/key(rss)=$((rss / keys))"
    kill $p; wait $p 2>/dev/null
done
