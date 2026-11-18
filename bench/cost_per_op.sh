#!/usr/bin/env bash
# Cost per command of each part of a server's path, 1 vCPU, pipeline 16,
# 100k keys. BIN is an Hotstash binary or the word "valkey".
. ~/.cargo/env
P=/usr/lib/linux-tools/6.8.0-149-generic/perf
cd /root/spike
BIN=${BIN:-/root/variants/layout}
if [ "$BIN" = valkey ]; then
    taskset -c 0 /root/src/valkey-9.1.3/src/valkey-server --port 6380 --save '' --appendonly no --io-threads 1 >/dev/null 2>&1 &
else
    taskset -c 0 "$BIN" --port 6380 --workers 1 >/dev/null 2>&1 &
fi
pid=$!
sleep 0.5
taskset -c 1-7 target/release/hload baseline --addr 127.0.0.1:6380 --secs 9 --clients 64 --pipeline 16 --keys 100000 2>/dev/null > /root/results/bd.load &
load=$!
sleep 3
$P stat -p $pid -e cycles:u,cycles:k,instructions:u -x, -- sleep 3 2> /root/results/bd.stat
$P record -F 2999 --call-graph dwarf,16384 -p $pid -o /root/results/bd.data -- sleep 2 >/dev/null 2>&1
wait $load
kill $pid
wait $pid 2>/dev/null
OPS=$(awk '/ops\/s/ {print $2}' /root/results/bd.load)
echo "ops/s $OPS"
awk -F, -v ops="$OPS" '{printf "  %s: %.0f per op\n", $3, $1/3/ops}' /root/results/bd.stat
NS=$(awk -v ops="$OPS" 'BEGIN {print 1e9/ops}')
echo "ns per op: $NS"
echo "-- user-space functions, inclusive and self ns per op"
$P report -i /root/results/bd.data --children --sort dso,symbol --stdio -g none 2>/dev/null \
  | grep -E '^ +[0-9]' | grep -v 'kernel.kallsyms' | rustfilt \
  | sed -E 's/::h[0-9a-f]{16}//g' \
  | grep -vE 'std::(sys|thread|panic)|core::panic|catch_unwind|start_thread|clone3|FnOnce|lifecycle|backtrace|Server>::start' \
  | awk -v ns="$NS" '{inc=$1; sub("%","",inc); self=$2; sub("%","",self); $1=$2=$3=""; printf "%5.0f incl %5.0f self %s\n", inc*ns/100, self*ns/100, $0}' \
  | cut -c1-170 | head -${TOP:-60}
