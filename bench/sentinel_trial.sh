#!/usr/bin/env bash
# A real Valkey Sentinel managing three nodes, HCache or Valkey: discovery,
# automatic failover when the primary dies, the old primary turned into a
# replica when it comes back, SENTINEL FAILOVER, and SENTINEL FAILOVER
# COORDINATED (CLIENT PAUSE and FAILOVER TO in one transaction). Each step
# prints PASS or FAIL; the Sentinel events follow.
#
# Usage: bench/sentinel_trial.sh hcache|valkey VALKEY_SRC [HCACHE_SERVER]
#   VALKEY_SRC: a directory with valkey-server, valkey-sentinel, valkey-cli.
# Ports 7500-7502 (nodes) and 7510-7512 (Sentinels) on 127.0.0.1.
set -uo pipefail
TYPE=$1
V=$2
H=${3:-}
W=${WORK:-/tmp/hcache-sentinel-$TYPE}
CLI="$V/valkey-cli"
NODES="7500 7501 7502"
SENTS="7510 7511 7512"
S="$CLI -p 7510"
PIDS=()
[ "$TYPE" = hcache ] && [ -z "$H" ] && { echo "hcache needs HCACHE_SERVER"; exit 2; }
rm -rf "$W"
mkdir -p "$W"

stop() {
    [ ${#PIDS[@]} -gt 0 ] || return
    kill "${PIDS[@]}" 2>/dev/null
    sleep 1
    kill -9 "${PIDS[@]}" 2>/dev/null
}
trap stop EXIT

declare -A NODE_PID
start_node() {
    local p=$1
    mkdir -p "$W/n$p"
    if [ "$TYPE" = hcache ]; then
        "$H" --bind 127.0.0.1 --port "$p" --workers 2 >>"$W/n$p/log" 2>&1 &
    else
        "$V/valkey-server" --bind 127.0.0.1 --port "$p" --save "" --appendonly no \
            --dir "$W/n$p" >>"$W/n$p/log" 2>&1 &
    fi
    NODE_PID[$p]=$!
    PIDS+=($!)
}
check() { # check NAME CONDITION...
    local name=$1
    shift
    if "$@"; then echo "PASS $name"; else echo "FAIL $name"; fi
}
primary() { $S sentinel get-master-addr-by-name mymaster | tail -1; }
role_of() { $CLI -p "$1" role | head -1; }
wait_switch() { # wait_switch FROM SECONDS: the new primary's port, or nothing
    local from=$1 i
    for i in $(seq 1 "$2"); do
        local now
        now=$(primary)
        [ "$now" != "$from" ] && { echo "$now $i"; return; }
        sleep 1
    done
}

for p in $NODES; do start_node "$p"; done
sleep 1
$CLI -p 7501 replicaof 127.0.0.1 7500 >/dev/null
$CLI -p 7502 replicaof 127.0.0.1 7500 >/dev/null
sleep 2
for s in $SENTS; do
    mkdir -p "$W/s$s"
    cat >"$W/s$s/sentinel.conf" <<EOF
port $s
bind 127.0.0.1
dir $W/s$s
logfile $W/s$s/log
sentinel monitor mymaster 127.0.0.1 7500 2
sentinel down-after-milliseconds mymaster 2000
sentinel failover-timeout mymaster 10000
sentinel parallel-syncs mymaster 1
EOF
    "$V/valkey-sentinel" "$W/s$s/sentinel.conf" >/dev/null 2>&1 &
    PIDS+=($!)
done
sleep 1
# WSL steps its wall clock by ~3 s every ~35 s, which Sentinel would take
# for a stall (TILT, 30 s without acting).
for s in $SENTS; do $CLI -p "$s" sentinel debug tilt-trigger 10000 >/dev/null; done
sleep 7

echo "== discovery"
check "two replicas" [ "$($S sentinel master mymaster | grep -A1 '^num-slaves$' | tail -1)" = 2 ]
check "two other Sentinels" \
    [ "$($S sentinel master mymaster | grep -A1 '^num-other-sentinels$' | tail -1)" = 2 ]
check "a 40-character run ID" \
    [ "$($S sentinel master mymaster | grep -A1 '^runid$' | tail -1 | tr -d '\n' | wc -c)" = 40 ]
check "quorum" [ "$($S sentinel ckquorum mymaster | cut -c1-2)" = OK ]

echo "== the primary dies"
$CLI -p 7500 set before-kill 1 >/dev/null
sleep 1
kill -9 "${NODE_PID[7500]}"
read -r NEW SECS <<<"$(wait_switch 7500 40)"
check "automatic failover (${SECS:-?} s, to ${NEW:-none})" [ -n "${NEW:-}" ]
sleep 5
check "the new primary has the data" [ "$($CLI -p "$NEW" get before-kill)" = 1 ]
for p in 7501 7502; do
    [ "$p" = "$NEW" ] && continue
    check "$p follows $NEW" [ "$($CLI -p "$p" info replication | grep -c "master_port:$NEW")" = 1 ]
done

echo "== the old primary comes back"
start_node 7500
sleep 1
for i in $(seq 1 30); do
    [ "$(role_of 7500)" = slave ] && break
    sleep 1
done
check "7500 turned into a replica (${i} s)" [ "$(role_of 7500)" = slave ]
sleep 3

echo "== SENTINEL FAILOVER"
FROM=$(primary)
$S sentinel failover mymaster >/dev/null
read -r NEW SECS <<<"$(wait_switch "$FROM" 30)"
check "manual failover (${SECS:-?} s, $FROM to ${NEW:-none})" [ -n "${NEW:-}" ]
for i in $(seq 1 30); do
    [ "$(role_of "$FROM")" = slave ] && break
    sleep 1
done
check "$FROM turned into a replica" [ "$(role_of "$FROM")" = slave ]
sleep 5

echo "== SENTINEL FAILOVER COORDINATED"
FROM=$(primary)
$CLI -p "$FROM" set before-coord 1 >/dev/null
$S sentinel failover mymaster coordinated >/dev/null
read -r NEW SECS <<<"$(wait_switch "$FROM" 30)"
check "coordinated failover (${SECS:-?} s, $FROM to ${NEW:-none})" [ -n "${NEW:-}" ]
sleep 6
check "$FROM is a replica" [ "$(role_of "$FROM")" = slave ]
check "the new primary has the data" [ "$($CLI -p "$NEW" get before-coord)" = 1 ]
check "the old primary refuses writes" \
    [ "$($CLI -p "$FROM" set x 1 | grep -c READONLY)" = 1 ]
check "no pause left on the old primary" \
    [ "$(timeout 3 "$CLI" -p "$FROM" publish ch m)" = 0 ]
$CLI -p "$NEW" set after-coord 1 >/dev/null
sleep 1
for p in $NODES; do
    check "$p has the new write" [ "$($CLI -p "$p" get after-coord)" = 1 ]
done

echo "== Sentinel events (7510)"
grep -E '\+(tilt|sdown|odown|elected-leader|selected-slave|promoted-slave|switch-master|convert-to-slave|slave-reconf-done)|-failover-abort' \
    "$W/s7510/log" | sed 's/^[0-9]*:X //'
