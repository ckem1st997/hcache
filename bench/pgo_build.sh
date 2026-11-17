#!/usr/bin/env bash
# Builds hcache-server with profile-guided optimisation (Linux): a build
# that records which branches and functions are hot runs a training load
# from hload, then the release build is laid out from that profile.
#
#   bash bench/pgo_build.sh [OUT]      (from spike/; OUT defaults to
#                                       target/pgo/hcache-server)
#
# Needs the llvm-tools rustup component for llvm-profdata. The training
# server listens on 127.0.0.1:6385 for a minute or so.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-target/pgo/hcache-server}
PORT=6385
WORK=target/pgo-work
rustup component add llvm-tools >/dev/null
PROFDATA=$(find "$(rustc --print sysroot)" -name llvm-profdata -type f | head -1)
[ -x "$PROFDATA" ] || { echo "llvm-profdata not found" >&2; exit 1; }
rm -rf "$WORK/profiles" && mkdir -p "$WORK/profiles" "$(dirname "$OUT")"
cargo build -q --release -p hload
RUSTFLAGS="-Cprofile-generate=$PWD/$WORK/profiles" \
    cargo build -q --release -p hcache-server --target-dir "$WORK/gen"
"$WORK/gen/release/hcache-server" --port $PORT --workers 1 >/dev/null 2>&1 &
pid=$!
trap 'kill $pid 2>/dev/null || true' EXIT
for _ in $(seq 50); do
    "$WORK/gen/release/hcache-server" healthcheck --port $PORT 2>/dev/null && break
    sleep 0.1
done
H=target/release/hload
A="--addr 127.0.0.1:$PORT"
# The GET/SET mix at two sizes, pipelined and not, writes alone, then the
# other workloads hload has, so the profile is not GET/SET alone.
for k in 1000 100000; do
    $H baseline $A --secs 4 --clients 32 --pipeline 16 --keys $k >/dev/null
done
$H baseline $A --secs 3 --clients 32 --pipeline 1 --keys 10000 >/dev/null
$H baseline $A --secs 3 --clients 32 --pipeline 16 --keys 10000 --write-pct 100 >/dev/null
for m in scan hashtag eval multi churn; do
    timeout 60 $H $m $A --secs 3 --clients 8 --big 20000 >/dev/null || echo "training $m: exit $?" >&2
done
# SIGTERM: the server writes its profile as it exits.
kill $pid && wait $pid || true
trap - EXIT
"$PROFDATA" merge -o "$WORK/merged.profdata" "$WORK"/profiles/*.profraw
RUSTFLAGS="-Cprofile-use=$PWD/$WORK/merged.profdata" \
    cargo build -q --release -p hcache-server --target-dir "$WORK/use"
cp "$WORK/use/release/hcache-server" "$OUT"
echo "built $OUT"
