# syntax=docker/dockerfile:1
#
# The HCache server: built on Debian 13, run on distroless (no shell, no
# package manager) as the unprivileged user 65532. From this folder:
#
#   docker build -t hcache .
#   docker run -d -p 6379:6379 -v hcache-data:/data -m 1g \
#     -e HCACHE_REQUIREPASS=... hcache
#
# The image listens beyond loopback, so the server refuses to start with
# no password: give HCACHE_REQUIREPASS (or add --protected-mode no).
#
# Prometheus metrics, under redis_exporter's names, on redis_exporter's
# port; scraped with HTTP Basic as "default" and the password:
#
#   docker run ... -p 9121:9121 -e HCACHE_METRICS_PORT=9121 hcache
#
# On Kubernetes, deploy/helm/hcache is a chart for all of this.
#
# For amd64 and arm64 at once (arm64 built under emulation, slowly):
#
#   docker buildx build --platform linux/amd64,linux/arm64 -t hcache .
#
# Under a memory limit (-m, or a Kubernetes limit) maxmemory follows it:
# see --maxmemory auto in USAGE, crates/hcache-server/src/main.rs. SIGTERM writes every
# change to /data and exits 0 within 20 seconds.

FROM rust:1-trixie AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p hcache-server \
    && cp target/release/hcache-server /hcache-server \
    && mkdir /data

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /hcache-server /usr/local/bin/hcache-server
# Owned by the user the server runs as; a new named volume takes this over.
COPY --from=build --chown=65532:65532 /data /data
WORKDIR /data
EXPOSE 6379
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=3 \
    CMD ["/usr/local/bin/hcache-server", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/hcache-server"]
CMD ["--bind", "0.0.0.0", "--data", "/data/hcache.aof"]
