//! Prometheus metrics over HTTP on a port of its own (`--metrics-port`):
//! `GET /metrics` gives INFO, named as redis_exporter names it, so that
//! the dashboards and alerts made for it work as they are. Only the fields
//! redis_exporter takes are given, each as it gives them.
//!
//! The users are RESP's: while the default user needs a password, so does
//! this port, as HTTP Basic, and the user must be allowed INFO. Each
//! connection asks once and is closed.

use crate::Shared;
use hcache_core::acl::User;
use hcache_core::cmd::Cmd;
use hcache_core::resp::Reply;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// What every name starts with, as redis_exporter's default.
const NAMESPACE: &str = "redis_";
/// How often the listener, and a connection still sending its request,
/// look at the stop flag.
const TICK: Duration = Duration::from_millis(200);
/// A request must be in within this, and its answer taken within
/// `WRITE_WAIT`.
const REQUEST_WAIT: Duration = Duration::from_secs(5);
const WRITE_WAIT: Duration = Duration::from_secs(5);
/// The longest request head: a scraper sends a few hundred bytes.
const REQUEST_MAX: usize = 8 * 1024;
/// Connections served at once; more are closed as they come.
const MAX_CONNS: usize = 16;
const LISTENER: Token = Token(0);
const TEXT_FORMAT: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The INFO fields redis_exporter gives, under its names, and whether as a
/// counter (else a gauge): those of Redis and Valkey and their Bloom and
/// JSON modules, not those of KeyDB, Tile38 or RediSearch.
#[rustfmt::skip]
const FIELDS: &[(&str, &str, bool)] = &[
    // Server
    ("uptime_in_seconds", "uptime_in_seconds", false),
    ("process_id", "process_id", false),
    ("io_threads_active", "io_threads_active", false),
    // Clients
    ("connected_clients", "connected_clients", false),
    ("active_clients", "active_clients", false),
    ("blocked_clients", "blocked_clients", false),
    ("maxclients", "max_clients", false),
    ("tracking_clients", "tracking_clients", false),
    ("clients_in_timeout_table", "clients_in_timeout_table", false),
    ("pubsub_clients", "pubsub_clients", false),
    ("watching_clients", "watching_clients", false),
    ("total_watched_keys", "total_watched_keys", false),
    ("total_blocking_keys", "total_blocking_keys", false),
    ("total_blocking_keys_on_nokey", "total_blocking_keys_on_nokey", false),
    ("client_longest_output_list", "client_longest_output_list", false),
    ("client_biggest_input_buf", "client_biggest_input_buf", false),
    ("client_recent_max_output_buffer", "client_recent_max_output_buffer_bytes", false),
    ("client_recent_max_input_buffer", "client_recent_max_input_buffer_bytes", false),
    // Memory
    ("allocator_active", "allocator_active_bytes", false),
    ("allocator_allocated", "allocator_allocated_bytes", false),
    ("allocator_resident", "allocator_resident_bytes", false),
    ("allocator_frag_ratio", "allocator_frag_ratio", false),
    ("allocator_frag_bytes", "allocator_frag_bytes", false),
    ("allocator_muzzy", "allocator_muzzy_bytes", false),
    ("allocator_rss_ratio", "allocator_rss_ratio", false),
    ("allocator_rss_bytes", "allocator_rss_bytes", false),
    ("used_memory", "memory_used_bytes", false),
    ("used_memory_rss", "memory_used_rss_bytes", false),
    ("used_memory_peak", "memory_used_peak_bytes", false),
    ("used_memory_lua", "memory_used_lua_bytes", false),
    ("used_memory_vm_eval", "memory_used_vm_eval_bytes", false),
    ("used_memory_scripts_eval", "memory_used_scripts_eval_bytes", false),
    ("used_memory_overhead", "memory_used_overhead_bytes", false),
    ("used_memory_startup", "memory_used_startup_bytes", false),
    ("used_memory_dataset", "memory_used_dataset_bytes", false),
    ("number_of_cached_scripts", "number_of_cached_scripts", false),
    ("number_of_functions", "number_of_functions", false),
    ("number_of_libraries", "number_of_libraries", false),
    ("used_memory_vm_functions", "memory_used_vm_functions_bytes", false),
    ("used_memory_scripts", "memory_used_scripts_bytes", false),
    ("used_memory_functions", "memory_used_functions_bytes", false),
    ("used_memory_vm_total", "memory_used_vm_total", false),
    ("maxmemory", "memory_max_bytes", false),
    ("mem_fragmentation_ratio", "mem_fragmentation_ratio", false),
    ("mem_fragmentation_bytes", "mem_fragmentation_bytes", false),
    ("mem_clients_slaves", "mem_clients_slaves", false),
    ("mem_clients_normal", "mem_clients_normal", false),
    ("mem_cluster_links", "mem_cluster_links_bytes", false),
    ("mem_aof_buffer", "mem_aof_buffer_bytes", false),
    ("mem_replication_backlog", "mem_replication_backlog_bytes", false),
    ("mem_replicas_repl_buffer", "mem_replicas_repl_buffer_bytes", false),
    ("expired_stale_perc", "expired_stale_percentage", false),
    ("mem_not_counted_for_evict", "mem_not_counted_for_eviction_bytes", false),
    ("mem_total_replication_buffers", "mem_total_replication_buffers_bytes", false),
    ("mem_overhead_db_hashtable_rehashing", "mem_overhead_db_hashtable_rehashing_bytes", false),
    ("lazyfree_pending_objects", "lazyfree_pending_objects", false),
    ("lazyfreed_objects", "lazyfreed_objects", false),
    ("active_defrag_running", "active_defrag_running", false),
    ("migrate_cached_sockets", "migrate_cached_sockets_total", false),
    ("active_defrag_hits", "defrag_hits", false),
    ("active_defrag_misses", "defrag_misses", false),
    ("active_defrag_key_hits", "defrag_key_hits", false),
    ("active_defrag_key_misses", "defrag_key_misses", false),
    // Persistence
    ("loading", "loading_dump_file", false),
    ("async_loading", "async_loading", false),
    ("rdb_changes_since_last_save", "rdb_changes_since_last_save", false),
    ("rdb_bgsave_in_progress", "rdb_bgsave_in_progress", false),
    ("rdb_last_save_time", "rdb_last_save_timestamp_seconds", false),
    ("rdb_last_bgsave_status", "rdb_last_bgsave_status", false),
    ("rdb_last_bgsave_time_sec", "rdb_last_bgsave_duration_sec", false),
    ("rdb_current_bgsave_time_sec", "rdb_current_bgsave_duration_sec", false),
    ("rdb_saves", "rdb_saves_total", false),
    ("rdb_last_cow_size", "rdb_last_cow_size_bytes", false),
    ("rdb_last_load_keys_expired", "rdb_last_load_expired_keys", false),
    ("rdb_last_load_keys_loaded", "rdb_last_load_loaded_keys", false),
    ("aof_enabled", "aof_enabled", false),
    ("aof_rewrite_in_progress", "aof_rewrite_in_progress", false),
    ("aof_rewrite_scheduled", "aof_rewrite_scheduled", false),
    ("aof_last_rewrite_time_sec", "aof_last_rewrite_duration_sec", false),
    ("aof_current_rewrite_time_sec", "aof_current_rewrite_duration_sec", false),
    ("aof_last_cow_size", "aof_last_cow_size_bytes", false),
    ("aof_current_size", "aof_current_size_bytes", false),
    ("aof_base_size", "aof_base_size_bytes", false),
    ("aof_pending_rewrite", "aof_pending_rewrite", false),
    ("aof_buffer_length", "aof_buffer_length", false),
    ("aof_rewrite_buffer_length", "aof_rewrite_buffer_length", false),
    ("aof_pending_bio_fsync", "aof_pending_bio_fsync", false),
    ("aof_delayed_fsync", "aof_delayed_fsync", false),
    ("aof_last_bgrewrite_status", "aof_last_bgrewrite_status", false),
    ("aof_last_write_status", "aof_last_write_status", false),
    ("module_fork_in_progress", "module_fork_in_progress", false),
    ("module_fork_last_cow_size", "module_fork_last_cow_size", false),
    // Stats
    ("current_eviction_exceeded_time", "current_eviction_exceeded_time_ms", false),
    ("pubsub_channels", "pubsub_channels", false),
    ("pubsub_patterns", "pubsub_patterns", false),
    ("pubsubshard_channels", "pubsubshard_channels", false),
    ("latest_fork_usec", "latest_fork_usec", false),
    ("tracking_total_keys", "tracking_total_keys", false),
    ("tracking_total_items", "tracking_total_items", false),
    ("tracking_total_prefixes", "tracking_total_prefixes", false),
    ("instantaneous_eventloop_cycles_per_sec", "instantaneous_eventloop_cycles_per_sec", false),
    ("instantaneous_eventloop_duration_usec", "instantaneous_eventloop_duration_usec", false),
    // Replication
    ("connected_slaves", "connected_slaves", false),
    ("repl_backlog_size", "replication_backlog_bytes", false),
    ("repl_backlog_active", "repl_backlog_is_active", false),
    ("repl_backlog_first_byte_offset", "repl_backlog_first_byte_offset", false),
    ("repl_backlog_histlen", "repl_backlog_history_bytes", false),
    ("master_repl_offset", "master_repl_offset", false),
    ("second_repl_offset", "second_repl_offset", false),
    ("slave_expires_tracked_keys", "slave_expires_tracked_keys", false),
    ("slave_priority", "slave_priority", false),
    ("sync_full", "replica_resyncs_full", false),
    ("sync_partial_ok", "replica_partial_resync_accepted", false),
    ("sync_partial_err", "replica_partial_resync_denied", false),
    ("replicas_repl_buffer_size", "replicas_repl_buffer_size_bytes", false),
    ("replicas_repl_buffer_peak", "replicas_repl_buffer_peak_bytes", false),
    ("replicas_waiting_psync", "replicas_waiting_psync", false),
    // Cluster
    ("cluster_stats_messages_sent", "cluster_messages_sent_total", false),
    ("cluster_stats_messages_received", "cluster_messages_received_total", false),
    // The Bloom and JSON modules of Valkey
    ("bf_bloom_total_memory_bytes", "bf_bloom_total_memory_bytes", false),
    ("bf_bloom_num_objects", "bf_bloom_num_objects", false),
    ("bf_bloom_num_filters_across_objects", "bf_bloom_num_filters_across_objects", false),
    ("bf_bloom_num_items_across_objects", "bf_bloom_num_items_across_objects", false),
    ("bf_bloom_capacity_across_objects", "bf_bloom_capacity_across_objects", false),
    ("bf_bloom_defrag_hits", "bf_bloom_defrag_hits_total", true),
    ("bf_bloom_defrag_misses", "bf_bloom_defrag_misses_total", true),
    ("json_total_memory_bytes", "json_total_memory_bytes", false),
    ("json_num_documents", "json_num_documents", false),
    // Counters
    ("total_connections_received", "connections_received_total", true),
    ("total_commands_processed", "commands_processed_total", true),
    ("rejected_connections", "rejected_connections_total", true),
    ("total_net_input_bytes", "net_input_bytes_total", true),
    ("total_net_output_bytes", "net_output_bytes_total", true),
    ("total_net_repl_input_bytes", "net_repl_input_bytes_total", true),
    ("total_net_repl_output_bytes", "net_repl_output_bytes_total", true),
    ("expired_subkeys", "expired_subkeys_total", true),
    ("expired_fields", "expired_fields_total", true),
    ("expired_keys", "expired_keys_total", true),
    ("expired_time_cap_reached_count", "expired_time_cap_reached_total", true),
    ("expire_cycle_cpu_milliseconds", "expire_cycle_cpu_time_ms_total", true),
    ("evicted_keys", "evicted_keys_total", true),
    ("evicted_clients", "evicted_clients_total", true),
    ("evicted_scripts", "evicted_scripts_total", true),
    ("total_eviction_exceeded_time", "eviction_exceeded_time_ms_total", true),
    ("keyspace_hits", "keyspace_hits_total", true),
    ("keyspace_misses", "keyspace_misses_total", true),
    ("used_cpu_sys", "cpu_sys_seconds_total", true),
    ("used_cpu_user", "cpu_user_seconds_total", true),
    ("used_cpu_sys_children", "cpu_sys_children_seconds_total", true),
    ("used_cpu_user_children", "cpu_user_children_seconds_total", true),
    ("used_cpu_sys_main_thread", "cpu_sys_main_thread_seconds_total", true),
    ("used_cpu_user_main_thread", "cpu_user_main_thread_seconds_total", true),
    ("unexpected_error_replies", "unexpected_error_replies", true),
    ("total_error_replies", "total_error_replies", true),
    ("dump_payload_sanitizations", "dump_payload_sanitizations", true),
    ("total_reads_processed", "total_reads_processed", true),
    ("total_writes_processed", "total_writes_processed", true),
    ("io_threaded_reads_processed", "io_threaded_reads_processed", true),
    ("io_threaded_writes_processed", "io_threaded_writes_processed", true),
    ("client_query_buffer_limit_disconnections", "client_query_buffer_limit_disconnections_total", true),
    ("client_output_buffer_limit_disconnections", "client_output_buffer_limit_disconnections_total", true),
    ("reply_buffer_shrinks", "reply_buffer_shrinks_total", true),
    ("reply_buffer_expands", "reply_buffer_expands_total", true),
    ("acl_access_denied_auth", "acl_access_denied_auth_total", true),
    ("acl_access_denied_cmd", "acl_access_denied_cmd_total", true),
    ("acl_access_denied_key", "acl_access_denied_key_total", true),
    ("acl_access_denied_channel", "acl_access_denied_channel_total", true),
];

/// The labels of `instance_info`, in redis_exporter's order.
const INSTANCE_LABELS: [&str; 10] = [
    "role",
    "redis_version",
    "redis_build_id",
    "redis_mode",
    "os",
    "maxmemory_policy",
    "tcp_port",
    "run_id",
    "process_id",
    "master_replid",
];

/// Listen on `addr`. The thread returned accepts until the server stops.
pub(crate) fn start(
    shared: Arc<Shared>,
    addr: SocketAddr,
) -> io::Result<(SocketAddr, JoinHandle<()>)> {
    let mut listener = TcpListener::bind(addr)?;
    let bound = listener.local_addr()?;
    let poll = Poll::new()?;
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;
    std::thread::Builder::new()
        .name("hcache-metrics".into())
        .spawn(move || accept(&shared, &listener, poll))
        .map(|thread| (bound, thread))
}

fn accept(shared: &Arc<Shared>, listener: &TcpListener, mut poll: Poll) {
    let mut events = Events::with_capacity(16);
    let mut conns: Vec<JoinHandle<()>> = Vec::new();
    while !shared.stop.load(SeqCst) {
        if let Err(e) = poll.poll(&mut events, Some(TICK)) {
            if e.kind() != ErrorKind::Interrupted {
                eprintln!("hcache: the metrics listener stopped: {e}");
                break;
            }
        }
        conns.retain(|conn| !conn.is_finished());
        loop {
            match listener.accept() {
                // Closed as it is dropped.
                Ok(_) if conns.len() >= MAX_CONNS => {}
                Ok((stream, _)) => {
                    let shared = shared.clone();
                    let spawned = std::thread::Builder::new()
                        .name("hcache-metrics-conn".into())
                        .spawn(move || serve(&shared, stream));
                    match spawned {
                        Ok(thread) => conns.push(thread),
                        Err(e) => eprintln!("hcache: no thread for a metrics connection: {e}"),
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                // Out of descriptors and the like: again after a tick.
                Err(e) => {
                    eprintln!("hcache: metrics accept: {e}");
                    break;
                }
            }
        }
    }
    // Each connection sees the stop within a tick, or its write times out.
    for conn in conns {
        if conn.join().is_err() {
            eprintln!("hcache: a metrics connection thread panicked");
        }
    }
}

fn serve(shared: &Shared, stream: mio::net::TcpStream) {
    let mut stream = TcpStream::from(stream);
    let ready = stream.set_nonblocking(false).and_then(|()| {
        stream.set_read_timeout(Some(TICK))?;
        stream.set_write_timeout(Some(WRITE_WAIT))
    });
    if let Err(e) = ready {
        eprintln!("hcache: cannot set up a metrics connection: {e}");
        return;
    }
    let answer = match read_head(shared, &mut stream) {
        Ok(Some(head)) => respond(shared, &head),
        Ok(None) => return,
        Err(too_long) => too_long,
    };
    // Best effort: a scraper that hung up asks again at its next turn.
    let _ = stream.write_all(&answer);
}

/// The request up to its blank line; `None` when the client hangs up, is
/// too slow or the server stops; the answer when it is too long.
fn read_head(shared: &Shared, stream: &mut TcpStream) -> Result<Option<Vec<u8>>, Vec<u8>> {
    let started = Instant::now();
    let mut head = Vec::new();
    let mut buf = [0u8; 2048];
    loop {
        if let Some(end) = head_end(&head) {
            head.truncate(end);
            return Ok(Some(head));
        }
        if head.len() > REQUEST_MAX {
            let body = b"request header too long\n";
            return Err(answer(
                "431 Request Header Fields Too Large",
                "",
                body,
                false,
            ));
        }
        if shared.stop.load(SeqCst) || started.elapsed() >= REQUEST_WAIT {
            return Ok(None);
        }
        match stream.read(&mut buf) {
            Ok(0) => return Ok(None),
            Ok(n) => head.extend_from_slice(&buf[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(_) => return Ok(None),
        }
    }
}

/// Where the head ends: at its first empty line, `\r\n` or bare `\n`.
fn head_end(head: &[u8]) -> Option<usize> {
    let crlf = head.windows(4).position(|w| w == b"\r\n\r\n");
    let lf = head.windows(2).position(|w| w == b"\n\n");
    match (crlf, lf) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn respond(shared: &Shared, head: &[u8]) -> Vec<u8> {
    let mut lines = head
        .split(|&b| b == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line));
    let request: Vec<&[u8]> = lines
        .next()
        .unwrap_or_default()
        .split(|&b| b == b' ')
        .collect();
    let [method, target, version] = request[..] else {
        return answer("400 Bad Request", "", b"bad request line\n", false);
    };
    if !version.starts_with(b"HTTP/1.") {
        return answer("400 Bad Request", "", b"HTTP/1 only\n", false);
    }
    let head_only = method == b"HEAD";
    let path = target.split(|&b| b == b'?').next().unwrap_or_default();
    if path != b"/metrics" {
        return answer("404 Not Found", "", b"only /metrics is here\n", head_only);
    }
    if method != b"GET" && !head_only {
        return answer("405 Method Not Allowed", "Allow: GET, HEAD\r\n", b"", false);
    }
    let authorization = lines.find_map(|line| {
        let (name, value) = line.split_at(line.iter().position(|&b| b == b':')?);
        name.eq_ignore_ascii_case(b"authorization")
            .then(|| value[1..].trim_ascii())
    });
    let user = match user(shared, authorization) {
        Some(user) => user,
        None => {
            let challenge = "WWW-Authenticate: Basic realm=\"hcache\", charset=\"UTF-8\"\r\n";
            return answer(
                "401 Unauthorized",
                challenge,
                b"a user and password are needed\n",
                head_only,
            );
        }
    };
    if user.check(b"INFO", None, &Cmd::Info(Vec::new())).is_err() {
        return answer(
            "403 Forbidden",
            "",
            b"this user may not run INFO\n",
            head_only,
        );
    }
    let info = match shared.engine.exec_conn(&Cmd::Info(Vec::new())) {
        Reply::Bulk(info) => String::from_utf8_lossy(&info).into_owned(),
        _ => String::new(),
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    answer("200 OK", "", render(&info, now).as_bytes(), head_only)
}

/// Whose request this is: the default user while it needs no password,
/// else the one HTTP Basic names, if its password is right.
fn user(shared: &Shared, authorization: Option<&[u8]>) -> Option<Arc<User>> {
    let acl = shared.engine.acl();
    let default = acl.default_user();
    if default.open() {
        return Some(default);
    }
    let encoded = authorization?.strip_prefix(b"Basic ")?;
    let decoded = crate::memcached::base64_decode(encoded)?;
    let colon = decoded.iter().position(|&b| b == b':')?;
    acl.auth(&decoded[..colon], &decoded[colon + 1..]).ok()
}

fn answer(status: &str, headers: &str, body: &[u8], head_only: bool) -> Vec<u8> {
    let content_type = match status.starts_with("200") {
        true => TEXT_FORMAT,
        false => "text/plain; charset=utf-8",
    };
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         {headers}Connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    if !head_only {
        out.extend_from_slice(body);
    }
    out
}

/// One metric and its samples, each as its labels and value.
struct Family {
    help: String,
    counter: bool,
    samples: Vec<(String, f64)>,
}

/// Samples by metric name, the first of each name and labels kept.
#[derive(Default)]
struct Families(BTreeMap<String, Family>);

impl Families {
    fn add(&mut self, name: &str, help: &str, counter: bool, labels: &[(&str, &str)], value: f64) {
        let family = self.0.entry(name.to_owned()).or_insert_with(|| Family {
            help: help.to_owned(),
            counter,
            samples: Vec::new(),
        });
        let mut text = String::new();
        for (i, (label, v)) in labels.iter().enumerate() {
            let sep = if i == 0 { "" } else { "," };
            let _ = write!(text, "{sep}{label}=\"{}\"", escape(v, true));
        }
        if !family.samples.iter().any(|(seen, _)| *seen == text) {
            family.samples.push((text, value));
        }
    }

    fn gauge(&mut self, name: &str, help: &str, labels: &[(&str, &str)], value: f64) {
        self.add(name, help, false, labels, value);
    }

    fn text(self) -> String {
        let mut out = String::new();
        for (name, family) in self.0 {
            let kind = if family.counter { "counter" } else { "gauge" };
            let _ = writeln!(
                out,
                "# HELP {NAMESPACE}{name} {}",
                escape(&family.help, false)
            );
            let _ = writeln!(out, "# TYPE {NAMESPACE}{name} {kind}");
            let mut samples = family.samples;
            samples.sort_by(|a, b| a.0.cmp(&b.0));
            for (labels, value) in samples {
                let braces = match labels.is_empty() {
                    true => String::new(),
                    false => format!("{{{labels}}}"),
                };
                let _ = writeln!(out, "{NAMESPACE}{name}{braces} {}", number(value));
            }
        }
        out
    }
}

/// A HELP text, or with `quotes` a label value, as the format escapes it.
fn escape(s: &str, quotes: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '"' if quotes => out.push_str("\\\""),
            c => out.push(c),
        }
    }
    out
}

fn number(v: f64) -> String {
    match v {
        v if v.is_nan() => "NaN".into(),
        v if v == f64::INFINITY => "+Inf".into(),
        v if v == f64::NEG_INFINITY => "-Inf".into(),
        v => v.to_string(),
    }
}

/// A value as redis_exporter reads it: ok and true are 1, err, fail and
/// false 0, anything else a number or nothing.
fn value(s: &str) -> Option<f64> {
    match s {
        "ok" | "true" => Some(1.0),
        "err" | "fail" | "false" => Some(0.0),
        s => s.parse().ok(),
    }
}

/// `k=v,k=v`, or nothing if a part has no `=`.
fn pairs(s: &str) -> Option<HashMap<&str, &str>> {
    s.split(',').map(|part| part.split_once('=')).collect()
}

/// The metrics of an INFO text, as redis_exporter makes them of it. `now`
/// is the Unix time, for `start_time_seconds`.
fn render(info: &str, now: f64) -> String {
    let mut out = Families::default();
    out.gauge("up", "Information about the Redis instance", &[], 1.0);
    let mut fields: HashMap<&str, &str> = HashMap::new();
    let (mut section, mut master_host, mut master_port) = ("", "", "");
    for line in info.lines().map(str::trim) {
        if let Some(name) = line.strip_prefix("# ") {
            section = name;
            continue;
        }
        let Some((key, val)) = line.split_once(':').filter(|_| line.len() >= 2) else {
            continue;
        };
        fields.entry(key).or_insert(val);
        if is_master_field(key, "host") {
            master_host = val;
        }
        if is_master_field(key, "port") {
            master_port = val;
        }
        let master = [("master_host", master_host), ("master_port", master_port)];
        match section {
            "Replication" => {
                if is_master_field(key, "link_status") {
                    let up = f64::from(u8::from(val == "up"));
                    out.gauge(
                        "master_link_up",
                        "Master link status on Redis slave",
                        &master,
                        up,
                    );
                    continue;
                }
                if let Some(name) = master_direct(key) {
                    let v = val.parse::<i64>().unwrap_or(0) as f64;
                    out.gauge(name, &format!("{name} metric"), &master, v);
                    continue;
                }
                if replica_line(&mut out, key, val) {
                    continue;
                }
            }
            "Server" if key == "uptime_in_seconds" => {
                if let Ok(uptime) = val.parse::<f64>() {
                    let help = "Start time of the Redis instance since unix epoch in seconds.";
                    out.gauge("start_time_seconds", help, &[], now.floor() - uptime);
                }
            }
            "Server" if key == "hz" || key == "configured_hz" => {
                if let Ok(hz) = val.parse::<i64>() {
                    out.gauge(key, &format!("{key} metric"), &[], hz as f64);
                }
            }
            "Keyspace" if keyspace_line(&mut out, key, val) => continue,
            _ => {}
        }
        field(&mut out, key, val);
    }
    let mut labels: Vec<(&str, &str)> = INSTANCE_LABELS
        .iter()
        .map(|&label| match label {
            "redis_mode" => (label, fields.get(label).or(fields.get("server_mode"))),
            _ => (label, fields.get(label)),
        })
        .map(|(label, v)| (label, v.copied().unwrap_or("")))
        .collect();
    for label in ["valkey_version", "valkey_release_stage"] {
        if let Some(v) = fields.get(label) {
            labels.push((label, v));
        }
    }
    out.gauge(
        "instance_info",
        "Information about the Redis instance",
        &labels,
        1.0,
    );
    if fields.get("role") == Some(&"slave") {
        let get = |k| fields.get(k).copied().unwrap_or("");
        let labels = [
            ("master_host", get("master_host")),
            ("master_port", get("master_port")),
            ("read_only", get("slave_read_only")),
        ];
        out.gauge(
            "slave_info",
            "Information about the Redis slave",
            &labels,
            1.0,
        );
    }
    out.text()
}

/// `master_<what>`, or `master_<n>_<what>` as KeyDB numbers its masters.
fn is_master_field(key: &str, what: &str) -> bool {
    let Some(rest) = key.strip_prefix("master") else {
        return false;
    };
    let rest = match rest.strip_prefix('_') {
        Some(after) => after,
        None => return false,
    };
    let rest = match rest.find(|c: char| !c.is_ascii_digit()) {
        Some(0) => rest,
        Some(i) if rest[i..].starts_with('_') => &rest[i + 1..],
        _ => rest,
    };
    rest.starts_with(what)
}

/// The replication fields redis_exporter gives with the master as labels.
fn master_direct(key: &str) -> Option<&'static str> {
    if key.starts_with("slave_repl_offset") {
        return Some("slave_repl_offset");
    }
    if is_master_field(key, "last_io_seconds_ago") {
        return Some("master_last_io_seconds_ago");
    }
    if is_master_field(key, "sync_in_progress") {
        return Some("master_sync_in_progress");
    }
    None
}

/// `slave0:ip=…,port=…,state=…,offset=…,lag=…` of a master.
fn replica_line(out: &mut Families, key: &str, val: &str) -> bool {
    let numbered = key
        .strip_prefix("slave")
        .is_some_and(|n| n.starts_with(|c: char| c.is_ascii_digit()));
    let Some(kv) = pairs(val).filter(|_| numbered) else {
        return false;
    };
    let Some(offset) = kv.get("offset").and_then(|v| v.parse::<f64>().ok()) else {
        return false;
    };
    let lag = match kv.get("lag") {
        Some(v) => match v.parse::<f64>() {
            Ok(lag) => Some(lag),
            Err(_) => return false,
        },
        None => None,
    };
    let get = |k| kv.get(k).copied().unwrap_or("");
    let labels = [
        ("slave_ip", get("ip")),
        ("slave_port", get("port")),
        ("slave_state", get("state")),
    ];
    out.gauge(
        "connected_slave_offset_bytes",
        "Offset of connected slave",
        &labels,
        offset,
    );
    if let Some(lag) = lag {
        out.gauge(
            "connected_slave_lag_seconds",
            "Lag of connected slave",
            &labels,
            lag,
        );
    }
    true
}

/// `db0:keys=…,expires=…,avg_ttl=…`.
fn keyspace_line(out: &mut Families, key: &str, val: &str) -> bool {
    if !key.starts_with("db") {
        return false;
    }
    let Some(kv) = pairs(val) else { return false };
    let defs: [(&[&str], &str, &str, f64, bool); 5] = [
        (
            &["keys"],
            "db_keys",
            "Total number of keys by DB",
            1.0,
            true,
        ),
        (
            &["expires"],
            "db_keys_expiring",
            "Total number of expiring keys by DB",
            1.0,
            true,
        ),
        (
            &["avg_ttl"],
            "db_avg_ttl_seconds",
            "Avg TTL in seconds",
            1e-3,
            false,
        ),
        (
            &["cached_keys"],
            "db_keys_cached",
            "Total number of cached keys by DB",
            1.0,
            false,
        ),
        (
            &["subexpiry", "keys_with_volatile_items"],
            "db_keys_with_expiring_items",
            "Total number of keys with expiring items by DB",
            1.0,
            false,
        ),
    ];
    let mut found = Vec::new();
    for (names, metric, help, scale, required) in defs {
        match names.iter().find_map(|n| kv.get(n)) {
            Some(v) => match v.parse::<f64>() {
                Ok(v) => found.push((metric, help, v * scale)),
                Err(_) => return false,
            },
            None if required => return false,
            None => {}
        }
    }
    for (metric, help, v) in found {
        out.gauge(metric, help, &[("db", key)], v);
    }
    true
}

/// Any other field: given if redis_exporter knows it or it starts with
/// db, cmdstat_ or cluster_, and its value reads as a number.
fn field(out: &mut Families, key: &str, val: &str) {
    let key: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let known = FIELDS.iter().find(|(field, _, _)| *field == key);
    let prefixed = ["db", "cmdstat_", "cluster_"]
        .iter()
        .any(|p| key.starts_with(p));
    if known.is_none() && !prefixed {
        return;
    }
    let Some(mut v) = value(val) else { return };
    let (mut name, counter) = known.map_or((key.as_str(), false), |(_, name, c)| (*name, *c));
    if name == "latest_fork_usec" {
        name = "latest_fork_seconds";
        v /= 1e6;
    }
    out.add(name, &format!("{name} metric"), counter, &[], v);
}

#[cfg(test)]
mod tests {
    use super::*;

    const INFO: &str = "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\nos:Linux\r\n\
        process_id:42\r\nuptime_in_seconds:100\r\n# Clients\r\nconnected_clients:3\r\n\
        maxclients:10000\r\n# Stats\r\ntotal_commands_processed:9\r\nkeyspace_hits:5\r\n\
        latest_fork_usec:2500\r\n# Replication\r\nrole:master\r\nconnected_slaves:1\r\n\
        slave0:ip=10.0.0.2,port=6380,state=online,offset=77,lag=1\r\n\
        # Persistence\r\nrdb_last_bgsave_status:ok\r\n# Keyspace\r\n\
        db0:keys=4,expires=1,avg_ttl=2500\r\ndb3:keys=1,expires=0,avg_ttl=0\r\n";

    #[test]
    fn info_comes_out_under_the_names_redis_exporter_gives() {
        let text = render(INFO, 1000.0);
        for line in [
            "redis_up 1",
            "# TYPE redis_connected_clients gauge",
            "redis_connected_clients 3",
            "redis_max_clients 10000",
            "# TYPE redis_commands_processed_total counter",
            "redis_commands_processed_total 9",
            "redis_keyspace_hits_total 5",
            "redis_latest_fork_seconds 0.0025",
            "redis_rdb_last_bgsave_status 1",
            "redis_start_time_seconds 900",
            "redis_uptime_in_seconds 100",
            "redis_db_keys{db=\"db0\"} 4",
            "redis_db_keys{db=\"db3\"} 1",
            "redis_db_keys_expiring{db=\"db0\"} 1",
            "redis_db_avg_ttl_seconds{db=\"db0\"} 2.5",
            "redis_connected_slave_offset_bytes{slave_ip=\"10.0.0.2\",slave_port=\"6380\",\
             slave_state=\"online\"} 77",
            "redis_connected_slave_lag_seconds{slave_ip=\"10.0.0.2\",slave_port=\"6380\",\
             slave_state=\"online\"} 1",
            "redis_instance_info{role=\"master\",redis_version=\"7.2.4\",redis_build_id=\"\",\
             redis_mode=\"standalone\",os=\"Linux\",maxmemory_policy=\"\",tcp_port=\"\",\
             run_id=\"\",process_id=\"42\",master_replid=\"\"} 1",
        ] {
            assert!(text.lines().any(|l| l == line), "{line} not in\n{text}");
        }
        // Not a field redis_exporter knows, nor a number.
        assert!(!text.contains("redis_role"), "{text}");
        assert!(!text.contains("redis_version "), "{text}");
    }

    #[test]
    fn a_replica_gives_its_link_and_master() {
        let info = "# Replication\r\nrole:slave\r\nmaster_host:10.0.0.1\r\nmaster_port:6379\r\n\
            master_link_status:down\r\nmaster_last_io_seconds_ago:-1\r\n\
            master_sync_in_progress:0\r\nslave_repl_offset:12\r\nslave_read_only:1\r\n";
        let text = render(info, 0.0);
        let master = "master_host=\"10.0.0.1\",master_port=\"6379\"";
        for line in [
            format!("redis_master_link_up{{{master}}} 0"),
            format!("redis_master_last_io_seconds_ago{{{master}}} -1"),
            format!("redis_slave_repl_offset{{{master}}} 12"),
            format!("redis_slave_info{{{master},read_only=\"1\"}} 1"),
        ] {
            assert!(text.lines().any(|l| l == line), "{line} not in\n{text}");
        }
    }

    #[test]
    fn label_values_and_help_are_escaped() {
        let mut out = Families::default();
        out.gauge("x", "a\\b\nc", &[("l", "q\"\\\n")], 1.0);
        assert_eq!(
            out.text(),
            "# HELP redis_x a\\\\b\\nc\n# TYPE redis_x gauge\nredis_x{l=\"q\\\"\\\\\\n\"} 1\n"
        );
    }

    #[test]
    fn a_request_head_ends_at_its_first_empty_line() {
        assert_eq!(head_end(b"GET / HTTP/1.1\r\nA: b\r\n\r\nrest"), Some(20));
        assert_eq!(head_end(b"GET / HTTP/1.0\n\n"), Some(14));
        assert_eq!(head_end(b"GET / HTTP/1.1\r\n"), None);
    }

    #[test]
    fn master_fields_may_be_numbered() {
        assert!(is_master_field("master_host", "host"));
        assert!(is_master_field("master_1_host", "host"));
        assert!(!is_master_field("masterhost", "host"));
        assert!(!is_master_field("master_replid", "host"));
    }
}
