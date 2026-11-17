use hcache_core::engine::{parse_bytes, Config, Engine, Protected};
use hcache_core::mem::CountingAlloc;
use hcache_core::persist::{Fsync, Options, Persistence};
use hcache_core::shard::Policy;
use hcache_server::container;
use hcache_server::tls::{self, ClientCerts};
use hcache_server::{OnShutdown, Server, ServerConfig, TlsListener};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
#[cfg(unix)]
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

/// Counts the heap in use, which is what `maxmemory` is compared with.
#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const USAGE: &str = "usage: hcache-server [--bind ADDR] [--port N] [--workers N] [--shards N] \
    [--databases N] [--hash-tags] [--maxmemory SIZE|auto] [--maxmemory-policy POLICY] \
    [--data FILE] [--memory-dbs N,N...] [--requirepass PASSWORD] \
    [--appendfsync always|everysec|no] \
    [--sync-interval-ms N] [--timeout SECONDS] [--thp off|inherit] [--protected-mode yes|no] \
    [--cluster-enabled yes|no [--cluster-databases N]] [--enable-debug-command no|yes|local] \
    [--tls-port N \
    --tls-cert-file FILE --tls-key-file FILE [--tls-ca-cert-file FILE \
    [--tls-auth-clients yes|optional]]] [--memcached-port N] [--metrics-port N] \
    [--SETTING VALUE...]\n\
       hcache-server healthcheck [--port N]\n\
    Every flag can also come from the environment, as HCACHE_ and its name in capitals with \
    underscores: HCACHE_WORKERS=4 is --workers 4, HCACHE_HASH_TAGS=yes is --hash-tags. The \
    command line wins.\n\
    --SETTING VALUE sets anything CONFIG SET takes, at start (HCACHE_SETTING=VALUE as well): \
    --lazyfree-lazy-user-del no, --notify-keyspace-events KEA, --slowlog-max-len 512.\n\
    --workers 1 runs every command on one thread; more spread connections over threads.\n\
    --memory-dbs 0,2 keeps those databases out of the data file: a cache beside the kept \
    ones, whose writes do not wait for the disk under --appendfsync always.\n\
    --maxmemory auto (the default), under a memory limit such as a container's, keeps a \
    quarter of it, at least 128 MiB but at most half, for the rest of the process; with no \
    limit there is no maxmemory.\n\
    --thp off (the default) turns transparent huge pages off for the process, as Redis and \
    Valkey do: huge pages took a fifth more memory per key for 2% more speed.\n\
    SIGTERM or SIGINT stops the server: every change goes to disk, then the process exits 0. \
    A second one, or one while the data file is loading, stops it at once. SHUTDOWN stops it \
    as the first one does.\n\
    --protected-mode yes (the default) refuses to listen beyond loopback with no password.\n\
    healthcheck exits 0 when a server on this machine answers PING on the plain port: \
    --port, else HCACHE_PORT, else 6379.\n\
    --appendfsync: always answers a write only once it is on disk; everysec (the default) \
    brings the disk up to date every --sync-interval-ms (100); no leaves that to the system.\n\
    --port 0 with --tls-port listens with TLS only.\n\
    --memcached-port N also speaks the Memcached text protocol, its classic and meta \
    commands, on that port of the --bind address: the keys of database 0, the same users \
    (log in as memcached -Y has it, a set whose value is \"user password\"), replicated \
    and kept as every other write. It has no TLS, so not with --port 0.\n\
    --metrics-port N serves Prometheus metrics over HTTP on that port of the --bind address: \
    GET /metrics gives INFO under the names redis_exporter gives it. While the default user \
    needs a password, so does this port (HTTP Basic, a user allowed INFO). It has no TLS.\n\
    --dir DIR and --dbfilename NAME (dump.rdb) name an RDB file, as Redis: SAVE and BGSAVE \
    write it (RDB 11, or RDB 80 for hash fields with deadlines), and a start whose database \
    is still empty loads it. Redis up to 7.2 and every Valkey read and write it.\n\
    --cluster-enabled yes answers as a one-node Redis Cluster that serves all 16384 slots: \
    cluster clients connect, the keys of one command must share a slot (CROSSSLOT), and \
    there are --cluster-databases (1) databases.\n\
    The password is better given in the environment variable HCACHE_REQUIREPASS: other \
    users of this machine can read a command line.";

/// The RDB file in --dir when --dbfilename does not name one, as Redis.
const DEFAULT_RDB_NAME: &str = "dump.rdb";

/// The environment variable that holds the password of the default user.
const PASSWORD_ENV: &str = "HCACHE_REQUIREPASS";
/// What the name of every other environment variable for the server
/// starts with.
const ENV_PREFIX: &str = "HCACHE_";

/// The flags that take a value. Each can also come from the environment.
const VALUE_FLAGS: [&str; 26] = [
    "bind",
    "port",
    "appendfsync",
    "sync-interval-ms",
    "timeout",
    "tls-port",
    "tls-cert-file",
    "tls-key-file",
    "tls-ca-cert-file",
    "tls-auth-clients",
    "workers",
    "shards",
    "databases",
    "maxmemory",
    "protected-mode",
    "thp",
    "maxmemory-policy",
    "data",
    "dir",
    "dbfilename",
    "memory-dbs",
    "cluster-enabled",
    "cluster-databases",
    "enable-debug-command",
    "memcached-port",
    "metrics-port",
];

/// Whether `var` is one of the variables Kubernetes, or a Docker link,
/// makes for a service called hcache: `HCACHE_PORT=tcp://10.0.0.7:6379`,
/// `HCACHE_SERVICE_HOST`, `HCACHE_PORT_6379_TCP_ADDR`. None of them is
/// meant for the server, and the first would stop it from starting.
fn service_link(var: &str, value: &str) -> bool {
    var.starts_with("HCACHE_SERVICE_")
        || var.starts_with("HCACHE_PORT_")
        || (var == "HCACHE_PORT" && value.contains("://"))
}

/// A runtime setting given at start, applied as CONFIG SET would.
struct Setting {
    name: String,
    value: String,
    /// The environment variable it came from; `None` for the command line.
    var: Option<String>,
}

struct Args {
    engine: Config,
    /// `None` for `auto`.
    maxmemory: Option<usize>,
    /// `--thp inherit`: leave transparent huge pages as the system has them.
    keep_thp: bool,
    /// `--protected-mode no`: listening beyond loopback with no password.
    open_ok: bool,
    server: ServerConfig,
    data: Option<PathBuf>,
    persist: Options,
    requirepass: Option<String>,
    /// In the order given: the environment first, so the command line wins.
    settings: Vec<Setting>,
}

/// The variables of the environment whose name starts with `ENV_PREFIX`,
/// but the password, which is read on its own.
fn env_vars() -> Result<Vec<(String, String)>, String> {
    let mut vars = Vec::new();
    for (name, value) in std::env::vars_os() {
        let Some(name) = name.to_str().filter(|n| n.starts_with(ENV_PREFIX)) else {
            continue;
        };
        if name == PASSWORD_ENV {
            continue;
        }
        let value = value
            .into_string()
            .map_err(|_| format!("{name} is not valid Unicode"))?;
        vars.push((name.to_string(), value));
    }
    // The order of the environment is the system's; this one is the same
    // from one start to the next.
    vars.sort();
    Ok(vars)
}

/// What the TLS flags said, until all of them are known.
#[derive(Default)]
struct TlsArgs {
    port: Option<u16>,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    ca: Option<PathBuf>,
    /// `--tls-auth-clients`: `Some(None)` for "no".
    clients: Option<Option<ClientCerts>>,
}

impl TlsArgs {
    fn listener(self, bind: SocketAddr) -> Result<Option<TlsListener>, String> {
        let Some(port) = self.port.filter(|port| *port != 0) else {
            let stray = self.cert.is_some() || self.key.is_some() || self.ca.is_some();
            return match stray || self.clients.is_some() {
                true => Err("the --tls flags need --tls-port".into()),
                false => Ok(None),
            };
        };
        let (Some(cert), Some(key)) = (self.cert, self.key) else {
            return Err("--tls-port needs --tls-cert-file and --tls-key-file".into());
        };
        let clients = match (&self.ca, self.clients) {
            (_, Some(None)) => None,
            // As Redis: with an authority, a certificate is asked for.
            (Some(ca), mode) => Some((
                ca.as_path(),
                mode.flatten().unwrap_or(ClientCerts::Required),
            )),
            (None, Some(Some(_))) => {
                return Err("--tls-auth-clients needs --tls-ca-cert-file".into())
            }
            (None, None) => None,
        };
        let config = tls::server_config(&cert, &key, clients).map_err(|e| e.to_string())?;
        Ok(Some(TlsListener {
            addr: SocketAddr::new(bind.ip(), port),
            config,
        }))
    }
}

/// `env` holds the variables `env_vars` found, `args` the command line.
fn parse_args(
    env: Vec<(String, String)>,
    args: impl Iterator<Item = String>,
) -> Result<Args, String> {
    let mut engine = Config::default();
    let mut maxmemory = None;
    let mut keep_thp = false;
    let mut open_ok = false;
    let mut server = ServerConfig::default();
    let mut data = None;
    let (mut rdb_dir, mut rdb_name) = (None, None);
    let mut persist = Options::default();
    let mut tls = TlsArgs::default();
    let mut port_given = false;
    let mut memcached_port: Option<u16> = None;
    let mut metrics_port: Option<u16> = None;
    let mut cluster_databases = None;
    let mut requirepass = match std::env::var(PASSWORD_ENV) {
        Ok(pass) => {
            // Not passed on to whatever this process starts. The same user
            // can still read it in /proc/<pid>/environ.
            std::env::remove_var(PASSWORD_ENV);
            Some(pass)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{PASSWORD_ENV} is not valid Unicode"))
        }
    };
    let mut settings = Vec::new();
    // The environment as flags, ahead of the command line.
    let mut flags = Vec::new();
    let mut links = Vec::new();
    for (var, value) in env {
        // Set but empty counts as not set, as compose files leave them.
        if value.is_empty() {
            continue;
        }
        if service_link(&var, &value) {
            links.push(var);
            continue;
        }
        // Read as the password on one system and as a setting on another,
        // or not at all: refused rather than guessed.
        if var.eq_ignore_ascii_case(PASSWORD_ENV) {
            return Err(format!("{var}: the password goes in {PASSWORD_ENV}"));
        }
        let name = var[ENV_PREFIX.len()..]
            .to_ascii_lowercase()
            .replace('_', "-");
        match name.as_str() {
            "hash-tags" => match value.as_str() {
                "yes" => flags.push("--hash-tags".to_string()),
                "no" => {}
                _ => return Err(format!("{var}: yes or no")),
            },
            flag if VALUE_FLAGS.contains(&flag) => {
                flags.push(format!("--{name}"));
                flags.push(value);
            }
            _ => settings.push(Setting {
                name,
                value,
                var: Some(var),
            }),
        }
    }
    if !links.is_empty() {
        eprintln!(
            "warning: ignored as Kubernetes or Docker service links: {} \
             (enableServiceLinks: false leaves them out)",
            links.join(" ")
        );
    }
    let mut args = flags.into_iter().chain(args);
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or_else(|| format!("{flag} needs a value"));
        // In any case, as CONFIG names: --Requirepass taken as a setting
        // would be overwritten by the password in the data file.
        match flag.to_ascii_lowercase().as_str() {
            "--bind" => {
                let ip = value()?.parse().map_err(|e| format!("--bind: {e}"))?;
                server.addr = SocketAddr::new(ip, server.addr.port());
            }
            "--port" => {
                port_given = true;
                server
                    .addr
                    .set_port(value()?.parse().map_err(|e| format!("--port: {e}"))?)
            }
            "--appendfsync" => {
                persist.fsync = Fsync::from_name(value()?.as_bytes())
                    .ok_or("--appendfsync: always, everysec or no")?
            }
            "--sync-interval-ms" => {
                let ms: u64 = value()?
                    .parse()
                    .map_err(|e| format!("--sync-interval-ms: {e}"))?;
                if ms == 0 {
                    return Err("--sync-interval-ms: at least 1".into());
                }
                persist.interval = Duration::from_millis(ms);
            }
            "--timeout" => {
                let secs = value()?.parse().map_err(|e| format!("--timeout: {e}"))?;
                server.idle_timeout = Some(Duration::from_secs(secs));
            }
            "--tls-port" => {
                tls.port = Some(value()?.parse().map_err(|e| format!("--tls-port: {e}"))?)
            }
            "--memcached-port" => {
                memcached_port = Some(
                    value()?
                        .parse()
                        .map_err(|e| format!("--memcached-port: {e}"))?,
                )
            }
            "--metrics-port" => {
                metrics_port = Some(
                    value()?
                        .parse()
                        .map_err(|e| format!("--metrics-port: {e}"))?,
                )
            }
            "--tls-cert-file" => tls.cert = Some(PathBuf::from(value()?)),
            "--tls-key-file" => tls.key = Some(PathBuf::from(value()?)),
            "--tls-ca-cert-file" => tls.ca = Some(PathBuf::from(value()?)),
            "--tls-auth-clients" => {
                tls.clients = match value()?.as_str() {
                    "yes" => Some(Some(ClientCerts::Required)),
                    "optional" => Some(Some(ClientCerts::Optional)),
                    "no" => Some(None),
                    _ => return Err("--tls-auth-clients: yes, optional or no".into()),
                }
            }
            "--workers" => {
                server.workers = value()?.parse().map_err(|e| format!("--workers: {e}"))?
            }
            "--shards" => engine.shards = value()?.parse().map_err(|e| format!("--shards: {e}"))?,
            "--databases" => {
                engine.databases = value()?.parse().map_err(|e| format!("--databases: {e}"))?
            }
            "--hash-tags" => engine.hash_tags = true,
            "--maxmemory" => {
                maxmemory = match value()?.as_str() {
                    "auto" => None,
                    size => Some(parse_bytes(size.as_bytes()).ok_or(
                        "--maxmemory: not a size (examples: 268435456, 256mb, 1gb) or auto",
                    )?),
                }
            }
            "--protected-mode" => {
                open_ok = match value()?.as_str() {
                    "yes" => false,
                    "no" => true,
                    _ => return Err("--protected-mode: yes or no".into()),
                }
            }
            "--thp" => {
                keep_thp = match value()?.as_str() {
                    "off" => false,
                    "inherit" => true,
                    _ => return Err("--thp: off or inherit".into()),
                }
            }
            "--maxmemory-policy" => {
                engine.maxmemory_policy = Policy::from_name(value()?.as_bytes())
                    .ok_or("--maxmemory-policy: unknown policy")?
            }
            "--data" => data = Some(PathBuf::from(value()?)),
            "--dir" => rdb_dir = Some(PathBuf::from(value()?)),
            "--dbfilename" => {
                let name = value()?;
                // As Redis: a name, the place is --dir. On Windows a colon
                // names a drive or a stream of the file instead.
                let mut parts = Path::new(&name).components();
                let plain = matches!(parts.next(), Some(Component::Normal(part)) if part == name.as_str())
                    && parts.next().is_none();
                if !plain || name.contains(['/', '\\', ':']) {
                    return Err("--dbfilename: a file name, without a directory".into());
                }
                rdb_name = Some(name);
            }
            "--memory-dbs" => {
                engine.memory_dbs = value()?
                    .split(',')
                    .map(|db| db.trim().parse())
                    .collect::<Result<_, _>>()
                    .map_err(|e| format!("--memory-dbs: {e} (example: 0,2)"))?
            }
            "--requirepass" => requirepass = Some(value()?),
            "--cluster-enabled" => {
                engine.cluster = match value()?.as_str() {
                    "yes" => true,
                    "no" => false,
                    _ => return Err("--cluster-enabled: yes or no".into()),
                }
            }
            "--enable-debug-command" => {
                engine.debug_command = Protected::from_name(&value()?)
                    .ok_or("--enable-debug-command: no, yes or local")?
            }
            "--cluster-databases" => {
                let n: usize = value()?
                    .parse()
                    .map_err(|e| format!("--cluster-databases: {e}"))?;
                if n == 0 {
                    return Err("--cluster-databases: at least 1".into());
                }
                cluster_databases = Some(n);
            }
            other => match other.strip_prefix("--").filter(|name| !name.is_empty()) {
                Some(name) => settings.push(Setting {
                    name: name.to_string(),
                    value: value()?,
                    var: None,
                }),
                None => return Err(format!("unknown argument {other}")),
            },
        }
    }
    if rdb_dir.is_some() || rdb_name.is_some() {
        let dir = rdb_dir.unwrap_or_else(|| PathBuf::from("."));
        engine.rdb_file = Some(dir.join(rdb_name.as_deref().unwrap_or(DEFAULT_RDB_NAME)));
    }
    // As Valkey: a cluster has `cluster-databases` (1); --databases does
    // not count.
    if engine.cluster {
        engine.databases = cluster_databases.unwrap_or(1);
    }
    if let Some(db) = engine.memory_dbs.iter().find(|db| **db >= engine.databases) {
        return Err(format!(
            "--memory-dbs: database {db} does not exist (--databases {})",
            engine.databases
        ));
    }
    server.tls = tls.listener(server.addr)?;
    server.memcached = memcached_port.map(|port| SocketAddr::new(server.addr.ip(), port));
    server.metrics = metrics_port.map(|port| SocketAddr::new(server.addr.ip(), port));
    // As Redis: port 0 beside a TLS port means TLS only.
    server.plain = !(port_given && server.addr.port() == 0);
    // It has no TLS: beside TLS only, it would be the way around it.
    if !server.plain && server.memcached.is_some() {
        return Err("--memcached-port speaks no TLS, so it cannot go with --port 0".into());
    }
    if !server.plain && server.tls.is_none() {
        return Err("--port 0 leaves nothing to listen on without --tls-port".into());
    }
    Ok(Args {
        engine,
        maxmemory,
        keep_thp,
        open_ok,
        server,
        data,
        persist,
        requirepass: requirepass.filter(|pass| !pass.is_empty()),
        settings,
    })
}

/// Load the RDB file at start, as Redis loads its dump. Nothing to load
/// is fine; so is a database that holds keys already, from the data file,
/// which then wins.
fn load_rdb_file(engine: &Engine, path: &Path) -> Result<(), String> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("cannot open {}: {e}", path.display())),
    };
    if !engine.is_empty() {
        println!(
            "{} not loaded: the database holds keys already (from --data)",
            path.display()
        );
        return Ok(());
    }
    let stats = engine
        .load_rdb(file)
        .map_err(|e| format!("cannot load {}: {e}", path.display()))?;
    println!(
        "loaded {} (RDB {}): {} keys, {} past their deadline left out, {} libraries",
        path.display(),
        stats.version,
        stats.keys,
        stats.expired,
        stats.libraries
    );
    Ok(())
}

/// Apply the settings given at start. One the command line names wrongly
/// stops the start; one from the environment is only warned about when
/// its name is unknown, since another program may own that variable.
fn apply_settings(engine: &Engine, settings: Vec<Setting>) -> Result<(), String> {
    for Setting { name, value, var } in settings {
        let Err(e) = engine.configure(&name, &value) else {
            continue;
        };
        match var {
            Some(var) if e.starts_with("unknown setting") => {
                eprintln!("warning: {var} is no flag or setting of hcache-server; ignored")
            }
            Some(var) => return Err(format!("{var}: {e}")),
            None if e.starts_with("unknown setting") => {
                return Err(format!("unknown argument --{name}"))
            }
            None => return Err(format!("--{name}: {e}")),
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let mut argv = std::env::args().skip(1).peekable();
    if argv.peek().map(String::as_str) == Some("healthcheck") {
        argv.next();
        return healthcheck(argv);
    }
    // SHUTDOWN, and on Unix the first SIGTERM or SIGINT, send here.
    let (asker, stop) = std::sync::mpsc::channel();
    // Before any other thread starts, so that every thread leaves them to
    // the one that waits for them.
    #[cfg(unix)]
    let serving = match container::Signals::block() {
        Ok(signals) => watch_signals(signals, asker.clone()),
        Err(e) => {
            eprintln!("cannot hold back SIGTERM and SIGINT: {e}");
            return ExitCode::FAILURE;
        }
    };
    let env = match env_vars() {
        Ok(env) => env,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let names: Vec<String> = env.iter().map(|(name, _)| name.clone()).collect();
    if !names.is_empty() {
        // Names only: a value may be a secret.
        println!("from the environment: {}", names.join(" "));
    }
    let Args {
        engine: mut engine_cfg,
        maxmemory,
        keep_thp,
        open_ok,
        server: mut server_cfg,
        data,
        persist,
        requirepass,
        settings,
    } = match parse_args(env, argv) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    // Before the heap grows: pages already huge stay so.
    if !keep_thp {
        if let Err(e) = container::disable_thp() {
            eprintln!("warning: cannot turn transparent huge pages off: {e}");
        }
    }
    match (maxmemory, container::memory_limit()) {
        (Some(bytes), Some(limit)) if bytes == 0 || bytes as u64 > limit / 10 * 9 => eprintln!(
            "warning: maxmemory {bytes} under a memory limit of {limit} bytes leaves the rest \
             of the process too little, and the system may kill it (--maxmemory auto fits)"
        ),
        (None, Some(limit)) => {
            let auto = container::auto_maxmemory(limit);
            engine_cfg.maxmemory = usize::try_from(auto).unwrap_or(usize::MAX);
            println!(
                "maxmemory {} of the {limit} bytes the memory limit allows",
                engine_cfg.maxmemory
            );
        }
        _ => {}
    }
    if let Some(bytes) = maxmemory {
        engine_cfg.maxmemory = bytes;
    }
    let workers = server_cfg.workers;
    let engine = Arc::new(Engine::new(engine_cfg));
    if let Err(e) = apply_settings(&engine, settings) {
        eprintln!("{e}\n{USAGE}");
        return ExitCode::from(2);
    }
    // Loaded before the first connection; kept until the process ends.
    let persistence = match data {
        Some(path) => match Persistence::open(engine.clone(), &path, persist) {
            Ok(persistence) => {
                let report = persistence.report();
                println!(
                    "loaded {}: {} records in {} frames, {} bytes of a torn tail dropped",
                    path.display(),
                    report.records,
                    report.frames,
                    report.dropped_bytes
                );
                Some(persistence)
            }
            Err(e) => {
                eprintln!("cannot open {}: {e}", path.display());
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    if let Some(path) = engine.rdb_file().map(Path::to_path_buf) {
        if let Err(e) = load_rdb_file(&engine, &path) {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    }
    // After the load: the password given now wins over the one in the file.
    // Only its hash is kept from here on.
    if let Some(pass) = requirepass {
        engine.acl().set_requirepass(Some(pass.as_bytes()));
    }
    let locked = !engine.acl().default_user().open();
    if !locked && !server_cfg.addr.ip().is_loopback() {
        if !open_ok {
            eprintln!(
                "refusing to listen on {} without a password: anyone who can reach it could \
                 read and change everything. Set {PASSWORD_ENV}, bind to loopback, or start \
                 with --protected-mode no",
                server_cfg.addr.ip()
            );
            return ExitCode::FAILURE;
        }
        eprintln!(
            "warning: listening on {} without a password; anyone who can reach it can read \
             and change everything (set {PASSWORD_ENV})",
            server_cfg.addr.ip()
        );
    }
    server_cfg.on_shutdown = Some(OnShutdown(Arc::new(move || {
        // A signal (0 for none): main does the rest.
        let _ = asker.send(0);
    })));
    let server = match Server::start(engine.clone(), server_cfg) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("cannot start: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "hcache spike listening on {} ({} workers, {} shards)",
        server.local_addr(),
        workers,
        engine.config().shards
    );
    if let Some(addr) = server.tls_addr() {
        println!("TLS on {addr}");
    }
    if let Some(addr) = server.memcached_addr() {
        println!("Memcached on {addr}");
    }
    if let Some(addr) = server.metrics_addr() {
        println!("metrics on http://{addr}/metrics");
    }
    if locked {
        println!("a password is required (AUTH)");
    }
    #[cfg(unix)]
    {
        serving.store(true, SeqCst);
        stop_on_signal(&stop, server, persistence)
    }
    #[cfg(not(unix))]
    {
        // Runs until SHUTDOWN, or until the process is killed.
        if stop.recv().is_err() {
            let _running = (server, persistence);
            loop {
                std::thread::park();
            }
        }
        server.shutdown();
        drop(persistence);
        println!("stopped");
        ExitCode::SUCCESS
    }
}

/// How long stopping may take before the process leaves anyway: inside
/// the 30 seconds Docker and Kubernetes wait by default before SIGKILL.
#[cfg(unix)]
const STOP_BUDGET: Duration = Duration::from_secs(20);

/// Takes SIGTERM and SIGINT for the whole process, on a thread of its own.
/// Until the returned flag is set, while the data file loads, a signal
/// ends the process at once, as being killed would. After, the first one
/// goes to the returned channel and a second ends the process at once.
#[cfg(unix)]
fn watch_signals(signals: container::Signals, stop: Sender<i32>) -> Arc<AtomicBool> {
    let serving = Arc::new(AtomicBool::new(false));
    let up = serving.clone();
    std::thread::spawn(move || {
        let mut asked = false;
        loop {
            let signal = match signals.wait() {
                Ok(signal) => signal,
                Err(e) => {
                    eprintln!("cannot wait for signals: {e}");
                    return;
                }
            };
            let name = container::signal_name(signal);
            if asked || !up.load(SeqCst) {
                eprintln!("{name} received: stopping now");
                std::process::exit(128 + signal);
            }
            asked = true;
            println!("{name} received: stopping (a second one stops at once)");
            // Gone only once main has stopped waiting for it.
            let _ = stop.send(signal);
        }
    });
    serving
}

/// Runs until SIGTERM, SIGINT or SHUTDOWN, then stops: no new connections,
/// the workers end, every change goes to disk, and the process exits 0.
/// Stopping for longer than `STOP_BUDGET` ends it at once.
#[cfg(unix)]
fn stop_on_signal(
    stop: &Receiver<i32>,
    server: Server,
    persistence: Option<Persistence>,
) -> ExitCode {
    if stop.recv().is_err() {
        // Nothing waits for signals: run until killed, as before.
        let _running = (server, persistence);
        loop {
            std::thread::park();
        }
    }
    let (done, stopped) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        server.shutdown();
        // Writes what is left and waits for the disk.
        drop(persistence);
        let _ = done.send(());
    });
    match stopped.recv_timeout(STOP_BUDGET) {
        Ok(()) => {
            println!("stopped");
            ExitCode::SUCCESS
        }
        Err(_) => {
            eprintln!(
                "stopping took over {} seconds: leaving without waiting",
                STOP_BUDGET.as_secs()
            );
            ExitCode::FAILURE
        }
    }
}

/// How long the health check waits for the server, at each step.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(2);

/// `hcache-server healthcheck [--port N]`: exits 0 when a server on this
/// machine answers PING. For container health checks, in an image with
/// no shell and no client.
fn healthcheck(mut args: impl Iterator<Item = String>) -> ExitCode {
    let mut port = health_port(std::env::var("HCACHE_PORT").ok().as_deref());
    while let Some(flag) = args.next() {
        let value = args.next().filter(|_| flag == "--port");
        match value.and_then(|value| value.parse().ok()) {
            Some(given) => port = given,
            None => {
                eprintln!("usage: hcache-server healthcheck [--port N]");
                return ExitCode::from(2);
            }
        }
    }
    match ping(port) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("unhealthy: {e}");
            ExitCode::FAILURE
        }
    }
}

/// The port healthcheck asks when no --port is given: the server's, if a
/// container set it with HCACHE_PORT, else 6379. A service link
/// (`tcp://...`) is not a port.
fn health_port(var: Option<&str>) -> u16 {
    var.and_then(|port| port.parse().ok()).unwrap_or(6379)
}

fn ping(port: u16) -> std::io::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&addr, HEALTH_TIMEOUT)?;
    stream.set_read_timeout(Some(HEALTH_TIMEOUT))?;
    stream.set_write_timeout(Some(HEALTH_TIMEOUT))?;
    stream.write_all(b"*1\r\n$4\r\nPING\r\n")?;
    let mut reply = [0; 64];
    let n = stream.read(&mut reply)?;
    let reply = &reply[..n];
    // Asking for a password is an answer too: the server is up.
    if reply.starts_with(b"+PONG") || reply.starts_with(b"-NOAUTH") {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "unexpected reply {:?}",
        String::from_utf8_lossy(reply)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hcache_core::cmd::Cmd;
    use hcache_core::lazyfree::Reason;
    use hcache_core::resp::Reply;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn cli(args: &[&str]) -> impl Iterator<Item = String> {
        args.iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn parse(vars: &[(&str, &str)], args: &[&str]) -> Result<Args, String> {
        parse_args(env(vars), cli(args))
    }

    fn failure(vars: &[(&str, &str)], args: &[&str]) -> String {
        match parse(vars, args) {
            Ok(_) => panic!("{vars:?} {args:?} was taken"),
            Err(e) => e,
        }
    }

    #[test]
    fn the_environment_gives_flags_and_the_command_line_wins() {
        let args = parse(
            &[
                ("HCACHE_WORKERS", "2"),
                ("HCACHE_SHARDS", "8"),
                ("HCACHE_APPENDFSYNC", "always"),
                ("HCACHE_SYNC_INTERVAL_MS", "250"),
            ],
            &["--workers", "3"],
        )
        .unwrap();
        assert_eq!(args.server.workers, 3);
        assert_eq!(args.engine.shards, 8);
        assert_eq!(args.persist.fsync, Fsync::Always);
        assert_eq!(args.persist.interval, Duration::from_millis(250));
        assert!(args.settings.is_empty());
    }

    #[test]
    fn hash_tags_take_yes_or_no_and_an_empty_value_is_no_value() {
        assert!(
            parse(&[("HCACHE_HASH_TAGS", "yes")], &[])
                .unwrap()
                .engine
                .hash_tags
        );
        assert!(
            !parse(&[("HCACHE_HASH_TAGS", "no")], &[])
                .unwrap()
                .engine
                .hash_tags
        );
        assert!(failure(&[("HCACHE_HASH_TAGS", "on")], &[]).contains("HCACHE_HASH_TAGS"));
        let args = parse(
            &[
                ("HCACHE_WORKERS", ""),
                ("HCACHE_LAZYFREE_LAZY_USER_DEL", ""),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(args.server.workers, ServerConfig::default().workers);
        assert!(args.settings.is_empty());
        assert!(failure(&[("HCACHE_WORKERS", "many")], &[]).contains("--workers"));
    }

    #[test]
    fn service_links_of_kubernetes_are_not_flags() {
        let args = parse(
            &[
                ("HCACHE_PORT", "tcp://10.0.0.7:6379"),
                ("HCACHE_PORT_6379_TCP_ADDR", "10.0.0.7"),
                ("HCACHE_SERVICE_HOST", "10.0.0.7"),
                ("HCACHE_SERVICE_PORT", "6379"),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(args.server.addr.port(), ServerConfig::default().addr.port());
        assert!(args.settings.is_empty());
        let args = parse(&[("HCACHE_PORT", "7000")], &[]).unwrap();
        assert_eq!(args.server.addr.port(), 7000);
    }

    #[test]
    fn healthcheck_asks_the_port_the_environment_gave_the_server() {
        assert_eq!(health_port(Some("7000")), 7000);
        assert_eq!(health_port(Some("tcp://10.0.0.7:6379")), 6379);
        assert_eq!(health_port(None), 6379);
    }

    #[test]
    fn flags_match_in_any_case_and_the_password_only_in_capitals() {
        let args = parse(&[], &["--Requirepass", "pw", "--APPENDFSYNC", "always"]).unwrap();
        assert_eq!(args.requirepass.as_deref(), Some("pw"));
        assert_eq!(args.persist.fsync, Fsync::Always);
        assert!(args.settings.is_empty());
        let e = failure(&[("HCACHE_requirepass", "s3cret")], &[]);
        assert!(
            e.contains("HCACHE_REQUIREPASS") && !e.contains("s3cret"),
            "{e}"
        );
    }

    #[test]
    fn memory_dbs_must_name_databases_that_exist() {
        let args = parse(&[], &["--memory-dbs", "0, 3", "--databases", "4"]).unwrap();
        assert_eq!(args.engine.memory_dbs, vec![0, 3]);
        let args = parse(&[("HCACHE_MEMORY_DBS", "1")], &[]).unwrap();
        assert_eq!(args.engine.memory_dbs, vec![1]);
        assert!(failure(&[], &["--memory-dbs", "4", "--databases", "4"]).contains("database 4"));
        assert!(failure(&[], &["--memory-dbs", "one"]).contains("--memory-dbs"));
        assert!(failure(&[], &["--memory-dbs", "-1"]).contains("--memory-dbs"));
    }

    #[test]
    fn the_memcached_port_does_not_open_a_tls_only_server() {
        let args = parse(&[], &["--memcached-port", "11211"]).unwrap();
        assert_eq!(args.server.memcached.map(|a| a.port()), Some(11211));
        let e = failure(&[], &["--port", "0", "--memcached-port", "11211"]);
        assert!(e.contains("--memcached-port"), "{e}");
    }

    #[test]
    fn other_names_are_settings_applied_once_the_engine_is_up() {
        let args = parse(
            &[
                ("HCACHE_LAZYFREE_LAZY_USER_DEL", "no"),
                ("HCACHE_SLOWLOG_MAX_LEN", "5"),
                ("HCACHE_NOT_OURS", "x"),
            ],
            &["--lazyfree-lazy-user-flush", "no", "--slowlog-max-len", "7"],
        )
        .unwrap();
        let engine = Engine::new(Config::default());
        apply_settings(&engine, args.settings).unwrap();
        let lazy = engine.lazy_free();
        assert!(!lazy.is_on(Reason::UserDel) && !lazy.is_on(Reason::UserFlush));
        assert!(lazy.is_on(Reason::Expire));
        // The command line came after the environment.
        assert_eq!(
            engine.exec_conn(&Cmd::ConfigGet(b"slowlog-max-len".to_vec())),
            Reply::Array(vec![Reply::bulk("slowlog-max-len"), Reply::bulk("7")])
        );
    }

    #[test]
    fn a_wrong_setting_stops_the_start() {
        let engine = Engine::new(Config::default());
        let apply = |vars: &[(&str, &str)], args: &[&str]| {
            apply_settings(&engine, parse(vars, args).unwrap().settings)
        };
        assert_eq!(
            apply(&[], &["--nope", "1"]),
            Err("unknown argument --nope".into())
        );
        assert!(apply(&[], &["--lazyfree-lazy-user-del", "maybe"])
            .unwrap_err()
            .starts_with("--lazyfree-lazy-user-del: "));
        assert!(apply(&[("HCACHE_LAZYFREE_LAZY_EXPIRE", "2")], &[])
            .unwrap_err()
            .starts_with("HCACHE_LAZYFREE_LAZY_EXPIRE: "));
        assert!(apply(&[], &["--memory-dbs-x", "1"]).is_err());
        // A glob is no setting's name.
        assert!(apply(&[], &["--lazyfree-*", "no"]).is_err());
        assert!(failure(&[], &["--slowlog-max-len"]).contains("needs a value"));
        assert!(failure(&[], &["stray"]).contains("unknown argument stray"));
    }

    #[test]
    fn dir_and_dbfilename_name_the_rdb_file() {
        let file =
            |vars: &[(&str, &str)], args: &[&str]| parse(vars, args).unwrap().engine.rdb_file;
        assert_eq!(file(&[], &[]), None);
        let both = file(&[], &["--dir", "data", "--dbfilename", "x.rdb"]);
        assert_eq!(both, Some(PathBuf::from("data").join("x.rdb")));
        assert_eq!(
            file(&[], &["--dir", "data"]),
            Some(PathBuf::from("data").join("dump.rdb"))
        );
        let env = file(&[("HCACHE_DBFILENAME", "y.rdb")], &[]);
        assert_eq!(env, Some(PathBuf::from(".").join("y.rdb")));
        assert!(failure(&[], &["--dbfilename", "a/b.rdb"]).contains("without a directory"));
        for bad in ["..", ".", "", "C:x.rdb", "x.rdb:ads", "a\\b"] {
            let e = failure(&[], &["--dbfilename", bad]);
            assert!(e.contains("without a directory"), "{bad}: {e}");
        }
    }

    #[test]
    fn a_start_loads_the_rdb_file_only_into_an_empty_database() {
        let dir = std::env::temp_dir().join(format!("hcache-start-rdb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dump.rdb");
        let set = |engine: &Engine, key: &str, value: &str| {
            let slot = engine.gate().register().unwrap();
            let args = vec![b"SET".to_vec(), key.into(), value.into()];
            engine.execute_blocking(slot, Cmd::parse(args).unwrap());
            engine.gate().unregister(slot);
        };
        let get = |engine: &Engine, key: &str| {
            let slot = engine.gate().register().unwrap();
            let reply = engine
                .execute_blocking(slot, Cmd::parse(vec![b"GET".to_vec(), key.into()]).unwrap());
            engine.gate().unregister(slot);
            reply
        };
        let source = Engine::new(Config::default());
        set(&source, "k", "from the file");
        std::fs::write(&path, source.save_rdb()).unwrap();

        let empty = Engine::new(Config::default());
        load_rdb_file(&empty, &path).unwrap();
        assert_eq!(get(&empty, "k"), Reply::bulk("from the file"));
        let kept = Engine::new(Config::default());
        set(&kept, "k", "from the data file");
        load_rdb_file(&kept, &path).unwrap();
        assert_eq!(get(&kept, "k"), Reply::bulk("from the data file"));
        load_rdb_file(&Engine::new(Config::default()), &dir.join("none.rdb")).unwrap();
        std::fs::write(&path, b"REDIS0011\xff").unwrap();
        let e = load_rdb_file(&Engine::new(Config::default()), &path).unwrap_err();
        assert!(e.contains("cannot load"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
