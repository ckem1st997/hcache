//! Command parsing: raw arguments in, a typed `Cmd` out.

use crate::clock::unix_ms;
use crate::cluster;
use crate::ops::{self, KeyOp, MultiOp};
use crate::resp::Shape;
use crate::resp::{parse_i64, Reply};

pub type Bytes = Vec<u8>;

/// A write sent to a replica.
pub const READONLY: &str = "READONLY You can't write against a read only replica.";

/// What a command does to a key's deadline. Times are in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expire {
    Keep,
    Clear,
    /// This long from now.
    In(i64),
    /// At this Unix time.
    At(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cond {
    Nx,
    Xx,
    Gt,
    Lt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOpts {
    /// Only `Nx` and `Xx` apply to SET.
    pub cond: Option<Cond>,
    pub expire: Expire,
    /// Reply with the old value instead of OK.
    pub get: bool,
    /// IFEQ: only over a string that holds this value.
    pub ifeq: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Txn {
    Multi,
    Exec,
    Discard,
    Watch(Vec<Bytes>),
    Unwatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sub {
    Subscribe(Vec<Bytes>),
    /// No channels means all of them.
    Unsubscribe(Vec<Bytes>),
    PSubscribe(Vec<Bytes>),
    PUnsubscribe(Vec<Bytes>),
    /// Channels of their own, apart from those of SUBSCRIBE: in a cluster
    /// each lives on the node of its slot.
    SSubscribe(Vec<Bytes>),
    /// No channels means all of them.
    SUnsubscribe(Vec<Bytes>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubQuery {
    Channels(Option<Bytes>),
    NumSub(Vec<Bytes>),
    NumPat,
    ShardChannels(Option<Bytes>),
    ShardNumSub(Vec<Bytes>),
}

/// A command moves through several layers between the parser and the
/// shard. Up to 128 bytes it is copied with a few moves in place; above
/// that every copy is a call to `memmove`, whose size checks were a fifth
/// of the branch mispredictions of a GET. The rare large variants are
/// boxed to stay below.
const _: () = assert!(std::mem::size_of::<Cmd>() <= 128);

#[derive(Debug, Clone, PartialEq)]
pub enum Cmd {
    // Connection level: answered without touching the keyspace.
    Ping(Option<Bytes>),
    Echo(Bytes),
    Quit,
    /// A command whose reply is fixed at parse time (CLIENT, COMMAND, ...).
    Static(Reply),
    /// INFO: the sections asked for, lower case; none for the default.
    Info(Vec<Bytes>),
    ScriptKill,
    Dbsize,
    /// HELLO, with the protocol version it asks for (none: keep the
    /// current one) and the user and password of its AUTH option.
    Hello(Option<u8>, Option<(Bytes, Bytes)>),
    /// AUTH: the user (none: "default") and the password.
    Auth(Option<Bytes>, Bytes),
    /// ACL with all its arguments, "ACL" included.
    Acl(Vec<Bytes>),
    /// COMMAND with all its arguments, "COMMAND" included.
    Command(Vec<Bytes>),
    /// CONFIG GET with its pattern.
    ConfigGet(Bytes),
    ConfigSet(Vec<(Bytes, Bytes)>),
    /// BGSAVE and BGREWRITEAOF: rewrite the data file soon.
    BgSave,
    /// SAVE: write every change and wait until it is on disk.
    Save,
    LastSave,
    Time,
    /// WAITAOF, with whether numlocal is 1. There are no replicas to wait
    /// for, so numreplicas only matters to the check of its value.
    WaitAof(bool),
    /// A CLIENT subcommand about the connection itself.
    Client(Box<ClientCmd>),
    /// MONITOR: the connection is sent every command the server gets.
    Monitor,
    SlowLog(SlowLogCmd),
    /// RESET: back to the state of a new connection.
    Reset,
    /// BLPOP and the like: the command to try, and how many milliseconds a
    /// connection waits for it to find something (0: without limit). Only a
    /// server connection outside MULTI waits; everywhere else it is the
    /// command alone.
    Block(Box<MultiOp>, u64),
    ScriptExists(Vec<Bytes>),
    ScriptFlush,
    /// (P|S)SUBSCRIBE and (P|S)UNSUBSCRIBE; handled by the connection.
    Sub(Sub),
    Publish(Bytes, Bytes),
    /// SPUBLISH: to the subscribers of SSUBSCRIBE.
    SPublish(Bytes, Bytes),
    PubSub(PubSubQuery),
    /// Transaction control; handled per connection (`txn.rs`).
    Txn(Txn),
    // Keyspace, one key.
    Get(Bytes),
    Set(Bytes, Bytes),
    /// SET with options, SETEX, PSETEX, GETSET.
    SetOpts(Bytes, Bytes, SetOpts),
    SetNx(Bytes, Bytes),
    GetDel(Bytes),
    GetEx(Bytes, Expire),
    Append(Bytes, Bytes),
    Strlen(Bytes),
    /// EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT.
    Expire {
        key: Bytes,
        when: Expire,
        cond: Option<Cond>,
    },
    /// TTL, or PTTL when the flag is set.
    Ttl(Bytes, bool),
    /// EXPIRETIME, or PEXPIRETIME when the flag is set.
    ExpireTime(Bytes, bool),
    Persist(Bytes),
    Type(Bytes),
    HExists(Bytes, Bytes),
    HMGet(Bytes, Vec<Bytes>),
    HSetNx(Bytes, Bytes, Bytes),
    HIncrBy(Bytes, Bytes, i64),
    HScan {
        key: Bytes,
        cursor: u64,
        pattern: Option<Bytes>,
        count: usize,
        novalues: bool,
    },
    IncrBy(Bytes, i64),
    HSet(Bytes, Vec<(Bytes, Bytes)>),
    /// HSET that replies OK instead of the number of new fields.
    HMSet(Bytes, Vec<(Bytes, Bytes)>),
    HGet(Bytes, Bytes),
    HDel(Bytes, Vec<Bytes>),
    HLen(Bytes),
    HGetAll(Bytes),
    /// One of the single-key commands of `ops.rs`.
    Key(Bytes, KeyOp),
    // Keyspace, keys known up front.
    /// One of the multi-key commands of `ops.rs`.
    Multi(Box<MultiOp>),
    Del(Vec<Bytes>),
    /// DEL that frees large values on another thread.
    Unlink(Vec<Bytes>),
    Exists(Vec<Bytes>),
    MSet(Vec<(Bytes, Bytes)>),
    MGet(Vec<Bytes>),
    // Whole keyspace, in chunks.
    RandomKey,
    Keys(Bytes),
    Scan {
        cursor: u64,
        pattern: Option<Bytes>,
        count: usize,
        ty: Option<Bytes>,
    },
    /// Handled by `TxnState`, which knows the connection's database.
    Select(i64),
    // Needs the node to itself.
    /// FLUSHALL and FLUSHDB: `Some(true)` for ASYNC, `Some(false)` for
    /// SYNC, `None` for what `lazyfree-lazy-user-flush` says.
    FlushAll(Option<bool>),
    FlushDb(Option<bool>),
    Move {
        key: Bytes,
        db: i64,
    },
    /// SWAPDB: the two databases trade their keys.
    SwapDb(i64, i64),
    /// CLUSTER, its subcommand and arity checked (`cluster::parse`).
    Cluster(Vec<Bytes>),
    /// ASKING: no slot ever moves away from a one-node cluster.
    Asking,
    Role,
    /// PSYNC: a replica asks to go on from an offset (counted from 1) of
    /// the stream with that ID; "?" and -1 ask for the whole dataset.
    /// With FAILOVER its primary hands over: this replica becomes the
    /// primary first.
    Psync(Bytes, i64, bool),
    /// FAILOVER: hand the primary's role to a replica, or stop a handover
    /// that is under way.
    Failover(Failover),
    /// SYNC: the whole dataset and then the stream, without IDs.
    Sync,
    /// REPLCONF and its arguments.
    Replconf(Vec<Bytes>),
    /// REPLICAOF (SLAVEOF): the primary to follow; `None` for NO ONE.
    ReplicaOf(Option<(String, u16)>),
    /// WAIT: for how many replicas, and how many milliseconds (0: no
    /// limit).
    Wait(i64, u64),
    /// SHUTDOWN: `abort` for ABORT, `failover` for FAILOVER.
    Shutdown {
        abort: bool,
        failover: bool,
    },
    /// DEBUG and its arguments, once `enable-debug-command` allows it.
    Debug(Vec<Bytes>),
    /// A queued transaction, with the database and version of each watched
    /// key.
    ExecBlock {
        queue: Vec<Cmd>,
        watched: Vec<(u16, Bytes, u64)>,
    },
    /// A command of the given database, for the places that cannot tell
    /// from the caller: a transaction's queue, the executor's job list.
    In(u16, Box<Cmd>),
    Eval {
        script: Bytes,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        /// EVAL_RO: the script may not write.
        read_only: bool,
    },
    EvalSha {
        sha: Bytes,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        read_only: bool,
    },
    ScriptLoad(Bytes),
    /// FUNCTION with all its arguments, "FUNCTION" included.
    Function(Vec<Bytes>),
    /// FCALL, or FCALL_RO when `read_only`.
    FCall {
        name: Bytes,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        read_only: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientCmd {
    SetName(Bytes),
    GetName,
    Id,
    Info,
    List,
    /// CLIENT SETINFO: the library's version, or else its name.
    SetInfo {
        version: bool,
        value: Bytes,
    },
    /// CLIENT TRACKING: on with these options, or off.
    Tracking(Option<Tracking>),
    /// CLIENT CACHING yes or no, for the next command.
    Caching(bool),
    TrackingInfo,
    GetRedir,
    /// CLIENT KILL: close the connections that match.
    Kill(KillFilter),
    /// CLIENT PAUSE: hold back the commands of the clients that write (or,
    /// with `all`, every command) for this many milliseconds.
    Pause {
        ms: u64,
        all: bool,
    },
    Unpause,
}

/// CLIENT KILL: which connections to close. Every filter given must match.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KillFilter {
    pub id: Option<u64>,
    pub addr: Option<Bytes>,
    pub laddr: Option<Bytes>,
    pub user: Option<Bytes>,
    /// TYPE: `Some(true)` for connections with subscriptions,
    /// `Some(false)` for the others.
    pub pubsub: Option<bool>,
    /// TYPE replica (slave): the links of this node's replicas.
    pub replica: bool,
    /// TYPE master: the link to this node's primary.
    pub master: bool,
    /// Only connections that are at least this many seconds old.
    pub maxage: Option<u64>,
    /// Leave the caller's own connection alone.
    pub skipme: bool,
    /// The old form, `CLIENT KILL addr:port`: it answers OK or an error,
    /// not a count.
    pub legacy: bool,
}

fn parse_kill(pairs: &[Bytes]) -> Result<KillFilter, Reply> {
    let mut filter = KillFilter {
        skipme: true,
        ..KillFilter::default()
    };
    for [name, value] in pairs.as_chunks::<2>().0 {
        let name = name.to_ascii_uppercase();
        match name.as_slice() {
            b"ID" => {
                let id = crate::resp::parse_i64(value).and_then(|id| u64::try_from(id).ok());
                match id {
                    Some(id) if id > 0 => filter.id = Some(id),
                    _ => return Err(Reply::err("ERR client-id should be greater than 0")),
                }
            }
            b"ADDR" => filter.addr = Some(value.clone()),
            b"LADDR" => filter.laddr = Some(value.clone()),
            b"USER" => filter.user = Some(value.clone()),
            b"TYPE" => match value.to_ascii_lowercase().as_slice() {
                b"normal" => filter.pubsub = Some(false),
                b"pubsub" => filter.pubsub = Some(true),
                b"master" => filter.master = true,
                b"replica" | b"slave" => filter.replica = true,
                _ => {
                    return Err(Reply::Error(format!(
                        "ERR Unknown client type '{}'",
                        String::from_utf8_lossy(&value[..value.len().min(64)])
                    )))
                }
            },
            b"MAXAGE" => {
                let age = crate::resp::parse_i64(value).and_then(|n| u64::try_from(n).ok());
                filter.maxage = Some(age.ok_or_else(syntax)?);
            }
            b"SKIPME" if value.eq_ignore_ascii_case(b"YES") => filter.skipme = true,
            b"SKIPME" if value.eq_ignore_ascii_case(b"NO") => filter.skipme = false,
            _ => return Err(syntax()),
        }
    }
    Ok(filter)
}

/// CLIENT TRACKING ON with its options.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tracking {
    /// Send the invalidations to the connection with this id instead.
    pub redirect: Option<u64>,
    /// Report every change under the prefixes, read or not.
    pub bcast: bool,
    pub prefixes: Vec<Bytes>,
    /// Remember keys only after CLIENT CACHING yes.
    pub optin: bool,
    /// Remember keys unless CLIENT CACHING no came first.
    pub optout: bool,
    /// Say nothing about changes this connection made itself.
    pub noloop: bool,
}

/// The logs of COMMANDLOG; SLOWLOG is the first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogKind {
    /// Commands that took long, in microseconds.
    Slow,
    /// Commands that were large, in bytes.
    LargeRequest,
    /// Commands whose reply was large, in bytes.
    LargeReply,
}

impl LogKind {
    pub const ALL: [LogKind; 3] = [LogKind::Slow, LogKind::LargeRequest, LogKind::LargeReply];

    fn from_name(raw: &[u8]) -> Result<LogKind, Reply> {
        LogKind::ALL
            .into_iter()
            .find(|kind| kind.name().as_bytes().eq_ignore_ascii_case(raw))
            .ok_or_else(|| {
                Reply::err(
                    "ERR type should be one of the following: slow, large-request, large-reply",
                )
            })
    }

    pub fn name(self) -> &'static str {
        match self {
            LogKind::Slow => "slow",
            LogKind::LargeRequest => "large-request",
            LogKind::LargeReply => "large-reply",
        }
    }
}

/// SLOWLOG and COMMANDLOG.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowLogCmd {
    /// The newest entries, this many at most; all of them without a number.
    Get(Option<usize>, LogKind),
    Len(LogKind),
    Reset(LogKind),
    Help,
    /// COMMANDLOG HELP.
    LogHelp,
}

/// The arguments of CLIENT TRACKING after the subcommand: `None` for OFF.
fn parse_tracking(args: &[Bytes]) -> Result<Option<Tracking>, Reply> {
    let is = |arg: &Bytes, word: &str| arg.eq_ignore_ascii_case(word.as_bytes());
    let on = if is(&args[0], "ON") {
        true
    } else if is(&args[0], "OFF") {
        false
    } else {
        return Err(syntax());
    };
    let mut t = Tracking::default();
    let mut rest = args[1..].iter();
    while let Some(word) = rest.next() {
        if is(word, "BCAST") {
            t.bcast = true;
        } else if is(word, "OPTIN") {
            t.optin = true;
        } else if is(word, "OPTOUT") {
            t.optout = true;
        } else if is(word, "NOLOOP") {
            t.noloop = true;
        } else if is(word, "REDIRECT") {
            let id = rest.next().and_then(|raw| parse_i64(raw));
            match id.and_then(|id| u64::try_from(id).ok()) {
                Some(id) if t.redirect.is_none() => t.redirect = Some(id),
                Some(_) => {
                    return Err(Reply::err(
                        "ERR A client can only redirect to a single other client",
                    ))
                }
                None => return Err(Reply::err("ERR Invalid client ID")),
            }
        } else if is(word, "PREFIX") {
            t.prefixes.push(rest.next().ok_or_else(syntax)?.clone());
        } else {
            return Err(syntax());
        }
    }
    if !on {
        return Ok(None);
    }
    if t.optin && t.optout {
        return Err(Reply::err(
            "ERR You can't specify both OPTIN mode and OPTOUT mode",
        ));
    }
    if t.bcast && (t.optin || t.optout) {
        return Err(Reply::err(
            "ERR OPTIN and OPTOUT are not compatible with BCAST",
        ));
    }
    if !t.bcast && !t.prefixes.is_empty() {
        return Err(Reply::err(
            "ERR PREFIX option requires BCAST mode to be enabled",
        ));
    }
    // Also what keeps the search for overlaps below short.
    if t.prefixes.len() > MAX_PREFIXES {
        return Err(too_many_prefixes());
    }
    for (n, prefix) in t.prefixes.iter().enumerate() {
        let clash = t.prefixes[..n]
            .iter()
            .find(|other| other.starts_with(prefix) || prefix.starts_with(other));
        if let Some(other) = clash {
            let shown =
                |raw: &Bytes| String::from_utf8_lossy(&raw[..raw.len().min(64)]).into_owned();
            return Err(Reply::Error(format!(
                "ERR Prefix '{}' overlaps with an existing prefix '{}'. Prefixes for a single \
                 client must not overlap.",
                shown(prefix),
                shown(other)
            )));
        }
    }
    Ok(Some(t))
}

/// A connection may track this many prefixes: every write looks at each.
pub const MAX_PREFIXES: usize = 256;

pub fn too_many_prefixes() -> Reply {
    Reply::Error(format!(
        "ERR too many prefixes: a client may track at most {MAX_PREFIXES}"
    ))
}

fn lines(text: &[&str]) -> Reply {
    Reply::Array(
        text.iter()
            .map(|line| Reply::Simple((*line).into()))
            .collect(),
    )
}

/// `SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE] [ABORT] [SAFE] [FAILOVER]`.
/// Whatever is chosen, every change goes to disk first, as Valkey does
/// with an append-only file.
fn parse_shutdown(args: &[Bytes]) -> Result<Cmd, Reply> {
    let (mut nosave, mut save, mut other) = (false, false, false);
    let (mut abort, mut failover) = (false, false);
    for arg in &args[1..] {
        match arg.to_ascii_uppercase().as_slice() {
            b"NOSAVE" => nosave = true,
            b"SAVE" => save = true,
            b"NOW" | b"FORCE" | b"SAFE" => other = true,
            b"FAILOVER" => failover = true,
            b"ABORT" => abort = true,
            _ => return Err(syntax()),
        }
    }
    if (abort && (nosave || save || other || failover)) || (nosave && save) {
        return Err(syntax());
    }
    Ok(Cmd::Shutdown { abort, failover })
}

/// The count of SLOWLOG GET and COMMANDLOG GET: `None` for all.
fn log_count(raw: &[u8]) -> Result<Option<usize>, Reply> {
    match parse_i64(raw) {
        Some(-1) => Ok(None),
        Some(count) if count >= 0 => Ok(usize::try_from(count).ok()),
        Some(_) => Err(Reply::err(
            "ERR count should be greater than or equal to -1",
        )),
        None => Err(Reply::err(NOT_INT)),
    }
}

/// `COMMANDLOG GET count type | LEN type | RESET type | HELP`.
fn parse_commandlog(args: &[Bytes]) -> Result<Cmd, Reply> {
    let sub = args[1].to_ascii_uppercase();
    Ok(Cmd::SlowLog(match (sub.as_slice(), args.len()) {
        (b"GET", 4) => {
            let count = log_count(&args[2])?;
            SlowLogCmd::Get(count, LogKind::from_name(&args[3])?)
        }
        (b"LEN", 3) => SlowLogCmd::Len(LogKind::from_name(&args[2])?),
        (b"RESET", 3) => SlowLogCmd::Reset(LogKind::from_name(&args[2])?),
        (b"HELP", 2) => SlowLogCmd::LogHelp,
        (b"GET" | b"LEN" | b"RESET" | b"HELP", _) => {
            return Err(Reply::Error(format!(
                "ERR wrong number of arguments for 'commandlog|{}' command",
                String::from_utf8_lossy(&sub).to_ascii_lowercase()
            )))
        }
        _ => {
            return Err(Reply::Error(format!(
                "ERR unknown subcommand '{}'. Try COMMANDLOG HELP.",
                String::from_utf8_lossy(&args[1][..args[1].len().min(64)])
            )))
        }
    }))
}

fn parse_slowlog(args: &[Bytes]) -> Result<Cmd, Reply> {
    let sub = args[1].to_ascii_uppercase();
    Ok(Cmd::SlowLog(match (sub.as_slice(), args.len()) {
        (b"GET", 2) => SlowLogCmd::Get(Some(10), LogKind::Slow),
        (b"GET", 3) => SlowLogCmd::Get(log_count(&args[2])?, LogKind::Slow),
        (b"LEN", 2) => SlowLogCmd::Len(LogKind::Slow),
        (b"RESET", 2) => SlowLogCmd::Reset(LogKind::Slow),
        (b"HELP", 2) => SlowLogCmd::Help,
        (b"GET" | b"LEN" | b"RESET" | b"HELP", _) => {
            return Err(Reply::Error(format!(
                "ERR wrong number of arguments for 'slowlog|{}' command",
                String::from_utf8_lossy(&sub).to_ascii_lowercase()
            )))
        }
        _ => {
            return Err(Reply::Error(format!(
                "ERR unknown subcommand '{}'. Try SLOWLOG HELP.",
                String::from_utf8_lossy(&args[1][..args[1].len().min(64)])
            )))
        }
    }))
}

/// LATENCY: nothing is measured, so every answer is the empty one.
fn parse_latency(args: &[Bytes]) -> Result<Cmd, Reply> {
    let sub = args[1].to_ascii_uppercase();
    let reply = match (sub.as_slice(), args.len()) {
        (b"LATEST", 2) | (b"HISTORY", 3) => Reply::Array(Vec::new()),
        (b"HISTOGRAM", _) => Reply::Map(Vec::new()),
        (b"RESET", _) => Reply::Int(0),
        (b"DOCTOR", 2) => Reply::bulk(
            "Dave, no latency spike was observed during the lifetime of this server: latency \
             events are not recorded by Hotstash.",
        ),
        (b"GRAPH", 3) => {
            return Err(Reply::Error(format!(
                "ERR No samples available for event '{}'",
                String::from_utf8_lossy(&args[2][..args[2].len().min(64)])
            )))
        }
        (b"HELP", 2) => lines(&[
            "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "DOCTOR",
            "    Return a human readable latency analysis report.",
            "GRAPH <event>",
            "    Return an ASCII latency graph for the <event> class.",
            "HISTORY <event>",
            "    Return time-latency samples for the <event> class.",
            "LATEST",
            "    Return the latest latency samples for all events.",
            "RESET [<event> ...]",
            "    Reset latency data of one or more <event> classes.",
            "HISTOGRAM [<command> ...]",
            "    Return a cumulative distribution of latencies of the commands.",
            "HELP",
            "    Print this help.",
        ]),
        (b"LATEST" | b"HISTORY" | b"DOCTOR" | b"GRAPH" | b"HELP", _) => {
            return Err(Reply::Error(format!(
                "ERR wrong number of arguments for 'latency|{}' command",
                String::from_utf8_lossy(&sub).to_ascii_lowercase()
            )))
        }
        _ => {
            return Err(Reply::Error(format!(
                "ERR unknown subcommand '{}'. Try LATENCY HELP.",
                String::from_utf8_lossy(&args[1][..args[1].len().min(64)])
            )))
        }
    };
    Ok(Cmd::Static(reply))
}

/// CLIENT LIST shows every name of every connection in one reply.
const MAX_CLIENT_NAME: usize = 256;

fn parse_client(args: &[Bytes]) -> Result<Cmd, Reply> {
    let sub = args[1].to_ascii_uppercase();
    let on_off = |raw: &Bytes| raw.eq_ignore_ascii_case(b"ON") || raw.eq_ignore_ascii_case(b"OFF");
    let client = match (sub.as_slice(), args.len()) {
        (b"SETNAME", 3) => {
            if args[2].len() > MAX_CLIENT_NAME {
                return Err(Reply::err(
                    "ERR Client names cannot be longer than 256 bytes",
                ));
            }
            if !args[2].iter().all(|b| (b'!'..=b'~').contains(b)) {
                return Err(Reply::err(
                    "ERR Client names cannot contain spaces, newlines or special characters.",
                ));
            }
            ClientCmd::SetName(args[2].clone())
        }
        (b"GETNAME", 2) => ClientCmd::GetName,
        (b"ID", 2) => ClientCmd::Id,
        (b"INFO", 2) => ClientCmd::Info,
        (b"LIST", _) => ClientCmd::List,
        (b"SETINFO", 4) => {
            let version = if args[2].eq_ignore_ascii_case(b"LIB-VER") {
                true
            } else if args[2].eq_ignore_ascii_case(b"LIB-NAME") {
                false
            } else {
                return Err(Reply::Error(format!(
                    "ERR Unrecognized option '{}'",
                    String::from_utf8_lossy(&args[2][..args[2].len().min(64)])
                )));
            };
            if args[3].len() > MAX_CLIENT_NAME {
                return Err(Reply::err(
                    "ERR lib-name and lib-ver cannot be longer than 256 bytes",
                ));
            }
            if args[3].iter().any(|b| !(b'!'..=b'~').contains(b)) {
                return Err(Reply::err(
                    "ERR lib-name and lib-ver cannot contain spaces, newlines or special characters.",
                ));
            }
            ClientCmd::SetInfo {
                version,
                value: args[3].clone(),
            }
        }
        // Accepted and without effect: nothing here evicts, touches or
        // pauses on behalf of one client.
        (b"NO-EVICT" | b"NO-TOUCH", 3) if on_off(&args[2]) => return Ok(Cmd::Static(Reply::Ok)),
        (b"REPLY", 3) if args[2].eq_ignore_ascii_case(b"ON") => return Ok(Cmd::Static(Reply::Ok)),
        (b"TRACKING", 3..) => ClientCmd::Tracking(parse_tracking(&args[2..])?),
        (b"CACHING", 3) if args[2].eq_ignore_ascii_case(b"YES") => ClientCmd::Caching(true),
        (b"CACHING", 3) if args[2].eq_ignore_ascii_case(b"NO") => ClientCmd::Caching(false),
        (b"CACHING", 3) => return Err(syntax()),
        (b"TRACKINGINFO", 2) => ClientCmd::TrackingInfo,
        (b"GETREDIR", 2) => ClientCmd::GetRedir,
        (b"KILL", 3) => ClientCmd::Kill(KillFilter {
            addr: Some(args[2].clone()),
            legacy: true,
            ..KillFilter::default()
        }),
        (b"KILL", n) if n >= 4 && n % 2 == 0 => ClientCmd::Kill(parse_kill(&args[2..])?),
        (b"KILL", _) => return Err(syntax()),
        (b"UNPAUSE", 2) => ClientCmd::Unpause,
        (b"PAUSE", 3 | 4) => {
            let all = match args.get(3) {
                None => true,
                Some(mode) if mode.eq_ignore_ascii_case(b"ALL") => true,
                Some(mode) if mode.eq_ignore_ascii_case(b"WRITE") => false,
                Some(_) => return Err(Reply::err("ERR CLIENT PAUSE mode must be WRITE or ALL")),
            };
            ClientCmd::Pause {
                ms: parse_wait_timeout(&args[2])?,
                all,
            }
        }
        (b"PAUSE", 5..) => return Err(syntax()),
        (b"REPLY" | b"UNBLOCK" | b"NO-EVICT" | b"NO-TOUCH", _) => {
            return Err(Reply::Error(format!(
                "ERR CLIENT {} is not supported in this form",
                String::from_utf8_lossy(&sub)
            )))
        }
        (
            b"SETNAME" | b"GETNAME" | b"ID" | b"INFO" | b"SETINFO" | b"GETREDIR" | b"UNPAUSE"
            | b"PAUSE" | b"TRACKING" | b"CACHING" | b"TRACKINGINFO",
            _,
        ) => {
            return Err(Reply::Error(format!(
                "ERR wrong number of arguments for 'client|{}' command",
                String::from_utf8_lossy(&sub).to_ascii_lowercase()
            )))
        }
        _ => {
            return Err(Reply::Error(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                String::from_utf8_lossy(&args[1][..args[1].len().min(64)])
            )))
        }
    };
    Ok(Cmd::Client(Box::new(client)))
}

/// FAILOVER `[TO host port [FORCE]] [TIMEOUT ms]`, or `ABORT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failover {
    Start {
        /// The replica to hand over to: any that has the whole stream when
        /// `None`. The port as given: one no replica listens on is refused.
        to: Option<(String, i64)>,
        /// Milliseconds to wait for the replica to catch up.
        timeout: Option<u64>,
        /// Hand over once the time is up even if it has not.
        force: bool,
    },
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// No lock, no gate. Works while a script is running.
    Conn,
    /// Runs on the shared side of the execution gate.
    Shared,
    /// Runs on the exclusive side of the execution gate.
    Exclusive,
}

const WRONG_ARGS: &str = "ERR wrong number of arguments for";
pub const NOT_INT: &str = "ERR value is not an integer or out of range";
/// Matching is O(pattern × key) per key, so the pattern length is bounded.
pub(crate) const MAX_PATTERN: usize = 1024;

pub(crate) fn arity(name: &str) -> Reply {
    Reply::Error(format!(
        "{WRONG_ARGS} '{}' command",
        name.to_ascii_lowercase()
    ))
}

impl Cmd {
    pub fn parse(mut args: Vec<Bytes>) -> Result<Cmd, Reply> {
        // The longest name is GEORADIUSBYMEMBER_RO.
        let mut name_buf = [0u8; 24];
        let raw = args
            .first()
            .ok_or_else(|| Reply::err("ERR empty command"))?;
        if raw.len() > name_buf.len() {
            return Err(unknown(&args));
        }
        let name = &mut name_buf[..raw.len()];
        name.copy_from_slice(raw);
        name.make_ascii_uppercase();
        let name = std::str::from_utf8(name).map_err(|_| unknown(&args))?;
        if let Some(fits) = ops::probe(name, args.len()) {
            return if fits {
                ops::parse_known(name, &mut args)
            } else {
                Err(arity(name))
            };
        }
        let n = args.len();
        // Arguments are moved out of `args` back to front.
        let mut pop = || args.pop().unwrap_or_default();

        let cmd = match name {
            "PING" => match n {
                1 => Cmd::Ping(None),
                2 => Cmd::Ping(Some(pop())),
                _ => return Err(arity(name)),
            },
            "ECHO" if n == 2 => Cmd::Echo(pop()),
            "QUIT" => Cmd::Quit,
            "SELECT" if n == 2 => match parse_i64(&pop()) {
                Some(db) => Cmd::Select(db),
                None => return Err(Reply::err("ERR invalid DB index")),
            },
            "MOVE" if n == 3 => {
                let db = parse_i64(&args[2]).ok_or_else(|| Reply::err(NOT_INT))?;
                Cmd::Move {
                    key: std::mem::take(&mut args[1]),
                    db,
                }
            }
            "SWAPDB" if n == 3 => {
                let db = |raw: &Bytes, which: &str| {
                    parse_i64(raw)
                        .ok_or_else(|| Reply::Error(format!("ERR invalid {which} DB index")))
                };
                Cmd::SwapDb(db(&args[1], "first")?, db(&args[2], "second")?)
            }
            "MONITOR" if n == 1 => Cmd::Monitor,
            "CLUSTER" if n >= 2 => cluster::parse(std::mem::take(&mut args))?,
            "ASKING" if n == 1 => Cmd::Asking,
            // A primary serves reads either way.
            "READONLY" | "READWRITE" if n == 1 => Cmd::Static(Reply::Ok),
            "ROLE" if n == 1 => Cmd::Role,
            "PSYNC" if n >= 3 => Cmd::Psync(
                std::mem::take(&mut args[1]),
                parse_i64(&args[2]).ok_or_else(|| Reply::err(NOT_INT))?,
                n > 3 && args[3].eq_ignore_ascii_case(b"FAILOVER"),
            ),
            "SYNC" if n == 1 => Cmd::Sync,
            "REPLCONF" => Cmd::Replconf(args.split_off(1)),
            "REPLICAOF" | "SLAVEOF" if n == 3 => parse_replicaof(&args)?,
            "FAILOVER" => parse_failover(&args)?,
            "SHUTDOWN" => parse_shutdown(&args)?,
            "DEBUG" if n >= 2 => Cmd::Debug(std::mem::take(&mut args)),
            "LOLWUT" => {
                if n >= 3 && args[1].eq_ignore_ascii_case(b"VERSION") {
                    parse_i64(&args[2]).ok_or_else(|| Reply::err(NOT_INT))?;
                }
                // No art: what Valkey shows for a version it has none for.
                Cmd::Static(Reply::Bulk(
                    format!("Hotstash ver. {}\n", env!("CARGO_PKG_VERSION")).into_bytes(),
                ))
            }
            "SLOWLOG" if n >= 2 => parse_slowlog(&args)?,
            "COMMANDLOG" if n >= 2 => parse_commandlog(&args)?,
            "LATENCY" if n >= 2 => parse_latency(&args)?,
            "CLIENT" if n >= 2 => parse_client(&args)?,
            "RESET" if n == 1 => Cmd::Reset,
            "COMMAND" => Cmd::Command(std::mem::take(&mut args)),
            "ACL" => Cmd::Acl(std::mem::take(&mut args)),
            "AUTH" => match n {
                2 => Cmd::Auth(None, pop()),
                3 => {
                    let pass = pop();
                    Cmd::Auth(Some(pop()), pass)
                }
                _ => return Err(arity(name)),
            },
            "CONFIG" if n >= 2 => {
                if args[1].eq_ignore_ascii_case(b"GET") && n == 3 {
                    Cmd::ConfigGet(args[2].to_ascii_lowercase())
                } else if args[1].eq_ignore_ascii_case(b"SET") && n >= 4 && n.is_multiple_of(2) {
                    let mut pairs = Vec::new();
                    let mut rest = args.drain(2..);
                    while let (Some(k), Some(v)) = (rest.next(), rest.next()) {
                        pairs.push((k.to_ascii_lowercase(), v));
                    }
                    Cmd::ConfigSet(pairs)
                } else if args[1].eq_ignore_ascii_case(b"GET")
                    || args[1].eq_ignore_ascii_case(b"SET")
                {
                    return Err(Reply::err(
                        "ERR wrong number of arguments for 'config' subcommand",
                    ));
                } else if args[1].eq_ignore_ascii_case(b"REWRITE") {
                    // Settings come from the command line: there is no file.
                    // An error when it runs, so that a transaction goes on.
                    Cmd::Static(Reply::err(
                        "ERR The server is running without a config file",
                    ))
                } else {
                    Cmd::Static(Reply::Ok)
                }
            }
            "BGSAVE" | "BGREWRITEAOF" => Cmd::BgSave,
            "LASTSAVE" if n == 1 => Cmd::LastSave,
            "TIME" if n == 1 => Cmd::Time,
            "SAVE" if n == 1 => Cmd::Save,
            "WAIT" if n == 3 => {
                let replicas = parse_i64(&args[1]).ok_or_else(|| Reply::err(NOT_INT))?;
                Cmd::Wait(replicas, parse_wait_timeout(&args[2])?)
            }
            "WAITAOF" if n == 4 => {
                let local = match parse_i64(&args[1]) {
                    Some(n @ 0..=1) => n == 1,
                    Some(_) => {
                        return Err(Reply::err(
                            "ERR value is out of range, value must between 0 and 1",
                        ))
                    }
                    None => return Err(Reply::err(NOT_INT)),
                };
                match parse_i64(&args[2]) {
                    Some(0..) => {}
                    Some(_) => {
                        return Err(Reply::err("ERR value is out of range, must be positive"))
                    }
                    None => return Err(Reply::err(NOT_INT)),
                }
                parse_wait_timeout(&args[3])?;
                Cmd::WaitAof(local)
            }
            "INFO" => Cmd::Info(args.drain(1..).map(|a| a.to_ascii_lowercase()).collect()),
            "DBSIZE" if n == 1 => Cmd::Dbsize,
            "HELLO" => {
                let (proto, auth) = parse_hello(&args)?;
                Cmd::Hello(proto, auth)
            }
            "SCRIPT" if n >= 2 => {
                let mut sub = std::mem::take(&mut args[1]);
                sub.make_ascii_uppercase();
                match (sub.as_slice(), n) {
                    (b"KILL", 2) => Cmd::ScriptKill,
                    (b"FLUSH", 2) => Cmd::ScriptFlush,
                    (b"FLUSH", 3)
                        if args[2].eq_ignore_ascii_case(b"ASYNC")
                            || args[2].eq_ignore_ascii_case(b"SYNC") =>
                    {
                        Cmd::ScriptFlush
                    }
                    (b"FLUSH", 3) => return Err(Reply::err("ERR syntax error")),
                    (b"LOAD", 3) => Cmd::ScriptLoad(args.pop().unwrap_or_default()),
                    (b"EXISTS", 3..) => Cmd::ScriptExists(args.split_off(2)),
                    (b"KILL" | b"FLUSH" | b"LOAD" | b"EXISTS", _) => {
                        return Err(Reply::err(
                            "ERR wrong number of arguments for 'script' subcommand",
                        ))
                    }
                    _ => return Err(Reply::err("ERR unknown SCRIPT subcommand")),
                }
            }
            "SUBSCRIBE" if n >= 2 => Cmd::Sub(Sub::Subscribe(args.split_off(1))),
            "UNSUBSCRIBE" => Cmd::Sub(Sub::Unsubscribe(args.split_off(1))),
            "PSUBSCRIBE" if n >= 2 => {
                if args[1..].iter().any(|p| p.len() > MAX_PATTERN) {
                    return Err(Reply::err("ERR pattern too long"));
                }
                Cmd::Sub(Sub::PSubscribe(args.split_off(1)))
            }
            "PUNSUBSCRIBE" => Cmd::Sub(Sub::PUnsubscribe(args.split_off(1))),
            "SSUBSCRIBE" if n >= 2 => Cmd::Sub(Sub::SSubscribe(args.split_off(1))),
            "SUNSUBSCRIBE" => Cmd::Sub(Sub::SUnsubscribe(args.split_off(1))),
            "PUBLISH" if n == 3 => {
                let payload = pop();
                Cmd::Publish(pop(), payload)
            }
            "SPUBLISH" if n == 3 => {
                let payload = pop();
                Cmd::SPublish(pop(), payload)
            }
            "PUBSUB" if n >= 2 => {
                let sub = args[1].to_ascii_uppercase();
                match (sub.as_slice(), n) {
                    (b"CHANNELS", 2) => Cmd::PubSub(PubSubQuery::Channels(None)),
                    (b"CHANNELS", 3) => Cmd::PubSub(PubSubQuery::Channels(args.pop())),
                    (b"NUMSUB", _) => Cmd::PubSub(PubSubQuery::NumSub(args.split_off(2))),
                    (b"NUMPAT", 2) => Cmd::PubSub(PubSubQuery::NumPat),
                    (b"SHARDCHANNELS", 2) => Cmd::PubSub(PubSubQuery::ShardChannels(None)),
                    (b"SHARDCHANNELS", 3) => Cmd::PubSub(PubSubQuery::ShardChannels(args.pop())),
                    (b"SHARDNUMSUB", _) => Cmd::PubSub(PubSubQuery::ShardNumSub(args.split_off(2))),
                    (b"NUMPAT", _) => return Err(arity("pubsub|numpat")),
                    (b"CHANNELS" | b"SHARDCHANNELS", _) => {
                        return Err(Reply::err(format!(
                            "ERR unknown subcommand or wrong number of arguments for '{}'. \
                             Try PUBSUB HELP.",
                            String::from_utf8_lossy(&args[1])
                        )))
                    }
                    _ => {
                        return Err(Reply::err(format!(
                            "ERR unknown subcommand '{}'. Try PUBSUB HELP.",
                            String::from_utf8_lossy(&args[1])
                        )))
                    }
                }
            }
            "GET" if n == 2 => Cmd::Get(pop()),
            "SET" => match n {
                3 => {
                    let v = pop();
                    Cmd::Set(pop(), v)
                }
                0..=2 => return Err(arity(name)),
                _ => parse_set(args)?,
            },
            "SETEX" | "PSETEX" if n == 4 => {
                let v = pop();
                let expire = parse_when(&pop(), name == "SETEX", false, true, name)?;
                let opts = SetOpts {
                    cond: None,
                    expire,
                    get: false,
                    ifeq: None,
                };
                Cmd::SetOpts(pop(), v, opts)
            }
            "GETSET" if n == 3 => {
                let v = pop();
                let opts = SetOpts {
                    cond: None,
                    expire: Expire::Clear,
                    get: true,
                    ifeq: None,
                };
                Cmd::SetOpts(pop(), v, opts)
            }
            "SETNX" if n == 3 => {
                let v = pop();
                Cmd::SetNx(pop(), v)
            }
            "GETDEL" if n == 2 => Cmd::GetDel(pop()),
            "GETEX" if n >= 2 => {
                let expire = match n {
                    2 => Expire::Keep,
                    3 if args[2].eq_ignore_ascii_case(b"PERSIST") => Expire::Clear,
                    4 => {
                        let (secs, absolute) = time_option(&args[2]).ok_or_else(syntax)?;
                        parse_when(&args[3], secs, absolute, true, name)?
                    }
                    _ => return Err(syntax()),
                };
                args.truncate(2);
                Cmd::GetEx(args.pop().unwrap_or_default(), expire)
            }
            "APPEND" if n == 3 => {
                let v = pop();
                Cmd::Append(pop(), v)
            }
            "STRLEN" if n == 2 => Cmd::Strlen(pop()),
            "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" if n == 3 || n == 4 => {
                let secs = !name.starts_with('P');
                let when = parse_when(&args[2], secs, name.ends_with("AT"), false, name)?;
                let cond = match args.get(3) {
                    Some(option) => Some(parse_cond(option)?),
                    None => None,
                };
                args.truncate(2);
                Cmd::Expire {
                    key: args.pop().unwrap_or_default(),
                    when,
                    cond,
                }
            }
            "TTL" | "PTTL" if n == 2 => Cmd::Ttl(pop(), name == "PTTL"),
            "EXPIRETIME" | "PEXPIRETIME" if n == 2 => Cmd::ExpireTime(pop(), name == "PEXPIRETIME"),
            "PERSIST" if n == 2 => Cmd::Persist(pop()),
            "TYPE" if n == 2 => Cmd::Type(pop()),
            "MULTI" if n == 1 => Cmd::Txn(Txn::Multi),
            "EXEC" if n == 1 => Cmd::Txn(Txn::Exec),
            "DISCARD" if n == 1 => Cmd::Txn(Txn::Discard),
            "UNWATCH" if n == 1 => Cmd::Txn(Txn::Unwatch),
            "WATCH" if n >= 2 => Cmd::Txn(Txn::Watch(args.split_off(1))),
            "INCR" if n == 2 => Cmd::IncrBy(pop(), 1),
            "DECR" if n == 2 => Cmd::IncrBy(pop(), -1),
            "INCRBY" | "DECRBY" if n == 3 => {
                let by = parse_i64(&pop()).ok_or_else(|| Reply::err(NOT_INT))?;
                let by = if name == "DECRBY" {
                    // DECRBY i64::MIN cannot be negated.
                    by.checked_neg()
                        .ok_or_else(|| Reply::err("ERR decrement would overflow"))?
                } else {
                    by
                };
                Cmd::IncrBy(pop(), by)
            }
            "HSET" | "HMSET" if n >= 4 && n.is_multiple_of(2) => {
                let mut pairs = Vec::with_capacity((n - 2) / 2);
                for _ in 0..(n - 2) / 2 {
                    let v = pop();
                    pairs.push((pop(), v));
                }
                pairs.reverse();
                if name == "HMSET" {
                    Cmd::HMSet(pop(), pairs)
                } else {
                    Cmd::HSet(pop(), pairs)
                }
            }
            "HGET" if n == 3 => {
                let f = pop();
                Cmd::HGet(pop(), f)
            }
            "HDEL" if n >= 3 => {
                let fields = args.split_off(2);
                Cmd::HDel(args.pop().unwrap_or_default(), fields)
            }
            "HLEN" if n == 2 => Cmd::HLen(pop()),
            "HEXISTS" if n == 3 => {
                let f = pop();
                Cmd::HExists(pop(), f)
            }
            "HMGET" if n >= 3 => {
                let fields = args.split_off(2);
                Cmd::HMGet(args.pop().unwrap_or_default(), fields)
            }
            "HSETNX" if n == 4 => {
                let v = pop();
                let f = pop();
                Cmd::HSetNx(pop(), f, v)
            }
            "HINCRBY" if n == 4 => {
                let by = parse_i64(&pop()).ok_or_else(|| Reply::err(NOT_INT))?;
                let f = pop();
                Cmd::HIncrBy(pop(), f, by)
            }
            "HSCAN" if n >= 3 => {
                let cursor = parse_cursor(&args[2])?;
                let opts = parse_scan_opts(&args[3..], false)?;
                args.truncate(2);
                Cmd::HScan {
                    key: args.pop().unwrap_or_default(),
                    cursor,
                    pattern: opts.pattern,
                    count: opts.count,
                    novalues: opts.novalues,
                }
            }
            "SCAN" if n >= 2 => {
                let cursor = parse_cursor(&args[1])?;
                let opts = parse_scan_opts(&args[2..], true)?;
                Cmd::Scan {
                    cursor,
                    pattern: opts.pattern,
                    count: opts.count,
                    ty: opts.ty,
                }
            }
            "HGETALL" if n == 2 => Cmd::HGetAll(pop()),
            "TOUCH" if n >= 2 => Cmd::Exists(args.split_off(1)),
            "DEL" if n >= 2 => Cmd::Del(args.split_off(1)),
            "UNLINK" if n >= 2 => Cmd::Unlink(args.split_off(1)),
            "EXISTS" if n >= 2 => Cmd::Exists(args.split_off(1)),
            "MGET" if n >= 2 => Cmd::MGet(args.split_off(1)),
            "MSET" if n >= 3 && n % 2 == 1 => {
                let mut pairs = Vec::with_capacity((n - 1) / 2);
                for _ in 0..(n - 1) / 2 {
                    let v = pop();
                    pairs.push((pop(), v));
                }
                pairs.reverse();
                Cmd::MSet(pairs)
            }
            "KEYS" if n == 2 => {
                let pattern = pop();
                if pattern.len() > MAX_PATTERN {
                    return Err(Reply::err("ERR pattern too long"));
                }
                Cmd::Keys(pattern)
            }
            "RANDOMKEY" if n == 1 => Cmd::RandomKey,
            "FLUSHALL" => Cmd::FlushAll(parse_flush(&args)?),
            "FLUSHDB" => Cmd::FlushDb(parse_flush(&args)?),
            "FUNCTION" if n >= 2 => Cmd::Function(args),
            "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" if n >= 3 => {
                let numkeys = parse_i64(&args[2]).ok_or_else(|| Reply::err(NOT_INT))?;
                if numkeys < 0 {
                    return Err(Reply::err("ERR Number of keys can't be negative"));
                }
                if numkeys as u64 > (n - 3) as u64 {
                    return Err(Reply::err(
                        "ERR Number of keys can't be greater than number of args",
                    ));
                }
                let rest = args.split_off(3 + numkeys as usize);
                let keys = args.split_off(3);
                args.truncate(2);
                let script = args.pop().unwrap_or_default();
                let read_only = name.ends_with("_RO");
                if name.starts_with("FCALL") {
                    Cmd::FCall {
                        name: script,
                        keys,
                        args: rest,
                        read_only,
                    }
                } else if name.starts_with("EVALSHA") {
                    Cmd::EvalSha {
                        sha: script,
                        keys,
                        args: rest,
                        read_only,
                    }
                } else {
                    Cmd::Eval {
                        script,
                        keys,
                        args: rest,
                        read_only,
                    }
                }
            }
            "ECHO" | "SELECT" | "CLIENT" | "CONFIG" | "DBSIZE" | "SCRIPT" | "GET" | "INCR"
            | "DECR" | "INCRBY" | "DECRBY" | "HSET" | "HMSET" | "HGET" | "HDEL" | "HLEN"
            | "HGETALL" | "DEL" | "UNLINK" | "EXISTS" | "MGET" | "MSET" | "KEYS" | "EVAL"
            | "EVALSHA" | "SETEX" | "PSETEX" | "GETSET" | "SETNX" | "GETDEL" | "GETEX"
            | "APPEND" | "STRLEN" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "TTL"
            | "PTTL" | "EXPIRETIME" | "PEXPIRETIME" | "PERSIST" | "TYPE" | "MULTI" | "EXEC"
            | "DISCARD" | "WATCH" | "UNWATCH" | "HEXISTS" | "HMGET" | "HSETNX" | "HINCRBY"
            | "HSCAN" | "SCAN" | "TOUCH" | "RANDOMKEY" | "SUBSCRIBE" | "PSUBSCRIBE" | "PUBLISH"
            | "SSUBSCRIBE" | "SPUBLISH" | "PUBSUB" | "LASTSAVE" | "TIME" | "EVAL_RO"
            | "EVALSHA_RO" | "RESET" | "MOVE" | "SWAPDB" | "FUNCTION" | "FCALL" | "FCALL_RO"
            | "MONITOR" | "SLOWLOG" | "LATENCY" | "WAIT" | "WAITAOF" | "CLUSTER" | "ASKING"
            | "READONLY" | "READWRITE" | "ROLE" | "COMMANDLOG" | "DEBUG" | "PSYNC" | "SYNC"
            | "REPLICAOF" | "SLAVEOF" => return Err(arity(name)),
            _ => return Err(unknown(&args)),
        };
        Ok(cmd)
    }

    /// The command as one of database `db`. Database 0 needs no mark.
    pub fn in_db(self, db: usize) -> Cmd {
        match (db, self) {
            (_, cmd @ Cmd::In(..)) | (0, cmd) => cmd,
            (db, cmd) => Cmd::In(db as u16, Box::new(cmd)),
        }
    }

    pub fn class(&self) -> Class {
        match self {
            Cmd::In(_, cmd) => cmd.class(),
            Cmd::Multi(op) if matches!(**op, MultiOp::Copy { db: Some(_), .. }) => Class::Exclusive,
            // The keys its patterns name are known only once the elements are.
            Cmd::Multi(op) if matches!(&**op, MultiOp::Sort { spec, .. } if spec.external()) => {
                Class::Exclusive
            }
            Cmd::Select(_) => Class::Conn,
            Cmd::Cluster(args) if cluster::reads_keys(args) => Class::Exclusive,
            Cmd::Ping(_)
            | Cmd::Echo(_)
            | Cmd::Quit
            | Cmd::Static(_)
            | Cmd::Info(_)
            | Cmd::ScriptKill
            | Cmd::Dbsize
            | Cmd::Hello(..)
            | Cmd::Auth(..)
            | Cmd::Acl(_)
            | Cmd::Command(_)
            | Cmd::ConfigGet(_)
            | Cmd::ConfigSet(_)
            | Cmd::BgSave
            | Cmd::Save
            | Cmd::LastSave
            | Cmd::Time
            | Cmd::WaitAof(_)
            | Cmd::Client(_)
            | Cmd::Monitor
            | Cmd::SlowLog(_)
            | Cmd::Reset
            | Cmd::Sub(_)
            | Cmd::Publish(..)
            | Cmd::SPublish(..)
            | Cmd::PubSub(_)
            | Cmd::ScriptExists(_)
            | Cmd::ScriptFlush
            | Cmd::Cluster(_)
            | Cmd::Asking
            | Cmd::Role
            | Cmd::Psync(..)
            | Cmd::Sync
            | Cmd::Replconf(_)
            | Cmd::ReplicaOf(_)
            | Cmd::Failover(_)
            | Cmd::Wait(..)
            | Cmd::Shutdown { .. }
            | Cmd::Debug(_)
            | Cmd::Txn(_) => Class::Conn,
            Cmd::Get(_)
            | Cmd::Set(..)
            | Cmd::SetOpts(..)
            | Cmd::SetNx(..)
            | Cmd::GetDel(_)
            | Cmd::GetEx(..)
            | Cmd::Append(..)
            | Cmd::Strlen(_)
            | Cmd::Expire { .. }
            | Cmd::Ttl(..)
            | Cmd::ExpireTime(..)
            | Cmd::Persist(_)
            | Cmd::Type(_)
            | Cmd::HExists(..)
            | Cmd::HMGet(..)
            | Cmd::HSetNx(..)
            | Cmd::HIncrBy(..)
            | Cmd::HScan { .. }
            | Cmd::Scan { .. }
            | Cmd::IncrBy(..)
            | Cmd::HSet(..)
            | Cmd::HMSet(..)
            | Cmd::HGet(..)
            | Cmd::HDel(..)
            | Cmd::HLen(_)
            | Cmd::HGetAll(_)
            | Cmd::Key(..)
            | Cmd::Multi(_)
            | Cmd::Block(..)
            | Cmd::Del(_)
            | Cmd::Unlink(_)
            | Cmd::Exists(_)
            | Cmd::MSet(_)
            | Cmd::MGet(_)
            | Cmd::RandomKey
            | Cmd::Keys(_) => Class::Shared,
            Cmd::FlushAll(_)
            | Cmd::FlushDb(_)
            | Cmd::Move { .. }
            | Cmd::SwapDb(..)
            | Cmd::Eval { .. }
            | Cmd::EvalSha { .. }
            | Cmd::ScriptLoad(_)
            | Cmd::Function(_)
            | Cmd::FCall { .. }
            | Cmd::ExecBlock { .. } => Class::Exclusive,
        }
    }

    /// How the reply is typed in RESP3.
    pub fn shape(&self) -> Shape {
        match self {
            Cmd::In(_, cmd) => cmd.shape(),
            Cmd::Key(_, op) => op.shape(),
            Cmd::Multi(op) | Cmd::Block(op, _) => op.shape(),
            Cmd::ExecBlock { queue, .. } => Shape::Each(queue.iter().map(Cmd::shape).collect()),
            cmd if cmd.replies_map() => Shape::Map,
            _ => Shape::Plain,
        }
    }

    /// Whether the reply, a flat array of pairs, is a map in RESP3.
    pub fn replies_map(&self) -> bool {
        match self {
            Cmd::In(_, cmd) => cmd.replies_map(),
            cmd => matches!(cmd, Cmd::HGetAll(_) | Cmd::Hello(..) | Cmd::ConfigGet(_)),
        }
    }

    /// Whether running this may change what the data file has to hold:
    /// keys, libraries of functions, users. A script may, whatever it does.
    /// The command as a replica runs it for its clients: a write is
    /// refused, and a function may only read. A script stays as it came:
    /// as in Valkey, its writes are refused as they come, while PFCOUNT
    /// and PUBLISH, refused to EVAL_RO, still run.
    pub fn on_replica(self) -> Result<Cmd, Reply> {
        match self {
            Cmd::In(db, cmd) => cmd.on_replica().map(|cmd| Cmd::In(db, Box::new(cmd))),
            cmd @ (Cmd::Eval { .. } | Cmd::EvalSha { .. }) => Ok(cmd),
            Cmd::FCall {
                name, keys, args, ..
            } => Ok(Cmd::FCall {
                name,
                keys,
                args,
                read_only: true,
            }),
            Cmd::Function(args) if crate::repl::function_writes(&args) => Err(Reply::err(READONLY)),
            // Queued while this node was the primary.
            Cmd::ExecBlock { queue, watched } => {
                match queue.into_iter().map(Cmd::on_replica).collect() {
                    Ok(queue) => Ok(Cmd::ExecBlock { queue, watched }),
                    Err(_) => Err(Reply::Error(format!(
                        "EXECABORT Transaction discarded because of: {READONLY}"
                    ))),
                }
            }
            cmd if cmd.is_write() => Err(Reply::err(READONLY)),
            cmd => Ok(cmd),
        }
    }

    /// Whether a pause of writes (a FAILOVER under way, CLIENT PAUSE WRITE)
    /// holds the command back: what may add to the stream the replicas get,
    /// PUBLISH included.
    pub fn pausable(&self) -> bool {
        match self {
            Cmd::In(_, cmd) => cmd.pausable(),
            Cmd::Publish(..) | Cmd::SPublish(..) => true,
            // It may write the cache in its key.
            Cmd::Multi(op) if matches!(**op, MultiOp::PfCount(_)) => true,
            Cmd::Eval { read_only, .. }
            | Cmd::EvalSha { read_only, .. }
            | Cmd::FCall { read_only, .. } => !read_only,
            Cmd::Function(args) => crate::repl::function_writes(args),
            Cmd::ExecBlock { queue, .. } => queue.iter().any(Cmd::pausable),
            cmd => cmd.is_write(),
        }
    }

    pub fn may_change(&self) -> bool {
        self.is_write()
            || self.class() == Class::Exclusive
            || matches!(self, Cmd::Acl(_) | Cmd::ConfigSet(_))
    }

    pub fn is_write(&self) -> bool {
        match self {
            Cmd::In(_, cmd) => return cmd.is_write(),
            Cmd::GetEx(_, expire) => return *expire != Expire::Keep,
            Cmd::Key(_, op) => return op.is_write(),
            Cmd::Multi(op) | Cmd::Block(op, _) => return op.is_write(),
            _ => {}
        }
        matches!(
            self,
            Cmd::Set(..)
                | Cmd::SetOpts(..)
                | Cmd::SetNx(..)
                | Cmd::GetDel(_)
                | Cmd::Append(..)
                | Cmd::Expire { .. }
                | Cmd::Persist(_)
                | Cmd::HSetNx(..)
                | Cmd::HIncrBy(..)
                | Cmd::IncrBy(..)
                | Cmd::HSet(..)
                | Cmd::HMSet(..)
                | Cmd::HDel(..)
                | Cmd::Del(_)
                | Cmd::Unlink(_)
                | Cmd::MSet(_)
                | Cmd::FlushAll(_)
                | Cmd::FlushDb(_)
                | Cmd::Move { .. }
                | Cmd::SwapDb(..)
        )
    }

    /// Whether the command can add data: refused when memory is full and
    /// nothing can be evicted.
    pub fn may_grow(&self) -> bool {
        match self {
            Cmd::In(_, cmd) => cmd.may_grow(),
            Cmd::Key(_, op) => op.may_grow(),
            Cmd::Multi(op) | Cmd::Block(op, _) => op.may_grow(),
            _ => matches!(
                self,
                Cmd::Set(..)
                    | Cmd::SetOpts(..)
                    | Cmd::SetNx(..)
                    | Cmd::Append(..)
                    | Cmd::IncrBy(..)
                    | Cmd::HSet(..)
                    | Cmd::HMSet(..)
                    | Cmd::HSetNx(..)
                    | Cmd::HIncrBy(..)
                    | Cmd::MSet(_)
            ),
        }
    }

    /// The key, for commands that touch exactly one.
    pub fn single_key(&self) -> Option<&[u8]> {
        match self {
            Cmd::Get(k)
            | Cmd::Set(k, _)
            | Cmd::IncrBy(k, _)
            | Cmd::HSet(k, _)
            | Cmd::HMSet(k, _)
            | Cmd::HGet(k, _)
            | Cmd::HDel(k, _)
            | Cmd::HLen(k)
            | Cmd::HGetAll(k)
            | Cmd::SetOpts(k, ..)
            | Cmd::SetNx(k, _)
            | Cmd::GetDel(k)
            | Cmd::GetEx(k, _)
            | Cmd::Append(k, _)
            | Cmd::Strlen(k)
            | Cmd::Expire { key: k, .. }
            | Cmd::Ttl(k, _)
            | Cmd::ExpireTime(k, _)
            | Cmd::Persist(k)
            | Cmd::Type(k)
            | Cmd::HExists(k, _)
            | Cmd::HMGet(k, _)
            | Cmd::HSetNx(k, ..)
            | Cmd::HIncrBy(k, ..)
            | Cmd::Key(k, _)
            | Cmd::HScan { key: k, .. } => Some(k),
            Cmd::Del(ks) | Cmd::Unlink(ks) | Cmd::Exists(ks) | Cmd::MGet(ks) if ks.len() == 1 => {
                Some(&ks[0])
            }
            Cmd::MSet(kv) if kv.len() == 1 => Some(&kv[0].0),
            _ => None,
        }
    }

    /// Visit every key of a keyed command.
    pub fn for_each_key(&self, mut f: impl FnMut(&[u8])) {
        match self {
            Cmd::In(_, cmd) => cmd.for_each_key(f),
            Cmd::Del(ks) | Cmd::Unlink(ks) | Cmd::Exists(ks) | Cmd::MGet(ks) => {
                ks.iter().for_each(|k| f(k))
            }
            Cmd::MSet(kv) => kv.iter().for_each(|(k, _)| f(k)),
            Cmd::Multi(op) | Cmd::Block(op, _) => op.for_each_key(f),
            other => {
                if let Some(k) = other.single_key() {
                    f(k)
                }
            }
        }
    }
}

pub(crate) fn syntax() -> Reply {
    Reply::err("ERR syntax error")
}

/// `FLUSHALL [ASYNC | SYNC]`: `Some(true)` for ASYNC.
fn parse_flush(args: &[Bytes]) -> Result<Option<bool>, Reply> {
    match args {
        [_] => Ok(None),
        [_, mode] if mode.eq_ignore_ascii_case(b"ASYNC") => Ok(Some(true)),
        [_, mode] if mode.eq_ignore_ascii_case(b"SYNC") => Ok(Some(false)),
        _ => Err(syntax()),
    }
}

/// FAILOVER [TO host port [FORCE]] [TIMEOUT ms], or FAILOVER ABORT, read
/// as Valkey reads it. Whether it can be done is the server's to say.
fn parse_failover(args: &[Bytes]) -> Result<Cmd, Reply> {
    if args.len() == 2 && args[1].eq_ignore_ascii_case(b"ABORT") {
        return Ok(Cmd::Failover(Failover::Abort));
    }
    let (mut to, mut timeout, mut force) = (None, None, false);
    let mut i = 1;
    while i < args.len() {
        let left = args.len() - i - 1;
        let arg = &args[i];
        if arg.eq_ignore_ascii_case(b"TIMEOUT") && left >= 1 && timeout.is_none() {
            let ms = parse_i64(&args[i + 1]).ok_or_else(|| Reply::err(NOT_INT))?;
            let ms = u64::try_from(ms)
                .ok()
                .filter(|&ms| ms > 0)
                .ok_or_else(|| Reply::err("ERR FAILOVER timeout must be greater than 0"))?;
            timeout = Some(ms);
            i += 2;
        } else if arg.eq_ignore_ascii_case(b"TO") && left >= 2 && to.is_none() {
            let port = parse_i64(&args[i + 2]).ok_or_else(|| Reply::err(NOT_INT))?;
            to = Some((String::from_utf8_lossy(&args[i + 1]).into_owned(), port));
            i += 3;
        } else if arg.eq_ignore_ascii_case(b"FORCE") && !force {
            force = true;
            i += 1;
        } else {
            return Err(syntax());
        }
    }
    Ok(Cmd::Failover(Failover::Start { to, timeout, force }))
}

/// The timeout of WAIT and WAITAOF, in milliseconds, checked as Redis
/// does; it is never waited for.
/// REPLICAOF host port, or NO ONE.
fn parse_replicaof(args: &[Bytes]) -> Result<Cmd, Reply> {
    if args[1].eq_ignore_ascii_case(b"NO") && args[2].eq_ignore_ascii_case(b"ONE") {
        return Ok(Cmd::ReplicaOf(None));
    }
    let port = parse_i64(&args[2])
        .and_then(|port| u16::try_from(port).ok())
        .ok_or_else(|| Reply::err("ERR Invalid master port"))?;
    let host =
        String::from_utf8(args[1].clone()).map_err(|_| Reply::err("ERR Invalid master host"))?;
    Ok(Cmd::ReplicaOf(Some((host, port))))
}

/// The timeout of WAIT and WAITAOF, in milliseconds.
fn parse_wait_timeout(raw: &[u8]) -> Result<u64, Reply> {
    let ms = parse_i64(raw)
        .ok_or_else(|| Reply::err("ERR timeout is not an integer or out of range"))?;
    if ms < 0 {
        return Err(Reply::err("ERR timeout is negative"));
    }
    let now = i64::try_from(crate::clock::unix_ms()).unwrap_or(i64::MAX);
    if ms > i64::MAX - now {
        return Err(Reply::err("ERR timeout is out of range"));
    }
    Ok(ms.unsigned_abs())
}

/// `SET key value [NX | XX | IFEQ value] [GET]
/// [EX s | PX ms | EXAT s | PXAT ms | KEEPTTL]`.
fn parse_set(mut args: Vec<Bytes>) -> Result<Cmd, Reply> {
    let mut opts = SetOpts {
        cond: None,
        expire: Expire::Clear,
        get: false,
        ifeq: None,
    };
    let mut i = 3;
    while i < args.len() {
        let option = &args[i];
        let free = opts.cond.is_none() && opts.ifeq.is_none();
        if option.eq_ignore_ascii_case(b"GET") {
            opts.get = true;
        } else if option.eq_ignore_ascii_case(b"NX") && free {
            opts.cond = Some(Cond::Nx);
        } else if option.eq_ignore_ascii_case(b"XX") && free {
            opts.cond = Some(Cond::Xx);
        } else if option.eq_ignore_ascii_case(b"IFEQ") && free && i + 1 < args.len() {
            opts.ifeq = Some(args[i + 1].clone());
            i += 1;
        } else if opts.expire != Expire::Clear {
            // A second expiry option, or anything unknown.
            return Err(syntax());
        } else if option.eq_ignore_ascii_case(b"KEEPTTL") {
            opts.expire = Expire::Keep;
        } else {
            let (secs, absolute) = time_option(option).ok_or_else(syntax)?;
            let raw = args.get(i + 1).ok_or_else(syntax)?;
            opts.expire = parse_when(raw, secs, absolute, true, "set")?;
            i += 1;
        }
        i += 1;
    }
    args.truncate(3);
    let val = args.pop().unwrap_or_default();
    Ok(Cmd::SetOpts(args.pop().unwrap_or_default(), val, opts))
}

/// EX, PX, EXAT or PXAT: whether it counts seconds, and whether it names
/// a point in time rather than a duration.
fn time_option(option: &[u8]) -> Option<(bool, bool)> {
    match option.to_ascii_uppercase().as_slice() {
        b"EX" => Some((true, false)),
        b"PX" => Some((false, false)),
        b"EXAT" => Some((true, true)),
        b"PXAT" => Some((false, true)),
        _ => None,
    }
}

/// An expiry argument. `strict` commands (SET and friends) refuse zero and
/// negative times; EXPIRE takes them and deletes the key.
pub(crate) fn parse_when(
    raw: &[u8],
    secs: bool,
    absolute: bool,
    strict: bool,
    cmd: &str,
) -> Result<Expire, Reply> {
    let v = parse_i64(raw).ok_or_else(|| Reply::err(NOT_INT))?;
    let invalid = || {
        Reply::Error(format!(
            "ERR invalid expire time in '{}' command",
            cmd.to_ascii_lowercase()
        ))
    };
    if strict && v <= 0 {
        return Err(invalid());
    }
    let ms = if secs {
        v.checked_mul(1000).ok_or_else(invalid)?
    } else {
        v
    };
    if absolute {
        return Ok(Expire::At(ms));
    }
    // The deadline, now + ms, has to fit.
    let now = i64::try_from(unix_ms()).unwrap_or(i64::MAX);
    if ms > i64::MAX - now {
        return Err(invalid());
    }
    Ok(Expire::In(ms))
}

pub(crate) fn parse_cond(option: &[u8]) -> Result<Cond, Reply> {
    const SHOWN: usize = 64;
    match option.to_ascii_uppercase().as_slice() {
        b"NX" => Ok(Cond::Nx),
        b"XX" => Ok(Cond::Xx),
        b"GT" => Ok(Cond::Gt),
        b"LT" => Ok(Cond::Lt),
        _ => {
            let shown = String::from_utf8_lossy(&option[..option.len().min(SHOWN)]);
            Err(Reply::Error(format!("ERR Unsupported option {shown}")))
        }
    }
}

fn parse_cursor(raw: &[u8]) -> Result<u64, Reply> {
    std::str::from_utf8(raw)
        .ok()
        .filter(|s| s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Reply::err("ERR invalid cursor"))
}

struct ScanOpts {
    pattern: Option<Bytes>,
    count: usize,
    ty: Option<Bytes>,
    novalues: bool,
}

/// `[MATCH pattern] [COUNT count]`, then `[TYPE type]` for SCAN or
/// `[NOVALUES]` for HSCAN.
fn parse_scan_opts(mut rest: &[Bytes], keyspace: bool) -> Result<ScanOpts, Reply> {
    const DEFAULT_COUNT: usize = 10;
    let mut opts = ScanOpts {
        pattern: None,
        count: DEFAULT_COUNT,
        ty: None,
        novalues: false,
    };
    while let Some(option) = rest.first() {
        if !keyspace && option.eq_ignore_ascii_case(b"NOVALUES") {
            opts.novalues = true;
            rest = &rest[1..];
            continue;
        }
        let value = rest.get(1).ok_or_else(syntax)?;
        if option.eq_ignore_ascii_case(b"MATCH") {
            if value.len() > MAX_PATTERN {
                return Err(Reply::err("ERR pattern too long"));
            }
            opts.pattern = Some(value.clone());
        } else if option.eq_ignore_ascii_case(b"COUNT") {
            let count = parse_i64(value).ok_or_else(|| Reply::err(NOT_INT))?;
            if count < 1 {
                return Err(syntax());
            }
            opts.count = usize::try_from(count).unwrap_or(usize::MAX);
        } else if keyspace && option.eq_ignore_ascii_case(b"TYPE") {
            opts.ty = Some(value.to_ascii_lowercase());
        } else {
            return Err(syntax());
        }
        rest = &rest[2..];
    }
    Ok(opts)
}

/// `HELLO [protover [AUTH username password] [SETNAME clientname]]`.
/// Returns the protocol version and the user and password of AUTH.
#[allow(clippy::type_complexity)]
fn parse_hello(args: &[Bytes]) -> Result<(Option<u8>, Option<(Bytes, Bytes)>), Reply> {
    const SHOWN_OPTION_LEN: usize = 64;
    let Some(version) = args.get(1) else {
        return Ok((None, None));
    };
    let mut auth = None;
    let proto = match parse_i64(version) {
        Some(2) => 2,
        Some(3) => 3,
        Some(_) => return Err(Reply::err("NOPROTO unsupported protocol version")),
        None => {
            return Err(Reply::err(
                "ERR Protocol version is not an integer or out of range",
            ))
        }
    };
    let mut rest = &args[2..];
    while let Some(option) = rest.first() {
        if option.eq_ignore_ascii_case(b"AUTH") && rest.len() >= 3 {
            auth = Some((rest[1].clone(), rest[2].clone()));
            rest = &rest[3..];
        } else if option.eq_ignore_ascii_case(b"SETNAME") && rest.len() >= 2 {
            rest = &rest[2..];
        } else {
            let shown = &option[..option.len().min(SHOWN_OPTION_LEN)];
            return Err(Reply::Error(format!(
                "ERR Syntax error in HELLO option '{}'",
                String::from_utf8_lossy(shown)
            )));
        }
    }
    Ok((Some(proto), auth))
}

/// Valkey's wording: the name and then the arguments, each cut to fit 128
/// bytes, and line breaks turned into spaces so the reply stays one line.
fn unknown(args: &[Bytes]) -> Reply {
    const SHOWN: usize = 128;
    // Valkey formats with %s, which stops at a zero byte.
    fn c_str(b: &[u8]) -> &[u8] {
        b.split(|&c| c == 0).next().unwrap_or(b)
    }
    let name = args.first().map_or(&[][..], |n| c_str(n));
    let mut text = b"ERR unknown command '".to_vec();
    text.extend_from_slice(&name[..name.len().min(SHOWN)]);
    text.extend_from_slice(b"', with args beginning with: ");
    let mut listed = 0;
    for arg in args.iter().skip(1) {
        if listed >= SHOWN {
            break;
        }
        let arg = c_str(arg);
        let take = arg.len().min(SHOWN - listed);
        text.push(b'\'');
        text.extend_from_slice(&arg[..take]);
        text.extend_from_slice(b"' ");
        listed += take + 3;
    }
    for b in &mut text {
        if *b == b'\r' || *b == b'\n' {
            *b = b' ';
        }
    }
    Reply::Error(String::from_utf8_lossy(&text).into_owned())
}
