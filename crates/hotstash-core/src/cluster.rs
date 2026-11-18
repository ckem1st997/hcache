//! A one-node Redis Cluster: the slot of a key, the check that a command's
//! keys share one slot, and the CLUSTER commands a cluster client asks a
//! node. This node serves all 16384 slots and knows no other node, so no
//! key is ever redirected.

use crate::cmd::{Bytes, Cmd, Sub, NOT_INT};
use crate::cmdtable::keys_of;
use crate::engine::hash_tag;
use crate::resp::{parse_i64, Reply};
use std::sync::atomic::{AtomicU16, Ordering};

/// Slots in a Redis Cluster.
pub const SLOTS: u16 = 16384;

pub const CROSSSLOT: &str = "CROSSSLOT Keys in request don't hash to the same slot";
pub const DISABLED: &str = "ERR This instance has cluster support disabled";

/// The cluster bus port is the client port plus this, as Redis has it.
const BUS_OFFSET: u32 = 10000;

/// CRC16/XMODEM by bytes, as Redis Cluster hashes keys.
const CRC16: [u16; 256] = {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

fn crc16(bytes: &[u8]) -> u16 {
    bytes.iter().fold(0, |crc, &b| {
        (crc << 8) ^ CRC16[usize::from((crc >> 8) as u8 ^ b)]
    })
}

/// The slot of `key`: of its hash tag when it has one.
pub fn key_slot(key: &[u8]) -> u16 {
    crc16(hash_tag(key).unwrap_or(key)) & (SLOTS - 1)
}

/// The one slot all of `cmd`'s keys, and the channels of the shard
/// pub/sub commands, are in; `Err` when they are in more than one,
/// `Ok(None)` when it has none.
pub fn slot_of(cmd: &Cmd) -> Result<Option<u16>, CrossSlot> {
    let mut slot = None;
    let mut crossed = false;
    let mut visit = |key: &[u8]| {
        let this = key_slot(key);
        crossed |= *slot.get_or_insert(this) != this;
    };
    match cmd {
        Cmd::Sub(Sub::SSubscribe(channels) | Sub::SUnsubscribe(channels)) => {
            channels.iter().for_each(|channel| visit(channel))
        }
        Cmd::SPublish(channel, _) => visit(channel),
        other => keys_of(other, &mut visit),
    }
    if crossed {
        Err(CrossSlot)
    } else {
        Ok(slot)
    }
}

/// The keys of a command are in more than one slot.
#[derive(Debug, PartialEq, Eq)]
pub struct CrossSlot;

/// This node, as the other nodes of a cluster would know it.
pub struct Node {
    id: String,
    shard_id: String,
    /// The client port, once the server listens.
    port: AtomicU16,
}

impl Node {
    pub fn new() -> Node {
        Node {
            id: random_id(),
            shard_id: random_id(),
            port: AtomicU16::new(0),
        }
    }

    pub fn set_port(&self, port: u16) {
        self.port.store(port, Ordering::Relaxed);
    }

    fn port(&self) -> u16 {
        self.port.load(Ordering::Relaxed)
    }
}

impl Default for Node {
    fn default() -> Self {
        Node::new()
    }
}

/// 40 hexadecimal digits, as Redis names nodes and shards. Not a secret:
/// the clock stands in when the system's generator is not there.
fn random_id() -> String {
    let mut raw = [0u8; 20];
    if getrandom::getrandom(&mut raw).is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        raw[..16].copy_from_slice(&nanos.to_le_bytes());
        raw[16..].copy_from_slice(&std::process::id().to_le_bytes());
    }
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

/// Whether the CLUSTER subcommand in `args` reads the keyspace: these run
/// where keys can be seen, the others are answered from `Node` alone.
pub fn reads_keys(args: &[Bytes]) -> bool {
    args.get(1).is_some_and(|sub| {
        sub.eq_ignore_ascii_case(b"COUNTKEYSINSLOT") || sub.eq_ignore_ascii_case(b"GETKEYSINSLOT")
    })
}

/// What CLUSTER COUNTKEYSINSLOT and GETKEYSINSLOT ask for, checked as
/// Redis does: the slot, and the most keys to return.
pub fn keys_request(args: &[Bytes]) -> Result<(u16, Option<usize>), Reply> {
    let int = |raw: &Bytes| parse_i64(raw).ok_or_else(|| Reply::err(NOT_INT));
    let slot = int(&args[2])?;
    let Some(count) = args.get(3) else {
        return u16::try_from(slot)
            .ok()
            .filter(|slot| *slot < SLOTS)
            .map(|slot| (slot, None))
            .ok_or_else(|| Reply::err("ERR Invalid slot"));
    };
    let count = int(count)?;
    match (u16::try_from(slot), usize::try_from(count)) {
        (Ok(slot), Ok(count)) if slot < SLOTS => Ok((slot, Some(count))),
        _ => Err(Reply::err("ERR Invalid slot or number of keys")),
    }
}

/// The arity of each CLUSTER subcommand, as COMMAND reports it: negative
/// for "at least".
const ARITY: &[(&str, i32)] = &[
    ("ADDSLOTS", -3),
    ("ADDSLOTSRANGE", -4),
    ("BUMPEPOCH", 2),
    ("CANCELSLOTMIGRATIONS", 2),
    ("COUNT-FAILURE-REPORTS", 3),
    ("COUNTKEYSINSLOT", 3),
    ("DELSLOTS", -3),
    ("DELSLOTSRANGE", -4),
    ("FAILOVER", -2),
    ("FLUSHSLOT", -3),
    ("FLUSHSLOTS", 2),
    ("FORGET", 3),
    ("GETKEYSINSLOT", 4),
    ("GETSLOTMIGRATIONS", 2),
    ("HELP", 2),
    ("INFO", 2),
    ("KEYSLOT", 3),
    ("LINKS", 2),
    ("MEET", -4),
    ("MIGRATESLOTS", -4),
    ("MYID", 2),
    ("MYSHARDID", 2),
    ("NODES", 2),
    ("REPLICAS", 3),
    ("REPLICATE", -3),
    ("RESET", -2),
    ("SAVECONFIG", 2),
    ("SET-CONFIG-EPOCH", 3),
    ("SETSLOT", -4),
    ("SHARDS", 2),
    ("SLAVES", 3),
    ("SLOT-STATS", -4),
    ("SLOTS", 2),
    ("SYNCSLOTS", -3),
];

/// `CLUSTER sub ...` as the parser checks it, before anything asks
/// whether cluster support is on.
pub fn parse(args: Vec<Bytes>) -> Result<Cmd, Reply> {
    let sub = &args[1];
    let Some(&(name, arity)) = ARITY
        .iter()
        .find(|(name, _)| name.as_bytes().eq_ignore_ascii_case(sub))
    else {
        return Err(Reply::Error(format!(
            "ERR unknown subcommand '{}'. Try CLUSTER HELP.",
            String::from_utf8_lossy(&sub[..sub.len().min(128)])
        )));
    };
    let n = args.len() as i32;
    if (arity >= 0 && n != arity) || n < -arity {
        return Err(Reply::Error(format!(
            "ERR wrong number of arguments for 'cluster|{}' command",
            name.to_ascii_lowercase()
        )));
    }
    Ok(Cmd::Cluster(args))
}

/// A CLUSTER subcommand that needs no keys.
pub fn command(node: Option<&Node>, args: &[Bytes]) -> Reply {
    let Some(node) = node else {
        return Reply::err(DISABLED);
    };
    let sub = args[1].to_ascii_uppercase();
    let bulk = |s: &str| Reply::Bulk(s.as_bytes().to_vec());
    match sub.as_slice() {
        b"HELP" => help(),
        b"INFO" => bulk(&info()),
        b"MYID" => bulk(&node.id),
        b"MYSHARDID" => bulk(&node.shard_id),
        b"NODES" => bulk(&format!(
            "{} :{}@{} myself,master - 0 0 0 connected 0-{}\n",
            node.id,
            node.port(),
            u32::from(node.port()) + BUS_OFFSET,
            SLOTS - 1
        )),
        b"SLOTS" => Reply::Array(vec![Reply::Array(vec![
            Reply::Int(0),
            Reply::Int(i64::from(SLOTS - 1)),
            Reply::Array(vec![
                bulk(""),
                Reply::Int(node.port().into()),
                bulk(&node.id),
                Reply::Map(Vec::new()),
            ]),
        ])]),
        b"SHARDS" => Reply::Array(vec![Reply::Map(vec![
            bulk("slots"),
            Reply::Array(vec![Reply::Int(0), Reply::Int(i64::from(SLOTS - 1))]),
            bulk("nodes"),
            Reply::Array(vec![Reply::Map(vec![
                bulk("id"),
                bulk(&node.id),
                bulk("port"),
                Reply::Int(node.port().into()),
                bulk("ip"),
                bulk(""),
                bulk("endpoint"),
                bulk(""),
                bulk("role"),
                bulk("master"),
                bulk("replication-offset"),
                Reply::Int(0),
                bulk("health"),
                bulk("online"),
            ])]),
            bulk("id"),
            bulk(&node.shard_id),
        ])]),
        b"KEYSLOT" => Reply::Int(key_slot(&args[2]).into()),
        b"LINKS" | b"GETSLOTMIGRATIONS" => Reply::Array(Vec::new()),
        b"REPLICAS" | b"SLAVES" | b"COUNT-FAILURE-REPORTS" if args[2] == node.id.as_bytes() => {
            if sub == b"COUNT-FAILURE-REPORTS" {
                Reply::Int(0)
            } else {
                Reply::Array(Vec::new())
            }
        }
        b"REPLICAS" | b"SLAVES" | b"COUNT-FAILURE-REPORTS" | b"FORGET" => Reply::Error(format!(
            "ERR Unknown node {}",
            String::from_utf8_lossy(&args[2][..args[2].len().min(128)])
        )),
        b"SAVECONFIG" => Reply::Ok,
        b"BUMPEPOCH" => Reply::Simple("STILL 0".into()),
        b"ADDSLOTS" | b"ADDSLOTSRANGE" => match parse_i64(&args[2]) {
            Some(slot @ 0..=16383) => Reply::Error(format!("ERR Slot {slot} is already busy")),
            _ => Reply::err("ERR Invalid or out of range slot"),
        },
        _ => Reply::Error(format!(
            "ERR CLUSTER {} is not supported: this node is a cluster of its own",
            String::from_utf8_lossy(&sub)
        )),
    }
}

fn info() -> String {
    format!(
        "cluster_state:ok\r\ncluster_slots_assigned:{SLOTS}\r\ncluster_slots_ok:{SLOTS}\r\n\
         cluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_nodes_pfail:0\r\n\
         cluster_nodes_fail:0\r\ncluster_voting_nodes_pfail:0\r\ncluster_voting_nodes_fail:0\r\n\
         cluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:0\r\n\
         cluster_my_epoch:0\r\ncluster_stats_messages_sent:0\r\n\
         cluster_stats_messages_received:0\r\ncluster_stats_bytes_sent:0\r\n\
         cluster_stats_bytes_received:0\r\ncluster_stats_pubsub_bytes_sent:0\r\n\
         cluster_stats_pubsub_bytes_received:0\r\ncluster_stats_module_bytes_sent:0\r\n\
         cluster_stats_module_bytes_received:0\r\n\
         total_cluster_links_buffer_limit_exceeded:0\r\n"
    )
}

fn help() -> Reply {
    const TEXT: &[&str] = &[
        "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "COUNTKEYSINSLOT <slot>",
        "    Return the number of keys in <slot>.",
        "GETKEYSINSLOT <slot> <count>",
        "    Return key names stored by current node in a slot.",
        "INFO",
        "    Return information about the cluster.",
        "KEYSLOT <key>",
        "    Return the hash slot for <key>.",
        "MYID",
        "    Return the node id.",
        "MYSHARDID",
        "    Return the node's shard id.",
        "NODES",
        "    Return cluster configuration seen by node. Output format:",
        "    <id> <ip:port@bus-port[,hostname]> <flags> <primary> <pings> <pongs> <epoch> <link> <slot> ...",
        "REPLICAS <node-id>",
        "    Return <node-id> replicas.",
        "SLOTS",
        "    Return information about slots range mappings. Each range is made of:",
        "    start, end, primary and replicas IP addresses, ports and ids",
        "SLOT-STATS",
        "    Return an array of slot usage statistics for slots assigned to the current node.",
        "SHARDS",
        "    Return information about slot range mappings and the nodes associated with them.",
        "ADDSLOTS <slot> [<slot> ...]",
        "    Assign slots to current node.",
        "ADDSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
        "    Assign slots which are between <start-slot> and <end-slot> to current node.",
        "BUMPEPOCH",
        "    Advance the cluster config epoch.",
        "COUNT-FAILURE-REPORTS <node-id>",
        "    Return number of failure reports for <node-id>.",
        "DELSLOTS <slot> [<slot> ...]",
        "    Delete slots information from current node.",
        "DELSLOTSRANGE <start slot> <end slot> [<start slot> <end slot> ...]",
        "    Delete slots information which are between <start-slot> and <end-slot> from current node.",
        "FAILOVER [FORCE|TAKEOVER]",
        "    Promote current replica node to being a master.",
        "FORGET <node-id>",
        "    Remove a node from the cluster.",
        "FLUSHSLOTS",
        "    Delete current node own slots information.",
        "MEET <ip> <port> [<bus-port>]",
        "    Connect nodes into a working cluster.",
        "REPLICATE <node-id>",
        "    Configure current node as replica to <node-id>.",
        "RESET [HARD|SOFT]",
        "    Reset current node (default: soft).",
        "SET-CONFIG-EPOCH <epoch>",
        "    Set config epoch of current node.",
        "SETSLOT <slot> (IMPORTING <node-id>|MIGRATING <node-id>|STABLE|NODE <node-id>)",
        "    Set slot state.",
        "SAVECONFIG",
        "    Force saving cluster configuration on disk.",
        "LINKS",
        "    Return information about all network links between this node and its peers.",
        "    Output format is an array where each array element is a map containing attributes of a link",
        "MIGRATESLOTS SLOTSRANGE start-slot end-slot [start-slot end-slot ...] NODE node-id [SLOTSRANGE start-slot end-slot [start-slot end-slot ...] NODE node-id ...]",
        "    Migrate the specified slot ranges from this node to the specified node.",
        "CANCELSLOTMIGRATIONS ALL",
        "    Cancel all migrations.",
        "GETSLOTMIGRATIONS",
        "    Get information about ongoing and recently finished slot imports and exports.",
        "HELP",
        "    Print this help.",
    ];
    Reply::Array(
        TEXT.iter()
            .map(|line| Reply::Simple((*line).into()))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    #[test]
    fn slots_match_redis() {
        // From the Redis Cluster specification and redis-cli.
        assert_eq!(crc16(b"123456789"), 0x31c3);
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"bar"), 5061);
        assert_eq!(key_slot(b""), 0);
        assert_eq!(key_slot(b"{user1000}.following"), key_slot(b"user1000"));
        assert_eq!(key_slot(b"foo{}{bar}"), crc16(b"foo{}{bar}") & 16383);
        assert_eq!(key_slot(b"foo{{bar}}zap"), key_slot(b"{bar"));
        assert_eq!(key_slot(b"foo{bar}{zap}"), key_slot(b"bar"));
    }

    #[test]
    fn keys_and_shard_channels_must_share_a_slot() {
        let slot = |parts: &[&str]| slot_of(&Cmd::parse(args(parts)).expect("parses"));
        assert_eq!(slot(&["MGET", "{a}1", "{a}2"]), Ok(Some(key_slot(b"a"))));
        assert_eq!(slot(&["MGET", "foo", "bar"]), Err(CrossSlot));
        assert_eq!(slot(&["PING"]), Ok(None));
        assert_eq!(slot(&["SSUBSCRIBE", "foo", "bar"]), Err(CrossSlot));
        assert_eq!(slot(&["SPUBLISH", "foo", "x"]), Ok(Some(12182)));
        assert_eq!(slot(&["WATCH", "foo", "bar"]), Err(CrossSlot));
        assert_eq!(
            slot(&["EVAL", "return 1", "2", "foo", "bar"]),
            Err(CrossSlot)
        );
    }

    #[test]
    fn subcommands_are_checked_before_cluster_support() {
        let err = |parts: &[&str]| match parse(args(parts)) {
            Err(Reply::Error(e)) => e,
            other => panic!("{parts:?}: {other:?}"),
        };
        assert_eq!(
            err(&["CLUSTER", "nope"]),
            "ERR unknown subcommand 'nope'. Try CLUSTER HELP."
        );
        assert_eq!(
            err(&["CLUSTER", "keyslot"]),
            "ERR wrong number of arguments for 'cluster|keyslot' command"
        );
        assert!(parse(args(&["CLUSTER", "addslots", "1", "2"])).is_ok());
    }

    #[test]
    fn slot_requests_are_checked() {
        let req = |parts: &[&str]| keys_request(&args(parts));
        assert_eq!(
            req(&["CLUSTER", "COUNTKEYSINSLOT", "16383"]),
            Ok((16383, None))
        );
        assert_eq!(
            req(&["CLUSTER", "COUNTKEYSINSLOT", "16384"]),
            Err(Reply::err("ERR Invalid slot"))
        );
        assert_eq!(
            req(&["CLUSTER", "GETKEYSINSLOT", "1", "-1"]),
            Err(Reply::err("ERR Invalid slot or number of keys"))
        );
        assert_eq!(
            req(&["CLUSTER", "GETKEYSINSLOT", "x", "1"]),
            Err(Reply::err(NOT_INT))
        );
    }
}
