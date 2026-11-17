//! The command table: every command this server implements, with the
//! metadata Redis 7.4 gives it (arity, flags, key positions, ACL
//! categories), and the COMMAND command built on it.

use crate::cmd::{Bytes, Cmd, Txn, MAX_PATTERN};
use crate::glob::glob_match;
use crate::resp::Reply;

pub struct CmdInfo {
    /// Lower case.
    pub name: &'static str,
    /// Redis convention: negative means at least that many.
    pub arity: i32,
    pub flags: &'static [&'static str],
    pub first_key: i32,
    pub last_key: i32,
    pub step: i32,
    /// ACL categories, without '@'.
    pub categories: &'static [&'static str],
    pub group: &'static str,
    pub summary: &'static str,
    /// Upper case; only for container commands.
    pub subcommands: &'static [&'static str],
}

/// No command name is longer than this.
const MAX_NAME: usize = 32;
/// Longest part of a client-supplied argument echoed in an error.
const SHOWN: usize = 64;

#[allow(clippy::too_many_arguments)]
const fn e(
    name: &'static str,
    arity: i32,
    flags: &'static [&'static str],
    first_key: i32,
    last_key: i32,
    step: i32,
    categories: &'static [&'static str],
    group: &'static str,
    summary: &'static str,
) -> CmdInfo {
    CmdInfo {
        name,
        arity,
        flags,
        first_key,
        last_key,
        step,
        categories,
        group,
        summary,
        subcommands: &[],
    }
}

/// A container command: its keys belong to its subcommands, its flags and
/// categories are the union of theirs.
const fn c(
    name: &'static str,
    arity: i32,
    flags: &'static [&'static str],
    categories: &'static [&'static str],
    group: &'static str,
    summary: &'static str,
    subcommands: &'static [&'static str],
) -> CmdInfo {
    CmdInfo {
        name,
        arity,
        flags,
        first_key: 0,
        last_key: 0,
        step: 0,
        categories,
        group,
        summary,
        subcommands,
    }
}

// Common flag sets.
const RO: &[&str] = &["readonly"];
const RO_FAST: &[&str] = &["readonly", "fast"];
const W: &[&str] = &["write"];
const W_FAST: &[&str] = &["write", "fast"];
const W_OOM: &[&str] = &["write", "denyoom"];
const W_OOM_FAST: &[&str] = &["write", "denyoom", "fast"];
const W_MOVABLE: &[&str] = &["write", "movablekeys"];
const W_OOM_MOVABLE: &[&str] = &["write", "denyoom", "movablekeys"];
const RO_MOVABLE: &[&str] = &["readonly", "movablekeys"];
const NO_AUTH: &[&str] = &[
    "noscript",
    "loading",
    "stale",
    "fast",
    "no_auth",
    "allow_busy",
];
const TXN_FAST: &[&str] = &["noscript", "loading", "stale", "fast", "allow_busy"];
const PUBSUB: &[&str] = &["pubsub", "noscript", "loading", "stale"];
const BGJOB: &[&str] = &["admin", "noscript", "no_async_loading"];
const ADMIN_CONTAINER: &[&str] = &["admin", "noscript", "loading", "stale"];
const LOAD_FAST: &[&str] = &["loading", "stale", "fast"];
// valkey-bloom's commands, as a module's.
const MOD_W_FAST: &[&str] = &["write", "denyoom", "module", "fast"];
const MOD_W: &[&str] = &["write", "denyoom", "module"];
const MOD_RO_FAST: &[&str] = &["readonly", "module", "fast"];
const MOD_W_FAST_ANY: &[&str] = &["write", "module", "fast"];
const MOD_RO_MOVABLE: &[&str] = &["readonly", "module", "movablekeys"];
// Valkey keeps `may_replicate` out of COMMAND's flags.
const EVAL: &[&str] = &[
    "noscript",
    "stale",
    "skip_monitor",
    "no_mandatory_keys",
    "movablekeys",
];
const EVAL_RO: &[&str] = &[
    "readonly",
    "noscript",
    "stale",
    "skip_monitor",
    "no_mandatory_keys",
    "movablekeys",
];

// Common category sets.
const STR_R_FAST: &[&str] = &["read", "string", "fast"];
const STR_R_SLOW: &[&str] = &["read", "string", "slow"];
const STR_W_FAST: &[&str] = &["write", "string", "fast"];
const STR_W_SLOW: &[&str] = &["write", "string", "slow"];
const BIT_R_FAST: &[&str] = &["read", "bitmap", "fast"];
const BIT_R_SLOW: &[&str] = &["read", "bitmap", "slow"];
const BIT_W_SLOW: &[&str] = &["write", "bitmap", "slow"];
const HASH_R_FAST: &[&str] = &["read", "hash", "fast"];
const HASH_R_SLOW: &[&str] = &["read", "hash", "slow"];
const HASH_W_FAST: &[&str] = &["write", "hash", "fast"];
const LIST_R_FAST: &[&str] = &["read", "list", "fast"];
const LIST_R_SLOW: &[&str] = &["read", "list", "slow"];
const LIST_W_FAST: &[&str] = &["write", "list", "fast"];
const LIST_W_SLOW: &[&str] = &["write", "list", "slow"];
const LIST_BLOCK: &[&str] = &["write", "list", "slow", "blocking"];
const SET_R_FAST: &[&str] = &["read", "set", "fast"];
const SET_R_SLOW: &[&str] = &["read", "set", "slow"];
const SET_W_FAST: &[&str] = &["write", "set", "fast"];
const SET_W_SLOW: &[&str] = &["write", "set", "slow"];
const Z_R_FAST: &[&str] = &["read", "sortedset", "fast"];
const Z_R_SLOW: &[&str] = &["read", "sortedset", "slow"];
const Z_W_FAST: &[&str] = &["write", "sortedset", "fast"];
const Z_W_SLOW: &[&str] = &["write", "sortedset", "slow"];
const Z_BLOCK_FAST: &[&str] = &["write", "sortedset", "fast", "blocking"];
const X_R_FAST: &[&str] = &["read", "stream", "fast"];
const X_R_SLOW: &[&str] = &["read", "stream", "slow"];
const X_W_FAST: &[&str] = &["write", "stream", "fast"];
const X_W_SLOW: &[&str] = &["write", "stream", "slow"];
const HLL_W_FAST: &[&str] = &["write", "hyperloglog", "fast"];
const HLL_W_SLOW: &[&str] = &["write", "hyperloglog", "slow"];
const HLL_R_SLOW: &[&str] = &["read", "hyperloglog", "slow"];
const GEO_R: &[&str] = &["read", "geo", "slow"];
const GEO_W: &[&str] = &["write", "geo", "slow"];
const KS_R_FAST: &[&str] = &["keyspace", "read", "fast"];
const KS_R_SLOW: &[&str] = &["keyspace", "read", "slow"];
const KS_W_FAST: &[&str] = &["keyspace", "write", "fast"];
const KS_W_SLOW: &[&str] = &["keyspace", "write", "slow"];
const KS_W_DANGER: &[&str] = &["keyspace", "write", "slow", "dangerous"];
const CONN_FAST: &[&str] = &["fast", "connection"];
const ADMIN: &[&str] = &["admin", "slow", "dangerous"];
const REPL_SYNC: &[&str] = &["admin", "noscript", "no_async_loading", "no_multi"];
const REPL_FOLLOW: &[&str] = &["admin", "noscript", "stale", "no_async_loading"];
const TXN_F: &[&str] = &["fast", "transaction"];
const SCRIPTING: &[&str] = &["slow", "scripting"];
const PUBSUB_SLOW: &[&str] = &["pubsub", "slow"];
const BF_W_FAST: &[&str] = &["write", "fast", "bloom"];
const BF_W_SLOW: &[&str] = &["write", "slow", "bloom"];
const BF_R_FAST: &[&str] = &["read", "fast", "bloom"];
const JSON_R_FAST: &[&str] = &["json", "read", "fast"];
const JSON_R_SLOW: &[&str] = &["json", "read", "slow"];
const JSON_W_FAST: &[&str] = &["json", "write", "fast"];
const JSON_W_SLOW: &[&str] = &["json", "write", "slow"];

/// Sorted by name: `lookup` searches it by bisection.
#[rustfmt::skip]
static TABLE: &[CmdInfo] = &[
    c("acl", -2, ADMIN_CONTAINER, ADMIN, "server", "Manage users and their permissions.",
        &["CAT", "DELUSER", "DRYRUN", "GENPASS", "GETUSER", "HELP", "LIST", "LOAD", "LOG", "SAVE",
          "SETUSER", "USERS", "WHOAMI"]),
    e("append", 3, W_OOM_FAST, 1, 1, 1, STR_W_FAST, "string", "Add bytes to the end of a string."),
    e("asking", 1, &["fast"], 0, 0, 0, CONN_FAST, "cluster",
        "Let the next command reach a slot that is being imported."),
    e("auth", -2, NO_AUTH, 0, 0, 0, CONN_FAST, "connection", "Log the connection in as a user."),
    // A module's commands have no arity of their own (-1).
    e("bf.add", -1, MOD_W_FAST, 1, 1, 1, BF_W_FAST, "module", "Add an item to a bloom filter."),
    e("bf.card", -1, MOD_RO_FAST, 1, 1, 1, BF_R_FAST, "module",
        "Count the items added to a bloom filter."),
    e("bf.exists", -1, MOD_RO_FAST, 1, 1, 1, BF_R_FAST, "module",
        "Whether an item may be in a bloom filter."),
    e("bf.info", -1, MOD_RO_FAST, 1, 1, 1, BF_R_FAST, "module", "Describe a bloom filter."),
    e("bf.insert", -1, MOD_W_FAST, 1, 1, 1, BF_W_FAST, "module",
        "Add items to a bloom filter, making it with the options given."),
    e("bf.load", -1, MOD_W, 1, 1, 1, BF_W_SLOW, "module",
        "Make a bloom filter from what its AOF rewrite wrote."),
    e("bf.madd", -1, MOD_W_FAST, 1, 1, 1, BF_W_FAST, "module", "Add items to a bloom filter."),
    e("bf.mexists", -1, MOD_RO_FAST, 1, 1, 1, BF_R_FAST, "module",
        "Whether items may be in a bloom filter."),
    e("bf.reserve", -1, MOD_W_FAST, 1, 1, 1, BF_W_FAST, "module", "Make an empty bloom filter."),
    e("bgrewriteaof", 1, BGJOB, 0, 0, 0, ADMIN, "server", "Rewrite the data file in the background."),
    e("bgsave", -1, BGJOB, 0, 0, 0, ADMIN, "server", "Save the dataset in the background."),
    e("bitcount", -2, RO, 1, 1, 1, BIT_R_SLOW, "bitmap", "Count the set bits of a string."),
    e("bitfield", -2, W_OOM, 1, 1, 1, BIT_W_SLOW, "bitmap",
        "Read and write integers of any width inside a string."),
    e("bitfield_ro", -2, RO_FAST, 1, 1, 1, BIT_R_FAST, "bitmap",
        "Read integers of any width inside a string."),
    e("bitop", -4, W_OOM, 2, -1, 1, BIT_W_SLOW, "bitmap", "Combine strings bit by bit into a key."),
    e("bitpos", -3, RO, 1, 1, 1, BIT_R_SLOW, "bitmap", "Find the first bit set or clear in a string."),
    e("blmove", 6, &["write", "denyoom", "blocking"], 1, 2, 1, LIST_BLOCK, "list",
        "Move an element between lists, waiting for one if needed."),
    e("blmpop", -5, &["write", "blocking", "movablekeys"], 0, 0, 0, LIST_BLOCK, "list",
        "Pop from the first non-empty list, waiting for one if needed."),
    e("blpop", -3, &["write", "blocking"], 1, -2, 1, LIST_BLOCK, "list",
        "Pop the head of the first non-empty list, waiting if needed."),
    e("brpop", -3, &["write", "blocking"], 1, -2, 1, LIST_BLOCK, "list",
        "Pop the tail of the first non-empty list, waiting if needed."),
    e("brpoplpush", 4, &["write", "denyoom", "blocking"], 1, 2, 1, LIST_BLOCK, "list",
        "Move a list's tail to another list's head, waiting if needed."),
    e("bzmpop", -5, &["write", "blocking", "movablekeys"], 0, 0, 0,
        &["write", "sortedset", "slow", "blocking"], "sorted-set",
        "Pop members from the first non-empty sorted set, waiting if needed."),
    e("bzpopmax", -3, &["write", "blocking", "fast"], 1, -2, 1, Z_BLOCK_FAST, "sorted-set",
        "Pop the highest-scored member, waiting if needed."),
    e("bzpopmin", -3, &["write", "blocking", "fast"], 1, -2, 1, Z_BLOCK_FAST, "sorted-set",
        "Pop the lowest-scored member, waiting if needed."),
    c("client", -2, ADMIN_CONTAINER, &["admin", "slow", "dangerous", "connection"], "connection",
        "Inspect and configure client connections.",
        &["CACHING", "GETNAME", "GETREDIR", "ID", "INFO", "KILL", "LIST", "NO-EVICT", "NO-TOUCH",
          "PAUSE", "REPLY", "SETINFO", "SETNAME", "TRACKING", "TRACKINGINFO", "UNBLOCK", "UNPAUSE"]),
    c("cluster", -2, &[], &["slow"], "cluster", "Ask about the cluster this node is part of.",
        &["ADDSLOTS", "ADDSLOTSRANGE", "BUMPEPOCH", "CANCELSLOTMIGRATIONS", "COUNT-FAILURE-REPORTS",
          "COUNTKEYSINSLOT", "DELSLOTS", "DELSLOTSRANGE", "FAILOVER", "FLUSHSLOT", "FLUSHSLOTS",
          "FORGET", "GETKEYSINSLOT", "GETSLOTMIGRATIONS", "HELP", "INFO", "KEYSLOT", "LINKS", "MEET",
          "MIGRATESLOTS", "MYID", "MYSHARDID", "NODES", "REPLICAS", "REPLICATE", "RESET",
          "SAVECONFIG", "SET-CONFIG-EPOCH", "SETSLOT", "SHARDS", "SLAVES", "SLOT-STATS", "SLOTS",
          "SYNCSLOTS"]),
    c("command", -1, &["loading", "stale"], &["slow", "connection"], "server",
        "Describe the commands of this server.",
        &["COUNT", "DOCS", "GETKEYS", "HELP", "INFO", "LIST"]),
    c("commandlog", -2, &[], &["slow"], "server",
        "Inspect the logs of slow commands, large requests and large replies.",
        &["GET", "HELP", "LEN", "RESET"]),
    c("config", -2, ADMIN_CONTAINER, ADMIN, "server", "Read and change the server configuration.",
        &["GET", "HELP", "RESETSTAT", "REWRITE", "SET"]),
    e("copy", -3, W_OOM, 1, 2, 1, KS_W_SLOW, "generic", "Copy a key's value to another key."),
    e("dbsize", 1, RO_FAST, 0, 0, 0, KS_R_FAST, "server", "Count the keys."),
    e("debug", -2, &["admin", "noscript", "loading", "stale", "protected"], 0, 0, 0, ADMIN, "server",
        "Run a debugging command (once enable-debug-command allows it)."),
    e("decr", 2, W_OOM_FAST, 1, 1, 1, STR_W_FAST, "string", "Subtract one from an integer."),
    e("decrby", 3, W_OOM_FAST, 1, 1, 1, STR_W_FAST, "string", "Subtract a number from an integer."),
    e("del", -2, W, 1, -1, 1, KS_W_SLOW, "generic", "Delete keys."),
    e("delifeq", 3, W_FAST, 1, 1, 1, STR_W_FAST, "string",
        "Delete a string if it holds a value."),
    e("discard", 1, TXN_FAST, 0, 0, 0, TXN_F, "transactions", "Drop the queued transaction."),
    e("dump", 2, RO, 1, 1, 1, KS_R_SLOW, "generic", "Serialize a key's value."),
    e("echo", 2, &["fast"], 0, 0, 0, CONN_FAST, "connection", "Reply with the argument."),
    e("eval", -3, EVAL, 0, 0, 0, SCRIPTING, "scripting", "Run a Lua script."),
    e("eval_ro", -3, EVAL_RO, 0, 0, 0, SCRIPTING, "scripting", "Run a Lua script that only reads."),
    e("evalsha", -3, EVAL, 0, 0, 0, SCRIPTING, "scripting", "Run a cached Lua script by its SHA1."),
    e("evalsha_ro", -3, EVAL_RO, 0, 0, 0, SCRIPTING, "scripting",
        "Run a cached read-only Lua script by its SHA1."),
    e("exec", 1, &["noscript", "loading", "stale", "skip_slowlog"], 0, 0, 0,
        &["slow", "transaction"], "transactions", "Run the queued transaction."),
    e("exists", -2, RO_FAST, 1, -1, 1, KS_R_FAST, "generic", "Count how many of the keys exist."),
    e("expire", -3, W_FAST, 1, 1, 1, KS_W_FAST, "generic", "Set a key's time to live in seconds."),
    e("expireat", -3, W_FAST, 1, 1, 1, KS_W_FAST, "generic", "Set a key's expiry as a Unix time."),
    e("expiretime", 2, RO_FAST, 1, 1, 1, KS_R_FAST, "generic", "Get a key's expiry as a Unix time."),
    e("failover", -1, &["admin", "noscript", "stale"], 0, 0, 0, ADMIN, "server",
        "Hand the primary's role to one of its replicas."),
    e("fcall", -3, EVAL, 0, 0, 0, SCRIPTING, "scripting", "Call a library function."),
    e("fcall_ro", -3, EVAL_RO, 0, 0, 0, SCRIPTING, "scripting",
        "Call a library function that only reads."),
    e("flushall", -1, W, 0, 0, 0, KS_W_DANGER, "server", "Delete every key."),
    e("flushdb", -1, W, 0, 0, 0, KS_W_DANGER, "server", "Delete every key of the database."),
    c("function", -2, &["noscript"], SCRIPTING, "scripting", "Manage libraries of Lua functions.",
        &["DELETE", "DUMP", "FLUSH", "HELP", "KILL", "LIST", "LOAD", "RESTORE", "STATS"]),
    e("geoadd", -5, W_OOM, 1, 1, 1, GEO_W, "geo", "Add places to a geospatial index."),
    e("geodist", -4, RO, 1, 1, 1, GEO_R, "geo", "Get the distance between two places."),
    e("geohash", -2, RO, 1, 1, 1, GEO_R, "geo", "Get places as geohash strings."),
    e("geopos", -2, RO, 1, 1, 1, GEO_R, "geo", "Get the longitude and latitude of places."),
    e("georadius", -6, W_OOM_MOVABLE, 1, 1, 1, GEO_W, "geo",
        "Find the places within a distance of a point (old form)."),
    e("georadius_ro", -6, RO, 1, 1, 1, GEO_R, "geo",
        "Find the places within a distance of a point, read-only (old form)."),
    e("georadiusbymember", -5, W_OOM_MOVABLE, 1, 1, 1, GEO_W, "geo",
        "Find the places within a distance of a place (old form)."),
    e("georadiusbymember_ro", -5, RO, 1, 1, 1, GEO_R, "geo",
        "Find the places within a distance of a place, read-only (old form)."),
    e("geosearch", -7, RO, 1, 1, 1, GEO_R, "geo", "Find the places inside a circle or a box."),
    e("geosearchstore", -8, W_OOM, 1, 2, 1, GEO_W, "geo",
        "Find the places inside a circle or a box and store them."),
    e("get", 2, RO_FAST, 1, 1, 1, STR_R_FAST, "string", "Get a string."),
    e("getbit", 3, RO_FAST, 1, 1, 1, BIT_R_FAST, "bitmap", "Get one bit of a string."),
    e("getdel", 2, W_FAST, 1, 1, 1, STR_W_FAST, "string", "Get a string and delete the key."),
    e("getex", -2, W_FAST, 1, 1, 1, STR_W_FAST, "string", "Get a string and change its expiry."),
    e("getrange", 4, RO, 1, 1, 1, STR_R_SLOW, "string", "Get part of a string."),
    e("getset", 3, W_OOM_FAST, 1, 1, 1, STR_W_FAST, "string", "Set a string and get the old one."),
    e("hdel", -3, W_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Delete fields of a hash."),
    e("hello", -1, NO_AUTH, 0, 0, 0, CONN_FAST, "connection", "Pick the protocol and greet."),
    e("hexists", 3, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash", "Check whether a hash field exists."),
    e("hexpire", -6, W_FAST, 1, 1, 1, HASH_W_FAST, "hash",
        "Set the time to live of hash fields in seconds."),
    e("hexpireat", -6, W_FAST, 1, 1, 1, HASH_W_FAST, "hash",
        "Set the expiry of hash fields as a Unix time."),
    e("hexpiretime", -5, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash",
        "Get the expiry of hash fields as a Unix time."),
    e("hget", 3, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash", "Get one field of a hash."),
    e("hgetall", 2, RO, 1, 1, 1, HASH_R_SLOW, "hash", "Get every field and value of a hash."),
    e("hgetdel", -5, W_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Get hash fields and delete them."),
    e("hgetex", -5, W_FAST, 1, 1, 1, HASH_W_FAST, "hash",
        "Get hash fields and change their expiry."),
    e("hincrby", 4, W_OOM_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Add an integer to a hash field."),
    e("hincrbyfloat", 4, W_OOM_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Add a float to a hash field."),
    e("hkeys", 2, RO, 1, 1, 1, HASH_R_SLOW, "hash", "Get the field names of a hash."),
    e("hlen", 2, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash", "Count the fields of a hash."),
    e("hmget", -3, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash", "Get several fields of a hash."),
    e("hmset", -4, W_OOM_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Set several hash fields."),
    e("hpersist", -5, W_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Remove the expiry of hash fields."),
    e("hpexpire", -6, W_FAST, 1, 1, 1, HASH_W_FAST, "hash",
        "Set the time to live of hash fields in ms."),
    e("hpexpireat", -6, W_FAST, 1, 1, 1, HASH_W_FAST, "hash",
        "Set the expiry of hash fields as a Unix time in ms."),
    e("hpexpiretime", -5, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash",
        "Get the expiry of hash fields as a Unix time in ms."),
    e("hpttl", -5, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash",
        "Get the time to live of hash fields in ms."),
    e("hrandfield", -2, RO, 1, 1, 1, HASH_R_SLOW, "hash", "Pick random fields of a hash."),
    e("hscan", -3, RO, 1, 1, 1, HASH_R_SLOW, "hash", "Walk the fields of a hash."),
    e("hset", -4, W_OOM_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Set hash fields."),
    e("hsetex", -6, W_OOM_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Set hash fields with an expiry."),
    e("hsetnx", 4, W_OOM_FAST, 1, 1, 1, HASH_W_FAST, "hash", "Set a hash field if it is new."),
    e("hstrlen", 3, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash", "Get the length of a hash value."),
    e("httl", -5, RO_FAST, 1, 1, 1, HASH_R_FAST, "hash",
        "Get the time to live of hash fields in seconds."),
    e("hvals", 2, RO, 1, 1, 1, HASH_R_SLOW, "hash", "Get the values of a hash."),
    e("incr", 2, W_OOM_FAST, 1, 1, 1, STR_W_FAST, "string", "Add one to an integer."),
    e("incrby", 3, W_OOM_FAST, 1, 1, 1, STR_W_FAST, "string", "Add a number to an integer."),
    e("incrbyfloat", 3, W_OOM_FAST, 1, 1, 1, STR_W_FAST, "string", "Add a float to a number."),
    e("info", -1, &["loading", "stale"], 0, 0, 0, &["slow", "dangerous"], "server",
        "Report server information and statistics."),
    e("json.arrappend", -4, MOD_W_FAST, 1, 1, 1, JSON_W_FAST, "module",
        "Add values to the end of arrays."),
    e("json.arrindex", -4, MOD_RO_FAST, 1, 1, 1, JSON_R_FAST, "module",
        "Find the first index of a value in arrays."),
    e("json.arrinsert", -5, MOD_W_FAST, 1, 1, 1, JSON_W_FAST, "module",
        "Insert values into arrays."),
    e("json.arrlen", -2, MOD_RO_FAST, 1, 1, 1, JSON_R_FAST, "module",
        "Count the elements of arrays."),
    e("json.arrpop", -2, MOD_W_FAST_ANY, 1, 1, 1, JSON_W_FAST, "module",
        "Remove and return an element of arrays."),
    e("json.arrtrim", 5, MOD_W_FAST_ANY, 1, 1, 1, JSON_W_FAST, "module", "Keep a range of arrays."),
    e("json.clear", -2, MOD_W_FAST_ANY, 1, 1, 1, JSON_W_FAST, "module",
        "Empty containers and zero numbers."),
    c("json.debug", -2, MOD_RO_MOVABLE, JSON_R_SLOW, "module", "Report on a document.",
        &["DEPTH", "FIELDS", "HELP", "MEMORY"]),
    e("json.del", -2, MOD_W_FAST_ANY, 1, 1, 1, JSON_W_FAST, "module",
        "Remove values from a document."),
    e("json.forget", -2, MOD_W_FAST_ANY, 1, 1, 1, JSON_W_FAST, "module",
        "Remove values from a document."),
    e("json.get", -2, MOD_RO_FAST, 1, 1, 1, JSON_R_FAST, "module",
        "Get values of a document as JSON."),
    e("json.merge", 4, MOD_W, 1, 1, 1, JSON_W_SLOW, "module",
        "Merge a JSON patch into a document."),
    e("json.mget", -3, MOD_RO_FAST, 1, -2, 1, JSON_R_FAST, "module",
        "Get one path of several documents."),
    e("json.mset", -4, MOD_W, 1, -3, 3, JSON_W_SLOW, "module", "Set values in several documents."),
    e("json.numincrby", 4, MOD_W_FAST_ANY, 1, 1, 1, JSON_W_FAST, "module", "Add to numbers."),
    e("json.nummultby", 4, MOD_W_FAST_ANY, 1, 1, 1, JSON_W_FAST, "module", "Multiply numbers."),
    e("json.objkeys", -2, MOD_RO_FAST, 1, 1, 1, JSON_R_FAST, "module",
        "Get the member names of objects."),
    e("json.objlen", -2, MOD_RO_FAST, 1, 1, 1, JSON_R_FAST, "module",
        "Count the members of objects."),
    e("json.resp", -2, MOD_RO_FAST, 1, 1, 1, JSON_R_FAST, "module",
        "Get values of a document as RESP."),
    e("json.set", -4, MOD_W, 1, 1, 1, JSON_W_SLOW, "module", "Set values of a document."),
    e("json.strappend", -3, MOD_W_FAST, 1, 1, 1, JSON_W_FAST, "module",
        "Add to the end of strings."),
    e("json.strlen", -2, MOD_RO_FAST, 1, 1, 1, JSON_R_FAST, "module", "Get the length of strings."),
    e("json.toggle", -2, MOD_W_FAST, 1, 1, 1, JSON_W_FAST, "module", "Flip booleans."),
    e("json.type", -2, MOD_RO_FAST, 1, 1, 1, JSON_R_FAST, "module", "Get the types of values."),
    e("keys", 2, RO, 0, 0, 0, &["keyspace", "read", "slow", "dangerous"], "generic",
        "List the keys matching a pattern."),
    e("lastsave", 1, LOAD_FAST, 0, 0, 0, &["admin", "fast", "dangerous"], "server",
        "Get the time of the last save."),
    c("latency", -2, ADMIN_CONTAINER, ADMIN, "server", "Report latency events (none are recorded).",
        &["DOCTOR", "GRAPH", "HELP", "HISTOGRAM", "HISTORY", "LATEST", "RESET"]),
    e("lcs", -3, RO, 1, 2, 1, STR_R_SLOW, "string",
        "Find the longest common subsequence of two strings."),
    e("lindex", 3, RO, 1, 1, 1, LIST_R_SLOW, "list", "Get a list element by index."),
    e("linsert", 5, W_OOM, 1, 1, 1, LIST_W_SLOW, "list", "Insert into a list next to an element."),
    e("llen", 2, RO_FAST, 1, 1, 1, LIST_R_FAST, "list", "Get the length of a list."),
    e("lmove", 5, W_OOM, 1, 2, 1, LIST_W_SLOW, "list", "Move an element between lists."),
    e("lmpop", -4, W_MOVABLE, 0, 0, 0, LIST_W_SLOW, "list", "Pop from the first non-empty list."),
    e("lolwut", -1, RO_FAST, 0, 0, 0, &["read", "fast"], "server",
        "Show the version of the server."),
    e("lpop", -2, W_FAST, 1, 1, 1, LIST_W_FAST, "list", "Pop from the head of a list."),
    e("lpos", -3, RO, 1, 1, 1, LIST_R_SLOW, "list", "Find the positions of an element in a list."),
    e("lpush", -3, W_OOM_FAST, 1, 1, 1, LIST_W_FAST, "list", "Push onto the head of a list."),
    e("lpushx", -3, W_OOM_FAST, 1, 1, 1, LIST_W_FAST, "list", "Push onto an existing list's head."),
    e("lrange", 4, RO, 1, 1, 1, LIST_R_SLOW, "list", "Get a range of list elements."),
    e("lrem", 4, W, 1, 1, 1, LIST_W_SLOW, "list", "Remove occurrences of an element from a list."),
    e("lset", 4, W_OOM, 1, 1, 1, LIST_W_SLOW, "list", "Replace a list element by index."),
    e("ltrim", 4, W, 1, 1, 1, LIST_W_SLOW, "list", "Keep only a range of a list."),
    c("memory", -2, RO, KS_R_SLOW, "server", "Inspect memory use.",
        &["DOCTOR", "HELP", "MALLOC-STATS", "PURGE", "USAGE"]),
    e("mget", -2, RO_FAST, 1, -1, 1, STR_R_FAST, "string", "Get several strings."),
    e("monitor", 1, &["admin", "noscript", "loading", "stale"], 0, 0, 0, ADMIN, "server",
        "Watch the commands the server receives."),
    e("move", 3, W_FAST, 1, 1, 1, KS_W_FAST, "generic", "Move a key to another database."),
    e("mset", -3, W_OOM, 1, -1, 2, STR_W_SLOW, "string", "Set several strings."),
    e("msetex", -4, W_OOM_MOVABLE, 0, 0, 0, STR_W_SLOW, "string",
        "Set several strings with one expiry."),
    e("msetnx", -3, W_OOM, 1, -1, 2, STR_W_SLOW, "string", "Set several strings if none exist."),
    e("multi", 1, TXN_FAST, 0, 0, 0, TXN_F, "transactions", "Start a transaction."),
    c("object", -2, RO, KS_R_SLOW, "generic", "Inspect how a key is stored.",
        &["ENCODING", "FREQ", "HELP", "IDLETIME", "REFCOUNT"]),
    e("persist", 2, W_FAST, 1, 1, 1, KS_W_FAST, "generic", "Remove a key's expiry."),
    e("pexpire", -3, W_FAST, 1, 1, 1, KS_W_FAST, "generic", "Set a key's time to live in ms."),
    e("pexpireat", -3, W_FAST, 1, 1, 1, KS_W_FAST, "generic",
        "Set a key's expiry as a Unix time in ms."),
    e("pexpiretime", 2, RO_FAST, 1, 1, 1, KS_R_FAST, "generic",
        "Get a key's expiry as a Unix time in ms."),
    e("pfadd", -2, W_OOM_FAST, 1, 1, 1, HLL_W_FAST, "hyperloglog",
        "Add elements to a HyperLogLog."),
    e("pfcount", -2, RO, 1, -1, 1, HLL_R_SLOW, "hyperloglog",
        "Estimate how many distinct elements were added."),
    e("pfmerge", -2, W_OOM, 1, -1, 1, HLL_W_SLOW, "hyperloglog", "Merge HyperLogLogs into one."),
    e("ping", -1, &["fast"], 0, 0, 0, CONN_FAST, "connection", "Check that the server answers."),
    e("psetex", 4, W_OOM, 1, 1, 1, STR_W_SLOW, "string", "Set a string with a time to live in ms."),
    e("psubscribe", -2, PUBSUB, 0, 0, 0, PUBSUB_SLOW, "pubsub", "Listen to channels by pattern."),
    e("psync", -3, REPL_SYNC, 0, 0, 0, ADMIN, "server",
        "Take the replication stream from an offset, or the whole dataset."),
    e("pttl", 2, RO_FAST, 1, 1, 1, KS_R_FAST, "generic", "Get a key's time to live in ms."),
    e("publish", 3, &["pubsub", "loading", "stale", "fast"], 0, 0, 0,
        &["pubsub", "fast"], "pubsub", "Send a message to a channel."),
    c("pubsub", -2, &["pubsub", "loading", "stale"], PUBSUB_SLOW, "pubsub",
        "Inspect channels and subscriptions.",
        &["CHANNELS", "NUMPAT", "NUMSUB", "SHARDCHANNELS", "SHARDNUMSUB"]),
    e("punsubscribe", -1, PUBSUB, 0, 0, 0, PUBSUB_SLOW, "pubsub", "Stop listening to patterns."),
    e("quit", -1, NO_AUTH, 0, 0, 0, CONN_FAST, "connection", "Close the connection."),
    e("randomkey", 1, RO, 0, 0, 0, KS_R_SLOW, "generic", "Get the name of a random key."),
    e("readonly", 1, LOAD_FAST, 0, 0, 0, CONN_FAST, "cluster",
        "Let the connection read from a replica."),
    e("readwrite", 1, LOAD_FAST, 0, 0, 0, CONN_FAST, "cluster",
        "Stop reading from replicas on this connection."),
    e("rename", 3, W, 1, 2, 1, KS_W_SLOW, "generic", "Rename a key."),
    e("renamenx", 3, W_FAST, 1, 2, 1, KS_W_FAST, "generic", "Rename a key if the new name is free."),
    e("replconf", -1, &["admin", "noscript", "loading", "stale", "allow_busy"], 0, 0, 0, ADMIN,
        "server", "Set up a replication link."),
    e("replicaof", 3, REPL_FOLLOW, 0, 0, 0, ADMIN, "server",
        "Follow a primary, or stop following one."),
    e("reset", 1, NO_AUTH, 0, 0, 0, CONN_FAST, "connection", "Reset the connection's state."),
    e("restore", -4, W_OOM, 1, 1, 1, KS_W_DANGER, "generic", "Create a key from a DUMP payload."),
    e("role", 1, &["noscript", "loading", "stale", "fast"], 0, 0, 0,
        &["admin", "fast", "dangerous"], "server", "Report the replication role of this node."),
    e("rpop", -2, W_FAST, 1, 1, 1, LIST_W_FAST, "list", "Pop from the tail of a list."),
    e("rpoplpush", 3, W_OOM, 1, 2, 1, LIST_W_SLOW, "list",
        "Move a list's tail to another list's head."),
    e("rpush", -3, W_OOM_FAST, 1, 1, 1, LIST_W_FAST, "list", "Push onto the tail of a list."),
    e("rpushx", -3, W_OOM_FAST, 1, 1, 1, LIST_W_FAST, "list", "Push onto an existing list's tail."),
    e("sadd", -3, W_OOM_FAST, 1, 1, 1, SET_W_FAST, "set", "Add members to a set."),
    e("save", 1, &["admin", "noscript", "no_async_loading", "no_multi"], 0, 0, 0, ADMIN, "server",
        "Save the dataset now."),
    e("scan", -2, RO, 0, 0, 0, KS_R_SLOW, "generic", "Walk the keys."),
    e("scard", 2, RO_FAST, 1, 1, 1, SET_R_FAST, "set", "Count the members of a set."),
    c("script", -2, &["noscript"], SCRIPTING, "scripting", "Manage the Lua script cache.",
        &["EXISTS", "FLUSH", "KILL", "LOAD"]),
    e("sdiff", -2, RO, 1, -1, 1, SET_R_SLOW, "set", "Subtract sets."),
    e("sdiffstore", -3, W_OOM, 1, -1, 1, SET_W_SLOW, "set", "Subtract sets into a key."),
    e("select", 2, LOAD_FAST, 0, 0, 0, CONN_FAST, "connection",
        "Pick the database of the connection."),
    e("set", -3, W_OOM, 1, 1, 1, STR_W_SLOW, "string", "Set a string."),
    e("setbit", 4, W_OOM, 1, 1, 1, BIT_W_SLOW, "bitmap", "Set or clear one bit of a string."),
    e("setex", 4, W_OOM, 1, 1, 1, STR_W_SLOW, "string", "Set a string with a time to live."),
    e("setnx", 3, W_OOM_FAST, 1, 1, 1, STR_W_FAST, "string", "Set a string if the key is new."),
    e("setrange", 4, W_OOM, 1, 1, 1, STR_W_SLOW, "string", "Overwrite part of a string."),
    e("shutdown", -1, &["admin", "noscript", "loading", "stale", "no_multi", "allow_busy"],
        0, 0, 0, ADMIN, "server", "Save everything to disk and stop the server."),
    e("sinter", -2, RO, 1, -1, 1, SET_R_SLOW, "set", "Intersect sets."),
    e("sintercard", -3, RO_MOVABLE, 0, 0, 0, SET_R_SLOW, "set", "Count an intersection of sets."),
    e("sinterstore", -3, W_OOM, 1, -1, 1, SET_W_SLOW, "set", "Intersect sets into a key."),
    e("sismember", 3, RO_FAST, 1, 1, 1, SET_R_FAST, "set", "Check a set membership."),
    e("slaveof", 3, REPL_FOLLOW, 0, 0, 0, ADMIN, "server",
        "Follow a primary (the old name of REPLICAOF)."),
    c("slowlog", -2, ADMIN_CONTAINER, ADMIN, "server", "Inspect the log of slow commands.",
        &["GET", "HELP", "LEN", "RESET"]),
    e("smembers", 2, RO, 1, 1, 1, SET_R_SLOW, "set", "Get every member of a set."),
    e("smismember", -3, RO_FAST, 1, 1, 1, SET_R_FAST, "set", "Check several set memberships."),
    e("smove", 4, W_FAST, 1, 2, 1, SET_W_FAST, "set", "Move a member between sets."),
    e("sort", -2, W_OOM_MOVABLE, 1, 1, 1, &["write", "set", "sortedset", "list", "slow", "dangerous"],
        "generic", "Sort the elements of a list, set or sorted set."),
    e("sort_ro", -2, RO_MOVABLE, 1, 1, 1, &["read", "set", "sortedset", "list", "slow", "dangerous"],
        "generic", "Sort the elements of a list, set or sorted set, read-only."),
    e("spop", -2, W_FAST, 1, 1, 1, SET_W_FAST, "set", "Pop random members of a set."),
    e("spublish", 3, &["pubsub", "loading", "stale", "fast"], 0, 0, 0,
        &["pubsub", "fast"], "pubsub", "Send a message to a shard channel."),
    e("srandmember", -2, RO, 1, 1, 1, SET_R_SLOW, "set", "Pick random members of a set."),
    e("srem", -3, W_FAST, 1, 1, 1, SET_W_FAST, "set", "Remove members from a set."),
    e("sscan", -3, RO, 1, 1, 1, SET_R_SLOW, "set", "Walk the members of a set."),
    e("ssubscribe", -2, PUBSUB, 0, 0, 0, PUBSUB_SLOW, "pubsub", "Listen to shard channels."),
    e("strlen", 2, RO_FAST, 1, 1, 1, STR_R_FAST, "string", "Get the length of a string."),
    e("subscribe", -2, PUBSUB, 0, 0, 0, PUBSUB_SLOW, "pubsub", "Listen to channels."),
    e("substr", 4, RO, 1, 1, 1, STR_R_SLOW, "string", "Get part of a string (old name)."),
    e("sunion", -2, RO, 1, -1, 1, SET_R_SLOW, "set", "Unite sets."),
    e("sunionstore", -3, W_OOM, 1, -1, 1, SET_W_SLOW, "set", "Unite sets into a key."),
    e("sunsubscribe", -1, PUBSUB, 0, 0, 0, PUBSUB_SLOW, "pubsub",
        "Stop listening to shard channels."),
    e("swapdb", 3, W_FAST, 0, 0, 0, &["keyspace", "write", "fast", "dangerous"], "server",
        "Swap two databases."),
    e("sync", 1, REPL_SYNC, 0, 0, 0, ADMIN, "server",
        "Take the whole dataset and then the replication stream."),
    e("time", 1, LOAD_FAST, 0, 0, 0, &["fast"], "server", "Get the server time."),
    e("touch", -2, RO_FAST, 1, -1, 1, KS_R_FAST, "generic", "Count the keys that exist."),
    e("ttl", 2, RO_FAST, 1, 1, 1, KS_R_FAST, "generic", "Get a key's time to live in seconds."),
    e("type", 2, RO_FAST, 1, 1, 1, KS_R_FAST, "generic", "Get the type of a key's value."),
    e("unlink", -2, W_FAST, 1, -1, 1, KS_W_FAST, "generic", "Delete keys."),
    e("unsubscribe", -1, PUBSUB, 0, 0, 0, PUBSUB_SLOW, "pubsub", "Stop listening to channels."),
    e("unwatch", 1, TXN_FAST, 0, 0, 0, TXN_F, "transactions", "Forget the watched keys."),
    e("wait", 3, &["blocking"], 0, 0, 0, &["slow", "connection", "blocking"], "generic",
        "Wait for replicas to take earlier writes."),
    e("waitaof", 4, &["blocking"], 0, 0, 0, &["slow", "connection", "blocking"], "generic",
        "Wait until earlier writes are on disk."),
    e("watch", -2, TXN_FAST, 1, -1, 1, TXN_F, "transactions", "Watch keys for the next EXEC."),
    e("xack", -4, W_FAST, 1, 1, 1, X_W_FAST, "stream", "Acknowledge stream entries."),
    e("xadd", -5, W_OOM_FAST, 1, 1, 1, X_W_FAST, "stream", "Append an entry to a stream."),
    e("xautoclaim", -6, W_FAST, 1, 1, 1, X_W_FAST, "stream",
        "Take over idle pending entries in a scan."),
    e("xclaim", -6, W_FAST, 1, 1, 1, X_W_FAST, "stream", "Take over pending entries."),
    e("xdel", -3, W_FAST, 1, 1, 1, X_W_FAST, "stream", "Delete stream entries."),
    c("xgroup", -2, W_OOM, X_W_SLOW, "stream", "Manage consumer groups.",
        &["CREATE", "CREATECONSUMER", "DELCONSUMER", "DESTROY", "SETID"]),
    c("xinfo", -2, RO, X_R_SLOW, "stream", "Inspect streams, groups and consumers.",
        &["CONSUMERS", "GROUPS", "STREAM"]),
    e("xlen", 2, RO_FAST, 1, 1, 1, X_R_FAST, "stream", "Count the entries of a stream."),
    e("xpending", -3, RO, 1, 1, 1, X_R_SLOW, "stream", "Inspect a group's pending entries."),
    e("xrange", -4, RO, 1, 1, 1, X_R_SLOW, "stream", "Get stream entries in ID order."),
    e("xread", -4, &["readonly", "blocking", "movablekeys"], 0, 0, 0,
        &["read", "stream", "slow", "blocking"], "stream", "Read from streams, waiting if needed."),
    e("xreadgroup", -7, &["write", "blocking", "movablekeys"], 0, 0, 0,
        &["write", "stream", "slow", "blocking"], "stream",
        "Read from streams as a group consumer."),
    e("xrevrange", -4, RO, 1, 1, 1, X_R_SLOW, "stream", "Get stream entries in reverse order."),
    e("xsetid", -3, W_OOM_FAST, 1, 1, 1, X_W_FAST, "stream", "Set a stream's last ID."),
    e("xtrim", -4, W, 1, 1, 1, X_W_SLOW, "stream", "Trim old entries off a stream."),
    e("zadd", -4, W_OOM_FAST, 1, 1, 1, Z_W_FAST, "sorted-set", "Add or update scored members."),
    e("zcard", 2, RO_FAST, 1, 1, 1, Z_R_FAST, "sorted-set", "Count the members of a sorted set."),
    e("zcount", 4, RO_FAST, 1, 1, 1, Z_R_FAST, "sorted-set", "Count members in a score range."),
    e("zdiff", -3, RO_MOVABLE, 0, 0, 0, Z_R_SLOW, "sorted-set", "Subtract sorted sets."),
    e("zdiffstore", -4, W_OOM_MOVABLE, 1, 1, 1, Z_W_SLOW, "sorted-set",
        "Subtract sorted sets into a key."),
    e("zincrby", 4, W_OOM_FAST, 1, 1, 1, Z_W_FAST, "sorted-set", "Add to a member's score."),
    e("zinter", -3, RO_MOVABLE, 0, 0, 0, Z_R_SLOW, "sorted-set", "Intersect sorted sets."),
    e("zintercard", -3, RO_MOVABLE, 0, 0, 0, Z_R_SLOW, "sorted-set",
        "Count an intersection of sorted sets."),
    e("zinterstore", -4, W_OOM_MOVABLE, 1, 1, 1, Z_W_SLOW, "sorted-set",
        "Intersect sorted sets into a key."),
    e("zlexcount", 4, RO_FAST, 1, 1, 1, Z_R_FAST, "sorted-set", "Count members in a lex range."),
    e("zmpop", -4, W_MOVABLE, 0, 0, 0, Z_W_SLOW, "sorted-set",
        "Pop members from the first non-empty sorted set."),
    e("zmscore", -3, RO_FAST, 1, 1, 1, Z_R_FAST, "sorted-set", "Get the scores of members."),
    e("zpopmax", -2, W_FAST, 1, 1, 1, Z_W_FAST, "sorted-set", "Pop the highest-scored members."),
    e("zpopmin", -2, W_FAST, 1, 1, 1, Z_W_FAST, "sorted-set", "Pop the lowest-scored members."),
    e("zrandmember", -2, RO, 1, 1, 1, Z_R_SLOW, "sorted-set", "Pick random members of a sorted set."),
    e("zrange", -4, RO, 1, 1, 1, Z_R_SLOW, "sorted-set", "Get a range of members."),
    e("zrangebylex", -4, RO, 1, 1, 1, Z_R_SLOW, "sorted-set", "Get members in a lex range."),
    e("zrangebyscore", -4, RO, 1, 1, 1, Z_R_SLOW, "sorted-set", "Get members in a score range."),
    e("zrangestore", -5, W_OOM, 1, 2, 1, Z_W_SLOW, "sorted-set",
        "Store a range of members into a key."),
    e("zrank", -3, RO_FAST, 1, 1, 1, Z_R_FAST, "sorted-set", "Get a member's rank."),
    e("zrem", -3, W_FAST, 1, 1, 1, Z_W_FAST, "sorted-set", "Remove members."),
    e("zremrangebylex", 4, W, 1, 1, 1, Z_W_SLOW, "sorted-set", "Remove members in a lex range."),
    e("zremrangebyrank", 4, W, 1, 1, 1, Z_W_SLOW, "sorted-set", "Remove members in a rank range."),
    e("zremrangebyscore", 4, W, 1, 1, 1, Z_W_SLOW, "sorted-set", "Remove members in a score range."),
    e("zrevrange", -4, RO, 1, 1, 1, Z_R_SLOW, "sorted-set",
        "Get a range of members, highest first."),
    e("zrevrangebylex", -4, RO, 1, 1, 1, Z_R_SLOW, "sorted-set",
        "Get members in a lex range, highest first."),
    e("zrevrangebyscore", -4, RO, 1, 1, 1, Z_R_SLOW, "sorted-set",
        "Get members in a score range, highest first."),
    e("zrevrank", -3, RO_FAST, 1, 1, 1, Z_R_FAST, "sorted-set",
        "Get a member's rank, highest first."),
    e("zscan", -3, RO, 1, 1, 1, Z_R_SLOW, "sorted-set", "Walk the members of a sorted set."),
    e("zscore", 3, RO_FAST, 1, 1, 1, Z_R_FAST, "sorted-set", "Get a member's score."),
    e("zunion", -3, RO_MOVABLE, 0, 0, 0, Z_R_SLOW, "sorted-set", "Unite sorted sets."),
    e("zunionstore", -4, W_OOM_MOVABLE, 1, 1, 1, Z_W_SLOW, "sorted-set",
        "Unite sorted sets into a key."),
];

/// Every ACL category Redis 7.4 defines, used here or not, so that rules
/// written for Redis (`-@geo`) are accepted.
static CATEGORIES: &[&str] = &[
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
    // Added by valkey-bloom.
    "bloom",
    // Added by valkey-json.
    "json",
];

pub fn all() -> &'static [CmdInfo] {
    TABLE
}

/// Case-insensitive; no allocation.
pub fn lookup(name: &[u8]) -> Option<&'static CmdInfo> {
    let mut buf = [0u8; MAX_NAME];
    let lower = buf.get_mut(..name.len())?;
    lower.copy_from_slice(name);
    lower.make_ascii_lowercase();
    let lower = &*lower;
    TABLE
        .binary_search_by(|info| info.name.as_bytes().cmp(lower))
        .ok()
        .map(|i| &TABLE[i])
}

/// `category` without '@', any case; "all" matches every command.
pub fn in_category(info: &CmdInfo, category: &[u8]) -> bool {
    category.eq_ignore_ascii_case(b"all")
        || info
            .categories
            .iter()
            .any(|c| c.as_bytes().eq_ignore_ascii_case(category))
}

pub fn categories() -> &'static [&'static str] {
    CATEGORIES
}

/// The keys of a parsed command. `Cmd::for_each_key` leaves out the keys
/// of EVAL and friends, WATCH and MOVE; they are added here.
pub(crate) fn keys_of(cmd: &Cmd, f: &mut dyn FnMut(&[u8])) {
    match cmd {
        Cmd::Eval { keys, .. }
        | Cmd::EvalSha { keys, .. }
        | Cmd::FCall { keys, .. }
        | Cmd::Txn(Txn::Watch(keys)) => keys.iter().for_each(|k| f(k)),
        Cmd::Move { key, .. } => f(key),
        Cmd::In(_, inner) => keys_of(inner, f),
        Cmd::ExecBlock { queue, .. } => queue.iter().for_each(|queued| keys_of(queued, f)),
        other => other.for_each_key(|k| f(k)),
    }
}

fn shown(raw: &[u8]) -> String {
    String::from_utf8_lossy(&raw[..raw.len().min(SHOWN)]).into_owned()
}

fn arity(sub: &str) -> Reply {
    Reply::Error(format!(
        "ERR wrong number of arguments for 'command|{sub}' command"
    ))
}

fn entry(info: &CmdInfo) -> Reply {
    let simple = |s: &str| Reply::Simple(s.to_owned());
    Reply::Array(vec![
        Reply::bulk(info.name),
        Reply::Int(info.arity.into()),
        Reply::Array(info.flags.iter().map(|f| simple(f)).collect()),
        Reply::Int(info.first_key.into()),
        Reply::Int(info.last_key.into()),
        Reply::Int(info.step.into()),
        Reply::Array(
            info.categories
                .iter()
                .map(|c| Reply::Simple(format!("@{c}")))
                .collect(),
        ),
        // Tips, key specifications, subcommands.
        Reply::Array(Vec::new()),
        Reply::Array(Vec::new()),
        Reply::Array(Vec::new()),
    ])
}

fn docs(info: &CmdInfo) -> Reply {
    Reply::Map(vec![
        Reply::bulk("summary"),
        Reply::bulk(info.summary),
        Reply::bulk("since"),
        Reply::bulk("1.0.0"),
        Reply::bulk("group"),
        Reply::bulk(info.group),
    ])
}

const HELP: &[&str] = &[
    "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "(no subcommand)",
    "    Return details about all commands.",
    "COUNT",
    "    Return the total number of commands.",
    "DOCS [<command-name> ...]",
    "    Return documentation details about the given commands, or all of them.",
    "GETKEYS <full-command>",
    "    Return the keys from a full command.",
    "INFO [<command-name> ...]",
    "    Return details about the given commands, or all of them.",
    "LIST [FILTERBY (MODULE <module-name>|ACLCAT <category>|PATTERN <pattern>)]",
    "    Return a list of all command names, or those matching the filter.",
    "HELP",
    "    Print this help.",
];

/// COMMAND and its subcommands. `args[0]` is "COMMAND".
pub fn command(args: &[Bytes]) -> Reply {
    let Some(sub) = args.get(1) else {
        return Reply::Array(TABLE.iter().map(entry).collect());
    };
    let n = args.len();
    let is = |word: &str| sub.eq_ignore_ascii_case(word.as_bytes());
    if is("COUNT") {
        if n != 2 {
            return arity("count");
        }
        return Reply::Int(i64::try_from(TABLE.len()).unwrap_or(i64::MAX));
    }
    if is("INFO") {
        if n == 2 {
            return Reply::Array(TABLE.iter().map(entry).collect());
        }
        return Reply::Array(
            args[2..]
                .iter()
                .map(|name| lookup(name).map_or(Reply::Nil, entry))
                .collect(),
        );
    }
    if is("DOCS") {
        let pairs = |info: &CmdInfo| [Reply::bulk(info.name), docs(info)];
        if n == 2 {
            return Reply::Map(TABLE.iter().flat_map(pairs).collect());
        }
        return Reply::Map(
            args[2..]
                .iter()
                .filter_map(|name| lookup(name))
                .flat_map(pairs)
                .collect(),
        );
    }
    if is("LIST") {
        return list(&args[2..]);
    }
    if is("GETKEYS") {
        if n < 3 {
            return arity("getkeys");
        }
        return getkeys(&args[2..]);
    }
    if is("HELP") {
        if n != 2 {
            return arity("help");
        }
        return Reply::Array(
            HELP.iter()
                .map(|l| Reply::Simple((*l).to_owned()))
                .collect(),
        );
    }
    Reply::Error(format!(
        "ERR unknown subcommand '{}'. Try COMMAND HELP.",
        shown(sub)
    ))
}

/// `COMMAND LIST [FILTERBY (MODULE m | ACLCAT c | PATTERN p)]`.
fn list(rest: &[Bytes]) -> Reply {
    let names = |keep: &dyn Fn(&CmdInfo) -> bool| {
        Reply::Array(
            TABLE
                .iter()
                .filter(|info| keep(info))
                .map(|info| Reply::bulk(info.name))
                .collect(),
        )
    };
    match rest {
        [] => names(&|_| true),
        [filterby, kind, arg] if filterby.eq_ignore_ascii_case(b"FILTERBY") => {
            if kind.eq_ignore_ascii_case(b"MODULE") {
                Reply::Array(Vec::new())
            } else if kind.eq_ignore_ascii_case(b"ACLCAT") {
                // An unknown category matches nothing, as in Redis.
                names(&|info| !arg.eq_ignore_ascii_case(b"all") && in_category(info, arg))
            } else if kind.eq_ignore_ascii_case(b"PATTERN") {
                if arg.len() > MAX_PATTERN {
                    return Reply::err("ERR pattern too long");
                }
                let pattern = arg.to_ascii_lowercase();
                names(&|info| glob_match(&pattern, info.name.as_bytes()))
            } else {
                Reply::err("ERR syntax error")
            }
        }
        _ => Reply::err("ERR syntax error"),
    }
}

/// `COMMAND GETKEYS cmd args...`: the keys `Cmd::parse` finds.
fn getkeys(full: &[Bytes]) -> Reply {
    let Some(info) = lookup(&full[0]) else {
        return Reply::err("ERR Invalid command specified");
    };
    // A container finds its subcommand first, as in Redis.
    let known = |sub: &Bytes| {
        info.subcommands
            .iter()
            .any(|name| sub.eq_ignore_ascii_case(name.as_bytes()))
    };
    if full.len() > 1 && !info.subcommands.is_empty() && !full[1..2].iter().any(known) {
        return Reply::err("ERR Invalid command specified");
    }
    let cmd = match Cmd::parse(full.to_vec()) {
        Ok(cmd) => cmd,
        Err(Reply::Error(e)) if e.starts_with("ERR wrong number of arguments") => {
            return Reply::err("ERR Invalid number of arguments specified for command")
        }
        // In the table but not parsed yet.
        Err(Reply::Error(e)) if e.starts_with("ERR unknown command") => {
            return Reply::err("ERR Invalid command specified")
        }
        Err(other) => return other,
    };
    let mut keys = Vec::new();
    match cmd {
        // A command that checks its arguments only when it runs, as a
        // module's does: the table says where its keys are.
        Cmd::Static(_) => keys = table_keys(info, full),
        cmd => keys_of(&cmd, &mut |k| keys.push(Reply::bulk(k))),
    }
    if keys.is_empty() {
        return Reply::err("ERR The command has no key arguments");
    }
    Reply::Array(keys)
}

/// The keys at the positions the table gives for `info`.
fn table_keys(info: &CmdInfo, full: &[Bytes]) -> Vec<Reply> {
    let (Ok(first), Ok(step)) = (usize::try_from(info.first_key), usize::try_from(info.step))
    else {
        return Vec::new();
    };
    if first == 0 || step == 0 {
        return Vec::new();
    }
    let last = match usize::try_from(info.last_key) {
        Ok(last) => last,
        Err(_) => full
            .len()
            .saturating_sub(info.last_key.unsigned_abs() as usize),
    };
    (first..=last.min(full.len().saturating_sub(1)))
        .step_by(step)
        .map(|at| Reply::Bulk(full[at].clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    fn bulks(parts: &[&str]) -> Reply {
        Reply::Array(parts.iter().map(|p| Reply::bulk(*p)).collect())
    }

    /// In the table, but `Cmd::parse` may not know them yet.
    const NOT_PARSED_YET: &[&str] = &["acl", "auth", "move"];

    #[test]
    fn names_unique_lower_and_sorted() {
        let mut seen = HashSet::new();
        for info in TABLE {
            assert!(seen.insert(info.name), "duplicate {}", info.name);
            assert_eq!(info.name, info.name.to_ascii_lowercase());
            assert!(info.name.len() <= MAX_NAME);
        }
        for pair in TABLE.windows(2) {
            assert!(
                pair[0].name < pair[1].name,
                "{} before {}",
                pair[0].name,
                pair[1].name
            );
        }
    }

    #[test]
    fn lookup_finds_every_entry_in_any_case() {
        for info in TABLE {
            let upper = info.name.to_ascii_uppercase();
            let mixed: String = info
                .name
                .chars()
                .enumerate()
                .map(|(i, ch)| {
                    if i % 2 == 0 {
                        ch.to_ascii_uppercase()
                    } else {
                        ch
                    }
                })
                .collect();
            for name in [info.name.to_owned(), upper, mixed] {
                let found = lookup(name.as_bytes()).map(|i| i.name);
                assert_eq!(found, Some(info.name), "{name}");
            }
        }
        assert!(lookup(b"nope").is_none());
        assert!(lookup(b"").is_none());
        assert!(lookup(&[b'a'; 1000]).is_none());
        assert!(lookup(b"ge").is_none());
        assert!(lookup(b"gett").is_none());
    }

    #[test]
    fn every_entry_is_known_to_the_parser() {
        for info in TABLE {
            let result = Cmd::parse(vec![info.name.as_bytes().to_vec()]);
            let unknown =
                matches!(&result, Err(Reply::Error(e)) if e.starts_with("ERR unknown command"));
            if NOT_PARSED_YET.contains(&info.name) {
                continue;
            }
            assert!(!unknown, "{} is not known to Cmd::parse", info.name);
        }
    }

    #[test]
    fn key_positions_match_the_parser() {
        // Their dummy arguments ("1" everywhere but the keys) do not parse.
        const SKIP: &[&str] = &["blmove", "linsert", "lmove", "zlexcount", "zremrangebylex"];
        let mut checked = 0;
        for info in TABLE {
            if info.first_key <= 0 || info.arity <= 0 {
                continue;
            }
            let n = info.arity as usize;
            let last = if info.last_key < 0 {
                (n as i32 + info.last_key) as usize
            } else {
                info.last_key as usize
            };
            let is_key = |i: usize| {
                i >= info.first_key as usize
                    && i <= last
                    && (i - info.first_key as usize).is_multiple_of(info.step as usize)
            };
            let mut parts = vec![info.name.to_owned()];
            parts.extend((1..n).map(|i| {
                if is_key(i) {
                    format!("key{i}")
                } else {
                    "1".into()
                }
            }));
            let expected: Vec<String> = (1..n)
                .filter(|&i| is_key(i))
                .map(|i| format!("key{i}"))
                .collect();
            let parsed = Cmd::parse(parts.iter().map(|p| p.as_bytes().to_vec()).collect());
            match parsed {
                Ok(cmd) => {
                    assert!(
                        !SKIP.contains(&info.name),
                        "{} parses now: drop it from SKIP",
                        info.name
                    );
                    let mut keys = Vec::new();
                    keys_of(&cmd, &mut |k| {
                        keys.push(String::from_utf8_lossy(k).into_owned())
                    });
                    keys.sort();
                    assert_eq!(keys, expected, "{}", info.name);
                    checked += 1;
                }
                Err(e) => assert!(
                    SKIP.contains(&info.name) || NOT_PARSED_YET.contains(&info.name),
                    "{}: {e:?}",
                    info.name
                ),
            }
        }
        assert!(checked >= 56, "only {checked} checked");
    }

    #[test]
    fn write_and_read_flags_match_categories() {
        let cats: HashSet<&str> = CATEGORIES.iter().copied().collect();
        for info in TABLE {
            let flag = |f: &str| info.flags.contains(&f);
            let cat = |c: &str| info.categories.contains(&c);
            if flag("write") {
                assert!(cat("write"), "{}", info.name);
            }
            // As in Redis, scripting commands stay out of @read.
            if flag("readonly") && !cat("scripting") {
                assert!(cat("read"), "{}", info.name);
            }
            if flag("admin") {
                assert!(cat("admin") && cat("dangerous"), "{}", info.name);
            }
            if flag("blocking") {
                assert!(cat("blocking"), "{}", info.name);
            }
            if flag("pubsub") {
                assert!(cat("pubsub"), "{}", info.name);
            }
            assert!(cat("fast") != cat("slow"), "{}: fast xor slow", info.name);
            assert_eq!(flag("fast"), cat("fast"), "{}", info.name);
            for c in info.categories {
                assert!(cats.contains(c), "{}: unknown category {c}", info.name);
            }
            for s in info.subcommands {
                assert_eq!(*s, s.to_ascii_uppercase());
            }
        }
    }

    #[test]
    fn categories_cover_and_match() {
        let get = lookup(b"GET").map(|i| i.name);
        assert_eq!(get, Some("get"));
        let info = lookup(b"get").expect("get");
        assert!(in_category(info, b"read"));
        assert!(in_category(info, b"READ"));
        assert!(in_category(info, b"all"));
        assert!(in_category(info, b"ALL"));
        assert!(!in_category(info, b"write"));
        assert!(!in_category(info, b"@read"));
        let flushall = lookup(b"flushall").expect("flushall");
        assert!(in_category(flushall, b"dangerous"));
        let keys = lookup(b"keys").expect("keys");
        assert!(in_category(keys, b"dangerous"));
    }

    #[test]
    fn count_and_info() {
        let count = command(&args(&["COMMAND", "COUNT"]));
        assert_eq!(count, Reply::Int(TABLE.len() as i64));
        assert!(command(&args(&["COMMAND", "count", "x"])).is_error());
        let info = command(&args(&["COMMAND", "INFO", "nope", "GET"]));
        let Reply::Array(items) = info else {
            panic!("{info:?}")
        };
        assert_eq!(items[0], Reply::Nil);
        let Reply::Array(get) = &items[1] else {
            panic!()
        };
        assert_eq!(get.len(), 10);
        assert_eq!(get[0], Reply::bulk("get"));
        assert_eq!(get[1], Reply::Int(2));
        assert_eq!(
            get[2],
            Reply::Array(vec![
                Reply::Simple("readonly".into()),
                Reply::Simple("fast".into())
            ])
        );
        let cats = ["@read", "@string", "@fast"]
            .iter()
            .map(|c| Reply::Simple((*c).into()))
            .collect();
        assert_eq!(get[6], Reply::Array(cats));
        let Reply::Array(all) = command(&args(&["COMMAND"])) else {
            panic!()
        };
        assert_eq!(all.len(), TABLE.len());
    }

    #[test]
    fn docs_list_help() {
        let Reply::Map(docs) = command(&args(&["COMMAND", "DOCS", "get", "nope"])) else {
            panic!()
        };
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0], Reply::bulk("get"));
        let Reply::Map(fields) = &docs[1] else {
            panic!()
        };
        assert!(!fields.contains(&Reply::bulk("complexity")));
        assert!(fields.contains(&Reply::bulk("group")));
        let Reply::Map(all) = command(&args(&["COMMAND", "DOCS"])) else {
            panic!()
        };
        assert_eq!(all.len(), TABLE.len() * 2);

        let Reply::Array(names) = command(&args(&["COMMAND", "LIST"])) else {
            panic!()
        };
        assert_eq!(names.len(), TABLE.len());
        let hashes = command(&args(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "HGET*"]));
        assert_eq!(hashes, bulks(&["hget", "hgetall", "hgetdel", "hgetex"]));
        let dangerous = command(&args(&[
            "COMMAND",
            "LIST",
            "filterby",
            "aclcat",
            "dangerous",
        ]));
        let Reply::Array(dangerous) = dangerous else {
            panic!()
        };
        assert!(dangerous.contains(&Reply::bulk("flushall")));
        assert!(!dangerous.contains(&Reply::bulk("get")));
        let none = command(&args(&["COMMAND", "LIST", "FILTERBY", "ACLCAT", "nope"]));
        assert_eq!(none, Reply::Array(Vec::new()));
        let module = command(&args(&["COMMAND", "LIST", "FILTERBY", "MODULE", "x"]));
        assert_eq!(module, Reply::Array(Vec::new()));
        assert!(command(&args(&["COMMAND", "LIST", "FILTERBY", "OTHER", "x"])).is_error());
        assert!(command(&args(&["COMMAND", "LIST", "FILTERBY"])).is_error());

        assert!(matches!(
            command(&args(&["COMMAND", "HELP"])),
            Reply::Array(_)
        ));
        assert_eq!(
            command(&args(&["COMMAND", "nope"])),
            Reply::err("ERR unknown subcommand 'nope'. Try COMMAND HELP.")
        );
    }

    #[test]
    fn getkeys() {
        let keys = |parts: &[&str]| {
            let mut full = vec!["COMMAND", "GETKEYS"];
            full.extend_from_slice(parts);
            command(&args(&full))
        };
        let sorted = |reply: Reply| {
            let Reply::Array(mut items) = reply else {
                panic!("{reply:?}")
            };
            items.sort_by(|a, b| match (a, b) {
                (Reply::Bulk(a), Reply::Bulk(b)) => a.cmp(b),
                _ => panic!("{a:?} {b:?}"),
            });
            Reply::Array(items)
        };
        assert_eq!(keys(&["MSET", "a", "1", "b", "2"]), bulks(&["a", "b"]));
        assert_eq!(
            keys(&["EVAL", "x", "2", "k1", "k2", "arg"]),
            bulks(&["k1", "k2"])
        );
        assert_eq!(
            keys(&["XREAD", "COUNT", "1", "STREAMS", "s1", "s2", "0", "0"]),
            bulks(&["s1", "s2"])
        );
        // Redis lists the destination first; the order here is the parser's.
        assert_eq!(
            sorted(keys(&["ZUNIONSTORE", "dst", "2", "a", "b"])),
            bulks(&["a", "b", "dst"])
        );
        assert_eq!(keys(&["BLPOP", "a", "b", "0"]), bulks(&["a", "b"]));
        assert_eq!(keys(&["LMPOP", "2", "a", "b", "LEFT"]), bulks(&["a", "b"]));
        assert_eq!(keys(&["watch", "w"]), bulks(&["w"]));

        assert_eq!(
            keys(&["NOPE", "a"]),
            Reply::err("ERR Invalid command specified")
        );
        assert_eq!(
            keys(&["GET"]),
            Reply::err("ERR Invalid number of arguments specified for command")
        );
        assert_eq!(
            keys(&["PING"]),
            Reply::err("ERR The command has no key arguments")
        );
        // Until the parser knows MOVE.
        let moved = keys(&["MOVE", "k", "1"]);
        assert!(
            moved == Reply::err("ERR Invalid command specified") || moved == bulks(&["k"]),
            "{moved:?}"
        );
        assert!(command(&args(&["COMMAND", "GETKEYS"])).is_error());
    }
}
