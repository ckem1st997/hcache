//! Users and their permissions, as Redis 7's ACL without selectors.
//!
//! A user is an immutable snapshot shared as `Arc<User>`; ACL SETUSER
//! builds a new one and swaps it in, and `Acl::generation` tells
//! connections that they should look their user up again. Passwords are
//! kept only as SHA-256 hashes and compared in constant time.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use crate::cmd::{Bytes, Cmd, Sub, MAX_PATTERN};
use crate::cmdtable::{self, CmdInfo};
use crate::glob::glob_match;
use crate::resp::{parse_i64, Reply};

const DEFAULT_USER: &[u8] = b"default";
const MAX_NAME: usize = 256;
/// Passwords, key patterns, channel patterns and command rules together.
const MAX_RULES: usize = 1024;
const MAX_USERS: usize = 10_000;
/// Longest part of a key, channel or name echoed in a reply.
const SHOWN: usize = 128;
const DEFAULT_GENPASS_BITS: i64 = 256;
const MAX_GENPASS_BITS: i64 = 4096;

const WRONGPASS: &str = "WRONGPASS invalid username-password pair or user is disabled.";
const UNKNOWN_RULE: &str = "Unknown command or category name in ACL";
const SYNTAX: &str = "Syntax error";
const NO_SUCH_PASS: &str = "The password you are trying to remove from the user does not exist";
const BAD_HASH: &str = "The password hash must be exactly 64 characters and contain only \
                        lowercase hexadecimal characters";
const KEY_AFTER_ALL: &str = "Adding a pattern after the * pattern (or the 'allkeys' flag) is \
                             not valid and does not have any effect. Try 'resetkeys' to start \
                             with an empty list of patterns";
const CHANNEL_AFTER_ALL: &str = "Adding a pattern after the * pattern (or the 'allchannels' \
                                 flag) is not valid and does not have any effect. Try \
                                 'resetchannels' to start with an empty list of channels";
const NO_SELECTORS: &str = "Selectors are not supported by this server";
const TOO_MANY_RULES: &str = "Too many rules for one user (the limit is 1024)";
const PATTERN_TOO_LONG: &str = "Pattern too long (the limit is 1024 bytes)";
const NO_ACL_FILE: &str = "ERR This Redis instance is not configured to use an ACL file. You \
                           may want to specify users via the ACL SETUSER command and then issue \
                           a CONFIG REWRITE (assuming you have a Redis configuration file set) \
                           in order to store users in the Redis configuration.";

type Hash = [u8; 32];

fn sha256(raw: &[u8]) -> Hash {
    Sha256::digest(raw).into()
}

/// No early exit: the time does not depend on where the hashes differ.
fn hash_eq(a: &Hash, b: &Hash) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    std::hint::black_box(diff) == 0
}

fn hex(raw: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    raw.iter()
        .flat_map(|b| [DIGITS[(b >> 4) as usize], DIGITS[(b & 15) as usize]])
        .map(char::from)
        .collect()
}

fn parse_hash(raw: &[u8]) -> Result<Hash, &'static str> {
    if raw.len() != 64 {
        return Err(BAD_HASH);
    }
    let mut out = [0u8; 32];
    for (byte, pair) in out.iter_mut().zip(raw.chunks(2)) {
        let digit = |c: u8| (c as char).to_digit(16).ok_or(BAD_HASH);
        *byte = (digit(pair[0])? * 16 + digit(pair[1])?) as u8;
    }
    Ok(out)
}

/// At most `SHOWN` bytes, printable ASCII as is and every other byte as
/// `\xHH`: safe to put in an error line.
fn shown(raw: &[u8]) -> String {
    let mut out = String::new();
    for &b in &raw[..raw.len().min(SHOWN)] {
        if (0x20..0x7f).contains(&b) && b != b'\\' && b != b'\'' {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("\\x{b:02x}"));
        }
    }
    out
}

/// A SETUSER modifier as echoed in an error: never a password or hash.
fn shown_rule(rule: &[u8]) -> String {
    match rule.first() {
        Some(&kind @ (b'>' | b'<' | b'#' | b'!')) => format!("{}<redacted>", char::from(kind)),
        _ => shown(rule),
    }
}

/// The command inside any `Cmd::In` wrapping.
fn unwrapped(cmd: &Cmd) -> &Cmd {
    match cmd {
        Cmd::In(_, inner) => unwrapped(inner),
        other => other,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// `+@all` or `-@all`.
    All(bool),
    Category(bool, &'static str),
    Command(bool, &'static str),
    /// Command and subcommand (upper case, as in the table).
    Sub(bool, &'static str, &'static str),
}

impl Rule {
    fn allow(self) -> bool {
        match self {
            Rule::All(a) | Rule::Category(a, _) | Rule::Command(a, _) | Rule::Sub(a, ..) => a,
        }
    }

    fn describe(self) -> String {
        let sign = if self.allow() { '+' } else { '-' };
        match self {
            Rule::All(_) => format!("{sign}@all"),
            Rule::Category(_, cat) => format!("{sign}@{cat}"),
            Rule::Command(_, name) => format!("{sign}{name}"),
            Rule::Sub(_, name, sub) => format!("{sign}{name}|{}", sub.to_ascii_lowercase()),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct KeyPattern {
    pattern: Bytes,
    read: bool,
    write: bool,
}

/// An immutable snapshot of one user.
#[derive(Clone)]
pub struct User {
    name: Bytes,
    enabled: bool,
    nopass: bool,
    passwords: Vec<Hash>,
    /// `~*`: every key, read and write.
    allkeys: bool,
    keys: Vec<KeyPattern>,
    /// `&*`: every channel and pattern.
    allchannels: bool,
    channels: Vec<Bytes>,
    /// Evaluated in order, the last one that matches wins; none: denied.
    rules: Vec<Rule>,
    unrestricted: bool,
    /// Tells this user from an earlier or later one of the same name.
    id: u64,
}

/// Written by hand: no password hash may reach a log.
impl fmt::Debug for User {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("User")
            .field("name", &shown(&self.name))
            .field("enabled", &self.enabled)
            .field("nopass", &self.nopass)
            .field("passwords", &self.passwords.len())
            .field("unrestricted", &self.unrestricted)
            .finish_non_exhaustive()
    }
}

impl User {
    /// A new user: off, no password, no keys, no channels, no commands.
    fn blank(name: &[u8]) -> User {
        User {
            name: name.to_vec(),
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            allkeys: false,
            keys: Vec::new(),
            allchannels: false,
            channels: Vec::new(),
            rules: Vec::new(),
            unrestricted: false,
            id: 0,
        }
    }

    /// "default" as a new server has it: on, nopass, ~*, &*, +@all.
    fn initial_default() -> User {
        User {
            enabled: true,
            nopass: true,
            allkeys: true,
            allchannels: true,
            rules: vec![Rule::All(true)],
            ..User::blank(DEFAULT_USER)
        }
        .sealed()
    }

    fn sealed(mut self) -> User {
        // `+@all` clears the rules before it, so an unrestricted list
        // starts with it and only allows after it.
        let all_commands = matches!(self.rules.first(), Some(Rule::All(true)))
            && self.rules.iter().all(|r| r.allow());
        self.unrestricted = self.enabled && self.allkeys && self.allchannels && all_commands;
        self
    }

    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// The same for every state of one user; a user deleted and made
    /// again under its old name has another.
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// True when a new connection is this user without AUTH: "default",
    /// on and nopass.
    pub fn open(&self) -> bool {
        self.name == DEFAULT_USER && self.enabled && self.nopass
    }

    /// Every command, key and channel allowed: callers skip `check`.
    pub fn unrestricted(&self) -> bool {
        self.unrestricted
    }

    fn rule_count(&self) -> usize {
        self.passwords.len() + self.keys.len() + self.channels.len() + self.rules.len()
    }

    fn password_ok(&self, hash: &Hash) -> bool {
        // `|` rather than `||`: every stored hash is compared.
        let matched = self
            .passwords
            .iter()
            .fold(false, |found, stored| found | hash_eq(stored, hash));
        self.enabled && (self.nopass || matched)
    }

    /// May this user run this command? `name` is the command name as sent,
    /// `sub` its second argument if any, `cmd` the parsed command.
    pub fn check(&self, name: &[u8], sub: Option<&[u8]>, cmd: &Cmd) -> Result<(), Reply> {
        if self.unrestricted {
            return Ok(());
        }
        let Some(info) = cmdtable::lookup(name) else {
            let name = String::from_utf8_lossy(name).to_ascii_lowercase();
            return Err(self.denied(&shown(name.as_bytes())));
        };
        // AUTH, HELLO, QUIT and RESET: always allowed, as in Redis, so a
        // connection can always change user.
        if !info.flags.contains(&"no_auth") {
            self.check_command(info, sub)?;
        }
        self.check_keys(info, cmd)?;
        self.check_channels(cmd)
    }

    fn denied(&self, what: &str) -> Reply {
        Reply::Error(format!(
            "NOPERM User {} has no permissions to run the '{what}' command",
            shown(&self.name)
        ))
    }

    fn check_command(&self, info: &CmdInfo, sub: Option<&[u8]>) -> Result<(), Reply> {
        // Stricter than Redis, where connections of a disabled user go on.
        if !self.enabled {
            return Err(self.denied(info.name));
        }
        let mut allowed = false;
        let mut by_sub = None;
        for &rule in &self.rules {
            let (matched, rule_sub) = match rule {
                Rule::All(_) => (true, None),
                Rule::Category(_, cat) => (cmdtable::in_category(info, cat.as_bytes()), None),
                Rule::Command(_, name) => (name == info.name, None),
                Rule::Sub(_, name, rule_sub) => (
                    name == info.name
                        && sub.is_some_and(|s| s.eq_ignore_ascii_case(rule_sub.as_bytes())),
                    Some(rule_sub),
                ),
            };
            if matched {
                allowed = rule.allow();
                by_sub = rule_sub;
            }
        }
        match (allowed, by_sub) {
            (true, _) => Ok(()),
            (false, None) => Err(self.denied(info.name)),
            (false, Some(sub)) => {
                Err(self.denied(&format!("{}|{}", info.name, sub.to_ascii_lowercase())))
            }
        }
    }

    /// Redis decides per key from each key's flags; here every key of a
    /// command that may write needs one pattern granting both read and
    /// write, and every other key one granting read. That never allows
    /// what Redis denies, and denies some write-only uses Redis allows
    /// (DEL or SET with only `%W~`). A command "may write" when the parsed
    /// command writes, the table flags it `write` (GETEX without options),
    /// or it is EVAL/EVALSHA (Redis gives their keys RW). The commands of
    /// a queued transaction are judged each on its own.
    fn check_keys(&self, info: &CmdInfo, cmd: &Cmd) -> Result<(), Reply> {
        if self.allkeys {
            return Ok(());
        }
        self.check_cmd_keys(cmd, info.flags.contains(&"write"))
    }

    fn check_cmd_keys(&self, cmd: &Cmd, table_write: bool) -> Result<(), Reply> {
        let cmd = unwrapped(cmd);
        if let Cmd::ExecBlock { queue, .. } = cmd {
            return queue.iter().try_for_each(|q| self.check_cmd_keys(q, false));
        }
        // BY and GET read whatever keys their patterns come to name: only
        // for a user who may read them all, as in Redis.
        if matches!(cmd, Cmd::Multi(op)
            if matches!(&**op, crate::ops::MultiOp::Sort { spec, .. } if spec.external()))
        {
            return Err(Reply::err(
                "ERR BY and GET options of SORT denied due to insufficient ACL permissions.",
            ));
        }
        let write = table_write
            || cmd.is_write()
            || matches!(
                cmd,
                Cmd::Eval {
                    read_only: false,
                    ..
                } | Cmd::EvalSha {
                    read_only: false,
                    ..
                } | Cmd::FCall {
                    read_only: false,
                    ..
                }
            );
        let mut refused: Option<Bytes> = None;
        cmdtable::keys_of(cmd, &mut |key| {
            if refused.is_none() && !self.key_ok(key, write) {
                refused = Some(key.to_vec());
            }
        });
        match refused {
            None => Ok(()),
            Some(key) => Err(Reply::Error(format!(
                "NOPERM No permissions to access the '{}' key",
                shown(&key)
            ))),
        }
    }

    fn key_ok(&self, key: &[u8], write: bool) -> bool {
        self.keys
            .iter()
            .any(|p| p.read && (p.write || !write) && glob_match(&p.pattern, key))
    }

    /// PUBLISH and SUBSCRIBE name channels, matched against the patterns;
    /// PSUBSCRIBE names patterns, which must equal an allowed one.
    fn check_channels(&self, cmd: &Cmd) -> Result<(), Reply> {
        if self.allchannels {
            return Ok(());
        }
        let literal = |ch: &Bytes| self.channels.iter().any(|p| glob_match(p, ch));
        let exact = |pat: &Bytes| self.channels.iter().any(|p| p == pat);
        let refused = match unwrapped(cmd) {
            Cmd::Publish(ch, _) | Cmd::SPublish(ch, _) => (!literal(ch)).then_some(ch),
            Cmd::Sub(Sub::Subscribe(chs) | Sub::SSubscribe(chs)) => {
                chs.iter().find(|ch| !literal(ch))
            }
            Cmd::Sub(Sub::PSubscribe(pats)) => pats.iter().find(|pat| !exact(pat)),
            Cmd::ExecBlock { queue, .. } => {
                return queue.iter().try_for_each(|q| self.check_channels(q))
            }
            _ => None,
        };
        match refused {
            None => Ok(()),
            Some(ch) => Err(Reply::Error(format!(
                "NOPERM No permissions to access the '{}' channel",
                shown(ch)
            ))),
        }
    }

    /// Apply one SETUSER modifier. The caller works on a copy and drops it
    /// on error, so a failed modifier changes nothing.
    fn apply(&mut self, op: &[u8]) -> Result<(), &'static str> {
        let word = |w: &str| op.eq_ignore_ascii_case(w.as_bytes());
        match op.first() {
            Some(b'>') => {
                let hash = sha256(&op[1..]);
                self.add_password(hash);
            }
            Some(b'#') => self.add_password(parse_hash(&op[1..])?),
            Some(b'<') => self.remove_password(&sha256(&op[1..]))?,
            Some(b'!') => self.remove_password(&parse_hash(&op[1..])?)?,
            Some(b'~') if op == b"~*" => self.all_keys(),
            Some(b'~') => self.add_key(&op[1..], true, true)?,
            Some(b'%') => {
                let tilde = op.iter().position(|&b| b == b'~').ok_or(SYNTAX)?;
                let flags = &op[1..tilde];
                if flags.is_empty() {
                    return Err(SYNTAX);
                }
                let (mut read, mut write) = (false, false);
                for flag in flags {
                    match flag.to_ascii_uppercase() {
                        b'R' => read = true,
                        b'W' => write = true,
                        _ => return Err(SYNTAX),
                    }
                }
                self.add_key(&op[tilde + 1..], read, write)?;
            }
            Some(b'&') if op == b"&*" => self.all_channels(),
            Some(b'&') => self.add_channel(&op[1..])?,
            Some(b'+') => self.command_rule(true, &op[1..])?,
            Some(b'-') => self.command_rule(false, &op[1..])?,
            Some(b'(') => return Err(NO_SELECTORS),
            _ if word("on") => self.enabled = true,
            _ if word("off") => self.enabled = false,
            _ if word("nopass") => {
                self.nopass = true;
                self.passwords.clear();
            }
            _ if word("resetpass") => {
                self.nopass = false;
                self.passwords.clear();
            }
            _ if word("allkeys") => self.all_keys(),
            _ if word("resetkeys") => {
                self.allkeys = false;
                self.keys.clear();
            }
            _ if word("allchannels") => self.all_channels(),
            _ if word("resetchannels") => {
                self.allchannels = false;
                self.channels.clear();
            }
            _ if word("allcommands") => self.rules = vec![Rule::All(true)],
            _ if word("nocommands") => self.rules = vec![Rule::All(false)],
            _ if word("reset") => {
                *self = User {
                    id: self.id,
                    ..User::blank(&self.name)
                }
            }
            _ if word("clearselectors") => return Err(NO_SELECTORS),
            _ => return Err(SYNTAX),
        }
        if self.rule_count() > MAX_RULES {
            return Err(TOO_MANY_RULES);
        }
        Ok(())
    }

    fn add_password(&mut self, hash: Hash) {
        self.nopass = false;
        if !self.passwords.contains(&hash) {
            self.passwords.push(hash);
        }
    }

    fn remove_password(&mut self, hash: &Hash) -> Result<(), &'static str> {
        let before = self.passwords.len();
        self.passwords.retain(|stored| !hash_eq(stored, hash));
        if self.passwords.len() == before {
            return Err(NO_SUCH_PASS);
        }
        Ok(())
    }

    fn all_keys(&mut self) {
        self.allkeys = true;
        self.keys.clear();
    }

    fn all_channels(&mut self) {
        self.allchannels = true;
        self.channels.clear();
    }

    fn add_key(&mut self, pattern: &[u8], read: bool, write: bool) -> Result<(), &'static str> {
        if self.allkeys {
            return Err(KEY_AFTER_ALL);
        }
        if pattern.len() > MAX_PATTERN {
            return Err(PATTERN_TOO_LONG);
        }
        // The same pattern twice: its permissions add up, as in Redis.
        match self.keys.iter_mut().find(|p| p.pattern == pattern) {
            Some(existing) => {
                existing.read |= read;
                existing.write |= write;
            }
            None => self.keys.push(KeyPattern {
                pattern: pattern.to_vec(),
                read,
                write,
            }),
        }
        Ok(())
    }

    fn add_channel(&mut self, pattern: &[u8]) -> Result<(), &'static str> {
        if self.allchannels {
            return Err(CHANNEL_AFTER_ALL);
        }
        if pattern.len() > MAX_PATTERN {
            return Err(PATTERN_TOO_LONG);
        }
        if !self.channels.iter().any(|p| p == pattern) {
            self.channels.push(pattern.to_vec());
        }
        Ok(())
    }

    /// `+@cat`, `+cmd` or `+cmd|sub` (and their `-` forms), with names
    /// resolved against the command table.
    fn command_rule(&mut self, allow: bool, spec: &[u8]) -> Result<(), &'static str> {
        if let Some(cat) = spec.strip_prefix(b"@") {
            if cat.eq_ignore_ascii_case(b"all") {
                // Every earlier rule is overridden.
                self.rules = vec![Rule::All(allow)];
                return Ok(());
            }
            let cat = cmdtable::categories()
                .iter()
                .find(|c| c.as_bytes().eq_ignore_ascii_case(cat))
                .ok_or(UNKNOWN_RULE)?;
            self.rules.push(Rule::Category(allow, cat));
            return Ok(());
        }
        let (name, sub) = match spec.iter().position(|&b| b == b'|') {
            Some(bar) => (&spec[..bar], Some(&spec[bar + 1..])),
            None => (spec, None),
        };
        let info = cmdtable::lookup(name).ok_or(UNKNOWN_RULE)?;
        let rule = match sub {
            None => Rule::Command(allow, info.name),
            Some(sub) => {
                let sub = info
                    .subcommands
                    .iter()
                    .find(|s| s.as_bytes().eq_ignore_ascii_case(sub))
                    .ok_or(UNKNOWN_RULE)?;
                Rule::Sub(allow, info.name, sub)
            }
        };
        self.rules.push(rule);
        Ok(())
    }

    fn describe_commands(&self) -> String {
        let mut parts: Vec<String> = Vec::with_capacity(self.rules.len() + 1);
        // Nothing matching means denied: say so unless `@all` comes first.
        if !matches!(self.rules.first(), Some(Rule::All(_))) {
            parts.push("-@all".into());
        }
        parts.extend(self.rules.iter().map(|r| r.describe()));
        parts.join(" ")
    }

    fn describe_keys(&self) -> Bytes {
        if self.allkeys {
            return b"~*".to_vec();
        }
        let mut out = Vec::new();
        for p in &self.keys {
            if !out.is_empty() {
                out.push(b' ');
            }
            match (p.read, p.write) {
                (true, true) => out.push(b'~'),
                (true, false) => out.extend_from_slice(b"%R~"),
                (false, true) => out.extend_from_slice(b"%W~"),
                (false, false) => out.extend_from_slice(b"%~"),
            }
            out.extend_from_slice(&p.pattern);
        }
        out
    }

    fn describe_channels(&self) -> Bytes {
        if self.allchannels {
            return b"&*".to_vec();
        }
        let mut out = Vec::new();
        for p in &self.channels {
            if !out.is_empty() {
                out.push(b' ');
            }
            out.push(b'&');
            out.extend_from_slice(p);
        }
        out
    }

    /// The SETUSER modifiers that make a blank user into this one.
    pub(crate) fn tokens(&self) -> Vec<Bytes> {
        let word = |w: &str| w.as_bytes().to_vec();
        let mut out = vec![word(if self.enabled { "on" } else { "off" })];
        if self.nopass {
            out.push(word("nopass"));
        }
        for hash in &self.passwords {
            out.push(format!("#{}", hex(hash)).into_bytes());
        }
        if self.allkeys {
            out.push(word("allkeys"));
        }
        for p in &self.keys {
            let mut token = match (p.read, p.write) {
                // Not "~": "~*" would come back as `allkeys`, which is
                // more than a pattern that happens to match everything.
                (true, true) => word("%RW~"),
                (true, false) => word("%R~"),
                (false, true) => word("%W~"),
                // Allows nothing, so there is nothing to keep.
                (false, false) => continue,
            };
            token.extend_from_slice(&p.pattern);
            out.push(token);
        }
        if self.allchannels {
            out.push(word("allchannels"));
        }
        for p in &self.channels {
            let mut token = word("&");
            token.extend_from_slice(p);
            out.push(token);
        }
        out.extend(self.rules.iter().map(|r| r.describe().into_bytes()));
        out
    }

    /// One line of ACL LIST.
    fn describe(&self) -> Bytes {
        let mut out = b"user ".to_vec();
        out.extend_from_slice(&self.name);
        out.extend_from_slice(if self.enabled { b" on" } else { b" off" });
        if self.nopass {
            out.extend_from_slice(b" nopass");
        }
        for hash in &self.passwords {
            out.extend_from_slice(b" #");
            out.extend_from_slice(hex(hash).as_bytes());
        }
        let keys = self.describe_keys();
        if !keys.is_empty() {
            out.push(b' ');
            out.extend_from_slice(&keys);
        }
        let channels = self.describe_channels();
        out.push(b' ');
        if channels.is_empty() {
            out.extend_from_slice(b"resetchannels");
        } else {
            out.extend_from_slice(&channels);
        }
        out.push(b' ');
        out.extend_from_slice(self.describe_commands().as_bytes());
        out
    }

    /// ACL GETUSER, in the Redis 7 shape.
    fn getuser_reply(&self) -> Reply {
        let mut flags = vec![Reply::bulk(if self.enabled { "on" } else { "off" })];
        if self.nopass {
            flags.push(Reply::bulk("nopass"));
        }
        let passwords = self.passwords.iter().map(|h| Reply::bulk(hex(h))).collect();
        Reply::Map(vec![
            Reply::bulk("flags"),
            Reply::Array(flags),
            Reply::bulk("passwords"),
            Reply::Array(passwords),
            Reply::bulk("commands"),
            Reply::bulk(self.describe_commands()),
            Reply::bulk("keys"),
            Reply::bulk(self.describe_keys()),
            Reply::bulk("channels"),
            Reply::bulk(self.describe_channels()),
            Reply::bulk("selectors"),
            Reply::Array(Vec::new()),
        ])
    }
}

struct Users {
    default: Arc<User>,
    others: BTreeMap<Bytes, Arc<User>>,
}

impl Users {
    fn get(&self, name: &[u8]) -> Option<Arc<User>> {
        if name == DEFAULT_USER {
            return Some(self.default.clone());
        }
        self.others.get(name).cloned()
    }

    /// Every user, sorted by name.
    fn sorted(&self) -> Vec<Arc<User>> {
        let mut all: Vec<Arc<User>> = self.others.values().cloned().collect();
        all.push(self.default.clone());
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    fn count(&self) -> usize {
        self.others.len() + 1
    }

    fn put(&mut self, user: User) {
        let user = Arc::new(user);
        if user.name == DEFAULT_USER {
            self.default = user;
        } else {
            self.others.insert(user.name.clone(), user);
        }
    }
}

pub struct Acl {
    users: RwLock<Users>,
    /// Bumped, under the write lock, by every change to a user.
    generation: AtomicU64,
    /// The id of the user made last; "default" is 0.
    last_id: AtomicU64,
}

impl Default for Acl {
    fn default() -> Self {
        Acl::new()
    }
}

impl fmt::Debug for Acl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Acl")
            .field("users", &self.users.read().count())
            .field("generation", &self.generation())
            .finish()
    }
}

fn arity(sub: &str) -> Reply {
    Reply::Error(format!(
        "ERR wrong number of arguments for 'acl|{sub}' command"
    ))
}

fn validate_name(name: &[u8]) -> Result<(), Reply> {
    if name.is_empty() {
        return Err(Reply::err("ERR Usernames can't be empty"));
    }
    if name.len() > MAX_NAME {
        return Err(Reply::err("ERR Usernames can't be longer than 256 bytes"));
    }
    if name.iter().any(|&b| b <= b' ' || b == 0x7f) {
        return Err(Reply::err(
            "ERR Usernames can't contain spaces or control characters",
        ));
    }
    Ok(())
}

/// Bytes for ACL GENPASS, from the operating system's generator.
fn random_bytes(n: usize) -> Result<Vec<u8>, Reply> {
    let mut out = vec![0; n];
    getrandom::getrandom(&mut out)
        .map_err(|_| Reply::err("ERR the system's random generator is not available"))?;
    Ok(out)
}

thread_local! {
    /// The user on whose behalf this thread runs a script; none: a caller
    /// that may do everything.
    static CALLER: RefCell<Option<Arc<User>>> = const { RefCell::new(None) };
}

/// Run `f` with `user` as the one `caller_check` judges. Only a user with
/// limits needs to be named.
pub fn with_caller<R>(user: Option<Arc<User>>, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<Arc<User>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CALLER.with(|c| *c.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(CALLER.with(|c| c.replace(user)));
    f()
}

/// Whether the commands of this thread's script need `caller_check`.
pub(crate) fn caller_restricted() -> bool {
    CALLER.with(|c| c.borrow().as_ref().is_some_and(|u| !u.unrestricted()))
}

/// `User::check` for the user this thread runs a script for.
pub(crate) fn caller_check(name: &[u8], sub: Option<&[u8]>, cmd: &Cmd) -> Result<(), Reply> {
    CALLER.with(|c| match c.borrow().as_ref() {
        Some(user) => user.check(name, sub, cmd),
        None => Ok(()),
    })
}

const HELP: &[&str] = &[
    "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CAT [<category>]",
    "    List all commands that belong to <category>, or all command categories",
    "    when no category is specified.",
    "DELUSER <username> [<username> ...]",
    "    Delete a list of users.",
    "DRYRUN <username> <command> [<arg> ...]",
    "    Returns whether the user can execute the given command without executing the command.",
    "GETUSER <username>",
    "    Get the user's details.",
    "GENPASS [<bits>]",
    "    Generate a secure 256-bit user password. The optional `bits` argument can",
    "    be used to specify a different size.",
    "LIST",
    "    Show users details in config file format.",
    "LOAD",
    "    Not supported: there is no ACL file.",
    "LOG [<count> | RESET]",
    "    Show the ACL log entries (always empty here).",
    "SAVE",
    "    Not supported: there is no ACL file.",
    "SETUSER <username> <attribute> [<attribute> ...]",
    "    Create or modify a user with the specified attributes.",
    "USERS",
    "    List all the registered usernames.",
    "WHOAMI",
    "    Return the current connection username.",
    "HELP",
    "    Print this help.",
];

impl Acl {
    /// One user, "default": on, nopass, ~*, &*, +@all.
    pub fn new() -> Acl {
        Acl {
            users: RwLock::new(Users {
                default: Arc::new(User::initial_default()),
                others: BTreeMap::new(),
            }),
            generation: AtomicU64::new(0),
            last_id: AtomicU64::new(0),
        }
    }

    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// `requirepass`: the default user's only password; `None` or empty
    /// makes it nopass again.
    pub fn set_requirepass(&self, pass: Option<&[u8]>) {
        let mut users = self.users.write();
        let mut user = (*users.default).clone();
        user.passwords.clear();
        match pass {
            Some(pass) if !pass.is_empty() => {
                user.nopass = false;
                user.passwords.push(sha256(pass));
            }
            _ => user.nopass = true,
        }
        users.put(user.sealed());
        self.bump();
    }

    pub fn default_user(&self) -> Arc<User> {
        self.users.read().default.clone()
    }

    pub fn user(&self, name: &[u8]) -> Option<Arc<User>> {
        self.users.read().get(name)
    }

    /// AUTH. An unknown user, a disabled one and a wrong password give the
    /// same error.
    pub fn auth(&self, name: &[u8], pass: &[u8]) -> Result<Arc<User>, Reply> {
        let hash = sha256(pass);
        match self.user(name) {
            Some(user) if user.password_ok(&hash) => Ok(user),
            Some(_) => Err(Reply::err(WRONGPASS)),
            None => {
                // About the same work as a known user with one password.
                std::hint::black_box(hash_eq(&hash, &[0; 32]));
                Err(Reply::err(WRONGPASS))
            }
        }
    }

    /// Every user by name, for the data file.
    pub(crate) fn users(&self) -> HashMap<Bytes, Arc<User>> {
        let users = self.users.read();
        users
            .sorted()
            .into_iter()
            .map(|user| (user.name.clone(), user))
            .collect()
    }

    /// Recovery: make `name` the user these SETUSER modifiers describe.
    pub(crate) fn restore_user(&self, name: &[u8], tokens: &[Bytes]) -> Result<(), String> {
        if validate_name(name).is_err() {
            return Err("a user with a name that is not allowed".into());
        }
        let mut users = self.users.write();
        let id = match name == DEFAULT_USER {
            true => 0,
            false => self.last_id.fetch_add(1, Ordering::AcqRel) + 1,
        };
        let mut user = User {
            id,
            ..User::blank(name)
        };
        for token in tokens {
            user.apply(token)
                .map_err(|why| format!("user '{}': {why}", shown(name)))?;
        }
        users.put(user.sealed());
        self.bump();
        Ok(())
    }

    /// Recovery: the data file says this user was deleted. "default" goes
    /// back to what a new server has.
    pub(crate) fn forget_user(&self, name: &[u8]) {
        let mut users = self.users.write();
        if name == DEFAULT_USER {
            users.put(User::initial_default());
        } else {
            users.others.remove(name);
        }
        self.bump();
    }

    /// Changes whenever any user changes.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// ACL and its subcommands. `args[0]` is "ACL"; `me` is the caller.
    pub fn command(&self, args: &[Bytes], me: &User) -> Reply {
        let Some(sub) = args.get(1) else {
            return Reply::err("ERR wrong number of arguments for 'acl' command");
        };
        let n = args.len();
        let is = |word: &str| sub.eq_ignore_ascii_case(word.as_bytes());
        if is("SETUSER") {
            if n < 3 {
                return arity("setuser");
            }
            return match self.set_user(&args[2], &args[3..]) {
                Ok(()) => Reply::Ok,
                Err(e) => e,
            };
        }
        if is("GETUSER") {
            if n != 3 {
                return arity("getuser");
            }
            return self
                .user(&args[2])
                .map_or(Reply::Nil, |u| u.getuser_reply());
        }
        if is("DELUSER") {
            if n < 3 {
                return arity("deluser");
            }
            return self.del_users(&args[2..]);
        }
        if is("LIST") || is("USERS") {
            if n != 2 {
                return arity(if is("LIST") { "list" } else { "users" });
            }
            let users = self.users.read().sorted();
            let each = |u: &Arc<User>| {
                if is("LIST") {
                    Reply::Bulk(u.describe())
                } else {
                    Reply::Bulk(u.name.clone())
                }
            };
            return Reply::Array(users.iter().map(each).collect());
        }
        if is("WHOAMI") {
            if n != 2 {
                return arity("whoami");
            }
            return Reply::Bulk(me.name.clone());
        }
        if is("CAT") {
            return match args.get(2) {
                None => Reply::Array(
                    cmdtable::categories()
                        .iter()
                        .map(|c| Reply::bulk(*c))
                        .collect(),
                ),
                Some(_) if n > 3 => arity("cat"),
                Some(cat) => {
                    if !cmdtable::categories()
                        .iter()
                        .any(|c| c.as_bytes().eq_ignore_ascii_case(cat))
                    {
                        return Reply::Error(format!("ERR Unknown category '{}'", shown(cat)));
                    }
                    Reply::Array(
                        cmdtable::all()
                            .iter()
                            .filter(|info| cmdtable::in_category(info, cat))
                            .map(|info| Reply::bulk(info.name))
                            .collect(),
                    )
                }
            };
        }
        if is("GENPASS") {
            let bits =
                match args.get(2) {
                    None => DEFAULT_GENPASS_BITS,
                    Some(_) if n > 3 => return arity("genpass"),
                    Some(raw) => match parse_i64(raw) {
                        Some(bits) if (1..=MAX_GENPASS_BITS).contains(&bits) => bits,
                        _ => return Reply::err(
                            "ERR ACL GENPASS argument must be the number of bits for the output \
                             password, a positive number up to 4096",
                        ),
                    },
                };
            let chars = (bits as usize).div_ceil(4);
            let mut text = match random_bytes(chars.div_ceil(2)) {
                Ok(bytes) => hex(&bytes),
                Err(e) => return e,
            };
            text.truncate(chars);
            return Reply::bulk(text);
        }
        if is("LOG") {
            return match args.get(2) {
                None => Reply::Array(Vec::new()),
                Some(_) if n > 3 => arity("log"),
                Some(raw) if raw.eq_ignore_ascii_case(b"RESET") => Reply::Ok,
                Some(raw) => match parse_i64(raw) {
                    Some(count) if count >= 0 => Reply::Array(Vec::new()),
                    _ => Reply::err("ERR value is out of range, must be positive"),
                },
            };
        }
        if is("SAVE") || is("LOAD") {
            if n != 2 {
                return arity(if is("SAVE") { "save" } else { "load" });
            }
            return Reply::err(NO_ACL_FILE);
        }
        if is("DRYRUN") {
            if n < 4 {
                return arity("dryrun");
            }
            return self.dry_run(&args[2], &args[3..]);
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
            "ERR unknown subcommand '{}'. Try ACL HELP.",
            shown(sub)
        ))
    }

    /// All or nothing: the rules apply to a copy that replaces the user
    /// only when every one of them is valid.
    fn set_user(&self, name: &[u8], rules: &[Bytes]) -> Result<(), Reply> {
        validate_name(name)?;
        let mut users = self.users.write();
        let mut user = match users.get(name) {
            Some(existing) => (*existing).clone(),
            None if users.count() >= MAX_USERS => {
                return Err(Reply::err("ERR Too many ACL users (the limit is 10000)"))
            }
            None => User {
                id: self.last_id.fetch_add(1, Ordering::AcqRel) + 1,
                ..User::blank(name)
            },
        };
        for rule in rules {
            user.apply(rule).map_err(|why| {
                Reply::Error(format!(
                    "ERR Error in ACL SETUSER modifier '{}': {why}",
                    shown_rule(rule)
                ))
            })?;
        }
        users.put(user.sealed());
        self.bump();
        Ok(())
    }

    fn del_users(&self, names: &[Bytes]) -> Reply {
        if names.iter().any(|n| n == DEFAULT_USER) {
            return Reply::err("ERR The 'default' user cannot be removed");
        }
        let mut users = self.users.write();
        let removed = names
            .iter()
            .filter(|n| users.others.remove(n.as_slice()).is_some())
            .count();
        if removed > 0 {
            self.bump();
        }
        Reply::Int(i64::try_from(removed).unwrap_or(i64::MAX))
    }

    fn dry_run(&self, name: &[u8], full: &[Bytes]) -> Reply {
        let Some(user) = self.user(name) else {
            return Reply::Error(format!("ERR User '{}' not found", shown(name)));
        };
        if cmdtable::lookup(&full[0]).is_none() {
            return Reply::Error(format!("ERR Command '{}' not found", shown(&full[0])));
        }
        let cmd = match Cmd::parse(full.to_vec()) {
            Ok(cmd) => cmd,
            Err(e) => return e,
        };
        let sub = full.get(1).map(Vec::as_slice);
        match user.check(&full[0], sub, &cmd) {
            Ok(()) => Reply::Ok,
            Err(Reply::Error(text)) => Reply::bulk(text),
            Err(other) => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    fn acl_cmd(acl: &Acl, parts: &[&str]) -> Reply {
        let mut full = vec!["ACL"];
        full.extend_from_slice(parts);
        let me = acl.default_user();
        acl.command(&args(&full), &me)
    }

    fn setuser(acl: &Acl, name: &str, rules: &[&str]) -> Reply {
        let mut full = vec!["SETUSER", name];
        full.extend_from_slice(rules);
        acl_cmd(acl, &full)
    }

    /// A user built from these rules, starting from nothing.
    fn user(rules: &[&str]) -> Arc<User> {
        let acl = Acl::new();
        assert_eq!(setuser(&acl, "u", rules), Reply::Ok, "{rules:?}");
        acl.user(b"u").expect("u")
    }

    fn run(user: &User, parts: &[&str]) -> Result<(), Reply> {
        let raw = args(parts);
        let cmd = Cmd::parse(raw.clone()).unwrap_or_else(|e| panic!("{parts:?}: {e:?}"));
        user.check(&raw[0], raw.get(1).map(Vec::as_slice), &cmd)
    }

    fn ok(user: &User, parts: &[&str]) {
        assert_eq!(run(user, parts), Ok(()), "{parts:?}");
    }

    fn denied(user: &User, parts: &[&str]) -> String {
        match run(user, parts) {
            Err(Reply::Error(e)) => {
                assert!(e.starts_with("NOPERM "), "{e}");
                e
            }
            other => panic!("{parts:?} allowed: {other:?}"),
        }
    }

    /// Command permission only, for commands the parser rejects.
    fn check_name(user: &User, name: &str, sub: Option<&str>) -> Result<(), Reply> {
        user.check(name.as_bytes(), sub.map(str::as_bytes), &Cmd::Quit)
    }

    /// Every string in the reply, readable (Debug shows bulks as numbers).
    fn text(reply: &Reply) -> String {
        match reply {
            Reply::Simple(s) | Reply::Error(s) => s.clone(),
            Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            Reply::Array(items) | Reply::Map(items) => {
                items.iter().map(text).collect::<Vec<_>>().join("\n")
            }
            other => format!("{other:?}"),
        }
    }

    #[test]
    fn default_user_is_open_and_unrestricted() {
        let acl = Acl::new();
        let d = acl.default_user();
        assert!(d.unrestricted() && d.open() && d.enabled());
        assert_eq!(d.name(), b"default");
        ok(&d, &["FLUSHALL"]);
        ok(&d, &["CONFIG", "SET", "a", "b"]);
        assert!(acl.auth(b"default", b"anything").is_ok());
        assert_eq!(
            acl_cmd(&acl, &["USERS"]),
            Reply::Array(vec![Reply::bulk("default")])
        );
    }

    #[test]
    fn requirepass_on_and_off() {
        let acl = Acl::new();
        acl.set_requirepass(Some(b"pw"));
        let d = acl.default_user();
        assert!(!d.open());
        assert!(
            d.unrestricted(),
            "a password is not a restriction on commands"
        );
        assert!(acl.auth(b"default", b"pw").is_ok());
        assert_eq!(
            acl.auth(b"default", b"nope").err(),
            Some(Reply::err(WRONGPASS))
        );
        assert_eq!(acl.auth(b"default", b"").err(), Some(Reply::err(WRONGPASS)));
        acl.set_requirepass(Some(b"other"));
        assert!(
            acl.auth(b"default", b"pw").is_err(),
            "the old password must go"
        );
        assert!(acl.auth(b"default", b"other").is_ok());
        acl.set_requirepass(None);
        assert!(acl.default_user().open());
        acl.set_requirepass(Some(b"pw"));
        acl.set_requirepass(Some(b""));
        assert!(acl.default_user().open());
        assert!(acl.auth(b"default", b"whatever").is_ok());
    }

    #[test]
    fn passwords_add_remove_nopass_resetpass() {
        let acl = Acl::new();
        assert_eq!(
            setuser(&acl, "alice", &["on", ">a", ">b", "+@all", "~*"]),
            Reply::Ok
        );
        assert!(acl.auth(b"alice", b"a").is_ok());
        assert!(acl.auth(b"alice", b"b").is_ok());
        assert!(acl.auth(b"alice", b"c").is_err());
        assert!(acl.auth(b"alice", b"").is_err());
        assert_eq!(setuser(&acl, "alice", &["<a"]), Reply::Ok);
        assert!(acl.auth(b"alice", b"a").is_err());
        assert!(acl.auth(b"alice", b"b").is_ok());
        let missing = setuser(&acl, "alice", &["<zzzsecret"]);
        assert!(missing.is_error());
        assert!(!text(&missing).contains("zzzsecret"), "{missing:?}");
        assert!(text(&missing).contains("does not exist"));

        assert_eq!(setuser(&acl, "alice", &["nopass"]), Reply::Ok);
        assert!(acl.auth(b"alice", b"anything").is_ok());
        assert!(acl.user(b"alice").expect("alice").passwords.is_empty());
        assert!(
            !acl.user(b"alice").expect("alice").open(),
            "only default is open"
        );

        assert_eq!(setuser(&acl, "alice", &[">p"]), Reply::Ok);
        assert!(acl.auth(b"alice", b"x").is_err(), ">p must clear nopass");
        assert!(acl.auth(b"alice", b"p").is_ok());

        assert_eq!(setuser(&acl, "alice", &["resetpass"]), Reply::Ok);
        assert!(acl.auth(b"alice", b"p").is_err());
        assert!(acl.auth(b"alice", b"").is_err());

        // By hash.
        let hash = hex(&sha256(b"hashed"));
        assert_eq!(setuser(&acl, "alice", &[&format!("#{hash}")]), Reply::Ok);
        assert!(acl.auth(b"alice", b"hashed").is_ok());
        assert_eq!(setuser(&acl, "alice", &[&format!("!{hash}")]), Reply::Ok);
        assert!(acl.auth(b"alice", b"hashed").is_err());
        for bad in [
            "#abc",
            "#zz",
            &format!("#{hash}0"),
            &format!("!{}", &hash[..63]),
        ] {
            let reply = setuser(&acl, "alice", &[bad]);
            assert!(text(&reply).contains("64 characters"), "{bad}: {reply:?}");
        }
        let upper = hex(&sha256(b"up")).to_ascii_uppercase();
        assert_eq!(setuser(&acl, "alice", &[&format!("#{upper}")]), Reply::Ok);
        assert!(acl.auth(b"alice", b"up").is_ok());
    }

    #[test]
    fn nopass_then_password_in_one_call() {
        let acl = Acl::new();
        assert_eq!(setuser(&acl, "u", &["on", ">a", "nopass"]), Reply::Ok);
        let u = acl.user(b"u").expect("u");
        assert!(u.nopass && u.passwords.is_empty());
        assert_eq!(setuser(&acl, "u", &["nopass", ">b"]), Reply::Ok);
        assert!(acl.auth(b"u", b"a").is_err());
        assert!(acl.auth(b"u", b"b").is_ok());
    }

    #[test]
    fn disabled_or_passwordless_user_cannot_auth() {
        let acl = Acl::new();
        assert_eq!(setuser(&acl, "bob", &["off", ">p", "+@all"]), Reply::Ok);
        assert!(acl.auth(b"bob", b"p").is_err());
        assert_eq!(setuser(&acl, "bob", &["nopass"]), Reply::Ok);
        assert!(acl.auth(b"bob", b"p").is_err());
        assert!(acl.auth(b"bob", b"").is_err());
        assert_eq!(setuser(&acl, "carol", &["on"]), Reply::Ok);
        assert!(
            acl.auth(b"carol", b"").is_err(),
            "no password and not nopass"
        );
        // A disabled user's connections lose their commands too.
        let bob = acl.user(b"bob").expect("bob");
        denied(&bob, &["GET", "k"]);
        ok(&bob, &["QUIT"]);
    }

    #[test]
    fn auth_errors_are_identical() {
        let acl = Acl::new();
        assert_eq!(setuser(&acl, "alice", &["on", ">right"]), Reply::Ok);
        assert_eq!(setuser(&acl, "off", &["off", ">right"]), Reply::Ok);
        let unknown = acl.auth(b"nobody", b"right").err();
        let wrong = acl.auth(b"alice", b"wrong").err();
        let disabled = acl.auth(b"off", b"right").err();
        assert!(unknown.is_some());
        assert_eq!(unknown, wrong);
        assert_eq!(wrong, disabled);
        let mut encoded = Vec::new();
        unknown.expect("err").encode(&mut encoded);
        assert_eq!(
            encoded,
            b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"
        );
    }

    #[test]
    fn all_minus_dangerous() {
        let u = user(&["on", "+@all", "-@dangerous", "~*", "&*"]);
        assert!(!u.unrestricted());
        let e = denied(&u, &["FLUSHALL"]);
        assert_eq!(
            e,
            "NOPERM User u has no permissions to run the 'flushall' command"
        );
        denied(&u, &["KEYS", "*"]);
        denied(&u, &["CONFIG", "GET", "x"]);
        denied(&u, &["INFO"]);
        ok(&u, &["GET", "k"]);
        ok(&u, &["SET", "k", "v"]);
    }

    #[test]
    fn only_get_in_any_case() {
        let u = user(&["on", "-@all", "+GeT", "~*"]);
        for name in ["GET", "get", "gEt"] {
            ok(&u, &[name, "k"]);
        }
        denied(&u, &["SET", "k", "v"]);
        denied(&u, &["MGET", "k"]);
        denied(&u, &["GETDEL", "k"]);
    }

    #[test]
    fn read_category_cannot_write() {
        let u = user(&["on", "+@read", "~*"]);
        ok(&u, &["GET", "k"]);
        ok(&u, &["HGETALL", "k"]);
        denied(&u, &["SET", "k", "v"]);
        denied(&u, &["DEL", "k"]);
        denied(&u, &["GETEX", "k"]);
        denied(&u, &["EVAL", "return 1", "0"]);
    }

    #[test]
    fn order_matters() {
        ok(&user(&["on", "+@all", "-get", "+get", "~*"]), &["GET", "k"]);
        ok(&user(&["on", "-get", "+@all", "~*"]), &["GET", "k"]);
        denied(&user(&["on", "+@all", "-get", "~*"]), &["GET", "k"]);
        denied(&user(&["on", "+get", "-@all", "~*"]), &["GET", "k"]);
        denied(&user(&["on", "+get", "-@read", "~*"]), &["GET", "k"]);
        ok(&user(&["on", "-@read", "+get", "~*"]), &["GET", "k"]);
        let u = user(&["on", "+@all", "-@all", "~*"]);
        assert_eq!(u.rules.len(), 1, "@all drops the rules before it");
    }

    #[test]
    fn subcommand_rules() {
        let u = user(&["on", "+client|setname"]);
        ok(&u, &["CLIENT", "SETNAME", "x"]);
        ok(&u, &["client", "setname", "x"]);
        denied(&u, &["CLIENT", "ID"]);
        denied(&u, &["CONFIG", "GET", "x"]);

        let u = user(&["on", "+client", "-client|kill"]);
        ok(&u, &["CLIENT", "ID"]);
        assert_eq!(
            check_name(&u, "CLIENT", Some("kill")),
            Err(Reply::err(
                "NOPERM User u has no permissions to run the 'client|kill' command"
            ))
        );
        assert!(check_name(&u, "client", Some("KILL")).is_err());

        let u = user(&["on", "+config|get"]);
        ok(&u, &["CONFIG", "GET", "x"]);
        let e = denied(&u, &["CONFIG", "SET", "a", "b"]);
        assert!(e.contains("'config'"), "{e}");

        // A subcommand rule never reaches other commands.
        let u = user(&["on", "-@all", "+config|get", "~*"]);
        denied(&u, &["GET", "get"]);

        let acl = Acl::new();
        for bad in ["+get|x", "+client|nope", "+client|", "+nope|get", "+|"] {
            let reply = setuser(&acl, "u", &[bad]);
            assert!(
                text(&reply).contains("Unknown command or category"),
                "{bad}: {reply:?}"
            );
        }
    }

    #[test]
    fn unknown_names_and_selectors() {
        let acl = Acl::new();
        assert_eq!(
            setuser(&acl, "u", &["+nope"]),
            Reply::err(
                "ERR Error in ACL SETUSER modifier '+nope': Unknown command or category name in ACL"
            )
        );
        assert!(text(&setuser(&acl, "u", &["-@nope"])).contains("Unknown command or category"));
        assert!(text(&setuser(&acl, "u", &["(+get ~k)"])).contains("Selectors"));
        assert!(text(&setuser(&acl, "u", &["clearselectors"])).contains("Selectors"));
        assert!(text(&setuser(&acl, "u", &["bogus"])).contains("Syntax error"));
        assert!(text(&setuser(&acl, "u", &["%X~k"])).contains("Syntax error"));
        assert!(text(&setuser(&acl, "u", &["%~k"])).contains("Syntax error"));
        assert!(text(&setuser(&acl, "u", &["%R"])).contains("Syntax error"));
        assert!(
            acl.user(b"u").is_none(),
            "no failed call may create the user"
        );
        // Categories Redis has and this server does not use are accepted.
        assert_eq!(setuser(&acl, "u", &["-@geo", "+@hyperloglog"]), Reply::Ok);
        // Echoed modifiers are bounded and escaped.
        let long = format!("+{}\r\n", "x".repeat(500));
        let reply = setuser(&acl, "u", &[&long]);
        let Reply::Error(e) = reply else { panic!() };
        assert!(
            e.len() < 400 && !e.contains('\r') && !e.contains('\n'),
            "{e}"
        );
    }

    #[test]
    fn key_patterns_with_multi_key_commands() {
        let u = user(&["on", "+@all", "~a*"]);
        let e = denied(&u, &["MSET", "a1", "1", "b1", "2"]);
        assert_eq!(e, "NOPERM No permissions to access the 'b1' key");
        ok(&u, &["MSET", "a1", "1", "a2", "2"]);
        denied(&u, &["RENAME", "a1", "b1"]);
        denied(&u, &["RENAME", "b1", "a1"]);
        ok(&u, &["RENAME", "a1", "a2"]);
        denied(&u, &["COPY", "a1", "b1"]);
        denied(&u, &["EVAL", "x", "1", "b1"]);
        ok(&u, &["EVAL", "x", "1", "a1", "b1"]);
        denied(&u, &["EVALSHA", "x", "2", "a1", "b1"]);
        denied(&u, &["EVAL_RO", "x", "1", "b1"]);
        denied(
            &u,
            &["XREAD", "COUNT", "1", "STREAMS", "a1", "b1", "0", "0"],
        );
        ok(&u, &["XREAD", "STREAMS", "a1", "a2", "0", "0"]);
        denied(&u, &["XREAD", "BLOCK", "0", "STREAMS", "b1", "0"]);
        denied(&u, &["BLPOP", "a1", "b1", "0"]);
        ok(&u, &["BLPOP", "a1", "a2", "0"]);
        denied(&u, &["ZUNIONSTORE", "a1", "1", "b1"]);
        denied(&u, &["ZUNIONSTORE", "b1", "1", "a1"]);
        ok(&u, &["ZUNIONSTORE", "a1", "1", "a2"]);
        denied(&u, &["SINTERSTORE", "b1", "a1"]);
        denied(&u, &["BITOP", "AND", "b1", "a1"]);
        denied(&u, &["LMOVE", "a1", "b1", "LEFT", "RIGHT"]);
        denied(&u, &["DEL", "a1", "b1"]);
        denied(&u, &["WATCH", "b1"]);
        denied(&u, &["GET", "b1"]);
        denied(&u, &["OBJECT", "ENCODING", "b1"]);
        denied(&u, &["XGROUP", "CREATE", "b1", "g", "$"]);
        // No key arguments: allowed by the command rules alone, as in Redis.
        ok(&u, &["KEYS", "*"]);
        ok(&u, &["PING"]);

        let none = user(&["on", "+@all"]);
        denied(&none, &["GET", "k"]);
        ok(&none, &["PING"]);
    }

    #[test]
    fn keys_and_channels_inside_wrappers() {
        let u = user(&["on", "+@all", "~a*", "&a*"]);
        // MOVE's key is not visited by `Cmd::for_each_key`.
        denied(&u, &["MOVE", "b1", "1"]);
        ok(&u, &["MOVE", "a1", "1"]);
        let wrap = |cmd: Cmd| Cmd::In(0, Box::new(cmd));
        let eval = Cmd::parse(args(&["EVAL", "x", "1", "b1"])).expect("eval");
        assert!(u.check(b"EVAL", None, &wrap(eval)).is_err());
        let publish = Cmd::Publish(b"b".to_vec(), b"m".to_vec());
        assert!(u.check(b"PUBLISH", None, &wrap(publish)).is_err());
        let ro = user(&["on", "+@all", "%R~a*"]);
        let eval = Cmd::parse(args(&["EVAL", "x", "1", "a1"])).expect("eval");
        assert!(ro.check(b"EVAL", None, &wrap(eval)).is_err());
        let queued = Cmd::ExecBlock {
            queue: vec![Cmd::Get(b"b1".to_vec())],
            watched: Vec::new(),
        };
        assert!(u.check(b"EXEC", None, &queued).is_err());
        // A queued write is judged as a write, whatever EXEC's own flags.
        let set = Cmd::parse(args(&["SET", "a1", "v"])).expect("set");
        let get = Cmd::parse(args(&["GET", "a1"])).expect("get");
        let queued = Cmd::ExecBlock {
            queue: vec![get.clone(), wrap(set)],
            watched: Vec::new(),
        };
        assert!(ro.check(b"EXEC", None, &queued).is_err());
        let queued = Cmd::ExecBlock {
            queue: vec![get],
            watched: Vec::new(),
        };
        assert_eq!(ro.check(b"EXEC", None, &queued), Ok(()));
    }

    #[test]
    fn read_and_write_key_patterns() {
        let u = user(&["on", "+@all", "%R~r*", "%W~w*", "~rw*"]);
        ok(&u, &["GET", "r1"]);
        ok(&u, &["EVAL_RO", "x", "1", "r1"]);
        denied(&u, &["SET", "r1", "v"]);
        denied(&u, &["EVAL", "x", "1", "r1"]);
        denied(&u, &["GETEX", "r1"]);
        denied(&u, &["GETEX", "r1", "PERSIST"]);
        // Write-only keys: denied even for writes (stricter than Redis).
        denied(&u, &["SET", "w1", "v"]);
        denied(&u, &["DEL", "w1"]);
        denied(&u, &["GET", "w1"]);
        ok(&u, &["SET", "rw1", "v"]);
        ok(&u, &["GET", "rw1"]);
        denied(&u, &["RENAME", "rw1", "r2"]);
        denied(&u, &["COPY", "r1", "rw2"]);

        // Read and write on one pattern through two modifiers.
        let u = user(&["on", "+@all", "%R~k", "%W~k"]);
        ok(&u, &["SET", "k", "v"]);
        let u = user(&["on", "+@all", "%WR~k"]);
        ok(&u, &["SET", "k", "v"]);
        let u = user(&["on", "+@all", "%rw~k"]);
        ok(&u, &["SET", "k", "v"]);
        assert_eq!(
            acl_get(&user(&["%R~a", "%W~b", "~c"]), "keys"),
            Reply::bulk("%R~a %W~b ~c")
        );
    }

    fn acl_get(u: &User, field: &str) -> Reply {
        let Reply::Map(items) = u.getuser_reply() else {
            panic!()
        };
        let at = items
            .iter()
            .position(|r| *r == Reply::bulk(field))
            .expect(field);
        items[at + 1].clone()
    }

    #[test]
    fn channels() {
        let u = user(&["on", "+@all", "~*", "&news.*"]);
        ok(&u, &["PUBLISH", "news.a", "m"]);
        let e = denied(&u, &["PUBLISH", "other", "m"]);
        assert_eq!(e, "NOPERM No permissions to access the 'other' channel");
        ok(&u, &["SUBSCRIBE", "news.a", "news.b"]);
        denied(&u, &["SUBSCRIBE", "news.a", "other"]);
        ok(&u, &["PSUBSCRIBE", "news.*"]);
        denied(&u, &["PSUBSCRIBE", "news.a*"]);
        denied(&u, &["PSUBSCRIBE", "*"]);
        denied(&u, &["PSUBSCRIBE", "news.*", "x"]);
        ok(&u, &["UNSUBSCRIBE", "other"]);
        ok(&u, &["PUBSUB", "CHANNELS"]);

        let all = user(&["on", "+@all", "~*", "allchannels"]);
        ok(&all, &["PSUBSCRIBE", "anything*"]);
        let star = user(&["on", "+@all", "~*", "&*"]);
        ok(&star, &["PSUBSCRIBE", "x*"]);
        assert!(star.unrestricted());

        let acl = Acl::new();
        assert!(text(&setuser(&acl, "u", &["&*", "&x"])).contains("allchannels"));
        assert!(text(&setuser(&acl, "u", &["allkeys", "~x"])).contains("allkeys"));
        assert!(text(&setuser(&acl, "u", &["~*", "%R~x"])).contains("allkeys"));
        assert_eq!(setuser(&acl, "u", &["~*", "resetkeys", "~x"]), Reply::Ok);
    }

    #[test]
    fn all_keys_but_no_channels() {
        let u = user(&["on", "~*", "resetchannels", "+@all"]);
        assert!(!u.unrestricted());
        ok(&u, &["GET", "k"]);
        denied(&u, &["PUBLISH", "c", "m"]);
        denied(&u, &["SUBSCRIBE", "c"]);
        denied(&u, &["PSUBSCRIBE", "*"]);
    }

    #[test]
    fn setuser_is_all_or_nothing() {
        let acl = Acl::new();
        assert_eq!(
            setuser(&acl, "alice", &["on", ">p", "+@all", "~*", "&*"]),
            Reply::Ok
        );
        let before = acl.generation();
        let reply = setuser(&acl, "alice", &["off", "resetpass", "-@all", "+nope"]);
        assert!(reply.is_error());
        assert_eq!(acl.generation(), before);
        let alice = acl.user(b"alice").expect("alice");
        assert!(alice.enabled() && alice.unrestricted());
        assert!(acl.auth(b"alice", b"p").is_ok());

        assert!(setuser(&acl, "bob", &["on", "+@all", "nope"]).is_error());
        assert!(acl.user(b"bob").is_none());
        assert_eq!(acl.generation(), before);
    }

    #[test]
    fn reset_clears_everything() {
        let acl = Acl::new();
        assert_eq!(
            setuser(&acl, "u", &["on", ">p", "~*", "&*", "+@all", "reset"]),
            Reply::Ok
        );
        let u = acl.user(b"u").expect("u");
        assert!(!u.enabled() && !u.nopass && u.passwords.is_empty());
        assert!(!u.allkeys && u.keys.is_empty() && !u.allchannels && u.channels.is_empty());
        assert!(u.rules.is_empty() && !u.unrestricted());
        assert!(acl.auth(b"u", b"p").is_err());
        assert_eq!(acl_get(&u, "commands"), Reply::bulk("-@all"));
        assert_eq!(
            acl_cmd(&acl, &["LIST"]),
            Reply::Array(vec![
                Reply::bulk("user default on nopass ~* &* +@all"),
                Reply::bulk("user u off resetchannels -@all"),
            ])
        );
        // reset then rules after it.
        assert_eq!(
            setuser(&acl, "u", &["reset", "on", "nopass", "+get", "~k"]),
            Reply::Ok
        );
        let u = acl.user(b"u").expect("u");
        ok(&u, &["GET", "k"]);
        denied(&u, &["GET", "j"]);
    }

    #[test]
    fn hashes_never_plaintext() {
        let acl = Acl::new();
        assert_eq!(
            setuser(&acl, "alice", &["on", ">secretpw", "+@all", "~*"]),
            Reply::Ok
        );
        let hash = hex(&sha256(b"secretpw"));
        let list = text(&acl_cmd(&acl, &["LIST"]));
        let get = text(&acl_cmd(&acl, &["GETUSER", "alice"]));
        for out in [&list, &get] {
            assert!(!out.contains("secretpw"), "{out}");
        }
        assert!(list.contains(&format!("#{hash}")), "{list}");
        assert!(get.contains(&hash), "{get}");
        let alice = acl.user(b"alice").expect("alice");
        let debug = format!("{alice:?} {acl:?}");
        assert!(
            !debug.contains("secretpw") && !debug.contains(&hash),
            "{debug}"
        );
        for rule in [
            "<secretpw2",
            "!0123456789",
            &format!("#{hash}x"),
            ">ok\u{0}",
        ] {
            let reply = setuser(&acl, "alice", &[rule, "+nope"]);
            let shown = text(&reply);
            assert!(reply.is_error());
            assert!(
                !shown.contains("secretpw") && !shown.contains("0123456789"),
                "{shown}"
            );
            assert!(!shown.contains(&hash), "{shown}");
        }
        assert_eq!(
            acl_get(&alice, "passwords"),
            Reply::Array(vec![Reply::bulk(hash.clone())])
        );
        assert_eq!(
            acl_get(&alice, "flags"),
            Reply::Array(vec![Reply::bulk("on")])
        );
        assert_eq!(acl_get(&alice, "selectors"), Reply::Array(Vec::new()));
    }

    #[test]
    fn deluser() {
        let acl = Acl::new();
        assert_eq!(
            acl_cmd(&acl, &["DELUSER", "default"]),
            Reply::err("ERR The 'default' user cannot be removed")
        );
        assert_eq!(setuser(&acl, "alice", &[]), Reply::Ok);
        assert_eq!(
            acl_cmd(&acl, &["DELUSER", "alice", "default"]),
            Reply::err("ERR The 'default' user cannot be removed")
        );
        assert!(
            acl.user(b"alice").is_some(),
            "the refused call removes nobody"
        );
        assert_eq!(
            acl_cmd(&acl, &["DELUSER", "alice", "nobody", "alice"]),
            Reply::Int(1)
        );
        assert!(acl.user(b"alice").is_none());
        assert!(acl.auth(b"alice", b"").is_err());
        assert!(acl.user(b"default").is_some());
    }

    #[test]
    fn generation_moves_only_on_change() {
        let acl = Acl::new();
        let g = acl.generation();
        let me = acl.default_user();
        for parts in [
            &["LIST"][..],
            &["USERS"],
            &["WHOAMI"],
            &["GETUSER", "default"],
            &["CAT"],
            &["GENPASS"],
            &["LOG"],
            &["DRYRUN", "default", "GET", "k"],
            &["DELUSER", "nobody"],
            &["SETUSER", "bad name"],
            &["SETUSER", "u", "+nope"],
        ] {
            let _ = acl_cmd(&acl, parts);
        }
        let _ = acl.auth(b"default", b"x");
        let _ = me.check(b"GET", None, &Cmd::Get(b"k".to_vec()));
        assert_eq!(acl.generation(), g);
        let mut last = g;
        let mut changed = |acl: &Acl| {
            assert!(acl.generation() > last);
            last = acl.generation();
        };
        assert_eq!(setuser(&acl, "u", &[]), Reply::Ok);
        changed(&acl);
        assert_eq!(setuser(&acl, "u", &["on"]), Reply::Ok);
        changed(&acl);
        acl.set_requirepass(Some(b"p"));
        changed(&acl);
        acl.set_requirepass(None);
        changed(&acl);
        assert_eq!(acl_cmd(&acl, &["DELUSER", "u"]), Reply::Int(1));
        changed(&acl);
        assert_eq!(acl_cmd(&acl, &["DELUSER", "u"]), Reply::Int(0));
        assert_eq!(acl.generation(), last);
    }

    #[test]
    fn name_limits() {
        let acl = Acl::new();
        assert_eq!(setuser(&acl, &"n".repeat(256), &[]), Reply::Ok);
        assert!(setuser(&acl, &"n".repeat(257), &[]).is_error());
        for bad in ["", "a b", "a\nb", "a\tb", "a\u{7f}", "a\u{0}b"] {
            assert!(setuser(&acl, bad, &[]).is_error(), "{bad:?}");
        }
        assert_eq!(
            acl_cmd(&acl, &["USERS"]),
            Reply::Array(vec![Reply::bulk("default"), Reply::bulk("n".repeat(256))])
        );
    }

    #[test]
    fn rule_limit() {
        let acl = Acl::new();
        let rules: Vec<String> = (0..MAX_RULES).map(|i| format!("~k{i}")).collect();
        let refs: Vec<&str> = rules.iter().map(String::as_str).collect();
        assert_eq!(setuser(&acl, "u", &refs), Reply::Ok);
        let reply = setuser(&acl, "u", &[">one-more"]);
        assert!(text(&reply).contains("Too many rules"), "{reply:?}");
        assert!(setuser(&acl, "u", &["+get"]).is_error());
        let more: Vec<String> = (0..2 * MAX_RULES).map(|i| format!("&c{i}")).collect();
        let refs: Vec<&str> = more.iter().map(String::as_str).collect();
        assert!(setuser(&acl, "v", &refs).is_error());
        assert!(acl.user(b"v").is_none());
        // Repeating a rule that resets does not count up.
        let resets = vec!["+@all"; 5000];
        assert_eq!(setuser(&acl, "w", &resets), Reply::Ok);
        let long = format!("~{}", "x".repeat(MAX_PATTERN + 1));
        assert!(text(&setuser(&acl, "w", &[&long])).contains("Pattern too long"));
    }

    #[test]
    fn user_limit() {
        let acl = Acl::new();
        for i in 1..MAX_USERS {
            assert_eq!(setuser(&acl, &format!("u{i}"), &[]), Reply::Ok);
        }
        let reply = setuser(&acl, "one-too-many", &[]);
        assert!(text(&reply).contains("Too many ACL users"), "{reply:?}");
        // Existing users can still change; deleting one makes room.
        assert_eq!(setuser(&acl, "u1", &["on"]), Reply::Ok);
        assert_eq!(acl_cmd(&acl, &["DELUSER", "u1"]), Reply::Int(1));
        assert_eq!(setuser(&acl, "one-too-many", &[]), Reply::Ok);
    }

    #[test]
    fn any_restriction_clears_unrestricted() {
        let full = ["on", "+@all", "~*", "&*"];
        assert!(user(&full).unrestricted());
        assert!(user(&["on", "allcommands", "allkeys", "allchannels"]).unrestricted());
        assert!(user(&["on", "+@all", "+get", "~*", "&*"]).unrestricted());
        for extra in [
            "off",
            "-get",
            "-@dangerous",
            "-client|kill",
            "resetkeys",
            "resetchannels",
        ] {
            let mut rules = full.to_vec();
            rules.push(extra);
            assert!(!user(&rules).unrestricted(), "{extra}");
        }
        assert!(!user(&["on", "+@all", "~a*", "&*"]).unrestricted());
        assert!(!user(&["on", "+@all", "%RW~*", "&*"]).unrestricted());
        assert!(!user(&["on", "+@all", "~*", "&a"]).unrestricted());
        assert!(!user(&["on", "-@all", "+@all", "-get", "~*", "&*"]).unrestricted());
        assert!(!user(&["on", "+@read", "~*", "&*"]).unrestricted());
        assert!(!user(&["on", "~*", "&*"]).unrestricted());
    }

    #[test]
    fn commands_missing_from_the_table() {
        let u = user(&["on", "+@all", "~*", "&*", "-get"]);
        let e = check_name(&u, "NOPE", None);
        assert_eq!(
            e,
            Err(Reply::err(
                "NOPERM User u has no permissions to run the 'nope' command"
            ))
        );
        let weird = check_name(&u, "a\r\nb", None);
        assert!(!format!("{weird:?}").contains('\r'));
        assert_eq!(
            check_name(&user(&["on", "+@all", "~*", "&*"]), "NOPE", None),
            Ok(())
        );
        // AUTH and friends need no permission; ACL does.
        let none = user(&["on"]);
        assert_eq!(check_name(&none, "AUTH", None), Ok(()));
        assert_eq!(check_name(&none, "hello", None), Ok(()));
        assert!(check_name(&none, "ACL", Some("whoami")).is_err());
        let acl_only = user(&["on", "+acl|whoami"]);
        assert_eq!(check_name(&acl_only, "ACL", Some("WHOAMI")), Ok(()));
        assert!(check_name(&acl_only, "ACL", Some("SETUSER")).is_err());
    }

    #[test]
    fn long_and_odd_keys_are_shown_safely() {
        let u = user(&["on", "+@all", "~a"]);
        let key = format!("{}\r\n\u{1}'z", "k".repeat(300));
        let e = denied(&u, &["GET", &key]);
        assert!(
            !e.contains('\r') && !e.contains('\n') && !e.contains('\u{1}'),
            "{e}"
        );
        assert!(e.len() < 200, "{}", e.len());
        let raw = Cmd::Get(vec![0xff, b'\\', 0]);
        let Err(Reply::Error(e)) = u.check(b"GET", None, &raw) else {
            panic!()
        };
        assert_eq!(
            e,
            "NOPERM No permissions to access the '\\xff\\x5c\\x00' key"
        );
    }

    #[test]
    fn other_subcommands() {
        let acl = Acl::new();
        let Reply::Array(cats) = acl_cmd(&acl, &["CAT"]) else {
            panic!()
        };
        assert!(cats.contains(&Reply::bulk("dangerous")));
        let Reply::Array(names) = acl_cmd(&acl, &["CAT", "Dangerous"]) else {
            panic!()
        };
        assert!(names.contains(&Reply::bulk("flushall")));
        assert!(!names.contains(&Reply::bulk("get")));
        assert_eq!(
            acl_cmd(&acl, &["CAT", "nope"]),
            Reply::err("ERR Unknown category 'nope'")
        );
        assert!(acl_cmd(&acl, &["CAT", "all"]).is_error());

        let Reply::Bulk(p1) = acl_cmd(&acl, &["GENPASS"]) else {
            panic!()
        };
        let Reply::Bulk(p2) = acl_cmd(&acl, &["GENPASS"]) else {
            panic!()
        };
        assert_eq!(p1.len(), 64);
        assert!(p1.iter().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(p1, p2);
        for (bits, len) in [("1", 1), ("5", 2), ("8", 2), ("4096", 1024)] {
            let Reply::Bulk(p) = acl_cmd(&acl, &["GENPASS", bits]) else {
                panic!()
            };
            assert_eq!(p.len(), len, "{bits}");
        }
        for bad in ["0", "-1", "4097", "x"] {
            assert!(acl_cmd(&acl, &["GENPASS", bad]).is_error(), "{bad}");
        }

        assert_eq!(acl_cmd(&acl, &["WHOAMI"]), Reply::bulk("default"));
        assert_eq!(acl_cmd(&acl, &["LOG"]), Reply::Array(Vec::new()));
        assert_eq!(acl_cmd(&acl, &["LOG", "10"]), Reply::Array(Vec::new()));
        assert_eq!(acl_cmd(&acl, &["log", "reset"]), Reply::Ok);
        assert_eq!(acl_cmd(&acl, &["SAVE"]), Reply::err(NO_ACL_FILE));
        assert_eq!(acl_cmd(&acl, &["LOAD"]), Reply::err(NO_ACL_FILE));
        assert!(matches!(acl_cmd(&acl, &["HELP"]), Reply::Array(_)));
        assert_eq!(acl_cmd(&acl, &["GETUSER", "nobody"]), Reply::Nil);

        assert_eq!(
            acl_cmd(&acl, &["nope"]),
            Reply::err("ERR unknown subcommand 'nope'. Try ACL HELP.")
        );
        assert_eq!(
            acl.command(&args(&["ACL"]), &acl.default_user()),
            Reply::err("ERR wrong number of arguments for 'acl' command")
        );
        for (parts, sub) in [
            (&["SETUSER"][..], "setuser"),
            (&["GETUSER"], "getuser"),
            (&["GETUSER", "a", "b"], "getuser"),
            (&["DELUSER"], "deluser"),
            (&["LIST", "x"], "list"),
            (&["USERS", "x"], "users"),
            (&["WHOAMI", "x"], "whoami"),
            (&["CAT", "a", "b"], "cat"),
            (&["GENPASS", "1", "2"], "genpass"),
            (&["LOG", "1", "2"], "log"),
            (&["SAVE", "x"], "save"),
            (&["DRYRUN", "default"], "dryrun"),
            (&["HELP", "x"], "help"),
        ] {
            assert_eq!(
                acl_cmd(&acl, parts),
                Reply::Error(format!(
                    "ERR wrong number of arguments for 'acl|{sub}' command"
                )),
                "{parts:?}"
            );
        }
    }

    #[test]
    fn whoami_is_the_caller() {
        let acl = Acl::new();
        assert_eq!(
            setuser(&acl, "alice", &["on", "nopass", "+@all"]),
            Reply::Ok
        );
        let alice = acl.user(b"alice").expect("alice");
        assert_eq!(
            acl.command(&args(&["ACL", "WHOAMI"]), &alice),
            Reply::bulk("alice")
        );
    }

    #[test]
    fn dry_run() {
        let acl = Acl::new();
        assert_eq!(setuser(&acl, "u", &["on", "+get", "~a*"]), Reply::Ok);
        assert_eq!(acl_cmd(&acl, &["DRYRUN", "u", "GET", "a1"]), Reply::Ok);
        assert_eq!(
            acl_cmd(&acl, &["DRYRUN", "u", "GET", "b1"]),
            Reply::bulk("NOPERM No permissions to access the 'b1' key")
        );
        assert_eq!(
            acl_cmd(&acl, &["DRYRUN", "u", "SET", "a1", "v"]),
            Reply::bulk("NOPERM User u has no permissions to run the 'set' command")
        );
        assert_eq!(
            acl_cmd(&acl, &["DRYRUN", "nobody", "GET", "a"]),
            Reply::err("ERR User 'nobody' not found")
        );
        assert_eq!(
            acl_cmd(&acl, &["DRYRUN", "u", "NOPE", "a"]),
            Reply::err("ERR Command 'NOPE' not found")
        );
        assert!(acl_cmd(&acl, &["DRYRUN", "u", "GET"]).is_error());
    }
}
