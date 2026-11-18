//! Whole RDB files, as Redis 7.2 (RDB 11) and Valkey 9 (RDB 80) write
//! them: every database out (`Engine::save_rdb`), a file in
//! (`Engine::load_rdb`).
//!
//! A file is `magic, aux fields, libraries, then for each database
//! SELECTDB, RESIZEDB and its keys, EOF, crc64 of everything before`. A
//! key is `[EXPIRETIME_MS deadline] type, name, value`, the value as a DUMP
//! payload holds it (`shard/dump.rs`).
//!
//! Both sides hold the exclusive side of the gate for the whole file: no
//! command runs meanwhile, so a file is one moment of the node. Saving
//! builds the file in memory; the caller writes it out after.
//!
//! A file is untrusted: it is read as it streams in, every length is
//! checked before it is used (`rdb::Input`), and a file that fails half way
//! leaves no key behind.

use crate::cmd::Cmd;
use crate::engine::{Engine, Exclusive, COMPAT_VERSION};
use crate::rdb::{self, Input};
use crate::resp::Reply;
use crate::shard::dump::read_entry;
use parking_lot::Mutex;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;

const OPCODE_SLOT_IMPORT: u8 = 243;
const OPCODE_SLOT_INFO: u8 = 244;
const OPCODE_FUNCTION2: u8 = 245;
const OPCODE_MODULE_AUX: u8 = 247;
const OPCODE_IDLE: u8 = 248;
const OPCODE_FREQ: u8 = 249;
const OPCODE_AUX: u8 = 250;
const OPCODE_RESIZEDB: u8 = 251;
pub(crate) const OPCODE_EXPIRETIME_MS: u8 = 252;
const OPCODE_EXPIRETIME: u8 = 253;
const OPCODE_SELECTDB: u8 = 254;
const OPCODE_EOF: u8 = 255;

const MAGIC_BYTES: usize = 9;
/// Type bytes that mean something else to the Redis forks behind RDB 12
/// to 79; Valkey refuses them in a file with the REDIS magic.
const FOREIGN_TYPES: std::ops::RangeInclusive<u8> = 22..=243;
/// Files before RDB 5 carry no checksum.
const FIRST_CHECKSUM_VERSION: u16 = 5;
/// A file read without `rdbchecksum` names this checksum.
const NO_CHECKSUM: u64 = 0;
/// Bytes read at a time for a string whose length the file only claims.
const CHUNK: usize = 1 << 20;

/// The reply to SAVE or BGSAVE while the RDB file is being written.
pub(crate) const SAVING: &str = "ERR Background save already in progress";

/// What a file held.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RdbStats {
    /// The RDB version of the file.
    pub version: u16,
    /// Keys stored.
    pub keys: u64,
    /// Keys left out because their deadline had passed.
    pub expired: u64,
    /// Libraries of functions.
    pub libraries: u64,
    /// The database the replication stream is in where the file leaves
    /// it (`repl-stream-db`), in a file made for a replica.
    pub stream_db: Option<u16>,
}

/// The writes of the RDB file, shared with the thread that does one: it
/// may outlive the command that started it.
#[derive(Default)]
pub(crate) struct RdbSaves {
    /// One is under way; only one may be at a time.
    saving: AtomicBool,
    /// Why the last one failed; `None` after a success.
    error: Mutex<Option<String>>,
}

impl RdbSaves {
    pub(crate) fn in_progress(&self) -> bool {
        self.saving.load(SeqCst)
    }

    pub(crate) fn failed(&self) -> bool {
        self.error.lock().is_some()
    }
}

/// The claim `rdb_save_begin` took, given back when dropped, however the
/// write ends, a panic included.
struct Claim {
    saves: Arc<RdbSaves>,
    last_save: Arc<AtomicU64>,
}

impl Claim {
    /// Write the file and keep how it went, for INFO and LASTSAVE.
    fn write(&self, path: &Path, bytes: &[u8]) -> Result<(), String> {
        let result =
            write_whole(path, bytes).map_err(|e| format!("writing {} failed: {e}", path.display()));
        if result.is_ok() {
            self.last_save
                .store(crate::clock::unix_ms() / 1000, Relaxed);
        }
        *self.saves.error.lock() = result.as_ref().err().cloned();
        result
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.saves.saving.store(false, SeqCst);
    }
}

fn magic(version: u16) -> String {
    // Valkey's `rdbUseValkeyMagic`: versions past the forks' range.
    if version > 79 {
        format!("VALKEY{version:03}")
    } else {
        format!("REDIS{version:04}")
    }
}

fn put_aux(out: &mut Vec<u8>, name: &str, value: &[u8]) {
    out.push(OPCODE_AUX);
    rdb::put_raw(out, name.as_bytes());
    rdb::put_string(out, value);
}

impl Engine {
    /// Every database as an RDB file: RDB 11, which Redis 7.2 and every
    /// Valkey read, unless a hash has fields with deadlines, which only
    /// RDB 80 (Valkey 9) holds. Takes the exclusive side of the gate: the
    /// caller must not be inside the shared side.
    pub fn save_rdb(&self) -> Vec<u8> {
        let _excl = self.exclusive();
        rdb_bytes(self, &[])
    }

    /// The RDB file SAVE and BGSAVE write (`Config::rdb_file`).
    pub fn rdb_file(&self) -> Option<&Path> {
        self.config().rdb_file.as_deref()
    }

    /// CONFIG GET dir: where the RDB file goes, else the working directory.
    pub(crate) fn rdb_dir(&self) -> String {
        let dir = match self.rdb_file().and_then(Path::parent) {
            Some(dir) if !dir.as_os_str().is_empty() => std::path::absolute(dir),
            _ => std::env::current_dir(),
        };
        dir.map(|d| d.display().to_string()).unwrap_or_default()
    }

    /// CONFIG GET dbfilename: empty without an RDB file.
    pub(crate) fn rdb_name(&self) -> String {
        let name = self.rdb_file().and_then(Path::file_name);
        name.map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    /// Claim the one write of the RDB file that may run at a time. `false`
    /// while one is under way.
    pub fn rdb_save_begin(&self) -> bool {
        !self.rdb_saves().saving.swap(true, SeqCst)
    }

    /// Give back a claim of `rdb_save_begin` that will not be used.
    pub fn rdb_save_cancel(&self) {
        self.rdb_saves().saving.store(false, SeqCst);
    }

    /// The claim `rdb_save_begin` took, as a value that gives it back.
    fn claim(&self) -> Claim {
        Claim {
            saves: self.rdb_saves().clone(),
            last_save: self.last_save_clock().clone(),
        }
    }

    /// After `rdb_save_begin`: write the RDB file, and give the claim back.
    /// The caller must not be inside the shared side of the gate. Sleeps.
    /// How it went is also kept for INFO.
    pub fn rdb_save_finish(&self) -> Result<(), String> {
        let claim = self.claim();
        let Some(path) = self.rdb_file() else {
            return Err("no RDB file is configured".into());
        };
        claim.write(path, &self.save_rdb())
    }

    /// BGSAVE in a transaction, after `rdb_save_begin`: the file is taken
    /// now, under the exclusive side the caller holds, and a thread of its
    /// own writes it, so the gate is not held while the disk works.
    pub(crate) fn rdb_save_held(&self) {
        let claim = self.claim();
        let Some(path) = self.rdb_file().map(Path::to_path_buf) else {
            return;
        };
        let bytes = rdb_bytes(self, &[]);
        let spawned = std::thread::Builder::new()
            .name("hotstash-rdb".into())
            .spawn(move || {
                // How it went is kept for INFO, as Redis keeps it.
                let _ = claim.write(&path, &bytes);
            });
        if let Err(e) = spawned {
            *self.rdb_saves().error.lock() =
                Some(format!("cannot start writing the RDB file: {e}"));
        }
    }

    /// SAVE, after `rdb_save_begin`: the RDB file, then the data file.
    /// Sleeps.
    pub fn rdb_save_reply(&self) -> Reply {
        if let Err(e) = self.rdb_save_finish() {
            return Reply::Error(format!("ERR {e}"));
        }
        match self.sync_now() {
            Some(Err(e)) => Reply::Error(format!("ERR writing the data file failed: {e}")),
            _ => Reply::Ok,
        }
    }

    /// Load an RDB file of Redis up to 7.2 (RDB 11) or Valkey 9 (RDB 80)
    /// into a node that holds no key. Keys whose deadline has passed are
    /// left out. On any error every key is gone again and nothing of the
    /// file stays. Takes the exclusive side of the gate: the caller must
    /// not be inside the shared side.
    pub fn load_rdb(&self, input: impl Read) -> Result<RdbStats, String> {
        let mut excl = self.exclusive();
        if !self.is_empty() {
            return Err("the database is not empty".into());
        }
        self.load_rdb_held(&mut excl, input)
    }

    /// `load_rdb` into an empty node whose exclusive side `excl` is.
    pub(crate) fn load_rdb_held(
        &self,
        excl: &mut Exclusive<'_>,
        input: impl Read,
    ) -> Result<RdbStats, String> {
        let mut file = FileInput::new(io::BufReader::with_capacity(CHUNK, input));
        let mut libraries = Vec::new();
        let mut loaded = Vec::new();
        let result = read_file(self, &mut file, &mut libraries);
        let result = result.and_then(|mut stats| {
            for code in &libraries {
                loaded.push(self.rdb_library(code)?);
            }
            stats.libraries = libraries.len() as u64;
            Ok(stats)
        });
        if result.is_err() {
            excl.run(Cmd::FlushAll(Some(false)));
            for name in &loaded {
                self.rdb_unload_library(name);
            }
        }
        result.map_err(|e| match file.error.take() {
            Some(io) => format!("reading the RDB file failed: {io}"),
            None => format!("bad RDB file: {e}"),
        })
    }
}

/// The file `save_rdb` returns; the caller holds the exclusive side.
pub(crate) fn rdb_bytes(engine: &Engine, aux: &[(&str, String)]) -> Vec<u8> {
    let mut libraries: Vec<_> = engine.library_sources().into_iter().collect();
    libraries.sort();
    let mut body = Vec::new();
    for (_, code) in &libraries {
        body.push(OPCODE_FUNCTION2);
        rdb::put_raw(&mut body, code);
    }
    let mut version = rdb::VERSION_11;
    let (mut db_keys, mut db_expires) = (0, 0);
    let mut keys = Vec::new();
    let mut current = 0;
    let flush = |body: &mut Vec<u8>, keys: &mut Vec<u8>, db, n, expires| {
        if n > 0 {
            body.push(OPCODE_SELECTDB);
            rdb::put_len(body, db);
            body.push(OPCODE_RESIZEDB);
            rdb::put_len(body, n);
            rdb::put_len(body, expires);
            body.append(keys);
        }
    };
    engine.rdb_each_shard(|db, shard| {
        if db != current {
            flush(&mut body, &mut keys, current as u64, db_keys, db_expires);
            (current, db_keys, db_expires) = (db, 0, 0);
        }
        let counts = shard.write_rdb(&mut keys);
        db_keys += counts.keys;
        db_expires += counts.expires;
        version = version.max(counts.version);
    });
    flush(&mut body, &mut keys, current as u64, db_keys, db_expires);
    body.push(OPCODE_EOF);

    let mut file = magic(version).into_bytes();
    let ver_name = if version > 79 {
        "valkey-ver"
    } else {
        "redis-ver"
    };
    put_aux(&mut file, ver_name, COMPAT_VERSION.as_bytes());
    put_aux(&mut file, "redis-bits", b"64");
    let ctime = crate::clock::unix_ms() / 1000;
    put_aux(&mut file, "ctime", ctime.to_string().as_bytes());
    put_aux(
        &mut file,
        "used-mem",
        crate::mem::used().to_string().as_bytes(),
    );
    put_aux(&mut file, "aof-base", b"0");
    for (name, value) in aux {
        put_aux(&mut file, name, value.as_bytes());
    }
    file.append(&mut body);
    let crc = rdb::crc64_update(0, &file);
    file.extend_from_slice(&crc.to_le_bytes());
    file
}

/// Write `bytes` beside `path` and move them over it, so that a crash
/// leaves the old file or the new one, never half of one. The file is the
/// owner's alone, as the data file is: it holds every key.
fn write_whole(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp = path.with_file_name(format!("temp-{}.rdb", std::process::id()));
    // Never write through whatever sits at that name: a link there would
    // send every key somewhere else.
    match std::fs::remove_file(&temp) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = options
        .open(&temp)
        .and_then(|mut f| f.write_all(bytes).and_then(|()| f.sync_all()))
        .and_then(|()| std::fs::rename(&temp, path));
    if result.is_err() {
        // What is left of a failed write is of no use to anyone.
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn read_file<R: Read>(
    engine: &Engine,
    file: &mut FileInput<R>,
    libraries: &mut Vec<Vec<u8>>,
) -> Result<RdbStats, String> {
    let short = || "the file ends early".to_string();
    let head = file.take(MAGIC_BYTES).ok_or_else(short)?;
    let (redis, version) = match head.split_at(MAGIC_BYTES - 3) {
        (b"REDIS0", digits) => (true, parse_version(digits)?),
        (b"VALKEY", digits) => (false, parse_version(digits)?),
        _ => return Err("not an RDB file".into()),
    };
    let known = if redis {
        (1..=rdb::VERSION_11).contains(&version)
    } else {
        version == rdb::VERSION_80
    };
    if !known {
        return Err(format!("RDB version {version} is not supported"));
    }
    let mut stats = RdbStats {
        version,
        ..RdbStats::default()
    };
    let now = i64::try_from(engine.now_ms()).unwrap_or(i64::MAX);
    let mut db = 0;
    let mut deadline = None;
    loop {
        let t = file.u8().ok_or_else(short)?;
        if redis && FOREIGN_TYPES.contains(&t) {
            return Err(format!("type {t} belongs to a Redis fork after 7.2"));
        }
        match t {
            OPCODE_EOF => break,
            OPCODE_EXPIRETIME_MS => deadline = Some(file.millis().ok_or_else(short)?),
            OPCODE_EXPIRETIME => {
                let secs = i32::from_le_bytes(file.array().ok_or_else(short)?);
                deadline = Some(i64::from(secs) * 1000);
            }
            OPCODE_FREQ => {
                file.u8().ok_or_else(short)?;
            }
            OPCODE_IDLE => {
                file.len().ok_or_else(short)?;
            }
            OPCODE_SELECTDB => {
                let n = file.len().ok_or_else(short)?;
                db = usize::try_from(n)
                    .ok()
                    .filter(|&n| n < engine.databases())
                    .ok_or_else(|| format!("database {n} does not exist here"))?;
            }
            OPCODE_RESIZEDB => {
                file.len().ok_or_else(short)?;
                file.len().ok_or_else(short)?;
            }
            OPCODE_SLOT_INFO => {
                for _ in 0..3 {
                    file.len().ok_or_else(short)?;
                }
            }
            // A slot migration under way when the file was written: this
            // node has no other node to take slots from.
            OPCODE_SLOT_IMPORT => return Err("the file holds a slot migration".into()),
            OPCODE_AUX => {
                let name = file.string().ok_or_else(short)?;
                let value = file.string().ok_or_else(short)?;
                if name == b"repl-stream-db" {
                    stats.stream_db = std::str::from_utf8(&value)
                        .ok()
                        .and_then(|db| db.parse().ok());
                }
            }
            OPCODE_FUNCTION2 => libraries.push(file.string().ok_or_else(short)?),
            OPCODE_MODULE_AUX => return Err("the file needs a module".into()),
            t => {
                let key = file.string().ok_or_else(short)?;
                let mut entry = read_entry(file, t, version).ok_or_else(|| {
                    let name = String::from_utf8_lossy(&key);
                    format!("the value of key '{name}' (type {t}) is malformed or not supported")
                })?;
                match deadline.take() {
                    Some(at) if at <= now && !engine.is_replica() => {
                        stats.expired += 1;
                        continue;
                    }
                    Some(at) => entry.deadline = Some(at as u64),
                    None => {}
                }
                if !engine.rdb_put(db, &key, entry) {
                    let name = String::from_utf8_lossy(&key);
                    return Err(format!("key '{name}' is in the file twice"));
                }
                stats.keys += 1;
            }
        }
    }
    if version >= FIRST_CHECKSUM_VERSION {
        let crc = file.crc;
        let stored = u64::from_le_bytes(file.raw::<8>().ok_or_else(short)?);
        if stored != NO_CHECKSUM && stored != crc {
            return Err("wrong checksum".into());
        }
    }
    if file.u8().is_some() {
        return Err("bytes after the end".into());
    }
    Ok(stats)
}

fn parse_version(digits: &[u8]) -> Result<u16, String> {
    std::str::from_utf8(digits)
        .ok()
        .filter(|d| d.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|d| d.parse().ok())
        .ok_or_else(|| "not an RDB file".into())
}

/// A file as it streams in, its checksum kept on the way.
struct FileInput<R> {
    inner: R,
    buf: Vec<u8>,
    crc: u64,
    unpacked: usize,
    /// What the reader reported, when it was not just the end.
    error: Option<io::Error>,
}

impl<R: Read> FileInput<R> {
    fn new(inner: R) -> Self {
        FileInput {
            inner,
            buf: Vec::new(),
            crc: 0,
            // A file may hold values of any size; only a payload a client
            // sends is held to what its bytes back (`rdb::decode`).
            unpacked: usize::MAX,
            error: None,
        }
    }

    /// `n` bytes into `buf`, growing it only as they arrive.
    fn fill(&mut self, n: usize) -> bool {
        self.buf.clear();
        while self.buf.len() < n {
            let at = self.buf.len();
            let step = (n - at).min(CHUNK);
            self.buf.resize(at + step, 0);
            if let Err(e) = self.inner.read_exact(&mut self.buf[at..]) {
                if e.kind() != io::ErrorKind::UnexpectedEof {
                    self.error = Some(e);
                }
                return false;
            }
        }
        true
    }

    /// The next `N` bytes, left out of the checksum.
    fn raw<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.fill(N).then(|| self.buf[..].try_into().ok())?
    }
}

impl<R: Read> Input for FileInput<R> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if !self.fill(n) {
            return None;
        }
        self.crc = rdb::crc64_update(self.crc, &self.buf);
        Some(&self.buf)
    }

    fn left(&self) -> usize {
        // A stream cannot tell; reservations stay small (`Input::room`).
        usize::MAX
    }

    fn unpacked(&mut self) -> &mut usize {
        &mut self.unpacked
    }
}
