//! Durability: one append-only file of key states.
//!
//! Every shard remembers which keys changed (`Shard::touch`). A background
//! thread visits the shards every `Options::interval`, writes the current
//! state of those keys (or the fact that they are gone) as one frame, and
//! syncs the file. Recovery reads the frames in order; the last state of a
//! key wins. Nothing on the command path waits for the disk, so a crash can
//! lose what was written during the last interval, and nothing older.
//!
//! A frame is all-or-nothing: its header carries the payload's length and
//! checksum and a checksum of the length itself. One round is collected
//! inside the shared side of the execution gate, so a transaction or
//! script is either wholly in a frame or not at all.
//!
//! Recovery tells a crash from damage. A crash leaves an incomplete or
//! zero-filled end of file; that tail is cut off (and kept in
//! `<file>.torn`). Anything wrong with more data behind it is damage, and
//! the file is refused rather than cut.
//!
//! When the file has grown enough it is rewritten: a fresh file with every
//! live key, renamed over the old one.
//!
//! While the database is open it holds a lock on `<file>.lock`, so two
//! processes cannot write to the same file. The data file itself stays
//! readable, and a copy of it taken at any moment is a valid database.
//!
//! File: `MAGIC`, then frames of
//! `u64 length, u64 xxh3(payload), u64 xxh3(length), payload`.
//! Payload: records `WIPE` | `DB n` | `DEL key` | `PUT key deadline value`,
//! and for what is not a key: `LIBPUT code` | `LIBDEL name` |
//! `USERPUT name rules` | `USERDEL name`.
//!
//! The libraries of functions and the users are kept the same way as
//! keys: when one changes, its new state is written with the next round.

use crate::acl::User;
use crate::bloom::Bloom;
use crate::cmd::Bytes;
use crate::dict::{hash_bytes, Dict};
use crate::engine::{Engine, SyncDone};
use crate::shard::{Entry, FieldTtl, Value};
use crate::stream::Stream;
use crate::zset::ZSet;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use xxhash_rust::xxh3::xxh3_64;

const MAGIC: &[u8; 8] = b"HCACHE2\n";
const FRAME_HEADER: u64 = 24;
/// A rewrite closes a frame once it holds this much. A frame of changes
/// holds one whole round, however large, because a round is atomic.
const FRAME_TARGET: usize = 4 << 20;
/// Keys written per lock hold while rewriting the file.
const DUMP_CHUNK: usize = 1024;

const TAG_WIPE: u8 = 0;
const TAG_DEL: u8 = 1;
const TAG_PUT: u8 = 2;
/// The records after it, to the end of the frame, belong to this database.
/// A frame starts in database 0.
const TAG_DB: u8 = 3;
/// A library of functions, as its source; it replaces one of its name.
const TAG_LIB_PUT: u8 = 4;
const TAG_LIB_DEL: u8 = 5;
/// A user, as the SETUSER modifiers that make it from a blank one.
const TAG_USER_PUT: u8 = 6;
const TAG_USER_DEL: u8 = 7;

const T_STR: u8 = 1;
const T_HASH: u8 = 2;
const T_LIST: u8 = 3;
const T_SET: u8 = 4;
const T_ZSET: u8 = 5;
const T_STREAM: u8 = 6;
/// A hash whose fields carry deadlines (0: none).
const T_HASH_TTL: u8 = 7;
/// A bloom filter, as BF.LOAD takes it.
const T_BLOOM: u8 = 8;
/// A JSON document, as JSON text.
const T_JSON: u8 = 9;

/// When what was written is forced to the disk (`appendfsync`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Fsync {
    /// Before the command that made a change is answered: a server holds
    /// the reply back until the change is on disk, and so does a session.
    /// Commands that arrive together share one sync.
    Always = 0,
    /// With every write, once per `Options::interval` (`everysec`).
    Interval = 1,
    /// Never (`no`): the operating system decides. A crash of the process
    /// loses at most one interval; a crash of the machine may lose more.
    Never = 2,
}

impl Fsync {
    /// By the names Redis gives `appendfsync`.
    pub fn from_name(raw: &[u8]) -> Option<Fsync> {
        match raw.to_ascii_lowercase().as_slice() {
            b"always" => Some(Fsync::Always),
            b"everysec" => Some(Fsync::Interval),
            b"no" => Some(Fsync::Never),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Fsync::Always => "always",
            Fsync::Interval => "everysec",
            Fsync::Never => "no",
        }
    }

    pub(crate) fn from_u8(raw: u8) -> Fsync {
        match raw {
            0 => Fsync::Always,
            2 => Fsync::Never,
            _ => Fsync::Interval,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    /// Time between two writes to disk: the most a crash can lose, unless
    /// `fsync` says otherwise.
    pub interval: Duration,
    pub fsync: Fsync,
    /// The file is rewritten when it is this many times larger than it
    /// was after the previous rewrite, and at least `rewrite_min_bytes`.
    pub rewrite_factor: u64,
    pub rewrite_min_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            interval: Duration::from_millis(100),
            fsync: Fsync::Interval,
            rewrite_factor: 2,
            rewrite_min_bytes: 16 << 20,
        }
    }
}

/// What recovery found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadReport {
    pub frames: u64,
    pub records: u64,
    /// Bytes of a torn tail that were cut off.
    pub dropped_bytes: u64,
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn put_len(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&(n as u32).to_le_bytes());
}

pub(crate) fn encode_wipe(out: &mut Vec<u8>) {
    out.push(TAG_WIPE);
}

pub(crate) fn encode_db(out: &mut Vec<u8>, db: usize) {
    out.push(TAG_DB);
    put_len(out, db);
}

pub(crate) fn encode_del(out: &mut Vec<u8>, key: &[u8]) {
    out.push(TAG_DEL);
    put_bytes(out, key);
}

pub(crate) fn encode_put(
    out: &mut Vec<u8>,
    key: &[u8],
    value: &Value,
    deadline: Option<u64>,
    fields: Option<&FieldTtl>,
) {
    out.push(TAG_PUT);
    put_bytes(out, key);
    out.extend_from_slice(&deadline.unwrap_or(0).to_le_bytes());
    match value {
        Value::Str(v) => {
            out.push(T_STR);
            put_bytes(out, v);
        }
        Value::Hash(hash) if fields.is_some_and(|ttl| !ttl.is_empty()) => {
            out.push(T_HASH_TTL);
            put_len(out, hash.len());
            hash.for_each(|f, v| {
                put_bytes(out, f);
                put_bytes(out, v);
                let at = fields.and_then(|ttl| ttl.get(f)).unwrap_or(0);
                out.extend_from_slice(&at.to_le_bytes());
            });
        }
        Value::Hash(hash) => {
            out.push(T_HASH);
            put_len(out, hash.len());
            hash.for_each(|f, v| {
                put_bytes(out, f);
                put_bytes(out, v);
            });
        }
        Value::List(list) => {
            out.push(T_LIST);
            put_len(out, list.len());
            list.iter().for_each(|v| put_bytes(out, v));
        }
        Value::Set(set) => {
            out.push(T_SET);
            put_len(out, set.len());
            set.for_each(|m, _| put_bytes(out, m));
        }
        Value::ZSet(zset) => {
            out.push(T_ZSET);
            put_len(out, zset.len());
            for (member, score) in zset.range(0, usize::MAX) {
                put_bytes(out, member);
                out.extend_from_slice(&score.to_bits().to_le_bytes());
            }
        }
        Value::Stream(stream) => {
            out.push(T_STREAM);
            stream.encode(out);
        }
        Value::Bloom(bloom) => {
            out.push(T_BLOOM);
            let mut blob = Vec::new();
            bloom.encode(&mut blob);
            put_bytes(out, &blob);
        }
        Value::Json(doc) => {
            out.push(T_JSON);
            put_bytes(out, &crate::json::to_json(doc));
        }
    }
}

/// What became of the libraries and the users between two states: first
/// what is gone or about to be replaced, then what is new. A library is
/// deleted before its replacement is written, because its functions may
/// have moved to another library in the same step.
fn encode_meta(
    out: &mut Vec<u8>,
    old_libs: &HashMap<String, Arc<[u8]>>,
    old_users: &HashMap<Bytes, Arc<User>>,
    libs: &HashMap<String, Arc<[u8]>>,
    users: &HashMap<Bytes, Arc<User>>,
) {
    let same_lib = |a: Option<&Arc<[u8]>>, b: &Arc<[u8]>| a.is_some_and(|a| Arc::ptr_eq(a, b));
    for (name, code) in old_libs {
        if !same_lib(libs.get(name), code) {
            out.push(TAG_LIB_DEL);
            put_bytes(out, name.as_bytes());
        }
    }
    for (name, code) in libs {
        if !same_lib(old_libs.get(name), code) {
            out.push(TAG_LIB_PUT);
            put_bytes(out, code);
        }
    }
    for name in old_users.keys() {
        if !users.contains_key(name) {
            out.push(TAG_USER_DEL);
            put_bytes(out, name);
        }
    }
    for (name, user) in users {
        if old_users
            .get(name)
            .is_some_and(|old| Arc::ptr_eq(old, user))
        {
            continue;
        }
        out.push(TAG_USER_PUT);
        put_bytes(out, name);
        let tokens = user.tokens();
        put_len(out, tokens.len());
        tokens.iter().for_each(|token| put_bytes(out, token));
    }
}

pub(crate) enum Record {
    Wipe,
    Db(usize),
    Del(Bytes),
    Put(Bytes, Entry),
    LibPut(Bytes),
    LibDel(Bytes),
    UserPut(Bytes, Vec<Bytes>),
    UserDel(Bytes),
}

/// Reads records out of one frame's payload. The payload passed its
/// checksum, but nothing here trusts it: every length is checked.
struct Decoder<'a>(&'a [u8]);

impl Decoder<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn len(&mut self) -> Option<usize> {
        let raw = self.take(4)?;
        Some(u32::from_le_bytes(raw.try_into().ok()?) as usize)
    }

    /// The size of a collection. An empty one is never stored: a key whose
    /// value is empty does not exist.
    fn count(&mut self) -> Option<usize> {
        self.len().filter(|&n| n > 0)
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn bytes(&mut self) -> Option<&[u8]> {
        let n = self.len()?;
        self.take(n)
    }

    fn record(&mut self) -> Option<Record> {
        match self.u8()? {
            TAG_WIPE => Some(Record::Wipe),
            TAG_DB => Some(Record::Db(self.len()?)),
            TAG_DEL => Some(Record::Del(self.bytes()?.to_vec())),
            TAG_LIB_PUT => Some(Record::LibPut(self.bytes()?.to_vec())),
            TAG_LIB_DEL => Some(Record::LibDel(self.bytes()?.to_vec())),
            TAG_USER_DEL => Some(Record::UserDel(self.bytes()?.to_vec())),
            TAG_USER_PUT => {
                let name = self.bytes()?.to_vec();
                let mut rules = Vec::new();
                for _ in 0..self.len()? {
                    rules.push(self.bytes()?.to_vec());
                }
                Some(Record::UserPut(name, rules))
            }
            TAG_PUT => {
                let key = self.bytes()?.to_vec();
                let deadline = Some(self.u64()?).filter(|&at| at != 0);
                let mut fields = None;
                let value = match self.u8()? {
                    T_HASH_TTL => {
                        let mut hash = Dict::new();
                        let mut ttl = FieldTtl::default();
                        for _ in 0..self.count()? {
                            let field = self.bytes()?.to_vec();
                            let value = self.bytes()?.to_vec();
                            let at = self.u64()?;
                            if at != 0 {
                                ttl.set(&field, at);
                            }
                            hash.insert(hash_bytes(&field), &field, value);
                        }
                        fields = Some(ttl);
                        Value::Hash(Box::new(hash))
                    }
                    T_STR => Value::Str(self.bytes()?.to_vec()),
                    T_HASH => {
                        let mut hash = Dict::new();
                        for _ in 0..self.count()? {
                            let field = self.bytes()?.to_vec();
                            let value = self.bytes()?.to_vec();
                            hash.insert(hash_bytes(&field), &field, value);
                        }
                        Value::Hash(Box::new(hash))
                    }
                    T_LIST => {
                        let mut list = VecDeque::new();
                        for _ in 0..self.count()? {
                            list.push_back(self.bytes()?.to_vec());
                        }
                        Value::List(list)
                    }
                    T_SET => {
                        let mut set = Dict::new();
                        for _ in 0..self.count()? {
                            let member = self.bytes()?;
                            set.insert(hash_bytes(member), member, ());
                        }
                        Value::Set(Box::new(set))
                    }
                    T_ZSET => {
                        let mut entries = Vec::new();
                        for _ in 0..self.count()? {
                            let member = self.bytes()?.to_vec();
                            let score = f64::from_bits(self.u64()?);
                            if score.is_nan() {
                                return None;
                            }
                            entries.push((member, score));
                        }
                        // As Valkey loads one: a listpack if it fits.
                        let mut zset = ZSet::default();
                        let longest = entries.iter().map(|(m, _)| m.len()).max().unwrap_or(0);
                        zset.expect(entries.len(), longest);
                        for (member, score) in &entries {
                            zset.insert(member, *score);
                        }
                        Value::ZSet(Box::new(zset))
                    }
                    T_STREAM => Value::Stream(Box::new(Stream::decode(&mut self.0)?)),
                    T_BLOOM => Value::Bloom(Box::new(Bloom::decode(self.bytes()?, None).ok()?)),
                    T_JSON => Value::Json(Box::new(crate::json::from_json(self.bytes()?)?)),
                    _ => return None,
                };
                Some(Record::Put(
                    key,
                    Entry {
                        value,
                        deadline,
                        fields,
                    },
                ))
            }
            _ => None,
        }
    }
}

/// `path` with `suffix` added to its file name.
fn beside(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// The path with its directory resolved, so that two spellings of one file
/// meet at the same lock.
fn resolve(path: &Path) -> io::Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "the data path names no file"))?;
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    Ok(dir.canonicalize()?.join(name))
}

/// How every file of the database is opened: readable and writable by its
/// owner only.
fn options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

/// Take the lock that keeps a second process off the data file at `path`.
/// It lasts as long as the returned handle.
fn lock(path: &Path) -> io::Result<File> {
    let file = options()
        .create(true)
        .truncate(false)
        .open(beside(path, ".lock"))?;
    file.try_lock().map_err(|e| {
        io::Error::other(format!(
            "data file {} is in use by another process or database handle ({e})",
            path.display()
        ))
    })?;
    Ok(file)
}

fn header(payload: &[u8]) -> [u8; FRAME_HEADER as usize] {
    let len = (payload.len() as u64).to_le_bytes();
    let mut header = [0u8; FRAME_HEADER as usize];
    header[..8].copy_from_slice(&len);
    header[8..16].copy_from_slice(&xxh3_64(payload).to_le_bytes());
    header[16..].copy_from_slice(&xxh3_64(&len).to_le_bytes());
    header
}

fn write_frame(file: &mut File, payload: &[u8]) -> io::Result<u64> {
    file.write_all(&header(payload))?;
    file.write_all(payload)?;
    Ok(FRAME_HEADER + payload.len() as u64)
}

fn damaged(what: &str, at: u64) -> io::Error {
    io::Error::new(
        ErrorKind::InvalidData,
        format!(
            "data file: {what} at byte {at}, with more data behind it. This is damage, not an \
             interrupted write, so the file was left untouched"
        ),
    )
}

/// Whether everything left in `reader` is zero.
fn rest_is_zero(reader: &mut impl Read) -> io::Result<bool> {
    let mut chunk = [0u8; 64 << 10];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(true),
            Ok(n) if chunk[..n].iter().any(|&b| b != 0) => return Ok(false),
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

/// Fill `buf` as far as the file goes; returns how much was read.
fn read_up_to(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// Read the frames of `file` into `engine`. Returns the report and the
/// offset just past the last good frame.
///
/// The end of the file may be the remains of a write that a crash
/// interrupted: a frame cut short, or one whose bytes were never written
/// and read back as zeros. That tail is dropped. A bad frame followed by
/// anything else is damage and an error.
fn load(file: &mut File, engine: &Engine) -> io::Result<(LoadReport, u64)> {
    let size = file.metadata()?.len();
    let mut report = LoadReport::default();
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(1 << 20, &mut *file);
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "not an HCache data file, or one of another version (unknown header)",
        ));
    }
    let mut good = MAGIC.len() as u64;
    let mut payload = Vec::new();
    loop {
        let mut head = [0u8; FRAME_HEADER as usize];
        let got = read_up_to(&mut reader, &mut head)?;
        if got < head.len() {
            // The file ends inside a header.
            break;
        }
        let len = u64::from_le_bytes(head[..8].try_into().unwrap_or_default());
        let sum = u64::from_le_bytes(head[8..16].try_into().unwrap_or_default());
        let len_sum = u64::from_le_bytes(head[16..].try_into().unwrap_or_default());
        if xxh3_64(&head[..8]) != len_sum {
            // A header that was only partly written has nothing but zeros
            // behind it.
            if rest_is_zero(&mut reader)? {
                break;
            }
            return Err(damaged("corrupt frame header", good));
        }
        // The length is now known to be what was written.
        if len > size.saturating_sub(good + FRAME_HEADER) {
            break;
        }
        payload.resize(len as usize, 0);
        if read_up_to(&mut reader, &mut payload)? < payload.len() {
            break;
        }
        if xxh3_64(&payload) != sum {
            if rest_is_zero(&mut reader)? {
                break;
            }
            return Err(damaged("corrupt frame", good));
        }
        // Decode the whole frame before applying any of it.
        let mut decoder = Decoder(&payload);
        let mut records = Vec::new();
        while !decoder.0.is_empty() {
            match decoder.record() {
                Some(record) => records.push(record),
                None => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        format!("data file: unreadable record in the frame at byte {good}"),
                    ))
                }
            }
        }
        let databases = engine.databases();
        if let Some(db) = records.iter().find_map(|r| match r {
            Record::Db(db) if *db >= databases => Some(*db),
            _ => None,
        }) {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("data file: it holds database {db}, but only {databases} are configured"),
            ));
        }
        report.records += records.len() as u64;
        let mut db = 0;
        for record in records {
            engine.load_record(record, &mut db).map_err(|why| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("data file: {why} (in the frame at byte {good})"),
                )
            })?;
        }
        report.frames += 1;
        good += FRAME_HEADER + len;
    }
    report.dropped_bytes = size.saturating_sub(good);
    Ok((report, good))
}

/// Keep the tail that is about to be cut off `file`, in case it was not
/// what it looked like. Zeros are not worth keeping.
fn keep_tail(file: &mut File, from: u64, path: &Path) -> io::Result<()> {
    file.seek(SeekFrom::Start(from))?;
    if rest_is_zero(&mut BufReader::new(&mut *file))? {
        return Ok(());
    }
    file.seek(SeekFrom::Start(from))?;
    let mut copy = options()
        .create(true)
        .truncate(true)
        .open(beside(path, ".torn"))?;
    io::copy(file, &mut copy)?;
    copy.sync_all()
}

/// Whether a file this short can only be a database whose creation was
/// interrupted.
fn is_unfinished(file: &mut File, size: u64) -> io::Result<bool> {
    if size >= MAGIC.len() as u64 {
        return Ok(false);
    }
    let mut start = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut start)?;
    Ok(MAGIC.starts_with(&start) || start.iter().all(|&b| b == 0))
}

enum Ctl {
    Sync(Ack),
    Stop,
}

/// Someone waiting for a sync. They hear of it whatever happens: a
/// request that is dropped unserved counts as failed.
struct Ack(Option<SyncDone>);

impl Ack {
    fn answer(mut self, result: Result<(), String>) {
        if let Some(done) = self.0.take() {
            done(result);
        }
    }
}

impl Drop for Ack {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            done(Err("the persistence thread has stopped".into()));
        }
    }
}

/// How long to wait before another try at replacing a data file that a
/// failed sync made untrustworthy.
const RECOVER_EVERY: Duration = Duration::from_secs(1);

/// `sync_data`, or a failure staged by a test.
fn sync_data(file: &File, path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if tests::FAILING.lock().as_deref() == Some(path) {
        return Err(io::Error::other("staged sync failure"));
    }
    let _ = path;
    file.sync_data()
}

/// A database kept on disk. Dropping it writes what is pending and stops
/// the background thread.
pub struct Persistence {
    ctl: Sender<Ctl>,
    thread: Option<JoinHandle<()>>,
    report: LoadReport,
}

impl Persistence {
    /// Load `path` into `engine` (creating the file when missing) and
    /// start writing changes to it. The engine must be empty and not yet
    /// in use.
    pub fn open(engine: Arc<Engine>, path: &Path, settings: Options) -> io::Result<Persistence> {
        let path = resolve(path)?;
        let lock = lock(&path)?;
        let mut file = options().create(true).truncate(false).open(&path)?;
        // A file from before may be readable by others, and it holds the
        // hashes of the users' passwords.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        let len = file.metadata()?.len();
        let (report, size) = if is_unfinished(&mut file, len)? {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
            (LoadReport::default(), MAGIC.len() as u64)
        } else {
            let (report, good) = load(&mut file, &engine)?;
            if report.dropped_bytes > 0 {
                keep_tail(&mut file, good, &path)?;
                file.set_len(good)?;
                file.sync_all()?;
            }
            (report, good)
        };
        let slot = engine
            .gate()
            .register()
            .ok_or_else(|| io::Error::other("no free execution gate slot for persistence"))?;
        engine.persist_enable();
        engine.set_fsync(settings.fsync);
        let logged_gen = engine.flush_gen();
        // What the file says of libraries and users is what was just loaded.
        let meta_gens = engine.meta_gens();
        let (libs, users) = (engine.library_sources(), engine.acl().users());
        let for_syncer = engine.clone();
        // From here on dropping the writer undoes the two steps above.
        let mut writer = Writer {
            engine,
            slot,
            file: Some(file),
            _lock: lock,
            path,
            size,
            base: size,
            logged_gen,
            buf: Vec::new(),
            options: settings,
            unsynced: false,
            broken: false,
            recover_after: Instant::now(),
            meta_gens,
            libs,
            users,
        };
        // The file still holds keys of a database that is now kept in
        // memory only: they go before anything else is written, or a later
        // start that keeps that database again would bring them back.
        if writer.engine.take_bgsave() {
            writer.rewrite()?;
        }
        let (ctl, requests) = mpsc::channel();
        let ask = Mutex::new(ctl.clone());
        for_syncer.set_syncer(Some(Arc::new(move |done| {
            // A request that cannot be sent is dropped, which answers it.
            let _ = ask.lock().send(Ctl::Sync(Ack(Some(done))));
        })));
        let thread = std::thread::Builder::new()
            .name("hcache-persist".into())
            .spawn(move || {
                let interval = writer.options.interval;
                let mut next_tick = Instant::now() + interval;
                loop {
                    // The deadline does not move when requests arrive, so
                    // a caller that syncs all the time cannot starve the
                    // rewrite.
                    let wait = next_tick.saturating_duration_since(Instant::now());
                    match requests.recv_timeout(wait) {
                        Ok(Ctl::Sync(ack)) => {
                            // Everyone who asked before this round began is
                            // served by it: one sync for all of them.
                            let mut acks = vec![ack];
                            let mut stop = false;
                            loop {
                                match requests.try_recv() {
                                    Ok(Ctl::Sync(ack)) => acks.push(ack),
                                    Ok(Ctl::Stop) => {
                                        stop = true;
                                        break;
                                    }
                                    Err(_) => break,
                                }
                            }
                            let result = writer.round(true);
                            writer.report(&result);
                            for ack in acks {
                                ack.answer(
                                    result.as_ref().map(|_| ()).map_err(ToString::to_string),
                                );
                            }
                            if stop {
                                break;
                            }
                        }
                        Ok(Ctl::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    if Instant::now() >= next_tick {
                        writer.tick();
                        next_tick = Instant::now() + interval;
                    }
                }
                // The last word is synced whatever the mode.
                let result = writer.round(true);
                writer.report(&result);
            })?;
        Ok(Persistence {
            ctl,
            thread: Some(thread),
            report,
        })
    }

    pub fn report(&self) -> LoadReport {
        self.report
    }

    /// Write every change made so far and wait until it is on disk.
    pub fn sync(&self) -> io::Result<()> {
        request_sync(&self.ctl)
    }
}

fn request_sync(ctl: &Sender<Ctl>) -> io::Result<()> {
    let (tx, done) = mpsc::channel();
    let ack = Ack(Some(Box::new(move |result| {
        // The caller may have given up waiting.
        let _ = tx.send(result);
    })));
    let gone = || io::Error::other("the persistence thread has stopped");
    // Dropped unsent, the request is answered as failed.
    let _ = ctl.send(Ctl::Sync(ack));
    done.recv().map_err(|_| gone())?.map_err(io::Error::other)
}

impl Drop for Persistence {
    fn drop(&mut self) {
        // A thread that is gone already has nothing left to stop.
        let _ = self.ctl.send(Ctl::Stop);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                eprintln!("hcache: the persistence thread panicked");
            }
        }
    }
}

struct Writer {
    engine: Arc<Engine>,
    slot: usize,
    /// `None` after a failure left no usable handle; reopened on demand.
    file: Option<File>,
    _lock: File,
    path: PathBuf,
    size: u64,
    /// Size right after the last rewrite.
    base: u64,
    /// The FLUSHALL generation the file reflects.
    logged_gen: u64,
    /// A collected round that is not on disk yet.
    buf: Vec<u8>,
    options: Options,
    /// A frame was written and not synced (`Fsync::Never`).
    unsynced: bool,
    /// A sync failed. What the system then did with the bytes it had not
    /// written is unknown, and a later sync that succeeds says nothing
    /// about them: nothing more is appended, and nothing is reported as
    /// saved, until the whole file has been written anew.
    broken: bool,
    recover_after: Instant,
    /// `Engine::meta_gens` when the libraries and users were last compared
    /// with what the file has, which is `libs` and `users`.
    meta_gens: (u64, u64),
    libs: HashMap<String, Arc<[u8]>>,
    users: HashMap<Bytes, Arc<User>>,
}

impl Drop for Writer {
    /// Runs when the thread ends, also by a panic: from here on nobody
    /// writes, so the shards stop collecting and the engine says so.
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.engine
                .persist_result(Some("the persistence thread panicked".into()));
        }
        self.engine.set_syncer(None);
        self.engine.persist_disable();
        self.engine.gate().unregister(self.slot);
    }
}

/// Removes the half-made rewrite unless it became the data file.
struct TempFile {
    path: PathBuf,
    keep: bool,
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if !self.keep {
            // Best effort: the half-made file is of no use to anyone.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl Writer {
    fn report(&self, result: &io::Result<()>) {
        self.engine
            .persist_result(result.as_ref().err().map(ToString::to_string));
        if let Err(e) = result {
            eprintln!("hcache: writing {} failed: {e}", self.path.display());
        }
    }

    /// One background step. Errors are reported and the step is retried at
    /// the next tick: the changes stay marked in memory until written.
    fn tick(&mut self) {
        let sync = self.engine.fsync() != Fsync::Never || self.broken;
        let result = self.round(sync).and_then(|()| {
            let grown = self.size >= self.options.rewrite_min_bytes
                && self.size >= self.base.saturating_mul(self.options.rewrite_factor);
            // A request is only taken once it can be served.
            if self.engine.take_bgsave() || grown {
                let rewritten = self.rewrite();
                if rewritten.is_err() {
                    // Not again until the file has grown as much once more.
                    self.base = self.size;
                }
                rewritten
            } else {
                Ok(())
            }
        });
        self.report(&result);
    }

    /// Append `buf` as one frame. With `sync`, everything written so far
    /// is on disk when this returns.
    fn flush(&mut self, sync: bool) -> io::Result<()> {
        if self.broken {
            return Err(io::Error::other(
                "a sync of the data file failed; it has to be written anew first",
            ));
        }
        if self.buf.is_empty() {
            if sync && self.unsynced {
                if let Some(file) = &self.file {
                    if let Err(e) = sync_data(file, &self.path) {
                        self.broken = true;
                        return Err(e);
                    }
                }
                self.unsynced = false;
            }
            return Ok(());
        }
        if self.file.is_none() {
            self.file = Some(options().open(&self.path)?);
        }
        let Some(file) = self.file.as_mut() else {
            return Err(io::Error::other("the data file is not open"));
        };
        // An earlier attempt may have left part of a frame behind.
        file.set_len(self.size)?;
        file.seek(SeekFrom::Start(self.size))?;
        let written = write_frame(file, &self.buf)?;
        self.size += written;
        self.buf.clear();
        self.unsynced = true;
        if sync {
            if let Err(e) = sync_data(file, &self.path) {
                self.broken = true;
                return Err(e);
            }
            self.unsynced = false;
        }
        Ok(())
    }

    /// After a failed sync: write the whole file anew from memory, which
    /// has everything. Not more often than `RECOVER_EVERY`.
    fn recover(&mut self) -> io::Result<()> {
        let now = Instant::now();
        if now < self.recover_after {
            return Err(io::Error::other(
                "a sync of the data file failed; writing it anew is retried shortly",
            ));
        }
        self.recover_after = now + RECOVER_EVERY;
        // What was collected and maybe written is in memory as well.
        self.buf.clear();
        self.rewrite()?;
        if self.broken {
            // FLUSHALL got in the way of the rewrite.
            return Err(io::Error::other("the data file is being written anew"));
        }
        Ok(())
    }

    /// Bring the file up to date with every change made so far.
    fn round(&mut self, sync: bool) -> io::Result<()> {
        if self.broken {
            return self.recover();
        }
        // First what an earlier, failed attempt left behind; on its own it
        // would not cover the changes made since.
        self.flush(sync)?;
        self.engine
            .persist_collect(self.slot, &mut self.logged_gen, &mut self.buf);
        self.collect_meta();
        self.flush(sync)
    }

    /// Append what became of the libraries and the users since the file
    /// last heard of them.
    fn collect_meta(&mut self) {
        // Read first: a change that comes after it is seen next time.
        let gens = self.engine.meta_gens();
        if gens == self.meta_gens {
            return;
        }
        let libs = self.engine.library_sources();
        let users = self.engine.acl().users();
        encode_meta(&mut self.buf, &self.libs, &self.users, &libs, &users);
        self.libs = libs;
        self.users = users;
        self.meta_gens = gens;
    }

    /// Replace the file by one that holds every live key once.
    fn rewrite(&mut self) -> io::Result<()> {
        let tmp_path = beside(&self.path, ".rewrite");
        // Never write through whatever sits at that name.
        match std::fs::remove_file(&tmp_path) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let mut tmp = options().create_new(true).open(&tmp_path)?;
        let mut guard = TempFile {
            path: tmp_path.clone(),
            keep: false,
        };
        tmp.write_all(MAGIC)?;
        let mut size = MAGIC.len() as u64;
        let mut frame = Vec::new();
        for shard in 0..self.engine.shard_count() {
            let mut cursor = 0;
            loop {
                let next = self.engine.persist_dump(
                    self.slot,
                    self.logged_gen,
                    shard,
                    cursor,
                    DUMP_CHUNK,
                    &mut frame,
                );
                let Some(next) = next else {
                    // FLUSHALL ran meanwhile: the old file plus the next
                    // round are still right, so just give up on this one.
                    drop(tmp);
                    return Ok(());
                };
                if frame.len() >= FRAME_TARGET || (next == 0 && !frame.is_empty()) {
                    size += write_frame(&mut tmp, &frame)?;
                    frame.clear();
                }
                cursor = next;
                if cursor == 0 {
                    break;
                }
            }
        }
        // The shards were read at different moments. One more round brings
        // every key that changed meanwhile to the same instant, so the new
        // file never shows half of a transaction. Should anything below
        // fail, that round stays in `buf` and goes to the old file instead.
        self.engine
            .persist_collect(self.slot, &mut self.logged_gen, &mut self.buf);
        if !self.buf.is_empty() {
            size += write_frame(&mut tmp, &self.buf)?;
        }
        // Every library and every user, as if the file had none.
        let meta_gens = self.engine.meta_gens();
        let libs = self.engine.library_sources();
        let users = self.engine.acl().users();
        encode_meta(&mut frame, &HashMap::new(), &HashMap::new(), &libs, &users);
        if !frame.is_empty() {
            size += write_frame(&mut tmp, &frame)?;
        }
        sync_data(&tmp, &self.path)?;
        tmp.sync_all()?;
        // The old handle is closed before its file is replaced. If the
        // rename fails, `file` stays empty and is reopened by path.
        self.file = None;
        if let Err(e) = std::fs::rename(&tmp_path, &self.path) {
            drop(tmp);
            return Err(e);
        }
        guard.keep = true;
        // From here on the new file is the data file, whatever else fails.
        self.file = Some(tmp);
        self.size = size;
        self.base = size;
        self.buf.clear();
        self.unsynced = false;
        self.broken = false;
        (self.meta_gens, self.libs, self.users) = (meta_gens, libs, users);
        #[cfg(unix)]
        if let Some(dir) = self.path.parent() {
            File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::Cmd;
    use crate::engine::Config;

    /// The data file whose syncs fail, for the test below.
    pub(super) static FAILING: Mutex<Option<PathBuf>> = Mutex::new(None);

    fn word(w: &str) -> Bytes {
        w.as_bytes().to_vec()
    }

    #[test]
    fn after_a_failed_sync_nothing_is_called_saved_until_the_file_is_new() {
        let dir = std::env::temp_dir().join(format!("hcache-sync-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("broken.hc");
        let _ = std::fs::remove_file(&path);
        let settings = Options {
            interval: Duration::from_secs(3600),
            ..Options::default()
        };
        let user = |engine: &Engine, name: &str| {
            engine
                .acl()
                .restore_user(name.as_bytes(), &[word("on"), word("nopass")])
                .unwrap();
        };
        {
            let engine = Arc::new(Engine::new(Config::default()));
            let p = Persistence::open(engine.clone(), &path, settings.clone()).unwrap();
            let path = resolve(&path).unwrap();
            user(&engine, "before");
            p.sync().unwrap();
            *FAILING.lock() = Some(path);
            user(&engine, "during");
            assert!(p.sync().is_err());
            // Not better by itself, and not by asking again.
            assert!(p.sync().is_err());
            assert!(engine.sync_now().unwrap().is_err());
            *FAILING.lock() = None;
            user(&engine, "after");
            // The next try at a new file is not at once.
            let deadline = Instant::now() + Duration::from_secs(10);
            while p.sync().is_err() {
                assert!(Instant::now() < deadline, "the file was never written anew");
                std::thread::sleep(Duration::from_millis(50));
            }
            user(&engine, "last");
            p.sync().unwrap();
        }
        let engine = Arc::new(Engine::new(Config::default()));
        let p = Persistence::open(engine.clone(), &path, settings).unwrap();
        assert_eq!(p.report().dropped_bytes, 0);
        for name in ["before", "during", "after", "last"] {
            assert!(engine.acl().user(name.as_bytes()).is_some(), "{name}");
        }
        drop(p);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_request_nobody_serves_is_answered_as_failed() {
        let (tx, rx) = mpsc::channel();
        let (ctl, requests) = mpsc::channel::<Ctl>();
        drop(requests);
        let _ = ctl.send(Ctl::Sync(Ack(Some(Box::new(move |result| {
            tx.send(result).unwrap();
        })))));
        assert!(rx.recv().unwrap().is_err());
        assert!(request_sync(&ctl).is_err());
    }

    #[test]
    fn a_write_to_a_memory_database_does_not_wait_for_the_disk() {
        let dir = std::env::temp_dir().join(format!("hcache-memdb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("memdb.hc");
        let _ = std::fs::remove_file(&path);
        let engine = Arc::new(Engine::new(Config {
            memory_dbs: vec![1],
            ..Config::default()
        }));
        let settings = Options {
            interval: Duration::from_secs(3600),
            fsync: Fsync::Always,
            ..Options::default()
        };
        let set = Cmd::Set(word("k"), word("v"));
        assert!(!engine.sync_before_reply(0, &set), "no data file yet");
        let _p = Persistence::open(engine.clone(), &path, settings).unwrap();
        assert!(engine.sync_before_reply(0, &set));
        assert!(!engine.sync_before_reply(1, &set));
        // Unless the command may reach a database that is kept.
        assert!(engine.sync_before_reply(1, &Cmd::FlushAll(None)));
        let to_kept = Cmd::Move {
            key: word("k"),
            db: 0,
        };
        assert!(engine.sync_before_reply(1, &to_kept));
        // Or changes what is kept but is no key.
        let acl = Cmd::parse(["ACL", "SETUSER", "bob", "on"].map(word).to_vec()).unwrap();
        assert!(engine.sync_before_reply(1, &acl));
    }
}
