//! Hotstash embedded in the application's own process.
//!
//! ```
//! let db = hotstash::Db::open(None, hotstash::Config::default()).unwrap();
//! let session = db.session().unwrap();
//! session.execute(&["SET", "k", "v"]);
//! assert_eq!(session.execute(&["GET", "k"]), hotstash::Reply::bulk("v"));
//! ```
//!
//! One `Session` per application thread. A session may sleep on a lock;
//! the event loops of a server started with `Db::listen` never do.

use hotstash_core::cmd::Cmd;
use hotstash_core::engine::Engine;
use hotstash_core::persist::Persistence;
use hotstash_core::txn::{TxnState, TxnStep};
use hotstash_server::Server;
use std::cell::{Cell, RefCell};
use std::fmt;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

pub use hotstash_core::engine::{Config, StatsSnapshot};
pub use hotstash_core::mem::{used as memory_used, CountingAlloc};
pub use hotstash_core::persist::{Fsync, LoadReport, Options as PersistOptions};
pub use hotstash_core::rdbfile::RdbStats;
pub use hotstash_core::resp::Reply;
pub use hotstash_core::shard::Policy;
pub use hotstash_server::ServerConfig;

#[derive(Debug)]
pub enum Error {
    /// More sessions and workers than `Config::max_slots`.
    TooManySessions,
    Io(std::io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::TooManySessions => f.write_str("no free session slot (Config::max_slots)"),
            Error::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone)]
pub struct Db {
    engine: Arc<Engine>,
    /// Shared by every clone and session; the last one to go stops it.
    persistence: Option<Arc<Persistence>>,
}

impl Db {
    /// Open a database: in memory only without a `path`, otherwise kept in
    /// that file, which is created when missing and loaded when present.
    pub fn open(path: Option<&Path>, config: Config) -> Result<Db, Error> {
        Self::open_with(path, config, PersistOptions::default())
    }

    pub fn open_with(
        path: Option<&Path>,
        config: Config,
        options: PersistOptions,
    ) -> Result<Db, Error> {
        let engine = Arc::new(Engine::new(config));
        let persistence = match path {
            Some(path) => Some(Arc::new(
                Persistence::open(engine.clone(), path, options).map_err(Error::Io)?,
            )),
            None => None,
        };
        Ok(Db {
            engine,
            persistence,
        })
    }

    /// What loading the data file found; `None` for an in-memory database.
    pub fn load_report(&self) -> Option<LoadReport> {
        self.persistence.as_ref().map(|p| p.report())
    }

    /// Wait until every change made so far is on disk. Without a data
    /// file there is nothing to wait for.
    pub fn sync(&self) -> Result<(), Error> {
        match &self.persistence {
            Some(persistence) => persistence.sync().map_err(Error::Io),
            None => Ok(()),
        }
    }

    pub fn session(&self) -> Result<Session, Error> {
        let slot = self
            .engine
            .gate()
            .register()
            .ok_or(Error::TooManySessions)?;
        Ok(Session {
            engine: self.engine.clone(),
            slot,
            txn: RefCell::new(TxnState::default()),
            _persistence: self.persistence.clone(),
            _not_sync: PhantomData,
        })
    }

    /// Also serve this database over TCP. One listener per database; the
    /// server stops when the returned handle is dropped.
    pub fn listen(&self, config: ServerConfig) -> Result<Server, Error> {
        let mut server = Server::start(self.engine.clone(), config).map_err(Error::Io)?;
        if let Some(persistence) = &self.persistence {
            // The server may outlive every `Db` and `Session`.
            server.keep_alive(persistence.clone());
        }
        Ok(server)
    }

    /// Shorthand for `listen` on `addr` with default settings.
    pub fn listen_on(&self, addr: SocketAddr) -> Result<Server, Error> {
        self.listen(ServerConfig {
            addr,
            ..ServerConfig::default()
        })
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.engine.stats()
    }

    /// Every database as an RDB file, for Redis 7.2 and Valkey to load:
    /// RDB 11, or RDB 80 (Valkey 9 only) when a hash has fields with
    /// deadlines. No command runs while it is taken.
    pub fn save_rdb(&self) -> Vec<u8> {
        self.engine.save_rdb()
    }

    /// Load an RDB file of Redis up to 7.2 or of Valkey 9 into a database
    /// that holds no key. On an error no key of the file stays.
    pub fn load_rdb(&self, input: impl std::io::Read) -> Result<RdbStats, String> {
        self.engine.load_rdb(input)
    }

    /// Move this database's clock forward, so tests of expiry need not
    /// sleep.
    #[doc(hidden)]
    pub fn advance_clock(&self, ms: u64) {
        self.engine.advance_clock(ms);
    }

    /// Remove every expired key now; returns how many.
    #[doc(hidden)]
    pub fn sweep_expired(&self) -> usize {
        self.engine.sweep_expired()
    }

    /// One round of background expiry, as a listening server runs it.
    #[doc(hidden)]
    pub fn expire_tick(&self) {
        self.engine.expire_tick();
    }
}

/// One application thread's handle. `Send` but not `Sync`: a session owns
/// one slot of the execution gate, and a slot serves one thread at a time.
pub struct Session {
    engine: Arc<Engine>,
    slot: usize,
    /// MULTI queue and WATCH list, as a connection has.
    txn: RefCell<TxnState>,
    _persistence: Option<Arc<Persistence>>,
    _not_sync: PhantomData<Cell<()>>,
}

impl Session {
    /// Run one command, given as its arguments (`["SET", "k", "v"]`).
    /// Errors come back as `Reply::Error`, as they would over the wire.
    pub fn execute<A: AsRef<[u8]>>(&self, args: &[A]) -> Reply {
        let args = args.iter().map(|a| a.as_ref().to_vec()).collect();
        let step = self.txn.borrow_mut().feed(&self.engine, Cmd::parse(args));
        match step {
            // Waits for everything written so far to be on disk; there are
            // no replicas.
            TxnStep::Run(Cmd::WaitAof(true)) if self.engine.has_data_file() => {
                match self.engine.sync_now() {
                    Some(Err(e)) => Reply::Error(format!("ERR writing the data file failed: {e}")),
                    _ => Reply::Array(vec![Reply::Int(1), Reply::Int(0)]),
                }
            }
            TxnStep::Run(cmd) => {
                let db = self.txn.borrow().db();
                let durable = cmd.may_change() && self.engine.sync_before_reply(db, &cmd);
                let reply = self
                    .engine
                    .with_db(db, || self.engine.execute_blocking(self.slot, cmd));
                // With `Fsync::Always` a change is on disk before its
                // caller hears of it.
                match durable.then(|| self.engine.sync_now()).flatten() {
                    Some(Err(e)) => Reply::Error(format!(
                        "ERR the command ran, but writing the data file failed: {e}"
                    )),
                    _ => reply,
                }
            }
            TxnStep::Reply(reply) => reply,
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.engine.gate().unregister(self.slot);
    }
}
