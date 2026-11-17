//! MULTI / EXEC / DISCARD / WATCH: the transaction state of one connection.
//!
//! Commands sent after MULTI are parsed and queued here; EXEC turns the
//! queue into one `Cmd::ExecBlock`, which runs on the exclusive side of the
//! gate, so nothing else touches the keyspace while it runs.
//!
//! WATCH remembers a write counter per key (`Engine::watch_version`). It
//! never misses a write, but it can abort a transaction whose keys were not
//! touched: counters are shared between keys, and a watched key that had
//! already expired when it was watched counts as modified once it is
//! collected.

use crate::cluster;
use crate::cmd::{Bytes, ClientCmd, Cmd, Sub, Txn};
use crate::engine::{Engine, Protected, DB_RANGE, DEBUG_REFUSED};
use crate::resp::Reply;

#[derive(Default)]
pub struct TxnState {
    /// `Some` between MULTI and EXEC or DISCARD.
    queue: Option<Vec<Cmd>>,
    /// A command failed to queue; EXEC must refuse.
    dirty: bool,
    watched: Vec<(u16, Bytes, u64)>,
    /// The database SELECT chose for this connection.
    db: u16,
    /// The database the next queued command will run in: a SELECT inside
    /// the transaction moves it.
    queued_db: u16,
    /// In cluster mode, the slot of the first queued command with keys,
    /// and whether a later one had keys in another.
    slot: Option<u16>,
    crossed: bool,
}

pub enum TxnStep {
    /// Answer the client with this; nothing to execute.
    Reply(Reply),
    /// Execute this command now.
    Run(Cmd),
}

impl TxnState {
    /// The database this connection's commands run in.
    pub fn db(&self) -> usize {
        self.db.into()
    }

    /// Decide what happens to one parsed command (or parse error).
    pub fn feed(&mut self, engine: &Engine, parsed: Result<Cmd, Reply>) -> TxnStep {
        let cmd = match parsed {
            Ok(cmd) => cmd,
            Err(error) => {
                self.dirty |= self.queue.is_some();
                return TxnStep::Reply(error);
            }
        };
        if matches!(cmd, Cmd::Debug(_)) && engine.debug_command() == Protected::No {
            self.dirty |= self.queue.is_some();
            return TxnStep::Reply(Reply::err(DEBUG_REFUSED));
        }
        let mut slot = None;
        if engine.node().is_some() {
            match cluster::slot_of(&cmd) {
                Ok(found) => slot = found,
                Err(cluster::CrossSlot) => {
                    self.dirty |= self.queue.is_some();
                    return TxnStep::Reply(Reply::err(cluster::CROSSSLOT));
                }
            }
            if matches!(cmd, Cmd::SwapDb(..)) {
                return self.queue_or_run(
                    None,
                    Cmd::Static(Reply::err("ERR SWAPDB is not allowed in cluster mode")),
                );
            }
        }
        let cmd = match cmd {
            Cmd::Select(db) => {
                let Some(db) = usize::try_from(db)
                    .ok()
                    .filter(|db| *db < engine.databases())
                else {
                    self.dirty |= self.queue.is_some();
                    return TxnStep::Reply(Reply::err(DB_RANGE));
                };
                return TxnStep::Reply(match &mut self.queue {
                    Some(queue) => {
                        self.queued_db = db as u16;
                        queue.push(Cmd::Static(Reply::Ok));
                        Reply::Simple("QUEUED".into())
                    }
                    None => {
                        self.db = db as u16;
                        Reply::Ok
                    }
                });
            }
            cmd => cmd,
        };
        if self.queue.is_none() {
            return match cmd {
                Cmd::Txn(txn) => self.control(engine, txn),
                cmd => TxnStep::Run(cmd),
            };
        }
        match cmd {
            Cmd::Txn(txn) => self.control(engine, txn),
            Cmd::Quit => TxnStep::Run(Cmd::Quit),
            Cmd::Reset => {
                *self = TxnState::default();
                TxnStep::Run(Cmd::Reset)
            }
            // Queued, and refused when EXEC runs it, as in Redis.
            Cmd::Sub(Sub::SSubscribe(_)) => self.queue_or_run(
                slot,
                Cmd::Static(Reply::err(
                    "ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client",
                )),
            ),
            Cmd::Hello(..)
            | Cmd::Auth(..)
            | Cmd::Acl(_)
            | Cmd::Sub(_)
            | Cmd::Monitor
            | Cmd::Shutdown { .. }
            // It waits for the disk, which a transaction cannot.
            | Cmd::Save => {
                self.dirty = true;
                TxnStep::Reply(Reply::err("ERR Command not allowed inside a transaction"))
            }
            Cmd::Client(sub)
                if matches!(
                    *sub,
                    ClientCmd::Tracking(_)
                        | ClientCmd::Caching(_)
                        | ClientCmd::TrackingInfo
                        | ClientCmd::GetRedir
                ) =>
            {
                self.dirty = true;
                TxnStep::Reply(Reply::err("ERR Command not allowed inside a transaction"))
            }
            cmd => {
                let cmd = Cmd::In(self.queued_db, Box::new(cmd));
                self.queue_or_run(slot, cmd)
            }
        }
    }

    /// Queue `cmd`, whose keys are in `slot`, inside a transaction; run it
    /// outside one.
    fn queue_or_run(&mut self, slot: Option<u16>, cmd: Cmd) -> TxnStep {
        let Some(queue) = &mut self.queue else {
            return TxnStep::Run(cmd);
        };
        queue.push(cmd);
        if let Some(slot) = slot {
            self.crossed |= *self.slot.get_or_insert(slot) != slot;
        }
        TxnStep::Reply(Reply::Simple("QUEUED".into()))
    }

    fn control(&mut self, engine: &Engine, txn: Txn) -> TxnStep {
        let reply = match (txn, &mut self.queue) {
            (Txn::Multi, Some(_)) => Reply::err("ERR MULTI calls can not be nested"),
            (Txn::Multi, None) => {
                self.queue = Some(Vec::new());
                self.queued_db = self.db;
                Reply::Ok
            }
            (Txn::Exec, None) => Reply::err("ERR EXEC without MULTI"),
            (Txn::Exec, Some(queue)) => {
                let queue = std::mem::take(queue);
                let watched = std::mem::take(&mut self.watched);
                self.queue = None;
                self.slot = None;
                if std::mem::take(&mut self.crossed) {
                    // Discarded, as Redis does before EXEC would run.
                    self.dirty = false;
                    Reply::err(cluster::CROSSSLOT)
                } else if std::mem::take(&mut self.dirty) {
                    Reply::err("EXECABORT Transaction discarded because of previous errors.")
                } else {
                    // A SELECT in the queue holds from here on, also when a
                    // watched key turns out to have changed.
                    self.db = self.queued_db;
                    return TxnStep::Run(Cmd::ExecBlock { queue, watched });
                }
            }
            (Txn::Discard, None) => Reply::err("ERR DISCARD without MULTI"),
            (Txn::Discard, Some(_)) => {
                *self = TxnState {
                    db: self.db,
                    ..TxnState::default()
                };
                Reply::Ok
            }
            (Txn::Watch(_), Some(_)) => Reply::err("ERR WATCH inside MULTI is not allowed"),
            (Txn::Watch(keys), None) => {
                for key in keys {
                    let version = engine.watch_version(&key);
                    self.watched.push((self.db, key, version));
                }
                Reply::Ok
            }
            (Txn::Unwatch, Some(queue)) => {
                // As in Redis, it is queued and does nothing by then.
                queue.push(Cmd::Static(Reply::Ok));
                Reply::Simple("QUEUED".into())
            }
            (Txn::Unwatch, None) => {
                self.watched.clear();
                Reply::Ok
            }
        };
        TxnStep::Reply(reply)
    }
}
