//! Who is subscribed to what. Connections live on their workers; this
//! table only knows how to address them.
//!
//! The locks here are held for a lookup or an insert, never across I/O or
//! a shard operation.

use hotstash_core::cmd::PubSubQuery;
use hotstash_core::glob::glob_match;
use hotstash_core::resp::Reply;
use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};

/// One subscribed connection: its worker, its slot there, and the
/// connection's own id, since slots are reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Sub {
    pub worker: usize,
    pub idx: usize,
    pub conn: u64,
}

pub(crate) struct Message {
    /// The pattern that matched, for a PSUBSCRIBE subscriber.
    pub pattern: Option<Vec<u8>>,
    /// From SPUBLISH, to a subscriber of SSUBSCRIBE.
    pub shard: bool,
    pub channel: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Default)]
pub(crate) struct Registry {
    channels: RwLock<HashMap<Vec<u8>, Vec<Sub>>>,
    patterns: RwLock<Vec<(Vec<u8>, Sub)>>,
    /// SSUBSCRIBE's, a namespace of its own.
    shard_channels: RwLock<HashMap<Vec<u8>, Vec<Sub>>>,
}

type Channels = RwLock<HashMap<Vec<u8>, Vec<Sub>>>;

fn add(channels: &Channels, channel: &[u8], sub: Sub) {
    let mut channels = channels.write().unwrap_or_else(PoisonError::into_inner);
    channels.entry(channel.to_vec()).or_default().push(sub);
}

fn remove(channels: &Channels, channel: &[u8], sub: Sub) {
    let mut channels = channels.write().unwrap_or_else(PoisonError::into_inner);
    if let Some(subs) = channels.get_mut(channel) {
        subs.retain(|s| *s != sub);
        if subs.is_empty() {
            channels.remove(channel);
        }
    }
}

fn names(channels: &Channels, pattern: Option<&Vec<u8>>) -> Reply {
    let channels = channels.read().unwrap_or_else(PoisonError::into_inner);
    let names = channels
        .keys()
        .filter(|name| pattern.is_none_or(|p| glob_match(p, name)));
    Reply::Array(names.map(|name| Reply::Bulk(name.clone())).collect())
}

fn counts(channels: &Channels, names: &[Vec<u8>]) -> Reply {
    let channels = channels.read().unwrap_or_else(PoisonError::into_inner);
    let mut out = Vec::with_capacity(names.len() * 2);
    for name in names {
        out.push(Reply::Bulk(name.clone()));
        out.push(Reply::Int(channels.get(name).map_or(0, Vec::len) as i64));
    }
    Reply::Array(out)
}

impl Registry {
    pub fn subscribe(&self, channel: &[u8], sub: Sub) {
        add(&self.channels, channel, sub);
    }

    pub fn unsubscribe(&self, channel: &[u8], sub: Sub) {
        remove(&self.channels, channel, sub);
    }

    pub fn ssubscribe(&self, channel: &[u8], sub: Sub) {
        add(&self.shard_channels, channel, sub);
    }

    pub fn sunsubscribe(&self, channel: &[u8], sub: Sub) {
        remove(&self.shard_channels, channel, sub);
    }

    /// Channels, patterns and shard channels with a subscriber, for INFO.
    pub fn sizes(&self) -> [usize; 3] {
        let len = |c: &Channels| c.read().unwrap_or_else(PoisonError::into_inner).len();
        let patterns = self.patterns.read().unwrap_or_else(PoisonError::into_inner);
        [
            len(&self.channels),
            patterns.len(),
            len(&self.shard_channels),
        ]
    }

    /// Everyone a message of SPUBLISH on `channel` goes to.
    pub fn shard_targets(&self, channel: &[u8]) -> Vec<Sub> {
        let channels = self
            .shard_channels
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        channels.get(channel).cloned().unwrap_or_default()
    }

    pub fn psubscribe(&self, pattern: &[u8], sub: Sub) {
        let mut patterns = self
            .patterns
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        patterns.push((pattern.to_vec(), sub));
    }

    pub fn punsubscribe(&self, pattern: &[u8], sub: Sub) {
        let mut patterns = self
            .patterns
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        patterns.retain(|(p, s)| !(*s == sub && p == pattern));
    }

    /// Everyone a message on `channel` goes to, with the pattern that
    /// matched for pattern subscribers.
    pub fn targets(&self, channel: &[u8]) -> Vec<(Sub, Option<Vec<u8>>)> {
        let mut out = Vec::new();
        {
            let channels = self.channels.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(subs) = channels.get(channel) {
                out.extend(subs.iter().map(|s| (*s, None)));
            }
        }
        let patterns = self.patterns.read().unwrap_or_else(PoisonError::into_inner);
        for (pattern, sub) in patterns.iter() {
            if glob_match(pattern, channel) {
                out.push((*sub, Some(pattern.clone())));
            }
        }
        out
    }

    pub fn query(&self, query: &PubSubQuery) -> Reply {
        match query {
            PubSubQuery::Channels(pattern) => names(&self.channels, pattern.as_ref()),
            PubSubQuery::NumSub(channels) => counts(&self.channels, channels),
            PubSubQuery::ShardChannels(pattern) => names(&self.shard_channels, pattern.as_ref()),
            PubSubQuery::ShardNumSub(channels) => counts(&self.shard_channels, channels),
            PubSubQuery::NumPat => {
                let patterns = self.patterns.read().unwrap_or_else(PoisonError::into_inner);
                Reply::Int(patterns.len() as i64)
            }
        }
    }
}
