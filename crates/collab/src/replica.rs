//! The replica seam under [`Live`](crate::Live): one yrs document with a fixed schema.
//!
//! [`LiveDoc`](crate::LiveDoc) (blocks of text, WordCraft's) is one schema. Another app adds its
//! own (a grid's sheets, a deck's slides, a PDF's annotations) by implementing [`Replica`], and
//! most of what such a schema needs is in [`YReplica`]:
//!
//! * the yrs `Doc` built as client 0 for the seed ([`YReplica::seed_doc`]) and rebuilt with the
//!   relay-granted space as its client id, the state carried across ([`YReplica::rebuilt`]);
//! * one `UndoManager` over the schema's roots tracking only this replica's own edits, and an
//!   untracked origin for edits that are sent but never undone ([`YReplica::untracked`]);
//! * remote updates applied under `catch_unwind` ([`integrate`]), with the known limit that a
//!   wasm build with `panic = "abort"` still aborts;
//! * deterministic writes for seeding ([`put_sorted`]) and `char` offsets over yrs byte offsets
//!   ([`byte_at`], [`byte_span`]).
//!
//! The wire does not change with the schema: one yrs v1 update per submit under scope `"doc"`,
//! a checkpoint is `snap: true` with the full state.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde_json::{Map, Value};
use yrs::undo::UndoManager;
use yrs::updates::decoder::Decode;
use yrs::{Any, ClientID, Doc, MapRef, Number, OffsetKind, Options, Origin, ReadTxn, StateVector, Transact, TransactionMut, Update};

use crate::ydoc::LiveError;

/// The origin of this replica's own edits, and the only one the undo manager tracks.
pub const LOCAL: &str = "craft-collab/local";
/// The origin of updates from other replicas, of the seed, and of the state carried across
/// [`YReplica::rebuilt`].
pub const REMOTE: &str = "craft-collab/remote";
/// The origin of this replica's own edits made through [`YReplica::untracked`]: sent like any
/// local edit, never undone.
pub const UNTRACKED: &str = "craft-collab/untracked";

/// One yrs replica with a fixed schema, as [`Live`](crate::Live) drives it.
///
/// [`LiveDoc`](crate::LiveDoc) is WordCraft's. Every implementation must keep the rules
/// [`Live`](crate::Live) relies on:
///
/// * [`seeded`](Replica::seeded) is a pure function of the seed: every replica seeding the same
///   file reaches the same item ids (built as client 0, in one transaction);
/// * no local edit is taken before [`set_client`](Replica::set_client) has given a nonzero
///   space;
/// * [`apply`](Replica::apply) never panics on hostile bytes (it marks itself
///   [`damaged`](Replica::damaged) instead), and its updates are never undone.
pub trait Replica: Sized + Send {
    /// What one remote update changed, in the schema's own terms.
    type Change: Clone + PartialEq + std::fmt::Debug;
    /// The seed, built as client 0 in one transaction from the file. Same file bytes, same
    /// state.
    type Seed: ?Sized;
    fn seeded(seed: &Self::Seed) -> Result<Self, LiveError>;
    fn from_state(state: &[u8]) -> Result<Self, LiveError>;
    fn set_client(&mut self, space: u32) -> Result<(), LiveError>;
    fn apply(&mut self, update: &[u8]) -> Result<Vec<Self::Change>, LiveError>;
    fn state(&self) -> Vec<u8>;
    fn undo(&mut self) -> Option<Vec<u8>>;
    fn redo(&mut self) -> Option<Vec<u8>>;
    fn damaged(&self) -> bool;
    /// Whether undoing the newest `steps` local edits would take away text another replica
    /// wrote, so the app refuses that undo. Asking changes nothing. A schema that cannot tell
    /// says `false`.
    fn undo_takes_others_text(&self, _steps: usize) -> bool {
        false
    }
}

/// The part of a replica every yrs schema shares: the document, its undo history, its client
/// id and whether it can still be trusted. A schema holds one beside its own roots.
pub struct YReplica {
    // Declared before `doc` so it is dropped first and unhooks itself from a live document.
    undo: UndoManager<()>,
    doc: Doc,
    /// The relay-granted space, once the schema's `set_client` has run.
    client: Option<u32>,
    /// yrs panicked part way through an apply, so this replica may hold half an update.
    damaged: bool,
    /// Local edits are made outside the undo history ([`YReplica::untracked`]).
    untracked: bool,
}

impl YReplica {
    /// A document for the seed or a restored state: client 0, which never mints a local edit.
    #[must_use]
    pub fn seed_doc() -> Doc {
        build_doc(0)
    }

    /// Wrap a built document. `scope` adds the schema's roots to the undo manager (it is
    /// called once, before the manager is used); the manager tracks only [`LOCAL`].
    pub fn assemble(doc: Doc, client: Option<u32>, scope: impl FnOnce(&Doc, &mut UndoManager<()>)) -> YReplica {
        // Spelled out because yrs has no `Default` for these on wasm, where it has no system clock.
        let options = yrs::undo::Options {
            // Each local edit is its own step, so the clock is never consulted.
            capture_timeout_millis: 0,
            tracked_origins: HashSet::new(),
            capture_transaction: None,
            timestamp: Arc::new(|| 0),
            init_undo_stack: Vec::new(),
            init_redo_stack: Vec::new(),
        };
        let mut undo = UndoManager::with_options(options);
        scope(&doc, &mut undo);
        undo.include_origin(Origin::from(LOCAL));
        YReplica { undo, doc, client, damaged: false, untracked: false }
    }

    /// A new document with client id `space` holding this replica's whole state, for the
    /// schema to [`assemble`](Self::assemble) again (with `Some(space)`). The carried state
    /// includes updates yrs is still holding for missing dependencies and the whole delete set.
    /// When it does not carry across, the error says so and this replica is unchanged.
    pub fn rebuilt(&self, space: u32) -> Result<Doc, LiveError> {
        let doc = build_doc(u64::from(space));
        if let Err(e) = integrate(&doc, &self.state()) {
            log::warn!("collab: this replica's own state did not carry across a client change: {e}");
            return Err(LiveError::from(e));
        }
        Ok(doc)
    }

    /// The document, for the schema's reads.
    #[must_use]
    pub fn doc(&self) -> &Doc {
        &self.doc
    }

    /// The relay-granted space, once set.
    #[must_use]
    pub fn client(&self) -> Option<u32> {
        self.client
    }

    /// Whether yrs panicked during an earlier apply.
    #[must_use]
    pub fn damaged(&self) -> bool {
        self.damaged
    }

    /// Carry the damaged mark across a rebuild, or set it (a test of a caught panic).
    pub fn set_damaged(&mut self, damaged: bool) {
        self.damaged = damaged;
    }

    /// Merge an update from another replica. A yrs panic marks this replica damaged.
    pub fn apply(&mut self, update: &[u8]) -> Result<(), LiveError> {
        if let Err(e) = integrate(&self.doc, update) {
            if e == Integrate::Panicked {
                self.damaged = true;
            }
            return Err(LiveError::Decode);
        }
        Ok(())
    }

    /// The whole document as one update (v1), for a checkpoint or a peer starting from nothing.
    #[must_use]
    pub fn state(&self) -> Vec<u8> {
        self.doc.transact().encode_state_as_update_v1(&StateVector::default())
    }

    /// Undo this replica's last local edit and return the update to send. `None` on a damaged
    /// replica too.
    pub fn undo(&mut self) -> Option<Vec<u8>> {
        if self.damaged || !self.undo.can_undo() {
            return None;
        }
        let before = self.doc.transact().state_vector();
        self.undo.undo_blocking().then(|| self.doc.transact().encode_diff_v1(&before))
    }

    /// What the newest `steps` undo steps inserted, newest first: what undoing them would
    /// take away, for a schema to check before it undoes.
    #[must_use]
    pub fn undo_insertions(&self, steps: usize) -> Vec<&yrs::IdSet> {
        self.undo.undo_stack().iter().rev().take(steps).map(|item| item.insertions()).collect()
    }

    /// Redo the last undone local edit and return the update to send. `None` on a damaged
    /// replica too.
    pub fn redo(&mut self) -> Option<Vec<u8>> {
        if self.damaged || !self.undo.can_redo() {
            return None;
        }
        let before = self.doc.transact().state_vector();
        self.undo.redo_blocking().then(|| self.doc.transact().encode_diff_v1(&before))
    }

    /// Whether local edits are being kept out of the undo history now.
    #[must_use]
    pub fn is_untracked(&self) -> bool {
        self.untracked
    }

    /// Keep local edits out of the undo history (`true`) or not; returns the previous setting.
    /// A schema's `untracked(|s| …)` sets it around its closure.
    pub fn set_untracked(&mut self, untracked: bool) -> bool {
        std::mem::replace(&mut self.untracked, untracked)
    }

    /// Run one local edit in its own transaction and return its update.
    ///
    /// Refused with [`LiveError::Damaged`] on a damaged replica and [`LiveError::NoClient`]
    /// before a space was granted. `edit` checks everything before it changes anything, because
    /// a yrs transaction commits whatever it holds when it is dropped, error or not.
    pub fn local<F>(&mut self, edit: F) -> Result<Vec<u8>, LiveError>
    where
        F: FnOnce(&mut TransactionMut) -> Result<(), LiveError>,
    {
        if self.damaged {
            return Err(LiveError::Damaged);
        }
        if self.client.is_none() {
            return Err(LiveError::NoClient);
        }
        let mut txn = self.doc.transact_mut_with(if self.untracked { UNTRACKED } else { LOCAL });
        edit(&mut txn)?;
        Ok(txn.encode_update_v1())
    }
}

/// Why [`integrate`] failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Integrate {
    #[error("the update did not decode or apply")]
    Refused,
    #[error("yrs panicked while applying the update")]
    Panicked,
}

impl From<Integrate> for LiveError {
    fn from(_: Integrate) -> LiveError {
        LiveError::Decode
    }
}

/// Decode and apply one update as a remote change.
///
/// yrs can panic on bytes that decode but do not describe a valid document, so the apply runs
/// under `catch_unwind`: a hostile or damaged update must not take the app down. That only holds
/// where panics unwind; a wasm build with `panic = "abort"` still aborts.
pub fn integrate(doc: &Doc, update: &[u8]) -> Result<(), Integrate> {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let update = Update::decode_v1(update).map_err(|_| Integrate::Refused)?;
        let mut txn = doc.transact_mut_with(REMOTE);
        txn.apply_update(update).map_err(|_| Integrate::Refused)
    }));
    match outcome {
        Ok(result) => result,
        Err(_) => {
            log::warn!("collab: yrs panicked on an update; it was refused");
            Err(Integrate::Panicked)
        }
    }
}

fn build_doc(client: u64) -> Doc {
    let mut options = Options::with_client_id(ClientID::new(client));
    options.offset_kind = OffsetKind::Bytes;
    // Never mutate on a remote apply: what this replica holds is exactly what it was sent.
    options.cleanup_formatting = false;
    Doc::with_options(options)
}

/// Write `attrs` into `map` in key order, one insert per key. yrs hands out clocks in the order
/// of writes, so a hash-ordered write would give two replicas seeding the same file different
/// item ids.
pub fn put_sorted(txn: &mut TransactionMut, map: &MapRef, attrs: &Map<String, Value>) {
    use yrs::Map as _;
    let sorted: BTreeMap<&String, &Value> = attrs.iter().collect();
    for (k, v) in sorted {
        map.insert(txn, k.as_str(), to_any(v));
    }
}

/// Byte offset of char `char_at`, which may be the end of the text.
pub fn byte_at(s: &str, char_at: usize) -> Result<u32, LiveError> {
    let byte = if char_at == 0 { Some(0) } else { s.char_indices().map(|(b, _)| b).chain(std::iter::once(s.len())).nth(char_at) };
    byte.and_then(|b| u32::try_from(b).ok()).ok_or(LiveError::OutOfRange)
}

/// Byte offset and byte length of `len` chars from `char_at`.
pub fn byte_span(s: &str, char_at: usize, len: usize) -> Result<(u32, u32), LiveError> {
    let start = byte_at(s, char_at)?;
    let end = byte_at(s, char_at.checked_add(len).ok_or(LiveError::OutOfRange)?)?;
    Ok((start, end - start))
}

/// A JSON value as a yrs value.
#[must_use]
pub fn to_any(v: &Value) -> Any {
    match v {
        Value::Null => Any::Null,
        Value::Bool(b) => Any::Bool(*b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => Any::Number(Number::Int(i)),
            None => Any::Number(Number::Float(n.as_f64().unwrap_or(f64::NAN))),
        },
        Value::String(s) => Any::String(Arc::from(s.as_str())),
        Value::Array(items) => Any::Array(items.iter().map(to_any).collect()),
        Value::Object(m) => Any::Map(Arc::new(m.iter().map(|(k, v)| (k.clone(), to_any(v))).collect::<HashMap<_, _>>())),
    }
}

/// A yrs value as JSON.
#[must_use]
pub fn to_json(a: &Any) -> Value {
    match a {
        Any::Null | Any::Undefined => Value::Null,
        Any::Bool(b) => Value::Bool(*b),
        Any::Number(Number::Int(i)) => Value::from(*i),
        Any::Number(Number::Float(f)) => serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number),
        Any::String(s) => Value::String(s.to_string()),
        Any::Buffer(b) => Value::Array(b.iter().map(|x| Value::from(*x)).collect()),
        Any::Array(items) => Value::Array(items.iter().map(to_json).collect()),
        Any::Map(m) => Value::Object(m.iter().map(|(k, v)| (k.clone(), to_json(v))).collect()),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use yrs::{GetString, Text};

    use super::*;

    /// A one-text schema: the smallest thing a `YReplica` can carry.
    fn replica(client: Option<u32>) -> (YReplica, yrs::TextRef) {
        let doc = YReplica::seed_doc();
        let text = doc.get_or_insert_text("t");
        let t = text.clone();
        (YReplica::assemble(doc, client, move |doc, undo| undo.expand_scope(doc, &t)), text)
    }

    /// No local edit before a space, and none on a damaged replica.
    #[test]
    fn local_edits_wait_for_a_space_and_stop_when_damaged() {
        let (mut y, text) = replica(None);
        assert_eq!(
            y.local(|txn| {
                text.insert(txn, 0, "x");
                Ok(())
            }),
            Err(LiveError::NoClient)
        );
        let (mut y, text) = replica(Some(3));
        assert!(
            y.local(|txn| {
                text.insert(txn, 0, "x");
                Ok(())
            })
            .is_ok()
        );
        y.set_damaged(true);
        assert_eq!(
            y.local(|txn| {
                text.insert(txn, 0, "y");
                Ok(())
            }),
            Err(LiveError::Damaged)
        );
        assert_eq!(y.undo(), None);
    }

    /// Untracked edits are sent but never undone; tracked ones are.
    #[test]
    fn untracked_edits_are_not_undone() {
        let (mut y, text) = replica(Some(3));
        y.local(|txn| {
            text.insert(txn, 0, "a");
            Ok(())
        })
        .unwrap();
        let was = y.set_untracked(true);
        y.local(|txn| {
            text.insert(txn, 1, "b");
            Ok(())
        })
        .unwrap();
        y.set_untracked(was);
        assert!(y.undo().is_some());
        let now = text.get_string(&y.doc().transact());
        assert_eq!(now, "b", "the tracked edit went, the untracked one stayed");
        assert_eq!(y.undo(), None);
    }

    /// A rebuild carries the state across to a document minting under the new space.
    #[test]
    fn a_rebuild_keeps_the_state() {
        let (mut y, text) = replica(Some(3));
        y.local(|txn| {
            text.insert(txn, 0, "kept");
            Ok(())
        })
        .unwrap();
        let doc = y.rebuilt(9).unwrap();
        let again = doc.get_or_insert_text("t");
        assert_eq!(again.get_string(&doc.transact()), "kept");
        assert_ne!(doc.client_id(), y.doc().client_id());
    }

    /// Bytes that do not decode are refused without damage; nothing changes.
    #[test]
    fn garbage_is_refused_and_the_replica_stays_sound() {
        let (mut y, _) = replica(Some(3));
        let before = y.state();
        assert_eq!(y.apply(&[0xff, 0x01, 0x02]), Err(LiveError::Decode));
        assert!(!y.damaged());
        assert_eq!(y.state(), before);
    }
}
