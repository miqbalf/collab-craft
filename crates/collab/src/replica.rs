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
use yrs::{Any, ClientID, Doc, MapRef, Number, OffsetKind, Options, Origin, ReadTxn, StateVector, Transact, TransactionMut, Update, WriteTxn};

use crate::ydoc::LiveError;

/// The origin of this replica's own edits, and the only one the undo manager tracks.
pub const LOCAL: &str = "craft-collab/local";
/// The origin of updates from other replicas, of the seed, and of the state carried across
/// [`YReplica::rebuilt`].
pub const REMOTE: &str = "craft-collab/remote";
/// The origin of this replica's own edits made through [`YReplica::untracked`]: sent like any
/// local edit, never undone.
pub const UNTRACKED: &str = "craft-collab/untracked";

/// How many applied updates may be waiting on missing dependencies before the replica gives up
/// and raises [`YReplica::needs_resync`].
pub const MAX_PENDING_UPDATES: usize = 2048;
/// How many encoded bytes of waiting updates the replica keeps before it gives up and raises
/// [`YReplica::needs_resync`].
pub const MAX_PENDING_BYTES: usize = 1024 * 1024;

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
    /// Updates were dropped because their dependencies never came ([`YReplica::needs_resync`]):
    /// the replica is sound but behind, and wants the room's full state.
    fn needs_resync(&self) -> bool {
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
    /// Updates were dropped because dependencies never came ([`YReplica::needs_resync`]).
    resync: bool,
    /// How many applies have left something held since nothing was last pending (an apply that
    /// partly landed counts too; duplicates of a held update count again).
    held: usize,
    /// The encoded size of those updates, from their incoming sizes.
    held_bytes: usize,
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
        YReplica { undo, doc, client, damaged: false, untracked: false, resync: false, held: 0, held_bytes: 0 }
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

    /// Whether updates were dropped because too many were waiting for dependencies that never
    /// came (see [`MAX_PENDING_UPDATES`]). The replica is sound but behind: ask the room for its
    /// full state and rebuild from it with [`rebuilt_from_state`](Self::rebuilt_from_state).
    #[must_use]
    pub fn needs_resync(&self) -> bool {
        self.resync
    }

    /// Carry the resync mark across a rebuild, or set it (a test).
    pub fn set_needs_resync(&mut self, resync: bool) {
        self.resync = resync;
    }

    /// Whether yrs panicked during an earlier apply.
    #[must_use]
    pub fn damaged(&self) -> bool {
        self.damaged
    }

    /// Carry the marks and the held counts of `old` across a rebuild that kept its state (the
    /// state includes what yrs holds, so the counts still describe it).
    pub fn carry_marks(&mut self, old: &YReplica) {
        self.damaged = old.damaged;
        self.resync = old.resync;
        self.held = old.held;
        self.held_bytes = old.held_bytes;
    }

    /// Carry the damaged mark across a rebuild, or set it (a test of a caught panic).
    pub fn set_damaged(&mut self, damaged: bool) {
        self.damaged = damaged;
    }

    /// Merge an update from another replica. A yrs panic marks this replica damaged.
    ///
    /// Updates may arrive in any order. One whose dependencies are missing is held by yrs. yrs
    /// 0.28 does not always apply a held update once its dependencies land (it only looks at the
    /// client ids of the update that just arrived), so whenever an apply advanced the state
    /// vector and something is still held, the held part is taken out ([`WriteTxn::prune_pending`])
    /// and applied again, until nothing more lands. What stays held is bounded by
    /// [`MAX_PENDING_UPDATES`] and [`MAX_PENDING_BYTES`]; past either it is dropped and
    /// [`needs_resync`](Self::needs_resync) is raised.
    pub fn apply(&mut self, update: &[u8]) -> Result<(), LiveError> {
        let before = self.doc.transact().state_vector();
        let first = integrate(&self.doc, update);
        if first == Err(Integrate::Panicked) {
            self.damaged = true;
            return Err(LiveError::Decode);
        }
        // Also after a refusal: yrs may have integrated part of the update first, which can
        // make a held one ready.
        if self.settle(before.clone(), if first.is_ok() { update.len() } else { 0 }, first.is_ok()) == Err(Integrate::Panicked) {
            self.damaged = true;
            return Err(LiveError::Decode);
        }
        if first.is_err() && self.doc.transact().state_vector() != before {
            // Part of it landed before yrs refused the rest (or a held update it released), so
            // what is held may be gone with the refusal: behind, but the apply did happen.
            log::warn!("collab: an update applied only in part; resync needed");
            self.drop_held();
            return Ok(());
        }
        first.map_err(|_| LiveError::Decode)
    }

    /// A fresh document with this replica's client id holding exactly `state` (a checkpoint, or
    /// the full state of a peer that is ahead), to catch up after
    /// [`needs_resync`](Self::needs_resync). It is built from nothing rather than applied onto
    /// this document, because yrs's own held-update buffer is not bounded by our caps and a
    /// stuck document would carry it along. The schema [`assemble`](Self::assemble)s the result
    /// again with the same client, which also clears the resync mark.
    ///
    /// The client id stays the same, because it is the space the relay granted and the app's
    /// own ids are minted from it; a fresh id cannot be invented here. That is only safe when
    /// `state` holds every edit this replica already sent, or the next edit would reuse a clock
    /// and be dropped by every peer as a duplicate. So the state's clock for this client must
    /// not be behind this replica's own: otherwise [`LiveError::Stale`]. Also refused: a state
    /// that does not decode or itself leaves updates waiting for dependencies. This replica is
    /// unchanged then.
    pub fn rebuilt_from_state(&self, state: &[u8]) -> Result<Doc, LiveError> {
        let own = self.doc.client_id();
        let doc = build_doc(own.get());
        integrate(&doc, state).map_err(LiveError::from)?;
        if doc.transact().has_missing_updates() {
            return Err(LiveError::Decode);
        }
        let theirs = doc.transact().state_vector().get(&own);
        if theirs < self.doc.transact().state_vector().get(&own) {
            return Err(LiveError::Stale);
        }
        Ok(doc)
    }

    /// After an apply of `incoming` bytes: retry what yrs holds when the apply advanced the
    /// state vector, then enforce the bounds on what is still held.
    ///
    /// Failing part way through a retry means some held updates were taken out and not all went
    /// back in, so this replica is behind without knowing it: resync is raised, and the apply
    /// that triggered it still counts as done.
    fn settle(&mut self, mut before: StateVector, incoming: usize, decoded: bool) -> Result<(), Integrate> {
        loop {
            let txn = self.doc.transact();
            if !txn.has_missing_updates() {
                self.held = 0;
                self.held_bytes = 0;
                return Ok(());
            }
            let now = txn.state_vector();
            drop(txn);
            if now == before {
                break;
            }
            before = now;
            match integrate_held(&self.doc) {
                Ok(()) => {}
                Err(Integrate::Panicked) => return Err(Integrate::Panicked),
                Err(Integrate::Refused) => {
                    log::warn!("collab: held updates did not apply together; dropped, resync needed");
                    self.drop_held();
                    return Ok(());
                }
            }
        }
        // Whatever is still held after this apply is counted against it, whether or not it also
        // advanced the state vector: an update with one block that applies and one that waits
        // would otherwise grow what yrs holds without ever being counted. (An apply that did not
        // decode touched nothing and is not counted.)
        if decoded {
            self.held += 1;
            self.held_bytes += incoming;
        }
        if self.held > MAX_PENDING_UPDATES || self.held_bytes > MAX_PENDING_BYTES {
            log::warn!(
                "collab: {} updates ({} bytes) are waiting for dependencies that did not come; dropped, resync needed",
                self.held,
                self.held_bytes
            );
            self.drop_held();
        }
        Ok(())
    }

    /// Throw away what is held and say the replica is behind.
    fn drop_held(&mut self) {
        drop(self.doc.transact_mut_with(REMOTE).prune_pending());
        self.held = 0;
        self.held_bytes = 0;
        self.resync = true;
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

/// Whether `bytes` are a yrs update (v1) that decodes and whose ids are sane: no block or delete
/// range runs past `u32::MAX`. For a relay to check an op before it orders it, so a crafted
/// update never reaches a browser replica, where a yrs panic aborts the page.
///
/// Runs under `catch_unwind`, so a panic while decoding is a refusal. This is a check of
/// the update on its own; whether it fits a document's history is only known to a replica.
#[must_use]
pub fn update_is_sound(bytes: &[u8]) -> bool {
    std::panic::catch_unwind(|| {
        let Ok(update) = Update::decode_v1(bytes) else {
            return false;
        };
        let ranges_ok = |set: &yrs::IdSet| set.iter().all(|(_, ranges)| ranges.iter().all(|r| r.start <= r.end));
        // A block whose clock + len overflows is dropped from `insertions` (wrapped), so every
        // client with blocks must still list a range starting at its first block.
        let (starts, inserted) = (update.state_vector_lower(), update.insertions(true));
        ranges_ok(update.delete_set())
            && ranges_ok(&inserted)
            && starts.iter().all(|(client, start)| inserted.get(client).is_some_and(|r| r.iter().next().is_some_and(|(r, _)| r.start == *start)))
    })
    .unwrap_or(false)
}

/// Take what yrs holds for missing dependencies out and apply it again, under `catch_unwind`
/// for the same reason as [`integrate`]. What still lacks a dependency goes back to being held.
fn integrate_held(doc: &Doc) -> Result<(), Integrate> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut txn = doc.transact_mut_with(REMOTE);
        match txn.prune_pending() {
            Some(held) => txn.apply_update(held).map_err(|_| Integrate::Refused),
            None => Ok(()),
        }
    }))
    .unwrap_or_else(|_| {
        log::warn!("collab: yrs panicked on held updates");
        Err(Integrate::Panicked)
    })
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

    use yrs::updates::encoder::Encode;
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

    /// A tiny deterministic generator, so the shuffles below are the same on every run.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }

        fn below(&mut self, n: usize) -> usize {
            usize::try_from(self.next()).unwrap() % n
        }

        fn shuffle<T>(&mut self, items: &mut [T]) {
            for i in (1..items.len()).rev() {
                items.swap(i, self.below(i + 1));
            }
        }
    }

    /// Like `replica`, but the document really mints under `client`, as a granted space does.
    fn peer(client: u32) -> (YReplica, yrs::TextRef) {
        let doc = build_doc(u64::from(client));
        let text = doc.get_or_insert_text("t");
        let t = text.clone();
        (YReplica::assemble(doc, Some(client), move |doc, undo| undo.expand_scope(doc, &t)), text)
    }

    fn rebuild(doc: Doc, client: u32) -> (YReplica, yrs::TextRef) {
        let text = doc.get_or_insert_text("t");
        let t = text.clone();
        (YReplica::assemble(doc, Some(client), move |doc, undo| undo.expand_scope(doc, &t)), text)
    }

    fn held_bytes(y: &YReplica) -> usize {
        let txn = y.doc().transact();
        txn.store().pending_update().map_or(0, |p| p.update.encode_v1().len())
    }

    fn pending(y: &YReplica) -> bool {
        y.doc().transact().has_missing_updates()
    }

    fn read(y: &YReplica) -> String {
        y.doc().get_or_insert_text("t").get_string(&y.doc().transact())
    }

    /// Updates from `peers` peers in one causal order: before each edit the peer may catch up on
    /// everything made so far (applied in order), so edits are concurrent but a peer's own
    /// dependencies always come earlier in the list.
    fn history(seed: u64, peers: usize, steps: usize) -> Vec<Vec<u8>> {
        let mut rng = Lcg(seed);
        let mut reps: Vec<(YReplica, yrs::TextRef)> = (0..peers).map(|i| peer(u32::try_from(i).unwrap() + 1)).collect();
        let mut seen = vec![0usize; peers];
        let mut all: Vec<Vec<u8>> = Vec::new();
        for step in 0..steps {
            let p = rng.below(peers);
            if rng.below(2) == 0 {
                for u in &all[seen[p]..] {
                    reps[p].0.apply(u).unwrap();
                }
                seen[p] = all.len();
            }
            let len = u32::try_from(read(&reps[p].0).chars().count()).unwrap();
            let text = reps[p].1.clone();
            let mark = u8::try_from(step % 26).unwrap();
            let delete = len > 3 && rng.below(4) == 0;
            let at = u32::try_from(rng.below(usize::try_from(len).unwrap() + 1)).unwrap();
            let update = reps[p]
                .0
                .local(|txn| {
                    if delete {
                        let at = at.min(len - 1);
                        text.remove_range(txn, at, 1);
                    } else {
                        text.insert(txn, at, &char::from(b'a' + mark).to_string());
                    }
                    Ok(())
                })
                .unwrap();
            // Local edits already sit in the author's replica; its own cursor moves past them
            // only once it has caught up on the others, which `apply` of its own update undoes.
            all.push(update);
        }
        all
    }

    fn in_order(all: &[Vec<u8>]) -> YReplica {
        let (mut y, _) = peer(99);
        for u in all {
            y.apply(u).unwrap();
        }
        y
    }

    const SEEDS: [u64; 8] = [1, 2, 3, 5, 8, 13, 17, 42];

    /// Any delivery order reaches the in-order state, and nothing is left pending.
    #[test]
    fn shuffled_delivery_equals_in_order_delivery() {
        for peers in [2, 3] {
            for seed in SEEDS {
                let all = history(seed, peers, 60);
                let expect = in_order(&all);
                assert!(!pending(&expect));
                for round in 0..20 {
                    let mut order = all.clone();
                    Lcg(seed * 1000 + round).shuffle(&mut order);
                    let (mut y, _) = peer(99);
                    for u in &order {
                        y.apply(u).unwrap();
                    }
                    assert_eq!(read(&y), read(&expect), "peers {peers} seed {seed} round {round}");
                    assert!(!pending(&y), "pending left: peers {peers} seed {seed} round {round}");
                    assert!(!y.needs_resync());
                }
            }
        }
    }

    /// Two peers edit; `peer 1`'s first update is withheld from `y`, so every later one from it
    /// stays held. Returns the withheld update, the rest, and the author's full state.
    fn withheld(extra: &[usize]) -> (Vec<u8>, Vec<Vec<u8>>, Vec<u8>) {
        let (mut a, text) = peer(1);
        let mut all = Vec::new();
        for (i, size) in extra.iter().enumerate() {
            let chunk = "x".repeat(*size);
            let at = u32::try_from(read(&a).len()).unwrap();
            let t = text.clone();
            all.push(
                a.local(|txn| {
                    t.insert(txn, at, &format!("{i}{chunk}"));
                    Ok(())
                })
                .unwrap(),
            );
        }
        let first = all.remove(0);
        let state = a.state();
        (first, all, state)
    }

    /// Past the entry cap the held updates are dropped and the resync mark is raised; a full
    /// state then brings the replica level.
    #[test]
    fn too_many_held_updates_raise_resync_and_a_full_state_recovers() {
        let sizes = vec![1; MAX_PENDING_UPDATES + 50];
        let (first, rest, state) = withheld(&sizes);
        let (mut y, _) = peer(99);
        for (i, u) in rest.iter().enumerate() {
            y.apply(u).unwrap();
            if i < MAX_PENDING_UPDATES {
                assert!(!y.needs_resync(), "raised early at {i}");
            }
        }
        assert!(y.needs_resync());
        assert!(rest.len() > MAX_PENDING_UPDATES);
        // Bounded: after the drop at most the 49 updates that came since are held.
        let held = held_bytes(&y);
        assert!(held < 1024, "held {held} bytes");
        let doc = y.rebuilt_from_state(&state).unwrap();
        let (mut y, _) = rebuild(doc, 99);
        assert!(!y.needs_resync());
        assert!(!pending(&y));
        let _ = &mut y;
        let (mut z, _) = peer(98);
        z.apply(&first).unwrap();
        for u in &rest {
            z.apply(u).unwrap();
        }
        assert_eq!(read(&y), read(&z));
    }

    /// The byte cap works too, with a handful of large updates.
    #[test]
    fn too_many_held_bytes_raise_resync() {
        let big = MAX_PENDING_BYTES / 2 + 1024;
        let (_, rest, state) = withheld(&[1, big, big, big]);
        let (mut y, _) = peer(99);
        y.apply(&rest[0]).unwrap();
        assert!(!y.needs_resync());
        y.apply(&rest[1]).unwrap();
        assert!(y.needs_resync(), "two of {big} bytes pass the cap");
        let held = held_bytes(&y);
        assert!(held < MAX_PENDING_BYTES, "held {held} bytes after the drop");
        let (y, _) = rebuild(y.rebuilt_from_state(&state).unwrap(), 99);
        assert!(!y.needs_resync());
        assert_eq!(read(&y).len(), 2 + 3 * (big + 1), "all text arrived");
    }

    /// Resync builds a new document with the same client id and drops everything held.
    #[test]
    fn rebuild_from_state_keeps_the_client_and_refuses_junk() {
        let (_, rest, state) = withheld(&[1, 1, 1]);
        let (y, _) = peer(99);
        let doc = y.rebuilt_from_state(&state).unwrap();
        assert_eq!(doc.client_id(), y.doc().client_id());
        assert_eq!(y.rebuilt_from_state(&[0xff, 1, 2]).err(), Some(LiveError::Decode));
        // A state that itself waits on a missing dependency is not a full state.
        assert_eq!(y.rebuilt_from_state(&rest[1]).err(), Some(LiveError::Decode));
    }

    /// Junk and corrupted updates, many seeds: never a panic out of `apply`, never a hang, and
    /// whatever panics inside yrs is caught and marks the replica damaged.
    #[test]
    fn junk_updates_never_panic_out() {
        let all = history(7, 3, 40);
        let start = std::time::Instant::now();
        for seed in 0..200_u64 {
            let mut rng = Lcg(seed);
            let (mut y, _) = peer(99);
            for u in &all[..10] {
                y.apply(u).unwrap();
            }
            let mut junk = all[rng.below(all.len())].clone();
            if seed % 3 == 0 {
                junk = (0..rng.below(64) + 1).map(|_| u8::try_from(rng.below(256)).unwrap()).collect();
            } else {
                for _ in 0..=rng.below(4) {
                    let at = rng.below(junk.len());
                    junk[at] = u8::try_from(rng.below(256)).unwrap();
                }
            }
            let _ = y.apply(&junk);
            let _ = y.state();
        }
        assert!(start.elapsed().as_secs() < 30, "junk took {:?}", start.elapsed());
    }

    /// One v1 update by hand: client `client` at clock 0, one string item "a" whose parent is
    /// the item `(pc, 0)`, with no origin.
    fn child_of(client: u8, pc: u8) -> Vec<u8> {
        vec![1, 1, client, 0, 0x04, 0, pc, 0, 1, b'a', 0]
    }

    /// Held updates that fail to apply together (one names a string item as its parent) raise
    /// resync, never an error for the update that did apply, and never panic or damage.
    #[test]
    fn a_failing_retry_raises_resync_and_still_counts_as_applied() {
        let (first, rest, _) = withheld(&[2, 1, 1]);
        let (mut y, _) = peer(99);
        y.apply(&child_of(5, 1)).unwrap();
        y.apply(&rest[0]).unwrap();
        assert!(pending(&y));
        assert!(!y.needs_resync());
        assert!(y.apply(&first).is_ok(), "the dependency did apply");
        assert!(y.needs_resync());
        assert!(!y.damaged());
        assert!(!pending(&y), "nothing is left held");
    }

    /// A rebuild from a state that lacks this replica's own sent edits is refused, because the
    /// next edit would reuse a clock.
    #[test]
    fn rebuild_from_a_stale_state_is_refused() {
        let (mut y, text) = peer(99);
        let before = y.state();
        y.local(|txn| {
            text.insert(txn, 0, "mine");
            Ok(())
        })
        .unwrap();
        assert_eq!(y.rebuilt_from_state(&before).err(), Some(LiveError::Stale));
        let after = y.state();
        assert!(y.rebuilt_from_state(&after).is_ok());
    }

    /// Measures the cost of an apply that stays held, with the buffer near the caps. Prints the
    /// worst and mean time per apply; asserts only a generous ceiling so a loaded machine does
    /// not flake. See the report for the numbers the caps were set from.
    #[test]
    fn held_buffer_worst_case_cost() {
        for (count, size) in [(MAX_PENDING_UPDATES - 1, 16), (64, MAX_PENDING_BYTES / 80), (MAX_PENDING_BYTES / 1024 - 1, 1024)] {
            let sizes: Vec<usize> = std::iter::repeat_n(size, count + 1).collect();
            let (first, rest, _) = withheld(&sizes);
            let (mut y, _) = peer(99);
            let (mut worst, mut total) = (std::time::Duration::ZERO, std::time::Duration::ZERO);
            for u in &rest {
                let t = std::time::Instant::now();
                y.apply(u).unwrap();
                let d = t.elapsed();
                worst = worst.max(d);
                total += d;
            }
            let t = std::time::Instant::now();
            y.apply(&first).unwrap();
            let release = t.elapsed();
            println!(
                "COST count {} size {size}: worst {worst:?} mean {:?} release {release:?} resync {}",
                rest.len(),
                total / u32::try_from(rest.len()).unwrap(),
                y.needs_resync()
            );
            assert!(worst.as_millis() < 2000 && release.as_millis() < 2000, "worst {worst:?} release {release:?}");
        }
    }

    fn varuint(mut n: u32, out: &mut Vec<u8>) {
        while n > 127 {
            out.push(u8::try_from(n & 127).unwrap() | 128);
            n >>= 7;
        }
        out.push(u8::try_from(n).unwrap());
    }

    /// Orphans from many distinct clients, each waiting on the same missing item: the held
    /// update names one client per orphan, which is what makes yrs's merge widest.
    #[test]
    fn held_buffer_cost_with_many_clients() {
        let (first, _, _) = withheld(&[2, 1]);
        let (mut y, _) = peer(99);
        let (mut worst, mut total, mut n) = (std::time::Duration::ZERO, std::time::Duration::ZERO, 0_u32);
        for i in 0..MAX_PENDING_UPDATES as u32 {
            let mut u = vec![1, 1];
            varuint(1000 + i, &mut u);
            u.extend([0, 0x04, 0, 1, 0, 1, b'a', 0]);
            let t = std::time::Instant::now();
            y.apply(&u).unwrap();
            let d = t.elapsed();
            worst = worst.max(d);
            total += d;
            n += 1;
            if y.needs_resync() {
                break;
            }
        }
        let t = std::time::Instant::now();
        y.apply(&first).unwrap();
        println!("COST clients {n}: worst {worst:?} mean {:?} release {:?} resync {}", total / n, t.elapsed(), y.needs_resync());
        assert!(worst.as_millis() < 2000);
    }

    /// Each update applies one block and leaves one waiting on a missing item. Counting only
    /// updates that advanced nothing would never trip the cap while yrs's pending grew.
    #[test]
    fn mixed_updates_that_half_apply_still_hit_the_cap() {
        let (_, _, _) = withheld(&[1, 1]);
        let (mut y, _) = peer(99);
        let start = std::time::Instant::now();
        let mut n = 0_u32;
        let mut worst = std::time::Duration::ZERO;
        while !y.needs_resync() && n < u32::try_from(MAX_PENDING_UPDATES).unwrap() + 10 {
            let mut u = vec![2, 1];
            varuint(2000, &mut u);
            varuint(n, &mut u);
            u.extend([0x04, 1, 1, b't', 1, b'a']);
            u.push(1);
            varuint(3000 + n, &mut u);
            u.extend([0, 0x04, 0, 1, 0, 1, b'a', 0]);
            let t = std::time::Instant::now();
            y.apply(&u).unwrap();
            worst = worst.max(t.elapsed());
            if n == 3 {
                assert!(pending(&y), "the orphan half is held");
                assert!(read(&y).len() >= 4, "the other half applied");
            }
            n += 1;
        }
        println!("COST mixed {n} updates: worst {worst:?} total {:?}", start.elapsed());
        assert!(y.needs_resync(), "raised before {n} updates");
        assert!(usize::try_from(n).unwrap() <= MAX_PENDING_UPDATES + 1);
        assert!(held_bytes(&y) < MAX_PENDING_BYTES);
        assert!(start.elapsed().as_secs() < 20, "took {:?}", start.elapsed());
    }

    /// Every update an honest peer makes, deletes and full states included, passes the relay's
    /// soundness check.
    #[test]
    fn honest_updates_are_sound() {
        for seed in SEEDS {
            let all = history(seed, 3, 60);
            for (i, u) in all.iter().enumerate() {
                assert!(update_is_sound(u), "seed {seed} update {i}");
            }
            assert!(update_is_sound(&in_order(&all).state()), "seed {seed} state");
        }
        let (_, rest, state) = withheld(&[1, 1, 1]);
        assert!(update_is_sound(&state));
        assert!(rest.iter().all(|u| update_is_sound(u)));
        assert!(update_is_sound(&[0, 0]), "the empty update");
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
