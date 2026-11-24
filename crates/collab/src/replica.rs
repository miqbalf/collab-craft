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
    /// [`apply`](Self::apply) for a checkpoint snapshot the room ordered, which holds the
    /// whole document: a schema on [`YReplica`] checks it with [`YReplica::apply_state`]'s
    /// higher item cap. By default the same as `apply`.
    fn apply_state(&mut self, update: &[u8]) -> Result<Vec<Self::Change>, LiveError> {
        self.apply(update)
    }
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
    /// Updates that wait for something this document lacks (an earlier clock of a client, an
    /// item they name), in arrival order. They are kept here rather than handed to yrs: yrs 0.28
    /// integrates an update past a gap in a client's clocks with a placeholder and then mixes up
    /// later updates that overlap it, and merges held updates with different content under the
    /// same clock (both panic). So yrs only ever sees an update it can integrate whole.
    /// Each with the id it waits for: it is looked at again once the state vector passes it.
    waiting: Vec<(Vec<u8>, (u64, u32))>,
    /// Parents of recently checked items, for the next check (see the `fits` module).
    seen: crate::fits::Seen,
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
        YReplica {
            undo,
            doc,
            client,
            damaged: false,
            untracked: false,
            resync: false,
            held: 0,
            held_bytes: 0,
            waiting: Vec::new(),
            seen: crate::fits::Seen::default(),
        }
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

    /// How many runs the replica's placement index holds (see the `fits` module).
    pub(crate) fn index_runs(&self) -> usize {
        self.seen.len()
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

    /// Whether updates are waiting for something this document lacks (see [`apply`](Self::apply)).
    #[must_use]
    pub fn is_waiting(&self) -> bool {
        !self.waiting.is_empty() || self.doc.transact().has_missing_updates()
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
        self.waiting.clone_from(&old.waiting);
    }

    /// Carry the damaged mark across a rebuild, or set it (a test of a caught panic).
    pub fn set_damaged(&mut self, damaged: bool) {
        self.damaged = damaged;
    }

    /// Merge an update from another replica. A yrs panic marks this replica damaged.
    ///
    /// Updates may arrive in any order. Each is first read and checked against this document
    /// (see [`integrate`]): one that puts content where its parent cannot hold it is refused
    /// with nothing changed. One that needs something this document lacks (an earlier clock of a
    /// client, an item it names, an item it deletes) waits here and is applied once that has
    /// landed; yrs only ever sees updates it can integrate whole. What waits is bounded by
    /// [`MAX_PENDING_UPDATES`] and [`MAX_PENDING_BYTES`]; past either it is dropped and
    /// [`needs_resync`](Self::needs_resync) is raised.
    ///
    /// Should yrs still hold something back, it is retried the same way: whenever an apply
    /// advanced the state vector, the held part is taken out ([`WriteTxn::prune_pending`]) and
    /// applied again, until nothing more lands.
    pub fn apply(&mut self, update: &[u8]) -> Result<(), LiveError> {
        self.apply_capped(update, crate::fits::MAX_ITEMS)
    }

    /// [`apply`](Self::apply) for a checkpoint snapshot (a full state the document service
    /// wrote, which the relay orders only from it): checked under the higher
    /// [`MAX_STATE_ITEMS`](crate::fits::MAX_STATE_ITEMS), because a checkpoint holds the whole
    /// document.
    pub fn apply_state(&mut self, update: &[u8]) -> Result<(), LiveError> {
        self.apply_capped(update, crate::fits::MAX_STATE_ITEMS)
    }

    fn apply_capped(&mut self, update: &[u8], cap: usize) -> Result<(), LiveError> {
        match readiness(&self.doc, update, &mut self.seen, cap) {
            Ready::Refused => return Err(LiveError::Decode),
            Ready::TooLarge => return Err(LiveError::TooLarge),
            Ready::Waiting(need) => {
                self.wait(update, need);
                return Ok(());
            }
            Ready::Now => {}
        }
        let applied = self.apply_now(update);
        if self.damaged {
            return applied;
        }
        self.release()?;
        if self.waiting.is_empty() && !self.doc.transact().has_missing_updates() {
            self.held = 0;
            self.held_bytes = 0;
        }
        applied
    }

    /// Keep `update` until what it needs has landed.
    fn wait(&mut self, update: &[u8], need: (u64, u32)) {
        self.waiting.push((update.to_vec(), need));
        self.held += 1;
        self.held_bytes += update.len();
        if self.held > MAX_PENDING_UPDATES || self.held_bytes > MAX_PENDING_BYTES {
            log::warn!(
                "collab: {} updates ({} bytes) are waiting for dependencies that did not come; dropped, resync needed",
                self.held,
                self.held_bytes
            );
            self.drop_held();
        }
    }

    /// Apply what waits and is ready now, until nothing more lands. One that is refused now (its
    /// parent turned out unable to hold it) is dropped.
    fn release(&mut self) -> Result<(), LiveError> {
        let mut seen = StateVector::default();
        loop {
            let now = self.doc.transact().state_vector();
            if self.waiting.is_empty() || now == seen {
                return Ok(());
            }
            let mut keep = Vec::new();
            for (u, need) in std::mem::take(&mut self.waiting) {
                // Only what the state vector has passed the need of can have changed.
                if now.get(&ClientID::new(need.0)) <= need.1 {
                    keep.push((u, need));
                    continue;
                }
                // Already read under its cap when it arrived.
                match readiness(&self.doc, &u, &mut self.seen, crate::fits::MAX_STATE_ITEMS) {
                    Ready::Waiting(need) => keep.push((u, need)),
                    Ready::Refused | Ready::TooLarge => log::warn!("collab: a waiting update does not fit the document; dropped"),
                    Ready::Now => {
                        let applied = self.apply_now(&u);
                        if self.damaged {
                            self.waiting = keep;
                            return applied;
                        }
                    }
                }
            }
            seen = now;
            self.waiting.extend(keep);
        }
    }

    /// Hand one ready update to yrs.
    fn apply_now(&mut self, update: &[u8]) -> Result<(), LiveError> {
        let before = self.doc.transact().state_vector();
        let first = integrate_checked(&self.doc, update, true, &mut self.seen, crate::fits::MAX_STATE_ITEMS);
        if first.is_ok() {
            self.seen.commit(&self.doc.transact());
        }
        if first == Err(Integrate::Panicked) {
            self.damaged = true;
            return Err(LiveError::Decode);
        }
        if first == Ok(true) {
            // Held updates that did not fit were dropped: behind, as after any dropped hold.
            self.resync = true;
        }
        let first = first.map(|_| ());
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
        first.map_err(LiveError::from)
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
        // The roots typed as this replica types them, so the fit check judges the state
        // against what it will be read as.
        let roots: Vec<(String, u8)> = self
            .doc
            .transact()
            .root_refs()
            .filter_map(|(name, out)| match out {
                yrs::Out::YArray(_) => Some((name.to_owned(), 0)),
                yrs::Out::YMap(_) => Some((name.to_owned(), 1)),
                yrs::Out::YText(_) => Some((name.to_owned(), 2)),
                yrs::Out::YXmlFragment(_) => Some((name.to_owned(), 3)),
                _ => None,
            })
            .collect();
        for (name, kind) in roots {
            match kind {
                0 => drop(doc.get_or_insert_array(name.as_str())),
                1 => drop(doc.get_or_insert_map(name.as_str())),
                2 => drop(doc.get_or_insert_text(name.as_str())),
                _ => drop(doc.get_or_insert_xml_fragment(name.as_str())),
            }
        }
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
                if self.waiting.is_empty() {
                    self.held = 0;
                    self.held_bytes = 0;
                }
                return Ok(());
            }
            let now = txn.state_vector();
            drop(txn);
            if now == before {
                break;
            }
            before = now;
            match integrate_held(&self.doc, &mut self.seen) {
                Ok(()) => {}
                Err(Integrate::Panicked) => return Err(Integrate::Panicked),
                Err(Integrate::Refused | Integrate::TooLarge) => {
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
        self.waiting.clear();
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
        let update = self.undo.undo_blocking().then(|| self.doc.transact().encode_diff_v1(&before))?;
        crate::fits::learn(&self.doc.transact(), &update, &before, &mut self.seen);
        Some(update)
    }

    /// Redo the last undone local edit and return the update to send. `None` on a damaged
    /// replica too.
    pub fn redo(&mut self) -> Option<Vec<u8>> {
        if self.damaged || !self.undo.can_redo() {
            return None;
        }
        let before = self.doc.transact().state_vector();
        let update = self.undo.redo_blocking().then(|| self.doc.transact().encode_diff_v1(&before))?;
        crate::fits::learn(&self.doc.transact(), &update, &before, &mut self.seen);
        Some(update)
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
        let before = self.doc.transact().state_vector();
        let update = {
            let mut txn = self.doc.transact_mut_with(if self.untracked { UNTRACKED } else { LOCAL });
            edit(&mut txn)?;
            txn.encode_update_v1()
        };
        crate::fits::learn(&self.doc.transact(), &update, &before, &mut self.seen);
        Ok(update)
    }
}

/// Why [`integrate`] failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Integrate {
    #[error("the update did not decode or apply")]
    Refused,
    #[error("the update is too large to check")]
    TooLarge,
    #[error("yrs panicked while applying the update")]
    Panicked,
}

impl From<Integrate> for LiveError {
    fn from(i: Integrate) -> LiveError {
        match i {
            Integrate::TooLarge => LiveError::TooLarge,
            _ => LiveError::Decode,
        }
    }
}

/// Decode and apply one update as a remote change.
///
/// yrs can panic on bytes that decode but do not describe a valid document, so the apply runs
/// under `catch_unwind`: a hostile or damaged update must not take the app down. That only holds
/// where panics unwind; a wasm build with `panic = "abort"` still aborts.
///
/// Before yrs sees it, the update is read by the gate's byte reader and every item is checked
/// against the document for a parent that can hold its content (see the `fits` module): yrs
/// takes a string inside an array and then loops for ever reading that array. Refused: an update
/// the reader does not take, and one with an item that does not fit. When the only misfit is in
/// what yrs already holds for missing dependencies, that held part is dropped (yrs would apply it
/// along with this update) and this update is applied.
pub fn integrate(doc: &Doc, update: &[u8]) -> Result<(), Integrate> {
    integrate_checked(doc, update, false, &mut crate::fits::Seen::default(), crate::fits::MAX_STATE_ITEMS).map(|_| ())
}

/// [`integrate`]; `Ok(true)` when held updates were dropped because they did not fit.
/// `checked`: the update alone was already checked against this document ([`readiness`]), so
/// it is only checked again together with what yrs holds, if anything.
fn integrate_checked(doc: &Doc, update: &[u8], checked: bool, seen: &mut crate::fits::Seen, cap: usize) -> Result<bool, Integrate> {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // The walk first: it refuses counts the bytes cannot back (an allocation bomb, which
        // aborts rather than unwinds) before yrs's decoder sizes anything by them.
        let incoming = match crate::walk::items_capped(update, cap) {
            crate::walk::Walked::Items(_, items) => items,
            crate::walk::Walked::Unread => return Err(Integrate::Refused),
            crate::walk::Walked::TooMany => {
                crate::fits::too_many(cap);
                return Err(Integrate::TooLarge);
            }
        };
        let decoded = Update::decode_v1(update).map_err(|_| Integrate::Refused)?;
        let mut txn = doc.transact_mut_with(REMOTE);
        let mut dropped = false;
        let held = txn.store().pending_update().is_some();
        let verdict = if checked && !held { crate::fits::Verdict::Fits } else { crate::fits::check(&txn, &with_held(&txn, &incoming), seen) };
        match verdict {
            crate::fits::Verdict::Refused if crate::fits::check(&txn, &incoming, seen) == crate::fits::Verdict::Refused => {
                log::warn!("collab: an update puts content where its parent cannot hold it; refused");
                return Err(Integrate::Refused);
            }
            crate::fits::Verdict::Refused => {
                log::warn!("collab: held updates put content where its parent cannot hold it; dropped");
                drop(txn.prune_pending());
                dropped = true;
            }
            _ => {}
        }
        let before = txn.state_vector();
        let applied = txn.apply_update(decoded);
        // What landed goes into the index: all of it when the check staged it, and whatever yrs
        // took of a refused update otherwise (so the next check does not miss it).
        if applied.is_ok() && (!checked || held) {
            seen.commit(&txn);
        } else if applied.is_err() {
            crate::fits::learn(&txn, update, &before, seen);
        }
        applied.map_err(|_| Integrate::Refused)?;
        Ok(dropped)
    }));
    match outcome {
        Ok(result) => result,
        Err(_) => {
            log::warn!("collab: yrs panicked on an update; it was refused");
            Err(Integrate::Panicked)
        }
    }
}

/// Whether an update can go to yrs now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ready {
    Now,
    /// It needs something the document lacks: the state vector to pass this id.
    Waiting((u64, u32)),
    /// It does not read, or does not fit the document.
    Refused,
    /// It has more items than one check takes.
    TooLarge,
}

/// Read `update` and check it against `doc`: [`Ready::Refused`] when the reader does not take it
/// or an item does not fit its parent (see the `fits` module), [`Ready::Waiting`] when it starts
/// past a client's clock in `doc`, skips clocks, names an item `doc` lacks, or deletes past what
/// `doc` and the update hold.
fn readiness(doc: &Doc, update: &[u8], seen: &mut crate::fits::Seen, cap: usize) -> Ready {
    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (shape, items) = match crate::walk::items_capped(update, cap) {
            crate::walk::Walked::Items(shape, items) => (shape, items),
            crate::walk::Walked::Unread => return Ready::Refused,
            crate::walk::Walked::TooMany => {
                crate::fits::too_many(cap);
                return Ready::TooLarge;
            }
        };
        let txn = doc.transact();
        let fits = crate::fits::check(&txn, &items, seen);
        if fits == crate::fits::Verdict::Refused {
            log::warn!("collab: an update puts content where its parent cannot hold it; refused");
            return Ready::Refused;
        }
        let sv = txn.state_vector();
        let mut reach: BTreeMap<u64, u32> = BTreeMap::new();
        for (client, ranges) in &shape.blocks {
            let mut have = sv.get(&ClientID::new(*client));
            for (start, end) in ranges {
                if *start > have {
                    return Ready::Waiting((*client, start - 1));
                }
                have = have.max(*end);
            }
            reach.insert(*client, have);
        }
        for (client, ranges) in &shape.deletes {
            let have = reach.get(client).copied().unwrap_or_else(|| sv.get(&ClientID::new(*client)));
            if let Some((_, end)) = ranges.iter().find(|(_, end)| *end > have) {
                return Ready::Waiting((*client, end - 1));
            }
        }
        match fits {
            crate::fits::Verdict::Waiting(id) => Ready::Waiting(id),
            _ => Ready::Now,
        }
    }));
    checked.unwrap_or(Ready::Refused)
}

/// `incoming` and the items yrs holds for missing dependencies (yrs applies those along with
/// the next update that releases them, so they are checked with it).
fn with_held<T: ReadTxn>(txn: &T, incoming: &[crate::walk::Placed]) -> Vec<crate::walk::Placed> {
    let mut all = incoming.to_vec();
    if let Some(held) = txn.store().pending_update() {
        use yrs::updates::encoder::Encode;
        // yrs's own encoding of what it holds: the reader takes it, or nothing is added.
        if let Some((_, items)) = crate::walk::items(&held.update.encode_v1()) {
            all.extend(items);
        }
    }
    all
}

/// The most blocks a peer's update may write: past it, the update is refused as too large to
/// check (and not as invalid). See the `fits` module for how it was sized.
pub const MAX_OP_BLOCKS: usize = crate::fits::MAX_ITEMS;

/// The most blocks a checkpoint snapshot from the document service may write.
pub const MAX_STATE_BLOCKS: usize = crate::fits::MAX_STATE_ITEMS;

/// How many blocks `bytes` write, read without allocating per block, with each client section
/// and each delete-set client and range counted as one more (they cost a reader a map entry
/// each, as a block does); `None` when the byte reader does not take them. For a relay to refuse an update past [`MAX_OP_BLOCKS`] as too
/// large before anything decodes it.
#[must_use]
pub fn update_blocks(bytes: &[u8]) -> Option<usize> {
    std::panic::catch_unwind(|| crate::walk::read(bytes).map(|shape| shape.count)).ok().flatten()
}

/// Whether `bytes` count more than `cap` entries as [`update_blocks`] counts them, read only
/// as far as the cap: a relay's check before it decodes anything, which keeps no more than the
/// cap's worth of the read's own maps however many sections or delete ranges the bytes carry.
/// `false` when the byte reader does not take them before the cap (the gate refuses those).
#[must_use]
pub fn update_exceeds_blocks(bytes: &[u8], cap: usize) -> bool {
    std::panic::catch_unwind(|| matches!(crate::walk::read_capped(bytes, cap), crate::walk::Counted::Over)).unwrap_or(false)
}

/// Whether `bytes` are a yrs update (v1) that decodes and whose ids are sane: no block or delete
/// range runs past `u32::MAX`. For a relay to check an op before it orders it, so a crafted
/// update never reaches a browser replica, where a yrs panic aborts the page.
///
/// Refused: bytes that do not decode, a clock, block end or id past 2^31 (anything below that
/// cannot wrap a `u32`), a client's blocks adding up past it, a client with two sections, forms
/// the byte reader and yrs would read differently (JSON content, the XmlHook type), anything the
/// byte reader does not understand, and any update where yrs's parse differs from the reader's
/// (see the `walk` module). Runs under `catch_unwind`, so a panic while decoding is a refusal.
/// This is a check of the update on its own; whether it fits a document's history is only known
/// to a replica.
#[must_use]
pub fn update_is_sound(bytes: &[u8]) -> bool {
    std::panic::catch_unwind(|| {
        // The walk reads the bytes itself, because nothing yrs exposes can see a clock that
        // wrapped; yrs's decode then has to accept the update and agree with the walk on it.
        let Some(shape) = crate::walk::read(bytes) else { return false };
        Update::decode_v1(bytes).is_ok_and(|update| crate::walk::agrees(&shape, &update))
    })
    .unwrap_or(false)
}

/// Take what yrs holds for missing dependencies out and apply it again, under `catch_unwind`
/// for the same reason as [`integrate`]. What still lacks a dependency goes back to being held.
fn integrate_held(doc: &Doc, seen: &mut crate::fits::Seen) -> Result<(), Integrate> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut txn = doc.transact_mut_with(REMOTE);
        match txn.prune_pending() {
            Some(held) => {
                // The same check as `integrate`: what was held may now resolve to a parent that
                // cannot hold it.
                let bytes = {
                    use yrs::updates::encoder::Encode;
                    held.encode_v1()
                };
                match crate::walk::items(&bytes) {
                    Some((_, items)) if crate::fits::check(&txn, &items, seen) != crate::fits::Verdict::Refused => {
                        let applied = txn.apply_update(held).map_err(|_| Integrate::Refused);
                        seen.commit(&txn);
                        applied
                    }
                    _ => {
                        log::warn!("collab: held updates put content where its parent cannot hold it");
                        Err(Integrate::Refused)
                    }
                }
            }
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
        y.is_waiting()
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

    /// A waiting update that yrs then refuses (it names a string item as its parent) is dropped
    /// alone: the honest updates waiting beside it land, nothing is left held, no resync is
    /// needed (every replica drops it the same way), nothing panics or is damaged.
    #[test]
    fn a_waiting_update_that_does_not_apply_is_dropped_alone() {
        let (first, rest, state) = withheld(&[2, 1, 1]);
        let (mut y, _) = peer(99);
        y.apply(&child_of(5, 1)).unwrap();
        y.apply(&rest[0]).unwrap();
        assert!(pending(&y));
        assert!(!y.needs_resync());
        assert!(y.apply(&first).is_ok(), "the dependency did apply");
        for u in &rest[1..] {
            y.apply(u).unwrap();
        }
        assert!(!y.needs_resync());
        assert!(!y.damaged());
        assert!(!pending(&y), "nothing is left held");
        let (author, _) = peer(98);
        integrate(author.doc(), &state).unwrap();
        assert_eq!(read(&y), read(&author));
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
    /// worst and mean time per apply; asserts a generous ceiling. See the report for the
    /// numbers the caps were set from.
    ///
    /// Wall time on a shared machine is noisy: another job can stall one run for seconds. So
    /// each case is run up to [`ATTEMPTS`] times on a fresh replica and judged on its best run
    /// (the least worst-apply and the least release among them). The bound stays real: a
    /// regression past it is slow on every run, and no run passes it.
    #[test]
    fn held_buffer_worst_case_cost() {
        const ATTEMPTS: usize = 5;
        const BOUND: std::time::Duration = std::time::Duration::from_secs(2);
        for (count, size) in [(MAX_PENDING_UPDATES - 1, 16), (64, MAX_PENDING_BYTES / 80), (MAX_PENDING_BYTES / 1024 - 1, 1024)] {
            let sizes: Vec<usize> = std::iter::repeat_n(size, count + 1).collect();
            let (first, rest, _) = withheld(&sizes);
            let (mut best_worst, mut best_release) = (std::time::Duration::MAX, std::time::Duration::MAX);
            for attempt in 1..=ATTEMPTS {
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
                    "COST count {} size {size} run {attempt}: worst {worst:?} mean {:?} release {release:?} resync {}",
                    rest.len(),
                    total / u32::try_from(rest.len()).unwrap(),
                    y.needs_resync()
                );
                best_worst = best_worst.min(worst);
                best_release = best_release.min(release);
                if best_worst < BOUND && best_release < BOUND {
                    break;
                }
            }
            assert!(best_worst < BOUND && best_release < BOUND, "best of {ATTEMPTS}: worst {best_worst:?} release {best_release:?}");
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

    /// Each update has one block that could apply and one that waits on a missing item. The
    /// whole update waits (yrs never sees half of one), and the cap still trips.
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
                assert!(pending(&y), "the updates wait");
                assert!(read(&y).is_empty(), "nothing of a waiting update lands");
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

    /// The byte walk and yrs agree on how many clocks each client's blocks span, for documents
    /// that use every content type yrs writes here: text with attributes and embeds, maps and
    /// arrays of nested values, deletes, and full states.
    #[test]
    fn the_walk_agrees_with_yrs_on_rich_documents() {
        use yrs::types::Attrs;
        use yrs::{Any, Array, Map};
        let doc = build_doc(7);
        let text = doc.get_or_insert_text("t");
        let map = doc.get_or_insert_map("m");
        let arr = doc.get_or_insert_array("a");
        let mut updates = Vec::new();
        let mut keep = |doc: &Doc, from: &StateVector| updates.push(doc.transact().encode_diff_v1(from));
        let bold: Attrs = [("b".into(), Any::Bool(true))].into_iter().collect();
        let nested = Any::Map(Arc::new(std::collections::HashMap::from([
            ("n".to_owned(), Any::Number(Number::Int(-3))),
            ("f".to_owned(), Any::Number(Number::Float(1.5))),
            ("s".to_owned(), Any::from("h\u{e9}llo \u{1f600}")),
            ("l".to_owned(), Any::Array(Arc::from(vec![Any::Null, Any::Undefined, Any::Bool(false), Any::from(vec![1u8, 2])]))),
        ])));
        type Step = Box<dyn Fn(&mut yrs::TransactionMut)>;
        let steps: Vec<Step> = vec![
            Box::new({
                let (text, bold) = (text.clone(), bold.clone());
                move |t| {
                    text.insert_with_attributes(t, 0, "na\u{ef}ve \u{1f600} text", bold.clone());
                    text.insert_embed(t, 2, Any::from("img"));
                }
            }),
            Box::new({
                let (map, nested) = (map.clone(), nested.clone());
                move |t| {
                    map.insert(t, "k", nested.clone());
                    map.insert(t, "k", 5);
                    map.insert(t, "sub", yrs::MapPrelim::from([("x", 1)]));
                }
            }),
            Box::new({
                let (arr, nested) = (arr.clone(), nested.clone());
                move |t| {
                    arr.insert(t, 0, nested.clone());
                    arr.insert(t, 1, yrs::TextPrelim::new("in array"));
                    arr.insert(t, 2, yrs::ArrayPrelim::from([1, 2, 3]));
                }
            }),
            Box::new({
                let text = text.clone();
                move |t| {
                    text.remove_range(t, 1, 4);
                    text.format(t, 0, 3, [("i".into(), Any::Bool(true))].into_iter().collect());
                }
            }),
            Box::new({
                let arr = arr.clone();
                move |t| arr.remove_range(t, 0, 1)
            }),
        ];
        for step in &steps {
            let from = doc.transact().state_vector();
            step(&mut doc.transact_mut());
            keep(&doc, &from);
        }
        updates.push(doc.transact().encode_state_as_update_v1(&StateVector::default()));
        for (i, u) in updates.iter().enumerate() {
            let shape = crate::walk::read(u).unwrap_or_else(|| panic!("update {i} refused"));
            let yrs_update = Update::decode_v1(u).unwrap();
            let inserted = yrs_update.insertions(true);
            for (client, ranges) in inserted.iter() {
                let total: u64 = ranges.iter().map(|r| u64::from(r.end - r.start)).sum();
                assert_eq!(shape.totals().get(&client.get()), Some(&total), "update {i} client {client:?}");
            }
            assert!(crate::walk::agrees(&shape, &yrs_update), "update {i}");
            assert!(update_is_sound(u), "update {i}");
        }
    }

    /// Byte patterns that make a block's clock wrap past the first block of a section, which
    /// yrs's own id ranges drop without a trace, and a client given two sections.
    #[test]
    fn overflow_after_the_first_block_is_refused() {
        let item = [0x04, 1, 1, b't', 1, b'a'];
        let huge = [0xf8, 0xff, 0xff, 0xff, 0x0f];
        // GC block of 0xFFFFFFF8 clocks, then a string item.
        let mut gc = vec![1, 2, 1, 0, 0x00];
        gc.extend(huge);
        gc.extend(item);
        gc.push(0);
        // The same with a Skip block.
        let mut skip = vec![1, 2, 1, 0, 0x0a];
        skip.extend(huge);
        skip.extend(item);
        skip.push(0);
        // A second section for client 1.
        let mut twice = vec![2, 1, 1, 0];
        twice.extend(item);
        twice.extend([1, 1, 0]);
        twice.extend(item);
        twice.push(0);
        // Small blocks that add up past the ceiling: GC of 2^31 - 1, then two clocks more.
        let mut add_up = vec![1, 3, 1, 0, 0x00, 0xff, 0xff, 0xff, 0xff, 0x07];
        add_up.extend(item);
        add_up.extend(item);
        add_up.push(0);
        for (name, bytes) in [("gc", &gc), ("skip", &skip), ("twice", &twice), ("add up", &add_up)] {
            assert!(Update::decode_v1(bytes).is_ok() || name == "gc" || name == "skip", "{name} should reach the walk as bytes yrs takes");
            assert!(!update_is_sound(bytes), "{name} passed");
        }
        // The honest shapes of the same bytes pass.
        let mut fine = vec![1, 2, 1, 0, 0x00, 5];
        fine.extend(item);
        fine.push(0);
        assert!(update_is_sound(&fine));
    }

    /// The two byte patterns from re-review 3 (I7): bytes yrs takes, where the round 3 walk read
    /// one thing and yrs another, hiding a garbage-collected block whose clock wraps.
    fn desync_patterns() -> [(&'static str, Vec<u8>); 2] {
        // JSON content with a count of 0: yrs reads one string (the walk's next section header),
        // then a section whose second GC block, inside the walk's binary payload, is 0xFFFFFFF8
        // long at clock 21.
        let mut json = vec![2, 1, 5, 0, 0x02, 1, 1, b't', 0, 1, 0x50, 2, 0x03, 1, 0, 20];
        let mut payload = vec![0x00, 0xf8, 0xff, 0xff, 0xff, 0x0f, 0x00];
        payload.resize(20, 0);
        json.extend(payload);
        json.push(0);
        // An XmlHook type: the walk read a hook name that yrs reads as a section of two blocks,
        // a deleted item 0xF7EFC7F4 long and a GC block 0xF0000870 long after it. The name bytes
        // are valid UTF-8 (U+10FFFF, U+10000).
        let mut hook = vec![2, 1, 5, 0, 0x07, 1, 1, b't', 5, 2, 0x09, 0x00];
        hook.extend([1, 1, 3, 0x04, 1, 11, 0xf4, 0x8f, 0xbf, 0xbf, 0x0f, 0x00, 0xf0, 0x90, 0x80, 0x80, 0x0f, 0]);
        hook.push(0);
        [("json", json), ("xml hook", hook)]
    }

    /// I7: JSON content and the XmlHook type are refused, and yrs really does take those bytes
    /// with a wrapping clock in them.
    #[test]
    fn json_and_xml_hook_desyncs_are_refused() {
        for (name, bytes) in desync_patterns() {
            let update = Update::decode_v1(&bytes).unwrap_or_else(|e| panic!("{name}: yrs refused it: {e}"));
            // yrs's parse has a different shape from anything the walk could report: client 3
            // (json) or 9 (hook) only exists in yrs's reading.
            assert!(update.state_vector_lower().iter().any(|(c, _)| c.get() == 3 || c.get() == 9), "{name}: {update:?}");
            assert!(crate::walk::read(&bytes).is_none(), "{name}: the walk took it");
            assert!(!update_is_sound(&bytes), "{name} passed");
        }
    }

    /// The agreement check on its own: had the walk been fooled the way round 3's was, the
    /// difference with yrs's parse refuses the update.
    #[test]
    fn a_parse_that_differs_from_yrs_is_refused() {
        let [(_, json), (_, hook)] = desync_patterns();
        // What round 3's walk saw: client 5 an empty JSON item, client 0x50 one binary item at 2.
        let json_view = crate::walk::Shape { blocks: [(0x50, vec![(2, 3)])].into(), ..Default::default() };
        assert!(!crate::walk::agrees(&json_view, &Update::decode_v1(&json).unwrap()));
        // Client 5 one type at 0, client 1 an empty string item at 3.
        let hook_view = crate::walk::Shape { blocks: [(5, vec![(0, 1)])].into(), upper: [(5, 1)].into(), ..Default::default() };
        assert!(!crate::walk::agrees(&hook_view, &Update::decode_v1(&hook).unwrap()));
        // And a correct view of an honest update agrees.
        let mut fine = vec![1, 2, 1, 0, 0x00, 5];
        fine.extend([0x04, 1, 1, b't', 1, b'a']);
        fine.push(0);
        let shape = crate::walk::read(&fine).unwrap();
        assert_eq!(shape.blocks, [(1, vec![(0, 6)])].into());
        assert!(crate::walk::agrees(&shape, &Update::decode_v1(&fine).unwrap()));
    }

    /// Integers yrs reads at a narrower width than they are written: a long form, or a value past
    /// `u32`, reads differently (yrs wraps its shifts), so only the shortest form is taken.
    #[test]
    fn long_or_wide_varints_are_refused() {
        let item = [0x04, 1, 1, b't', 1, b'a'];
        let with = |head: &[u8]| {
            let mut u = head.to_vec();
            u.extend(item);
            u.push(0);
            u
        };
        assert!(update_is_sound(&with(&[1, 1, 1, 0])));
        // The clock 0 written in two bytes.
        assert!(!update_is_sound(&with(&[1, 1, 1, 0x80, 0x00])));
        // A block count of 2^32 + 1, which yrs reads as 1.
        assert!(!update_is_sound(&with(&[1, 0x81, 0x80, 0x80, 0x80, 0x10, 1, 0])));
        // Parent info 2^32 + 1, which yrs reads as 1 (named) where a wider reader reads an id.
        assert!(!update_is_sound(&[1, 1, 1, 0, 0x04, 0x81, 0x80, 0x80, 0x80, 0x10, 1, b't', 1, b'a', 0]));
    }

    /// Seeds for yrs issues known to panic or abort on crafted input (y-crdt #675/#750, #720,
    /// #733, #743, #749): the gate refuses each before yrs reads it, and fast, since an
    /// allocation bomb aborts rather than unwinds and no `catch_unwind` would help.
    #[test]
    fn known_yrs_crashers_are_refused_before_yrs_reads_them() {
        let item = |info: u8, content: &[u8]| {
            let mut u = vec![1, 1, 5, 0, info, 1, 1, b't'];
            u.extend(content);
            u.push(0);
            u
        };
        let t = std::time::Instant::now();
        let cases: Vec<(&str, Vec<u8>)> = vec![
            // #675/#750: counts the bytes cannot back (4e9 sections, 4e9 blocks, 4e9 Any values,
            // a 4 GB string, 4e9 delete-set clients).
            ("sections", vec![0xff, 0xff, 0xff, 0xff, 0x0f, 1, 5, 0]),
            ("blocks", vec![1, 0xff, 0xff, 0xff, 0xff, 0x0f, 5, 0, 0x04]),
            ("any values", item(0x08, &[0xff, 0xff, 0xff, 0xff, 0x0f, 0x7f])),
            ("string", item(0x04, &[0xff, 0xff, 0xff, 0xff, 0x0f, b'a'])),
            ("delete set", vec![0, 0xff, 0xff, 0xff, 0xff, 0x0f, 1, 1, 0, 1]),
            // #720: content ref 18 (info 0x12, JSON once masked), which applies and then panics
            // at the next encode.
            ("content ref 18", item(0x12, &[1, 1, b'1'])),
            // #733: a weak link type (type ref 7), whose splice panics (and is only built with
            // yrs's `weak` feature, which no crate here turns on).
            ("weak link", item(0x07, &[7])),
            // #743: a client id past 2^53.
            ("client id", vec![1, 1, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x10, 0, 0x04, 1, 1, b't', 1, b'a', 0]),
            // #749's cousin in values: an Any nested 100 deep.
            ("deep any", {
                let mut c = vec![1];
                c.extend(std::iter::repeat_n([0x75, 1], 100).flatten());
                c.push(0x7f);
                item(0x08, &c)
            }),
        ];
        for (name, bytes) in &cases {
            assert!(!update_is_sound(bytes), "{name} passed the gate");
            let doc = YReplica::seed_doc();
            assert!(integrate(&doc, bytes).is_err(), "{name} was integrated");
        }
        assert!(t.elapsed() < std::time::Duration::from_millis(500), "the refusals took {:?}", t.elapsed());
    }

    fn varint(out: &mut Vec<u8>, mut n: u64) {
        while n >= 0x80 {
            out.push((n as u8 & 0x7f) | 0x80);
            n >>= 7;
        }
        out.push(n as u8);
    }

    /// An update of millions of two-byte blocks (16 MiB of GC blocks of one clock each, which
    /// fit anywhere) is refused as too large to check, fast and without listing a block: the
    /// walk counts them first. A relay counts them the same way before it decodes anything.
    #[test]
    fn a_flood_of_tiny_blocks_is_refused_as_too_large_before_it_is_listed() {
        const BLOCKS: u64 = 8 << 20;
        let mut bomb = vec![1];
        varint(&mut bomb, BLOCKS);
        bomb.extend([0x7f, 0]);
        for _ in 0..BLOCKS {
            bomb.extend([0, 1]);
        }
        bomb.push(0);
        assert!(bomb.len() >= 16 << 20);
        let doc = YReplica::seed_doc();
        doc.get_or_insert_array("a");
        let mut y = YReplica::assemble(doc, None, |_, _| {});
        crate::walk::LISTED.with(|n| n.set(0));
        let t = std::time::Instant::now();
        assert_eq!(y.apply(&bomb), Err(LiveError::TooLarge));
        let took = t.elapsed();
        assert_eq!(y.apply_state(&bomb), Err(LiveError::TooLarge), "past the state cap too");
        assert_eq!(integrate(&YReplica::seed_doc(), &bomb), Err(Integrate::TooLarge));
        assert_eq!(crate::walk::LISTED.with(std::cell::Cell::get), 0, "blocks were listed");
        assert_eq!(update_blocks(&bomb), Some(BLOCKS as usize + 1), "the blocks and their one section");
        // About 1.1 s in debug on a build host at load 25 (88c15ed and after); listing the
        // 8 M blocks would cost far more, and gigabytes.
        assert!(took < std::time::Duration::from_secs(2), "the refusals took {took:?}");
        assert_eq!(LiveError::from(Integrate::TooLarge), LiveError::TooLarge);
    }

    /// R3-M2: client sections and delete-set entries count toward the cap, as blocks do. An
    /// update of one GC block in each of 600 k client sections writes 600 k blocks, under the
    /// cap, but costs a reader 1.2 M entries; a delete set of 1.1 M ranges writes no block at
    /// all. Both are refused as too large, before anything is listed.
    #[test]
    fn client_sections_and_delete_entries_count_toward_the_cap() {
        const SECTIONS: u64 = 600_000;
        let mut sections = Vec::new();
        varint(&mut sections, SECTIONS);
        for client in 0..SECTIONS {
            sections.push(1);
            varint(&mut sections, client + 1);
            sections.extend([0, 0, 1]);
        }
        sections.push(0);
        const RANGES: u64 = (1 << 20) + 50_000;
        let mut deletes = vec![0, 1, 7];
        varint(&mut deletes, RANGES);
        for i in 0..RANGES {
            varint(&mut deletes, 2 * i);
            deletes.push(1);
        }
        assert_eq!(update_blocks(&sections), Some(2 * SECTIONS as usize));
        assert_eq!(update_blocks(&deletes), Some(1 + RANGES as usize));
        for (name, bytes) in [("sections", &sections), ("deletes", &deletes)] {
            let mut y = YReplica::assemble(YReplica::seed_doc(), None, |_, _| {});
            crate::walk::LISTED.with(|n| n.set(0));
            assert_eq!(y.apply(bytes), Err(LiveError::TooLarge), "{name}");
            assert_eq!(crate::walk::LISTED.with(std::cell::Cell::get), 0, "{name}: blocks were listed");
        }
    }

    /// R3-M2: a client section with no blocks (yrs never writes one) is refused: a flood of
    /// them would cost a map entry each for nothing.
    #[test]
    fn a_client_section_with_no_blocks_is_refused() {
        let empty = [1, 0, 5, 0, 0];
        assert!(!update_is_sound(&empty));
        assert_eq!(update_blocks(&empty), None);
        let mut y = YReplica::assemble(YReplica::seed_doc(), None, |_, _| {});
        assert_eq!(y.apply(&empty), Err(LiveError::Decode));
        // An empty update (no sections, no deletes) is still taken.
        assert!(update_is_sound(&[0, 0]));
    }

    /// C7b review M-2: the counting read stops as soon as the count passes the cap, before it
    /// keeps anything more per section. 600 k one-block sections pass 2^20 entries about
    /// halfway; bytes after that point are never read, so a tail the reader would refuse
    /// does not turn "too large" into "unreadable", and nothing is listed.
    #[test]
    fn the_counting_read_stops_at_the_cap() {
        const SECTIONS: u64 = 600_000;
        let mut bytes = Vec::new();
        varint(&mut bytes, SECTIONS);
        for client in 0..SECTIONS {
            bytes.push(1);
            varint(&mut bytes, client + 1);
            bytes.extend([0, 0, 1]);
        }
        // Not a delete set: the reader refuses this tail if it gets there.
        bytes.extend([0xff, 0xff, 0xff]);
        crate::walk::LISTED.with(|n| n.set(0));
        assert!(matches!(crate::walk::items_capped(&bytes, crate::fits::MAX_ITEMS), crate::walk::Walked::TooMany), "the read went past the cap");
        assert_eq!(crate::walk::LISTED.with(std::cell::Cell::get), 0);
        let mut y = YReplica::assemble(YReplica::seed_doc(), None, |_, _| {});
        assert_eq!(y.apply(&bytes), Err(LiveError::TooLarge));
    }

    /// How many blocks the largest honest updates write, against the caps. A 1 MiB offline
    /// bundle of single letters each typed apart from the last (the densest a client makes:
    /// none merge), and a 1 MiB seed of short paragraphs (a paste of a whole file).
    #[test]
    #[ignore = "measurement (about 90 s in debug); run with --ignored --nocapture"]
    fn honest_updates_stay_far_below_the_item_caps() {
        use yrs::{ReadTxn, Text};
        let typist = yrs::Doc::with_client_id(5);
        let t = typist.get_or_insert_text("t");
        {
            let mut txn = typist.transact_mut();
            // At the front each time: no two letters merge into one item.
            for _ in 0..(1 << 20) / 8 {
                t.insert(&mut txn, 0, "x");
            }
        }
        let bundle = typist.transact().encode_state_as_update_v1(&yrs::StateVector::default());
        let paragraphs: Vec<crate::SeedBlock> =
            (0..(1 << 20) / 24).map(|i| crate::SeedBlock { id: i + 1, text: format!("paragraph {i:>8}"), ..crate::SeedBlock::default() }).collect();
        let seed = crate::LiveDoc::seeded(&paragraphs).unwrap().state();
        for (name, update) in [("offline letters", &bundle), ("seeded paragraphs", &seed)] {
            let blocks = update_blocks(update).unwrap();
            let per_mib = blocks as f64 / (update.len() as f64 / f64::from(1 << 20));
            println!("C7 {name}: {} bytes, {blocks} blocks, {per_mib:.0} per MiB", update.len());
            assert!(update.len() >= 1 << 19, "{name} is only {} bytes", update.len());
            assert!(per_mib < crate::fits::MAX_ITEMS as f64 / 2.0, "{name}: {per_mib:.0} blocks per MiB");
            assert!(per_mib * 16.0 < crate::fits::MAX_STATE_ITEMS as f64, "{name}: a 16 MiB checkpoint this dense");
        }
    }

    /// The fit check costs about the same per item however far into a long array an op lands:
    /// it reads each neighbour's place from an index, not by walking the array to it (which
    /// was O(position) per neighbour, so an op of many items placed deep in a large array
    /// cost seconds).
    #[test]
    fn the_fit_check_does_not_walk_the_document_per_item() {
        use yrs::{Array, ReadTxn};
        const LONG: u64 = 40_000;
        const OP: u64 = 10_000;
        let raw = yrs::Doc::with_client_id(5);
        let a = raw.get_or_insert_array("a");
        {
            let mut txn = raw.transact_mut();
            // Each at the front, so none merge: item k sits right of item k + 1.
            for _ in 0..LONG {
                a.insert(&mut txn, 0, true);
            }
        }
        let doc = YReplica::seed_doc();
        doc.get_or_insert_array("a");
        let mut y = YReplica::assemble(doc, None, |_, _| {});
        y.apply(&raw.transact().encode_state_as_update_v1(&yrs::StateVector::default())).unwrap();
        // One op of OP items from client 9, each between item k + 1 and item k, deep in.
        let mut op = vec![1];
        varint(&mut op, OP);
        op.extend([9, 0]);
        for k in 0..OP {
            op.push(0xc8);
            for (client, clock) in [(5, k + 1), (5, k)] {
                varint(&mut op, client);
                varint(&mut op, clock);
            }
            op.extend([1, 0x7e]);
        }
        op.push(0);
        let t = std::time::Instant::now();
        y.apply(&op).unwrap();
        let took = t.elapsed();
        assert_eq!(y.doc().get_or_insert_array("a").len(&y.doc().transact()), (LONG + OP) as u32);
        assert!(took < std::time::Duration::from_secs(1), "the op took {took:?}");
    }

    /// A full state whose origin chains run far past the nesting cap is judged, not left
    /// waiting: the cap is on how deep types nest, not on how long a chain is. A document
    /// edited in turn by many sessions (each a new client id) makes one: each letter typed at
    /// the front names the previous session's as its right origin, so the chain crosses from
    /// one client's run of the update into the next, in whichever order yrs writes them.
    #[test]
    fn long_origin_chains_in_one_update_are_judged() {
        use yrs::updates::decoder::Decode;
        for ids in [(100..180).collect::<Vec<u64>>(), (100..180).rev().collect()] {
            let mut state = Vec::new();
            for id in ids {
                let peer = yrs::Doc::with_client_id(id);
                if !state.is_empty() {
                    peer.transact_mut().apply_update(yrs::Update::decode_v1(&state).unwrap()).unwrap();
                }
                peer.get_or_insert_text("t").insert(&mut peer.transact_mut(), 0, "x");
                state = peer.transact().encode_state_as_update_v1(&yrs::StateVector::default());
            }
            let (mut y, t) = replica(None);
            y.apply(&state).unwrap();
            assert!(!y.is_waiting(), "the state waits");
            assert_eq!(t.get_string(&y.doc().transact()).len(), 80);
        }
    }

    /// Edits of every kind the Craft apps make, on three peers that sync now and then.
    #[derive(Debug, Clone)]
    enum Edit {
        Text { at: u16, s: u8, bold: bool },
        Embed { at: u16 },
        Cut { at: u16, n: u8 },
        Format { at: u16, n: u8 },
        MapSet { key: u8, v: u8 },
        MapNested { key: u8, kind: u8 },
        MapCut { key: u8 },
        ArrayPut { at: u16, v: u8 },
        ArrayCut { at: u16, n: u8 },
        Inner { at: u16, s: u8 },
        Sync { from: u8 },
    }

    fn edit() -> impl proptest::strategy::Strategy<Value = (u8, Edit)> {
        use proptest::prelude::*;
        let e = prop_oneof![
            (any::<u16>(), any::<u8>(), any::<bool>()).prop_map(|(at, s, bold)| Edit::Text { at, s, bold }),
            any::<u16>().prop_map(|at| Edit::Embed { at }),
            (any::<u16>(), any::<u8>()).prop_map(|(at, n)| Edit::Cut { at, n }),
            (any::<u16>(), any::<u8>()).prop_map(|(at, n)| Edit::Format { at, n }),
            (any::<u8>(), any::<u8>()).prop_map(|(key, v)| Edit::MapSet { key, v }),
            (any::<u8>(), any::<u8>()).prop_map(|(key, kind)| Edit::MapNested { key, kind }),
            any::<u8>().prop_map(|key| Edit::MapCut { key }),
            (any::<u16>(), any::<u8>()).prop_map(|(at, v)| Edit::ArrayPut { at, v }),
            (any::<u16>(), any::<u8>()).prop_map(|(at, n)| Edit::ArrayCut { at, n }),
            (any::<u16>(), any::<u8>()).prop_map(|(at, s)| Edit::Inner { at, s }),
            any::<u8>().prop_map(|from| Edit::Sync { from }),
        ];
        (0_u8..3, e)
    }

    fn value(v: u8) -> Any {
        let s = ["x", "h\u{e9}", "\u{1f600}", ""][usize::from(v % 4)];
        match v % 7 {
            0 => Any::Number(Number::Int(i64::from(v) * -1_000_003)),
            1 => Any::Number(Number::Float(f64::from(v) / 3.0)),
            2 => Any::from(s),
            3 => Any::Array(Arc::from(vec![Any::Null, Any::Undefined, Any::Bool(v.is_multiple_of(2)), Any::from(vec![v, 0])])),
            4 => Any::Map(Arc::new(std::collections::HashMap::from([
                ("k".to_owned(), Any::from(s)),
                ("n".to_owned(), Any::Map(Arc::new(std::collections::HashMap::new()))),
            ]))),
            5 => Any::Number(Number::Int(i64::MAX - i64::from(v))),
            _ => Any::Null,
        }
    }

    /// The pieces of `text`, each a character or an embed: its byte offsets and whether it is a
    /// character. Under byte offsets yrs counts an embed in `len` but cannot remove it by range,
    /// so cuts and formats only span characters.
    fn pieces(text: &yrs::TextRef, t: &TransactionMut) -> Vec<(u32, u32, bool)> {
        let mut out = Vec::new();
        let mut at = 0;
        for d in text.diff(t, yrs::types::text::YChange::identity) {
            if let yrs::Out::Any(Any::String(s)) = d.insert {
                for c in s.chars() {
                    let w = u32::try_from(c.len_utf8()).unwrap();
                    out.push((at, at + w, true));
                    at += w;
                }
            } else {
                out.push((at, at + 1, false));
                at += 1;
            }
        }
        out
    }

    /// Where to insert into `pieces`: before any piece or at the end.
    fn insert_at(pieces: &[(u32, u32, bool)], at: u16) -> u32 {
        let i = usize::from(at) % (pieces.len() + 1);
        pieces.get(i).map_or_else(|| pieces.last().map_or(0, |p| p.1), |p| p.0)
    }

    /// A run of up to `n % 4 + 1` characters in `pieces`, as (offset, length).
    fn char_run(pieces: &[(u32, u32, bool)], at: u16, n: u8) -> Option<(u32, u32)> {
        let chars: Vec<usize> = (0..pieces.len()).filter(|i| pieces[*i].2).collect();
        let k = *chars.get(usize::from(at) % chars.len().max(1))?;
        let mut j = k;
        while j + 1 < pieces.len() && pieces[j + 1].2 && j - k < usize::from(n % 4) {
            j += 1;
        }
        Some((pieces[k].0, pieces[j].1 - pieces[k].0))
    }

    /// Runs `edits` on three peers and returns every update they made: each edit's diff, the
    /// diffs a sync carries, full states, diffs against old states, and merges (which hold skips).
    fn honest_updates(edits: &[(u8, Edit)]) -> Vec<Vec<u8>> {
        use yrs::types::Attrs;
        use yrs::{Array, Map, Out};
        let docs: Vec<Doc> = (1..=3).map(build_doc).collect();
        let mut out = Vec::new();
        let mut old = Vec::new();
        for (p, e) in edits {
            let doc = &docs[usize::from(*p)];
            let (text, map, arr) = (doc.get_or_insert_text("t"), doc.get_or_insert_map("m"), doc.get_or_insert_array("a"));
            let from = doc.transact().state_vector();
            old.push(from.clone());
            if let Edit::Sync { from: q } = e {
                let src = &docs[usize::from(*q % 3)];
                let diff = src.transact().encode_diff_v1(&from);
                doc.transact_mut().apply_update(Update::decode_v1(&diff).unwrap()).unwrap();
                out.push(diff);
                continue;
            }
            {
                let mut t = doc.transact_mut();
                let p = pieces(&text, &t);
                match e {
                    Edit::Text { at: i, s, bold } => {
                        let s = ["a", "na\u{ef}ve ", "\u{1f600}x", "\u{4e2d}\u{6587}"][usize::from(s % 4)];
                        let attrs: Attrs = if *bold { [("b".into(), Any::Bool(true))].into_iter().collect() } else { Attrs::new() };
                        text.insert_with_attributes(&mut t, insert_at(&p, *i), s, attrs);
                    }
                    Edit::Embed { at: i } => {
                        // A map, so a diff tells it from text.
                        let img = Any::Map(Arc::new(std::collections::HashMap::from([("src".to_owned(), Any::from("img"))])));
                        text.insert_embed(&mut t, insert_at(&p, *i), img);
                    }
                    Edit::Cut { at: i, n } => {
                        if let Some((from, len)) = char_run(&p, *i, *n) {
                            text.remove_range(&mut t, from, len);
                        }
                    }
                    Edit::Format { at: i, n } => {
                        if let Some((from, len)) = char_run(&p, *i, *n) {
                            text.format(&mut t, from, len, [("i".into(), Any::Bool(n.is_multiple_of(2)))].into_iter().collect());
                        }
                    }
                    Edit::MapSet { key, v } => {
                        map.insert(&mut t, format!("k{}", key % 5), value(*v));
                    }
                    Edit::MapNested { key, kind } => {
                        let key = format!("k{}", key % 5);
                        match kind % 3 {
                            0 => {
                                let _ = map.insert(&mut t, key, yrs::MapPrelim::from([("x", value(*kind))]));
                            }
                            1 => {
                                let _ = map.insert(&mut t, key, yrs::ArrayPrelim::from([value(*kind), value(kind / 2)]));
                            }
                            _ => {
                                let _ = map.insert(&mut t, key, yrs::TextPrelim::new("in map"));
                            }
                        }
                    }
                    Edit::MapCut { key } => {
                        map.remove(&mut t, &format!("k{}", key % 5));
                    }
                    Edit::ArrayPut { at: i, v } => {
                        let i = u32::from(*i) % (arr.len(&t) + 1);
                        match v % 3 {
                            0 => {
                                let _ = arr.insert(&mut t, i, value(*v));
                            }
                            1 => {
                                let _ = arr.insert(&mut t, i, yrs::TextPrelim::new("in array"));
                            }
                            _ => {
                                let _ = arr.insert(&mut t, i, yrs::ArrayPrelim::from([value(*v)]));
                            }
                        }
                    }
                    Edit::ArrayCut { at: i, n } if arr.len(&t) > 0 => {
                        let n_all = arr.len(&t);
                        let i = u32::from(*i) % n_all;
                        arr.remove_range(&mut t, i, (u32::from(*n) % 3 + 1).min(n_all - i));
                    }
                    Edit::Inner { at: i, s } => {
                        // Edit a nested type made earlier, in the array or the map.
                        let nested = (0..arr.len(&t))
                            .filter_map(|j| arr.get(&t, j))
                            .chain((0..5).filter_map(|k| map.get(&t, &format!("k{k}"))))
                            .nth(usize::from(*s % 4));
                        match nested {
                            Some(Out::YText(inner)) => {
                                let at = insert_at(&pieces(&inner, &t), *i);
                                inner.insert(&mut t, at, "\u{e9}y");
                            }
                            Some(Out::YMap(inner)) => {
                                let _ = inner.insert(&mut t, format!("i{}", s % 3), value(*s));
                            }
                            Some(Out::YArray(inner)) => {
                                let n = inner.len(&t);
                                inner.insert(&mut t, u32::from(*i) % (n + 1), value(*s));
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            out.push(doc.transact().encode_diff_v1(&from));
        }
        for (i, doc) in docs.iter().enumerate() {
            out.push(doc.transact().encode_state_as_update_v1(&StateVector::default()));
            if let Some(sv) = old.get(old.len() * i / 3) {
                out.push(doc.transact().encode_state_as_update_v1(sv));
            }
        }
        // Merges of every other update leave gaps that yrs fills with skips.
        let every_other: Vec<&[u8]> = out.iter().step_by(2).map(Vec::as_slice).collect();
        out.push(yrs::merge_updates_v1(every_other).unwrap());
        let tail: Vec<&[u8]> = out.iter().skip(out.len() / 2).map(Vec::as_slice).collect();
        out.push(yrs::merge_updates_v1(tail).unwrap());
        out
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config { cases: 128, rng_seed: proptest::test_runner::RngSeed::Fixed(0x00c6_0004), failure_persistence: None, ..Default::default() })]

        /// For honest updates from random edit sequences the walk and yrs agree, and the gate
        /// takes every one.
        #[test]
        fn the_walk_and_yrs_agree_on_honest_updates(edits in proptest::collection::vec(edit(), 1..40)) {
            for (i, u) in honest_updates(&edits).iter().enumerate() {
                let shape = crate::walk::read(u);
                proptest::prop_assert!(shape.is_some(), "update {} refused by the walk: {:?}", i, u);
                let update = Update::decode_v1(u).unwrap();
                proptest::prop_assert!(crate::walk::agrees(shape.as_ref().unwrap(), &update), "update {} disagrees: {:?}", i, u);
                proptest::prop_assert!(update_is_sound(u), "update {} refused", i);
            }
        }
    }

    /// Damaged honest updates: whenever both the walk and yrs take the bytes, they agree. A
    /// failure here is a byte form the two read differently.
    #[test]
    fn mutated_updates_the_walk_takes_agree_with_yrs() {
        let edits: Vec<(u8, Edit)> = (0..60_u8)
            .map(|i| {
                let e = match i % 10 {
                    0 => Edit::Text { at: u16::from(i), s: i, bold: i.is_multiple_of(3) },
                    1 => Edit::Embed { at: u16::from(i) },
                    2 => Edit::MapSet { key: i, v: i },
                    3 => Edit::MapNested { key: i, kind: i },
                    4 => Edit::ArrayPut { at: u16::from(i), v: i },
                    5 => Edit::Inner { at: u16::from(i), s: i },
                    6 => Edit::Format { at: u16::from(i), n: i },
                    7 => Edit::Cut { at: u16::from(i), n: i },
                    8 => Edit::ArrayCut { at: u16::from(i), n: i },
                    _ => Edit::Sync { from: i },
                };
                (i % 3, e)
            })
            .collect();
        let mut honest = honest_updates(&edits);
        honest.extend(desync_patterns().into_iter().map(|(_, b)| b));
        let mut rng = Lcg(0xc6);
        let (mut both, mut tried) = (0_u32, 0_u32);
        for _ in 0..60_000 {
            let mut m = honest[rng.below(honest.len())].clone();
            for _ in 0..=rng.below(3) {
                let at = rng.below(m.len().max(1));
                let b = u8::try_from(rng.below(256)).unwrap();
                match rng.below(4) {
                    0 if !m.is_empty() => m[at] = b,
                    1 if !m.is_empty() => m[at] ^= 1 << rng.below(8),
                    2 => m.insert(at.min(m.len()), b),
                    _ if !m.is_empty() => {
                        m.remove(at);
                    }
                    _ => {}
                }
            }
            tried += 1;
            let Some(shape) = crate::walk::read(&m) else { continue };
            let Ok(Ok(update)) = std::panic::catch_unwind(|| Update::decode_v1(&m)) else { continue };
            both += 1;
            assert!(crate::walk::agrees(&shape, &update), "the walk and yrs disagree on {m:?}");
        }
        println!("MUTATED {tried} tried, {both} taken by both");
        assert!(both > 1000, "too few mutations parse: {both}");
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
