//! A block document over yrs, shared by every Craft app that edits paragraphs, slides or frames.
//!
//! [`LiveDoc`] knows blocks, text, runs and attributes. It does not know what a paragraph or a
//! slide is: WordCraft maps a paragraph onto a block, DeckCraft can map a text frame onto one.
//!
//! # Layout
//!
//! * root `"body"`: a `YArray` of block ids, each stored as an `i64` (`id as i64`). Its order is
//!   the document order.
//! * root `"blocks"`: a `YMap` from the decimal id to a `YMap` holding
//!   * `"t"`: a `YText` with the block's characters. Runs are its formatting attributes.
//!   * `"a"`: a `YMap` with the block's own attributes (a style, an alignment).
//!
//! Removing a block takes its id out of `"body"` only. Its `"blocks"` entry stays, as a
//! tombstone, so typing that raced the removal still lands somewhere. [`LiveDoc::apply`] then
//! reports [`BlockChange::Text`] for an id that is not in [`LiveDoc::order`], and the app can show
//! that text as recovered.
//!
//! # Client ids
//!
//! yrs fixes the client id when a `Doc` is built. [`LiveDoc::seeded`] builds with client id 0, the
//! file's own, in one transaction whose content is a pure function of the blocks, so every replica
//! that loads the same file reaches the same item ids and the same [`LiveDoc::state`] bytes.
//! [`LiveDoc::set_client`] rebuilds the `Doc` with the relay-granted space as its client id and
//! carries the whole state across. Local edits are refused with [`LiveError::NoClient`] until
//! then, because two peers typing as client 0 would mint the same ids for different characters.
//!
//! # Offsets
//!
//! Every `char_at` and `len` counts Unicode scalar values (Rust `char`s). yrs 0.28 offers only
//! UTF-8 bytes or UTF-16 units, so the `Doc` counts bytes and this module converts.
//!
//! # Undo
//!
//! One yrs `UndoManager` over both roots, tracking only transactions this replica made through
//! its own edit methods. Updates that arrive through [`LiveDoc::apply`] are never undone. Every
//! local edit is its own undo step. A `set_client` starts a fresh history.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};
use yrs::types::text::YChange;
use yrs::types::{Attrs, Event, PathSegment};
use yrs::{
    Any, Array, ArrayRef, DeepObservable, Doc, Map as _, MapPrelim, MapRef, Number, Out, ReadTxn, Text, TextPrelim, TextRef, Transact, TransactionMut,
};

use crate::replica::{REMOTE, Replica, YReplica, byte_span, integrate, put_sorted, to_any, to_json};

const BODY: &str = "body";
const BLOCKS: &str = "blocks";
const TEXT: &str = "t";
const ATTRS: &str = "a";
/// The key the change observer is registered under.
const OBSERVER: &str = "craft-collab/changes";

/// Attributes: a block's own, or one run's.
pub type Attributes = Map<String, Value>;

/// One block as the file holds it, and as [`LiveDoc::block`] reads it back.
///
/// `runs` are `(length in chars, attributes)` in text order. [`LiveDoc::block`] returns them in
/// one canonical form, which [`LiveDoc::seeded`] round-trips exactly:
///
/// * a block with no formatting at all has no runs;
/// * otherwise the runs cover the whole text, unformatted stretches carry an empty map, adjacent
///   runs never have equal attributes, and no run is empty.
///
/// A `null` run attribute means "not set", as it does in yrs. Seeding accepts runs that stop
/// short of the end of the text (the rest is unformatted) and ignores any length past it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SeedBlock {
    pub id: u64,
    pub text: String,
    pub attrs: Attributes,
    pub runs: Vec<(usize, Attributes)>,
}

/// What one [`LiveDoc::apply`] changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BlockChange {
    /// The block is now at `at` in [`LiveDoc::order`].
    Inserted { at: usize, id: u64 },
    /// The block left [`LiveDoc::order`]. It stays readable through [`LiveDoc::block`].
    Removed { id: u64 },
    /// The block's text or runs changed. The id may be one no longer in [`LiveDoc::order`]: a
    /// concurrent edit to a removed block.
    Text { id: u64 },
    /// The block's own attributes changed.
    Attrs { id: u64 },
}

/// Why a [`LiveDoc`] call was refused. Nothing is changed when one is returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LiveError {
    /// No block with this id has ever existed here.
    #[error("no such block")]
    UnknownBlock,
    /// The block exists but has been removed from the order.
    #[error("the block was removed")]
    Gone,
    /// A position or length past the end of the text, or of the order.
    #[error("position out of range")]
    OutOfRange,
    /// A full state that is behind this replica's own sent edits: rebuilding from it would
    /// reuse clocks, so it is refused.
    #[error("that state is behind this replica's own edits")]
    Stale,
    /// The update bytes did not decode, or did not apply.
    #[error("the update did not decode")]
    Decode,
    /// No client id has been set yet, so a local edit would collide with other peers.
    #[error("no client id yet")]
    NoClient,
    /// A block with the new id already exists, live or removed.
    #[error("a block with that id already exists")]
    Exists,
    /// A merge named the same block twice.
    #[error("cannot merge a block with itself")]
    SameBlock,
    /// yrs panicked during an earlier apply, so this replica may hold half an update. It takes
    /// no more local edits; rebuild it from a snapshot or reopen the file.
    #[error("the shared document is damaged; reopen the file")]
    Damaged,
}

/// One replica of a block document.
///
/// See the [module documentation](self) for the layout, the client id rules and the undo scope.
pub struct LiveDoc {
    /// The document, its undo history, its client id and its damage mark.
    y: YReplica,
    body: ArrayRef,
    blocks: MapRef,
    /// Text and attribute changes the observer saw, drained by [`LiveDoc::apply`].
    seen: Arc<Mutex<Vec<BlockChange>>>,
}

impl LiveDoc {
    /// The document the file holds, as client 0.
    ///
    /// Deterministic: equal blocks give equal [`state`](Self::state) bytes on every replica.
    /// Attribute values that are JSON objects are the exception for the bytes only, because yrs
    /// encodes a nested map in hash order. The item ids, and so convergence, are unaffected.
    ///
    /// Two blocks with the same id are refused with [`LiveError::Exists`]: the order could not
    /// say which of them is meant.
    pub fn seeded(blocks: &[SeedBlock]) -> Result<LiveDoc, LiveError> {
        let mut ids = BTreeSet::new();
        if !blocks.iter().all(|b| ids.insert(b.id)) {
            return Err(LiveError::Exists);
        }
        let doc = YReplica::seed_doc();
        let body = doc.get_or_insert_array(BODY);
        let map = doc.get_or_insert_map(BLOCKS);
        {
            let mut txn = doc.transact_mut_with(REMOTE);
            for block in blocks {
                let at = body.len(&txn);
                body.insert(&mut txn, at, id_any(block.id));
                put_block(&mut txn, &map, block);
            }
        }
        Ok(assemble(doc, None))
    }

    /// Mint later local edits with client id `space`.
    ///
    /// Space 0 is the seed's own, so `set_client(0)` is a no-op (logged): the replica keeps
    /// refusing local edits. Setting the space this replica already has is also a no-op.
    /// Any other space rebuilds the `Doc` with that client id and the current state, and starts a
    /// fresh undo history. The carried state includes updates yrs is still holding for missing
    /// dependencies and the whole delete set. If it does not carry across, the replica is left
    /// as it was (still without a client id) and the error says so.
    pub fn set_client(&mut self, space: u32) -> Result<(), LiveError> {
        if space == 0 {
            log::warn!("collab: client id 0 belongs to the seed and is never adopted");
            return Ok(());
        }
        if self.y.client() == Some(space) {
            return Ok(());
        }
        let doc = self.y.rebuilt(space)?;
        let old = std::mem::replace(self, assemble(doc, Some(space)));
        self.y.carry_marks(&old.y);
        Ok(())
    }

    /// Catch up after [`needs_resync`](Self::needs_resync): replace the document with a fresh one
    /// (same client id) built from `state`, a full state from the room, and clear the mark.
    /// The undo history starts again. On error this replica is unchanged.
    pub fn resync_from_state(&mut self, state: &[u8]) -> Result<(), LiveError> {
        let doc = self.y.rebuilt_from_state(state)?;
        let client = self.y.client();
        *self = assemble(doc, client);
        Ok(())
    }

    /// A replica restored from a full state, such as a checkpoint snapshot.
    ///
    /// Like [`seeded`](Self::seeded) it has no client id yet, so local edits wait for
    /// [`set_client`](Self::set_client). Tombstoned blocks come back as tombstones. A joiner that
    /// starts from a snapshot must not also seed the file: the file's text, seeded as client 0,
    /// would be different items from the ones the snapshot holds.
    pub fn from_state(state: &[u8]) -> Result<LiveDoc, LiveError> {
        let doc = YReplica::seed_doc();
        // Typed first, so the fit check judges the state against the roots it will be read as.
        doc.get_or_insert_array(BODY);
        doc.get_or_insert_map(BLOCKS);
        integrate(&doc, state)?;
        Ok(assemble(doc, None))
    }

    /// Whether yrs panicked during an earlier apply. Such a replica may hold half an update and
    /// should be rebuilt (from a snapshot, or by reopening) rather than trusted.
    #[must_use]
    pub fn damaged(&self) -> bool {
        self.y.damaged()
    }

    /// Whether updates were dropped because their dependencies never came (the replica holds
    /// bounded numbers of waiting updates, see [`YReplica::needs_resync`]). It is sound but
    /// behind; it needs the room's full state.
    #[must_use]
    pub fn needs_resync(&self) -> bool {
        self.y.needs_resync()
    }

    /// Merge an update from another replica, and say what it changed.
    ///
    /// Updates may arrive in any order and more than once. One whose dependencies have not
    /// arrived yet is held and lands when they do (up to a bound, past which
    /// [`needs_resync`](Self::needs_resync) is raised).
    pub fn apply(&mut self, update: &[u8]) -> Result<Vec<BlockChange>, LiveError> {
        let before = self.order();
        self.drain_seen();
        self.y.apply(update)?;
        let after = self.order();
        let seen = self.drain_seen();

        let was: BTreeSet<u64> = before.iter().copied().collect();
        let now: BTreeSet<u64> = after.iter().copied().collect();
        let mut out: Vec<BlockChange> = before.iter().filter(|id| !now.contains(id)).map(|id| BlockChange::Removed { id: *id }).collect();
        let mut inserted = BTreeSet::new();
        for (at, id) in after.iter().enumerate() {
            if !was.contains(id) && inserted.insert(*id) {
                out.push(BlockChange::Inserted { at, id: *id });
            }
        }
        for change in seen {
            let id = match change {
                BlockChange::Text { id } | BlockChange::Attrs { id } => id,
                _ => continue,
            };
            if !inserted.contains(&id) && !out.contains(&change) {
                out.push(change);
            }
        }
        Ok(out)
    }

    /// Insert `text` at `char_at`, carrying exactly `run_attrs` (not the formatting around it).
    pub fn insert_text(&mut self, id: u64, char_at: usize, text: &str, run_attrs: &Attributes) -> Result<Vec<u8>, LiveError> {
        let attrs = run_attrs_of(run_attrs);
        self.local(|txn, s| {
            let t = s.live_text(txn, id)?;
            let at = *char_offsets(txn, &t)?.get(char_at).ok_or(LiveError::OutOfRange)?;
            t.insert_with_attributes(txn, at, text, attrs);
            Ok(())
        })
    }

    /// Delete `len` chars from `char_at`.
    pub fn delete_text(&mut self, id: u64, char_at: usize, len: usize) -> Result<Vec<u8>, LiveError> {
        self.local(|txn, s| {
            let t = s.live_text(txn, id)?;
            let (at, bytes) = span_of(&char_offsets(txn, &t)?, char_at, len)?;
            if bytes > 0 {
                t.remove_range(txn, at, bytes);
            }
            Ok(())
        })
    }

    /// Set run attributes over `len` chars from `char_at`. A `null` value clears that attribute;
    /// attributes not named are left alone. A remote replica reports this as [`BlockChange::Text`].
    pub fn format(&mut self, id: u64, char_at: usize, len: usize, run_attrs: &Attributes) -> Result<Vec<u8>, LiveError> {
        let attrs: Attrs = run_attrs.iter().map(|(k, v)| (Arc::from(k.as_str()), to_any(v))).collect();
        self.local(|txn, s| {
            let t = s.live_text(txn, id)?;
            let (at, bytes) = span_of(&char_offsets(txn, &t)?, char_at, len)?;
            if bytes > 0 && !attrs.is_empty() {
                t.format(txn, at, bytes, attrs);
            }
            Ok(())
        })
    }

    /// Replace the block's own attributes with `attrs`.
    pub fn set_attrs(&mut self, id: u64, attrs: &Attributes) -> Result<Vec<u8>, LiveError> {
        self.local(|txn, s| {
            let (block, _) = s.live_block(txn, id)?;
            let a = attrs_map(txn, &block).ok_or(LiveError::UnknownBlock)?;
            let current = read_attrs(txn, &a);
            for key in current.keys().filter(|k| !attrs.contains_key(*k)) {
                a.remove(txn, key);
            }
            for (key, value) in attrs {
                if current.get(key) != Some(value) {
                    a.insert(txn, key.as_str(), to_any(value));
                }
            }
            Ok(())
        })
    }

    /// Move the text and runs from `char_at` onward into a new block `new_id`, placed right
    /// after `id`. The new block's attributes copy the original's.
    ///
    /// The tail is moved by deleting it here and writing it afresh into the new block, so the
    /// new block's characters are new items. Typing another peer does in the tail at the same
    /// time therefore lands in the original block, at the split point, not in the new one.
    ///
    /// Two peers splitting the same block at the same time each move the tail, so the merged
    /// document holds it twice. Every replica still ends identical; only the intent is lost.
    /// That is accepted for phase 1.
    pub fn split(&mut self, id: u64, char_at: usize, new_id: u64) -> Result<Vec<u8>, LiveError> {
        self.local(|txn, s| {
            let (block, index) = s.live_block(txn, id)?;
            if s.blocks.get(txn, &key(new_id)).is_some() {
                return Err(LiveError::Exists);
            }
            let t = text_of(txn, &block).ok_or(LiveError::UnknownBlock)?;
            let offsets = char_offsets(txn, &t)?;
            let total = offsets.len() - 1;
            let (at, bytes) = span_of(&offsets, char_at, total.checked_sub(char_at).ok_or(LiveError::OutOfRange)?)?;
            let tail = slice_chunks(chunks(txn, &t), char_at);
            let attrs = attrs_map(txn, &block).map(|a| read_attrs(txn, &a)).unwrap_or_default();
            if bytes > 0 {
                t.remove_range(txn, at, bytes);
            }
            let fresh = SeedBlock {
                id: new_id,
                text: tail.iter().map(|(s, _)| s.as_str()).collect(),
                attrs,
                runs: tail.iter().map(|(s, a)| (s.chars().count(), a.clone())).collect(),
            };
            put_block(txn, &s.blocks, &fresh);
            s.body.insert(txn, index + 1, id_any(new_id));
            Ok(())
        })
    }

    /// Append `second`'s text and runs to the end of `first`, then remove `second` the way
    /// [`remove_block`](Self::remove_block) does. [`LiveError::Gone`] unless both are in the order.
    pub fn merge(&mut self, first: u64, second: u64) -> Result<Vec<u8>, LiveError> {
        if first == second {
            return Err(LiveError::SameBlock);
        }
        self.local(|txn, s| {
            let (a, _) = s.live_block(txn, first).map_err(|_| LiveError::Gone)?;
            let (b, _) = s.live_block(txn, second).map_err(|_| LiveError::Gone)?;
            let into = text_of(txn, &a).ok_or(LiveError::UnknownBlock)?;
            let from = text_of(txn, &b).ok_or(LiveError::UnknownBlock)?;
            let moved = chunks(txn, &from);
            let mut at = into.len(txn);
            for (text, attrs) in moved {
                let len = u32::try_from(text.len()).map_err(|_| LiveError::OutOfRange)?;
                into.insert_with_attributes(txn, at, &text, run_attrs_of(&attrs));
                at += len;
            }
            s.remove_all(txn, second);
            Ok(())
        })
    }

    /// Insert a new block at `at` in the order.
    pub fn insert_block(&mut self, at: usize, block: &SeedBlock) -> Result<Vec<u8>, LiveError> {
        self.local(|txn, s| {
            let index = s.raw_index(txn, at)?;
            if s.blocks.get(txn, &key(block.id)).is_some() {
                return Err(LiveError::Exists);
            }
            s.body.insert(txn, index, id_any(block.id));
            put_block(txn, &s.blocks, block);
            Ok(())
        })
    }

    /// Take a block out of the order. Its content stays, as a tombstone.
    pub fn remove_block(&mut self, id: u64) -> Result<Vec<u8>, LiveError> {
        self.local(|txn, s| {
            s.live_block(txn, id)?;
            s.remove_all(txn, id);
            Ok(())
        })
    }

    /// The block ids in document order.
    #[must_use]
    pub fn order(&self) -> Vec<u64> {
        let txn = self.y.doc().transact();
        order_of(&txn, &self.body)
    }

    /// One block's text, attributes and runs, including a removed block's.
    #[must_use]
    pub fn block(&self, id: u64) -> Option<SeedBlock> {
        let txn = self.y.doc().transact();
        let Some(Out::YMap(block)) = self.blocks.get(&txn, &key(id)) else {
            return None;
        };
        let attrs = attrs_map(&txn, &block).map(|a| read_attrs(&txn, &a)).unwrap_or_default();
        let parts = text_of(&txn, &block).map(|t| chunks(&txn, &t)).unwrap_or_default();
        let text = parts.iter().map(|(s, _)| s.as_str()).collect();
        Some(SeedBlock { id, text, attrs, runs: canonical_runs(parts.iter().map(|(s, a)| (s.chars().count(), a.clone()))) })
    }

    /// Undo this replica's last local edit and return the update to send.
    /// `None` on a [`damaged`](Self::damaged) replica too.
    pub fn undo(&mut self) -> Option<Vec<u8>> {
        self.y.undo()
    }

    /// Redo the last undone local edit and return the update to send.
    /// `None` on a [`damaged`](Self::damaged) replica too.
    pub fn redo(&mut self) -> Option<Vec<u8>> {
        self.y.redo()
    }

    /// The whole document as one update (v1), for a checkpoint or a peer starting from nothing.
    #[must_use]
    pub fn state(&self) -> Vec<u8> {
        self.y.state()
    }

    /// Run `edits` with this replica's local edits kept out of its undo history: they are sent
    /// like any other, and [`undo`](Self::undo) never takes them back. For edits the app makes
    /// on the user's behalf in answer to a remote edit (text recovered from a block this
    /// replica removed), which the user's own Undo must not erase.
    pub fn untracked<R>(&mut self, edits: impl FnOnce(&mut LiveDoc) -> R) -> R {
        let was = self.y.set_untracked(true);
        let out = edits(self);
        self.y.set_untracked(was);
        out
    }

    /// Run one local edit in its own transaction and return its update.
    ///
    /// `edit` checks everything before it changes anything, because a yrs transaction commits
    /// whatever it holds when it is dropped, error or not.
    fn local<F>(&mut self, edit: F) -> Result<Vec<u8>, LiveError>
    where
        F: FnOnce(&mut TransactionMut, &Roots) -> Result<(), LiveError>,
    {
        let roots = Roots { body: self.body.clone(), blocks: self.blocks.clone() };
        let update = self.y.local(|txn| edit(txn, &roots))?;
        // The observer saw this replica's own change; nobody needs it reported back.
        self.drain_seen();
        Ok(update)
    }

    /// Mark the replica behind, as an overflow of held updates would.
    #[cfg(test)]
    pub(crate) fn force_resync(&mut self, on: bool) {
        self.y.set_needs_resync(on);
    }

    /// Mark the replica damaged, as a caught yrs panic would.
    #[cfg(test)]
    pub(crate) fn force_damaged(&mut self) {
        self.y.set_damaged(true);
    }

    /// How many changes the observer is holding, for a test of local edits draining them.
    #[cfg(test)]
    pub(crate) fn seen_len(&self) -> usize {
        self.seen.lock().map(|s| s.len()).unwrap_or(0)
    }

    fn drain_seen(&self) -> Vec<BlockChange> {
        match self.seen.lock() {
            Ok(mut seen) => std::mem::take(&mut *seen),
            Err(_) => Vec::new(),
        }
    }
}

impl Replica for LiveDoc {
    type Change = BlockChange;
    type Seed = [SeedBlock];
    fn seeded(seed: &[SeedBlock]) -> Result<LiveDoc, LiveError> {
        LiveDoc::seeded(seed)
    }
    fn from_state(state: &[u8]) -> Result<LiveDoc, LiveError> {
        LiveDoc::from_state(state)
    }
    fn set_client(&mut self, space: u32) -> Result<(), LiveError> {
        LiveDoc::set_client(self, space)
    }
    fn apply(&mut self, update: &[u8]) -> Result<Vec<BlockChange>, LiveError> {
        LiveDoc::apply(self, update)
    }
    fn state(&self) -> Vec<u8> {
        LiveDoc::state(self)
    }
    fn undo(&mut self) -> Option<Vec<u8>> {
        LiveDoc::undo(self)
    }
    fn redo(&mut self) -> Option<Vec<u8>> {
        LiveDoc::redo(self)
    }
    fn damaged(&self) -> bool {
        LiveDoc::damaged(self)
    }
    fn needs_resync(&self) -> bool {
        LiveDoc::needs_resync(self)
    }
}

/// The two roots, handed to a local edit alongside its transaction.
struct Roots {
    body: ArrayRef,
    blocks: MapRef,
}

impl Roots {
    /// A block that is in the order, with its index there.
    /// A block that is in the order, with the body index of its first occurrence.
    fn live_block<T: ReadTxn>(&self, txn: &T, id: u64) -> Result<(MapRef, u32), LiveError> {
        let Some(Out::YMap(block)) = self.blocks.get(txn, &key(id)) else {
            return Err(LiveError::UnknownBlock);
        };
        let index = raw_ids(txn, &self.body).iter().position(|o| *o == Some(id)).ok_or(LiveError::Gone)?;
        Ok((block, u32::try_from(index).map_err(|_| LiveError::OutOfRange)?))
    }

    /// Take every occurrence of `id` out of the body. Two peers inserting the same id at once
    /// leave it there twice; removing only one would leave the block in the order.
    fn remove_all(&self, txn: &mut TransactionMut, id: u64) {
        let at: Vec<u32> =
            raw_ids(txn, &self.body).iter().enumerate().filter(|(_, o)| **o == Some(id)).filter_map(|(i, _)| u32::try_from(i).ok()).collect();
        for i in at.into_iter().rev() {
            self.body.remove(txn, i);
        }
    }

    /// The body index that position `at` of [`order_of`] maps to, which may be the end.
    fn raw_index<T: ReadTxn>(&self, txn: &T, at: usize) -> Result<u32, LiveError> {
        let raw = raw_ids(txn, &self.body);
        let mut seen = BTreeSet::new();
        let mut visible = 0;
        for (i, id) in raw.iter().enumerate() {
            if let Some(id) = id
                && seen.insert(*id)
            {
                if visible == at {
                    return u32::try_from(i).map_err(|_| LiveError::OutOfRange);
                }
                visible += 1;
            }
        }
        if visible == at { u32::try_from(raw.len()).map_err(|_| LiveError::OutOfRange) } else { Err(LiveError::OutOfRange) }
    }

    fn live_text<T: ReadTxn>(&self, txn: &T, id: u64) -> Result<TextRef, LiveError> {
        let (block, _) = self.live_block(txn, id)?;
        text_of(txn, &block).ok_or(LiveError::UnknownBlock)
    }
}

/// Wrap a built document with its roots, its change observer and its undo manager.
fn assemble(doc: Doc, client: Option<u32>) -> LiveDoc {
    let body = doc.get_or_insert_array(BODY);
    let blocks = doc.get_or_insert_map(BLOCKS);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    blocks.observe_deep(OBSERVER, move |_, events| {
        let found: Vec<BlockChange> = events.iter().filter_map(change_of).collect();
        if let Ok(mut seen) = sink.lock() {
            seen.extend(found);
        }
    });
    let (b, m) = (body.clone(), blocks.clone());
    // One undo manager over both roots, tracking only this replica's own edits.
    let y = YReplica::assemble(doc, client, move |doc, undo| {
        undo.expand_scope(doc, &b);
        undo.expand_scope(doc, &m);
    });
    LiveDoc { y, body, blocks, seen }
}

/// The block a nested event belongs to, from its path below `"blocks"`.
fn change_of(event: &Event) -> Option<BlockChange> {
    let path = event.path();
    let (Some(PathSegment::Key(id)), Some(PathSegment::Key(field))) = (path.front(), path.get(1)) else {
        return None;
    };
    let id = id.parse().ok()?;
    match field.as_ref() {
        TEXT => Some(BlockChange::Text { id }),
        ATTRS => Some(BlockChange::Attrs { id }),
        _ => None,
    }
}

fn key(id: u64) -> String {
    id.to_string()
}

fn id_any(id: u64) -> Any {
    Any::Number(Number::Int(id as i64))
}

/// Every body entry, by index. `None` for an entry that is not an id.
fn raw_ids<T: ReadTxn>(txn: &T, body: &ArrayRef) -> Vec<Option<u64>> {
    body.iter(txn)
        .map(|v| match v {
            Out::Any(Any::Number(n)) => n.as_i64().map(|i| i as u64),
            _ => None,
        })
        .collect()
}

/// The order: body ids, each kept at its first occurrence only. Two peers inserting the same id
/// at once leave it in the body twice, and every replica keeps the same (first) one.
fn order_of<T: ReadTxn>(txn: &T, body: &ArrayRef) -> Vec<u64> {
    let mut seen = BTreeSet::new();
    raw_ids(txn, body).into_iter().flatten().filter(|id| seen.insert(*id)).collect()
}

fn text_of<T: ReadTxn>(txn: &T, block: &MapRef) -> Option<TextRef> {
    match block.get(txn, TEXT) {
        Some(Out::YText(t)) => Some(t),
        _ => None,
    }
}

fn attrs_map<T: ReadTxn>(txn: &T, block: &MapRef) -> Option<MapRef> {
    match block.get(txn, ATTRS) {
        Some(Out::YMap(m)) => Some(m),
        _ => None,
    }
}

fn read_attrs<T: ReadTxn>(txn: &T, attrs: &MapRef) -> Attributes {
    attrs
        .iter(txn)
        .map(|(k, v)| {
            let value = match v {
                Out::Any(a) => to_json(&a),
                _ => Value::Null,
            };
            (k.to_owned(), value)
        })
        .collect()
}

/// Write one block into `"blocks"`, deterministically.
///
/// Every write is in a fixed order (attribute keys sorted, one format call per key and maximal
/// span), because yrs hands out clocks in the order of writes, and a hash-ordered write would
/// give two replicas seeding the same file different ids.
fn put_block(txn: &mut TransactionMut, blocks: &MapRef, block: &SeedBlock) {
    let inner = blocks.insert(txn, key(block.id), MapPrelim::default());
    let text = inner.insert(txn, TEXT, TextPrelim::new(block.text.clone()));
    let attrs = inner.insert(txn, ATTRS, MapPrelim::default());
    put_sorted(txn, &attrs, &block.attrs);

    // Char ranges of each run, clamped to the text.
    let total = block.text.chars().count();
    let mut spans = Vec::new();
    let mut start = 0;
    for (len, attrs) in &block.runs {
        let end = (start + len).min(total);
        if end > start {
            spans.push((start, end, attrs));
        }
        start = end;
    }
    let keys: BTreeSet<&String> = spans.iter().flat_map(|(_, _, a)| a.iter().filter(|(_, v)| !v.is_null()).map(|(k, _)| k)).collect();
    for k in keys {
        // Maximal spans where this key has one non-null value.
        let mut open: Option<(usize, usize, &Value)> = None;
        let flush = |span: Option<(usize, usize, &Value)>, txn: &mut TransactionMut| {
            if let Some((s, e, v)) = span
                && let Ok((at, bytes)) = byte_span(&block.text, s, e - s)
            {
                let mut one = Attrs::new();
                one.insert(Arc::from(k.as_str()), to_any(v));
                text.format(txn, at, bytes, one);
            }
        };
        for (s, e, a) in &spans {
            match (a.get(k.as_str()).filter(|v| !v.is_null()), open) {
                (Some(v), Some((os, oe, ov))) if oe == *s && ov == v => open = Some((os, *e, ov)),
                (Some(v), prev) => {
                    flush(prev, txn);
                    open = Some((*s, *e, v));
                }
                (None, prev) => {
                    flush(prev, txn);
                    open = None;
                }
            }
        }
        flush(open, txn);
    }
}

/// The text as `(string, attributes)` chunks, attributes without nulls.
/// Where each char boundary of a block's text sits in yrs offsets: `n + 1` entries for `n`
/// chars, the last being the text's whole length.
///
/// Not the bytes of [`get_string`](yrs::GetString::get_string): yrs counts an embed or a
/// nested type in a text as one unit, and the string leaves it out, so offsets taken from the
/// string fall short after one and a cut runs past the end (`Couldn't remove … elements`, a
/// panic). WordCraft never writes either, but a crafted update can put one in a block's text
/// (the `fits` check lets them be, since a text may hold embeds). A char boundary next to one
/// sits after it, and a span of chars takes the embeds between its chars with it.
fn char_offsets<T: ReadTxn>(txn: &T, text: &TextRef) -> Result<Vec<u32>, LiveError> {
    let mut out = Vec::new();
    let mut at: usize = 0;
    for d in text.diff(txn, YChange::identity) {
        match d.insert {
            Out::Any(Any::String(s)) => {
                for c in s.chars() {
                    out.push(u32::try_from(at).map_err(|_| LiveError::OutOfRange)?);
                    at += c.len_utf8();
                }
            }
            _ => at += 1,
        }
    }
    out.push(u32::try_from(at).map_err(|_| LiveError::OutOfRange)?);
    Ok(out)
}

/// Offset and length in yrs units of `len` chars from `char_at`, by [`char_offsets`].
fn span_of(offsets: &[u32], char_at: usize, len: usize) -> Result<(u32, u32), LiveError> {
    let end = char_at.checked_add(len).ok_or(LiveError::OutOfRange)?;
    match (offsets.get(char_at), offsets.get(end)) {
        (Some(&a), Some(&b)) => Ok((a, b - a)),
        _ => Err(LiveError::OutOfRange),
    }
}

fn chunks<T: ReadTxn>(txn: &T, text: &TextRef) -> Vec<(String, Attributes)> {
    text.diff(txn, YChange::identity)
        .into_iter()
        .filter_map(|d| {
            let Out::Any(Any::String(s)) = d.insert else {
                return None;
            };
            let attrs = d
                .attributes
                .map(|a| a.iter().filter(|(_, v)| !matches!(v, Any::Null)).map(|(k, v)| (k.to_string(), to_json(v))).collect())
                .unwrap_or_default();
            Some((s.to_string(), attrs))
        })
        .collect()
}

/// The chunks from char `from` onward.
fn slice_chunks(parts: Vec<(String, Attributes)>, from: usize) -> Vec<(String, Attributes)> {
    let mut skip = from;
    let mut out = Vec::new();
    for (s, a) in parts {
        let n = s.chars().count();
        if skip >= n {
            skip -= n;
            continue;
        }
        out.push((s.chars().skip(skip).collect(), a));
        skip = 0;
    }
    out
}

/// Merge adjacent equal runs, drop empty ones, and give a block with no formatting no runs.
fn canonical_runs(runs: impl Iterator<Item = (usize, Attributes)>) -> Vec<(usize, Attributes)> {
    let mut out: Vec<(usize, Attributes)> = Vec::new();
    for (len, attrs) in runs.filter(|(len, _)| *len > 0) {
        match out.last_mut() {
            Some((n, last)) if *last == attrs => *n += len,
            _ => out.push((len, attrs)),
        }
    }
    if out.iter().all(|(_, a)| a.is_empty()) {
        out.clear();
    }
    out
}

/// Run attributes for an insert: nulls dropped, so the inserted text carries exactly these.
fn run_attrs_of(attrs: &Attributes) -> Attrs {
    attrs.iter().filter(|(_, v)| !v.is_null()).map(|(k, v)| (Arc::from(k.as_str()), to_any(v))).collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn doc() -> LiveDoc {
        let mut d = LiveDoc::seeded(&[SeedBlock { id: 1, text: "hello".to_owned(), ..SeedBlock::default() }]).unwrap();
        d.set_client(1).unwrap();
        d
    }

    /// A state is checked against the typed roots it is read with: a string straight in the
    /// body (an array of block ids) is refused on restore, as it is on apply.
    #[test]
    fn a_restore_checks_the_state_against_the_typed_roots() {
        let string_in_body = [1, 1, 50, 0, 0x04, 1, 4, b'b', b'o', b'd', b'y', 2, 0xc3, 0xa9, 0];
        assert!(LiveDoc::from_state(&string_in_body).is_err(), "from_state");
        let mut d = doc();
        assert!(d.apply(&string_in_body).is_err(), "apply");
        assert!(d.y.rebuilt_from_state(&string_in_body).is_err(), "rebuilt_from_state");
    }

    /// An item that names no parent takes its origin's map key along with its parent (yrs
    /// copies `parent_sub` from the left neighbour). So a keyed entry crafted into a block's
    /// text, then a string placed right of it, is a keyed string in the text, which panics at
    /// the next local cut (yrs text.rs:845). The fit check follows the key down the origin
    /// chain and refuses the string.
    #[test]
    fn a_string_that_inherits_a_key_in_a_text_is_refused() {
        use yrs::branch::BranchID;
        let mut d = doc();
        let text_id = {
            let txn = d.y.doc().transact();
            let blocks = txn.get_map(BLOCKS).unwrap();
            let Some(Out::YMap(block)) = blocks.get(&txn, &key(1)) else { panic!("no block 1") };
            let text = text_of(&txn, &block).unwrap();
            let BranchID::Nested(id) = AsRef::<yrs::branch::Branch>::as_ref(&text).id() else { panic!("a root text") };
            id
        };
        let mut keyed = vec![1, 1, 77, 0, 0x28, 0];
        for n in [text_id.client.get(), u64::from(text_id.clock)] {
            assert!(n < 0x80, "{n}");
            keyed.push(n as u8);
        }
        keyed.extend([1, b'x', 1, 0x7d, 1, 0]);
        d.apply(&keyed).unwrap();
        // A string, origin the keyed entry, no parent named.
        assert!(d.apply(&[1, 1, 77, 1, 0x84, 77, 0, 1, b'a', 0]).is_err(), "a keyed string got into the text");
        d.delete_text(1, 0, 5).unwrap();
        assert_eq!(d.block(1).unwrap().text, "");
    }

    /// An embed another peer put in a block's text (WordCraft never writes one; a crafted update
    /// can) is counted the way yrs counts it: local cuts, formats, inserts and splits around and
    /// over it neither panic nor land on the wrong characters.
    #[test]
    fn local_edits_count_an_embed_in_the_text() {
        use yrs::updates::decoder::Decode;
        use yrs::{OffsetKind, Options, Update};
        // Before the first char, after a two-byte one, and at the end.
        for at_byte in [0_u32, 3, 8] {
            let mut d = LiveDoc::seeded(&[SeedBlock { id: 1, text: "h\u{e9}llo w".to_owned(), ..SeedBlock::default() }]).unwrap();
            d.set_client(1).unwrap();
            // Another peer, in raw yrs, puts an embed at byte `at_byte` of block 1's text.
            let fresh = || {
                let mut options = Options::with_client_id(yrs::block::ClientID::new(77));
                options.offset_kind = OffsetKind::Bytes;
                Doc::with_options(options)
            };
            let raw = fresh();
            raw.transact_mut().apply_update(Update::decode_v1(&d.state()).unwrap()).unwrap();
            let blocks = raw.get_or_insert_map(BLOCKS);
            let before = raw.transact().state_vector();
            {
                let mut txn = raw.transact_mut();
                let Some(Out::YMap(block)) = blocks.get(&txn, &key(1)) else { panic!("no block 1") };
                let text = text_of(&txn, &block).unwrap();
                text.insert_embed(&mut txn, at_byte, Any::from("img"));
            }
            // A string embed reads back as text, so the replica refuses it outright.
            assert!(d.apply(&raw.transact().encode_diff_v1(&before)).is_err(), "a string embed was taken");
            let raw = fresh();
            raw.transact_mut().apply_update(Update::decode_v1(&d.state()).unwrap()).unwrap();
            let blocks = raw.get_or_insert_map(BLOCKS);
            let before = raw.transact().state_vector();
            {
                let mut txn = raw.transact_mut();
                let Some(Out::YMap(block)) = blocks.get(&txn, &key(1)) else { panic!("no block 1") };
                let text = text_of(&txn, &block).unwrap();
                let img = Any::Map(Arc::new([("src".to_owned(), Any::from("img"))].into_iter().collect()));
                text.insert_embed(&mut txn, at_byte, img);
            }
            d.apply(&raw.transact().encode_diff_v1(&before)).unwrap();
            assert_eq!(d.block(1).unwrap().text, "h\u{e9}llo w", "the embed is not text");

            d.format(1, 0, 7, &[("b".to_owned(), Value::Bool(true))].into_iter().collect()).unwrap();
            d.insert_text(1, 7, "orld", &Map::new()).unwrap();
            d.delete_text(1, 1, 2).unwrap();
            assert_eq!(d.block(1).unwrap().text, "hlo world");
            d.split(1, 3, 2).unwrap();
            assert_eq!(d.block(1).unwrap().text, "hlo");
            assert_eq!(d.block(2).unwrap().text, " world");
            d.merge(1, 2).unwrap();
            assert_eq!(d.block(1).unwrap().text, "hlo world");
            d.delete_text(1, 0, 9).unwrap();
            assert_eq!(d.block(1).unwrap().text, "");
            assert!(!d.damaged());
        }
    }

    /// The observer sees local changes too; they are drained, not left to pile up.
    #[test]
    fn local_edits_leave_nothing_in_the_change_buffer() {
        let mut d = doc();
        for i in 0..50 {
            d.insert_text(1, i, "x", &Map::new()).unwrap();
        }
        assert_eq!(d.seen_len(), 0);
    }

    /// A damaged replica takes no local edit, undo or redo.
    #[test]
    fn a_damaged_replica_refuses_local_edits() {
        let mut d = doc();
        d.insert_text(1, 0, "x", &Map::new()).unwrap();
        d.force_damaged();
        assert_eq!(d.insert_text(1, 0, "y", &Map::new()), Err(LiveError::Damaged));
        assert_eq!(d.undo(), None);
        assert_eq!(d.redo(), None);
    }
}
