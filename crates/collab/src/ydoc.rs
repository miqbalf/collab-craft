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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};
use yrs::types::text::YChange;
use yrs::types::{Attrs, Event, PathSegment};
use yrs::undo::UndoManager;
use yrs::updates::decoder::Decode;
use yrs::{
    Any, Array, ArrayRef, ClientID, DeepObservable, Doc, GetString, Map as _, MapPrelim, MapRef, Number, OffsetKind, Options, Origin, Out, ReadTxn,
    StateVector, Text, TextPrelim, TextRef, Transact, TransactionMut, Update,
};

const BODY: &str = "body";
const BLOCKS: &str = "blocks";
const TEXT: &str = "t";
const ATTRS: &str = "a";
/// The origin of this replica's own edits, and the only one the undo manager tracks.
const LOCAL: &str = "craft-collab/local";
/// The origin of updates from other replicas, and of the state carried across `set_client`.
const REMOTE: &str = "craft-collab/remote";
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
}

/// One replica of a block document.
///
/// See the [module documentation](self) for the layout, the client id rules and the undo scope.
pub struct LiveDoc {
    // Declared before `doc` so it is dropped first and unhooks itself from a live document.
    undo: UndoManager<()>,
    doc: Doc,
    body: ArrayRef,
    blocks: MapRef,
    /// The relay-granted space, once [`LiveDoc::set_client`] has run.
    client: Option<u32>,
    /// Text and attribute changes the observer saw, drained by [`LiveDoc::apply`].
    seen: Arc<Mutex<Vec<BlockChange>>>,
}

impl LiveDoc {
    /// The document the file holds, as client 0.
    ///
    /// Deterministic: equal blocks give equal [`state`](Self::state) bytes on every replica.
    /// Attribute values that are JSON objects are the exception for the bytes only, because yrs
    /// encodes a nested map in hash order. The item ids, and so convergence, are unaffected.
    #[must_use]
    pub fn seeded(blocks: &[SeedBlock]) -> LiveDoc {
        let doc = build_doc(0);
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
        assemble(doc, None)
    }

    /// Mint later local edits with client id `space`.
    ///
    /// Space 0 is the seed's own, so `set_client(0)` is a no-op (logged): the replica keeps
    /// refusing local edits. Setting the space this replica already has is also a no-op.
    /// Any other space rebuilds the `Doc` with that client id and the current state, and starts a
    /// fresh undo history.
    pub fn set_client(&mut self, space: u32) {
        if space == 0 {
            log::warn!("collab: client id 0 belongs to the seed and is never adopted");
            return;
        }
        if self.client == Some(space) {
            return;
        }
        let state = self.state();
        let doc = build_doc(u64::from(space));
        let applied = Update::decode_v1(&state).ok().map(|update| {
            let mut txn = doc.transact_mut_with(REMOTE);
            txn.apply_update(update).is_ok()
        });
        if applied != Some(true) {
            log::warn!("collab: this replica's own state did not carry across a client change");
            return;
        }
        *self = assemble(doc, Some(space));
    }

    /// Merge an update from another replica, and say what it changed.
    ///
    /// Updates may arrive in any order and more than once. One whose dependencies have not
    /// arrived yet is held by yrs and lands when they do.
    pub fn apply(&mut self, update: &[u8]) -> Result<Vec<BlockChange>, LiveError> {
        let update = Update::decode_v1(update).map_err(|_| LiveError::Decode)?;
        let before = self.order();
        self.drain_seen();
        {
            let mut txn = self.doc.transact_mut_with(REMOTE);
            txn.apply_update(update).map_err(|_| LiveError::Decode)?;
        }
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
            let at = byte_at(&t.get_string(txn), char_at)?;
            t.insert_with_attributes(txn, at, text, attrs);
            Ok(())
        })
    }

    /// Delete `len` chars from `char_at`.
    pub fn delete_text(&mut self, id: u64, char_at: usize, len: usize) -> Result<Vec<u8>, LiveError> {
        self.local(|txn, s| {
            let t = s.live_text(txn, id)?;
            let (at, bytes) = byte_span(&t.get_string(txn), char_at, len)?;
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
            let (at, bytes) = byte_span(&t.get_string(txn), char_at, len)?;
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
            let string = t.get_string(txn);
            let total = string.chars().count();
            let (at, bytes) = byte_span(&string, char_at, total.checked_sub(char_at).ok_or(LiveError::OutOfRange)?)?;
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
            let (b, index) = s.live_block(txn, second).map_err(|_| LiveError::Gone)?;
            let into = text_of(txn, &a).ok_or(LiveError::UnknownBlock)?;
            let from = text_of(txn, &b).ok_or(LiveError::UnknownBlock)?;
            let moved = chunks(txn, &from);
            let mut at = into.len(txn);
            for (text, attrs) in moved {
                let len = u32::try_from(text.len()).map_err(|_| LiveError::OutOfRange)?;
                into.insert_with_attributes(txn, at, &text, run_attrs_of(&attrs));
                at += len;
            }
            s.body.remove(txn, index);
            Ok(())
        })
    }

    /// Insert a new block at `at` in the order.
    pub fn insert_block(&mut self, at: usize, block: &SeedBlock) -> Result<Vec<u8>, LiveError> {
        self.local(|txn, s| {
            let index = u32::try_from(at).map_err(|_| LiveError::OutOfRange)?;
            if index > s.body.len(txn) {
                return Err(LiveError::OutOfRange);
            }
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
            let (_, index) = s.live_block(txn, id)?;
            s.body.remove(txn, index);
            Ok(())
        })
    }

    /// The block ids in document order.
    #[must_use]
    pub fn order(&self) -> Vec<u64> {
        let txn = self.doc.transact();
        order_of(&txn, &self.body)
    }

    /// One block's text, attributes and runs, including a removed block's.
    #[must_use]
    pub fn block(&self, id: u64) -> Option<SeedBlock> {
        let txn = self.doc.transact();
        let Some(Out::YMap(block)) = self.blocks.get(&txn, &key(id)) else {
            return None;
        };
        let attrs = attrs_map(&txn, &block).map(|a| read_attrs(&txn, &a)).unwrap_or_default();
        let parts = text_of(&txn, &block).map(|t| chunks(&txn, &t)).unwrap_or_default();
        let text = parts.iter().map(|(s, _)| s.as_str()).collect();
        Some(SeedBlock { id, text, attrs, runs: canonical_runs(parts.iter().map(|(s, a)| (s.chars().count(), a.clone()))) })
    }

    /// Undo this replica's last local edit and return the update to send.
    pub fn undo(&mut self) -> Option<Vec<u8>> {
        if !self.undo.can_undo() {
            return None;
        }
        let before = self.doc.transact().state_vector();
        self.undo.undo_blocking().then(|| self.doc.transact().encode_diff_v1(&before))
    }

    /// Redo the last undone local edit and return the update to send.
    pub fn redo(&mut self) -> Option<Vec<u8>> {
        if !self.undo.can_redo() {
            return None;
        }
        let before = self.doc.transact().state_vector();
        self.undo.redo_blocking().then(|| self.doc.transact().encode_diff_v1(&before))
    }

    /// The whole document as one update (v1), for a checkpoint or a peer starting from nothing.
    #[must_use]
    pub fn state(&self) -> Vec<u8> {
        self.doc.transact().encode_state_as_update_v1(&StateVector::default())
    }

    /// Run one local edit in its own transaction and return its update.
    ///
    /// `edit` checks everything before it changes anything, because a yrs transaction commits
    /// whatever it holds when it is dropped, error or not.
    fn local<F>(&mut self, edit: F) -> Result<Vec<u8>, LiveError>
    where
        F: FnOnce(&mut TransactionMut, &Roots) -> Result<(), LiveError>,
    {
        if self.client.is_none() {
            return Err(LiveError::NoClient);
        }
        let roots = Roots { body: self.body.clone(), blocks: self.blocks.clone() };
        let mut txn = self.doc.transact_mut_with(LOCAL);
        edit(&mut txn, &roots)?;
        Ok(txn.encode_update_v1())
    }

    fn drain_seen(&self) -> Vec<BlockChange> {
        match self.seen.lock() {
            Ok(mut seen) => std::mem::take(&mut *seen),
            Err(_) => Vec::new(),
        }
    }
}

/// The two roots, handed to a local edit alongside its transaction.
struct Roots {
    body: ArrayRef,
    blocks: MapRef,
}

impl Roots {
    /// A block that is in the order, with its index there.
    fn live_block<T: ReadTxn>(&self, txn: &T, id: u64) -> Result<(MapRef, u32), LiveError> {
        let Some(Out::YMap(block)) = self.blocks.get(txn, &key(id)) else {
            return Err(LiveError::UnknownBlock);
        };
        let index = order_of(txn, &self.body).iter().position(|o| *o == id).ok_or(LiveError::Gone)?;
        Ok((block, u32::try_from(index).map_err(|_| LiveError::OutOfRange)?))
    }

    fn live_text<T: ReadTxn>(&self, txn: &T, id: u64) -> Result<TextRef, LiveError> {
        let (block, _) = self.live_block(txn, id)?;
        text_of(txn, &block).ok_or(LiveError::UnknownBlock)
    }
}

fn build_doc(client: u64) -> Doc {
    let mut options = Options::with_client_id(ClientID::new(client));
    options.offset_kind = OffsetKind::Bytes;
    // Never mutate on a remote apply: what this replica holds is exactly what it was sent.
    options.cleanup_formatting = false;
    Doc::with_options(options)
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
    undo.expand_scope(&doc, &body);
    undo.expand_scope(&doc, &blocks);
    undo.include_origin(Origin::from(LOCAL));
    LiveDoc { undo, doc, body, blocks, client, seen }
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

fn order_of<T: ReadTxn>(txn: &T, body: &ArrayRef) -> Vec<u64> {
    body.iter(txn)
        .filter_map(|v| match v {
            Out::Any(Any::Number(n)) => n.as_i64().map(|i| i as u64),
            _ => None,
        })
        .collect()
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
    let sorted: BTreeMap<&String, &Value> = block.attrs.iter().collect();
    for (k, v) in sorted {
        attrs.insert(txn, k.as_str(), to_any(v));
    }

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

/// Byte offset of char `char_at`, which may be the end of the text.
fn byte_at(s: &str, char_at: usize) -> Result<u32, LiveError> {
    let byte = if char_at == 0 { Some(0) } else { s.char_indices().map(|(b, _)| b).chain(std::iter::once(s.len())).nth(char_at) };
    byte.and_then(|b| u32::try_from(b).ok()).ok_or(LiveError::OutOfRange)
}

/// Byte offset and byte length of `len` chars from `char_at`.
fn byte_span(s: &str, char_at: usize, len: usize) -> Result<(u32, u32), LiveError> {
    let start = byte_at(s, char_at)?;
    let end = byte_at(s, char_at.checked_add(len).ok_or(LiveError::OutOfRange)?)?;
    Ok((start, end - start))
}

fn to_any(v: &Value) -> Any {
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

fn to_json(a: &Any) -> Value {
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
