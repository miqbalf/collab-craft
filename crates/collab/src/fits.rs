//! Whether every item of an update lands under a parent that can hold it.
//!
//! yrs takes an item of any content under any parent, and an item whose origin and right origin
//! sit in two different parents. No Craft app (nor Yjs) writes either, and yrs cannot read them
//! back:
//!
//! - a string item inside an array makes yrs's array iterator (`BlockIter::slice`) spin for
//!   ever once the string has a character wider than one offset unit, so a tab that reads that
//!   array hangs;
//! - an item placed between neighbours of two different parents links one parent's list into
//!   the other's, and a later local edit there panics (`Couldn't remove … elements`).
//!
//! Where an item lands is only known against the document (an item names its origin and takes
//! its origin's parent), so this check runs in the replica before yrs integrates an update, and
//! on the relay's shadow document.
//!
//! The rules: a list-like parent (array, map, xml element or fragment) takes no string, embed or
//! format content; an item's origin and right origin must share a parent. A text-like parent, a
//! parent whose type is not known yet (a root no app has opened), and a parent that is not a
//! type at all (yrs refuses or garbage-collects those itself) take anything.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use yrs::branch::{BranchID, BranchPtr};
use yrs::types::TypeRef;
use yrs::{Assoc, ClientID, ID, ReadTxn, StateVector, StickyIndex};

use crate::walk::{Placed, Under};

/// What a parent can hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    /// Text and xml text: characters, embeds, formats, nested types.
    Text,
    /// Array, map, xml element and fragment: values, binaries, nested types and documents.
    List,
    /// Not known (a root no app has typed yet), or not a type at all; yrs decides.
    Open,
}

/// A parent: which one (when it is a type), and what it can hold.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Parent {
    key: Option<Key>,
    class: Class,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Key {
    Root(String),
    Nested((u64, u32)),
}

/// The outcome of [`check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Every item's parent is known and can hold it.
    Fits,
    /// Some item lands where it cannot go.
    Refused,
    /// No item is refused, but some parent is not known yet: the document lacks this id.
    Waiting((u64, u32)),
}

fn class_of(t: &TypeRef) -> Class {
    match t {
        TypeRef::Text | TypeRef::XmlText => Class::Text,
        TypeRef::Array | TypeRef::Map | TypeRef::XmlElement(_) | TypeRef::XmlFragment => Class::List,
        _ => Class::Open,
    }
}

fn class_of_byte(b: u8) -> Class {
    match b {
        2 | 6 => Class::Text,
        0 | 1 | 3 | 4 => Class::List,
        _ => Class::Open,
    }
}

fn of_branch(b: &BranchPtr) -> Parent {
    let key = match b.id() {
        BranchID::Root(name) => Key::Root(name.to_string()),
        BranchID::Nested(id) => Key::Nested((id.client.get(), id.clock)),
    };
    Parent { key: Some(key), class: class_of(b.type_ref()) }
}

const OPEN: Parent = Parent { key: None, class: Class::Open };

/// How deep shared types may nest. yrs deletes, reads and encodes nested types recursively:
/// 5,000 levels overflow a 1 MiB stack (a browser's) natively in release, which aborts (yrs
/// #749). No Craft app nests more than a few levels.
pub(crate) const MAX_NESTING: usize = 64;

/// How many types `p` sits under, counting itself (a root is 0): through the update's own type
/// items, then up the document, which is a walk of each parent's list, so it stops past
/// [`MAX_NESTING`].
fn depth_of<T: ReadTxn>(
    txn: &T,
    sv: &StateVector,
    at: &Local<'_>,
    p: &Parent,
    memo: &mut HashMap<usize, Resolved>,
    depths: &mut HashMap<(u64, u32), usize>,
) -> usize {
    let mut key = p.key.clone();
    let mut chain = Vec::new();
    let mut depth = 0;
    let base = loop {
        match &key {
            None | Some(Key::Root(_)) => break 0,
            Some(Key::Nested(id)) => {
                if let Some(d) = depths.get(id) {
                    break *d;
                }
                if chain.len() > MAX_NESTING {
                    break MAX_NESTING;
                }
                chain.push(*id);
                key = match at.find(*id) {
                    // A type of this update: where it goes.
                    Some(j) => match parent_of(txn, sv, at, j, memo, 0) {
                        Resolved::Known(q) => q.key,
                        _ => None,
                    },
                    None => StickyIndex::from_id(yid(*id), Assoc::After).get_offset(txn).map(|o| of_branch(&o.branch)).and_then(|q| q.key),
                };
            }
        }
    };
    // Record every type on the way, deepest last.
    for id in chain.iter().rev() {
        depth += 1;
        depths.insert(*id, base + depth);
    }
    base + chain.len()
}

/// Whether content of kind `kind` may go under `class` (`keyed`: as a map entry of it).
///
/// A list takes no string, embed or format, keyed or not (a keyed string makes a map event
/// unwrap `None`). A text's list takes no plain value, binary or sub-document: yrs counts them
/// in the text's length but no reader shows them, so offsets an app computes from the
/// characters run past the end (`Couldn't remove … elements`). As map entries (attributes) a
/// text takes plain values, binaries and types, but no string, embed or format: those are list
/// content, and the fuzz found a keyed string that makes a later cut panic (text.rs:845 again).
fn allowed(class: Class, kind: u8, keyed: bool) -> bool {
    match class {
        Class::List => !matches!(kind, 4..=6),
        Class::Text if keyed => !matches!(kind, 4..=6),
        Class::Text => !matches!(kind, 3 | 8 | 9),
        Class::Open => true,
    }
}

/// Parents of items checked before, so the next keystroke (whose origin is the last one) does
/// not walk its parent's list again. Item parents never change, so an entry never goes stale.
#[derive(Default)]
pub(crate) struct Seen {
    map: BTreeMap<(u64, u32), (u32, Parent)>,
    /// What the last [`check`] that fit found, kept by [`Seen::commit`] once the update lands.
    /// Not before: an update that waits, or that yrs holds back, may never land, and another
    /// can bring the same ids with other content (found by the fuzz: a stale entry hid a split).
    staged: Vec<((u64, u32), u32, Parent)>,
}

/// How many items [`Seen`] keeps before it starts over.
const SEEN_CAP: usize = 4096;

impl Seen {
    fn find(&self, id: (u64, u32)) -> Option<Parent> {
        let (start, (len, p)) = self.map.range(..=id).next_back()?;
        (start.0 == id.0 && id.1 < start.1.saturating_add(*len)).then(|| p.clone())
    }

    /// Keep what the last check staged, for the items `txn`'s document now holds.
    pub(crate) fn commit<T: ReadTxn>(&mut self, txn: &T) {
        let sv = txn.state_vector();
        for (id, len, p) in std::mem::take(&mut self.staged) {
            if len > 0 && present(&sv, (id.0, id.1 + (len - 1))) {
                self.keep(id, len, p);
            }
        }
    }

    fn keep(&mut self, id: (u64, u32), len: u32, p: Parent) {
        if self.map.len() >= SEEN_CAP {
            self.map.clear();
        }
        self.map.insert(id, (len, p));
    }
}

/// The items of the update, by first id.
struct Local<'a> {
    items: &'a [Placed],
    index: BTreeMap<(u64, u32), usize>,
    sv: &'a StateVector,
    seen: &'a Seen,
}

impl Local<'_> {
    /// The item of the update that covers `id`, unless the document already holds that id: yrs
    /// skips what it already has, so the document's item is the one that counts (an update can
    /// carry other content under a clock the document already used).
    fn find(&self, id: (u64, u32)) -> Option<usize> {
        if present(self.sv, id) {
            return None;
        }
        let (_, &i) = self.index.range(..=id).next_back()?;
        let p = &self.items[i];
        (p.id.0 == id.0 && id.1 < p.id.1.saturating_add(p.len)).then_some(i)
    }
}

/// Where an item may be resolved: what yrs makes of it.
#[derive(Clone)]
enum Resolved {
    Known(Parent),
    /// A dependency is missing: this id.
    Waiting((u64, u32)),
    /// Origin and right origin sit in different parents, or the item names itself.
    Split,
}

/// Check `items` (one update, or an update and what yrs holds, in written order) against the
/// document `txn` reads.
pub(crate) fn check<T: ReadTxn>(txn: &T, items: &[Placed], seen: &mut Seen) -> Verdict {
    let sv = txn.state_vector();
    // yrs skips what the document already holds, and integrates the rest of an item that it
    // partly holds beside the last clock it holds (keeping its right origin).
    let items: Vec<Placed> = items
        .iter()
        .filter_map(|p| {
            let have = sv.get(&ClientID::new(p.id.0));
            if have <= p.id.1 {
                return Some(p.clone());
            }
            if have >= p.id.1.saturating_add(p.len) {
                return None;
            }
            let last = (p.id.0, have - 1);
            let under = match &p.under {
                Under::Beside(_, right) => Under::Beside(Some(last), *right),
                named => Under::Within(Box::new(named.clone()), last),
            };
            let cut = have - p.id.1;
            Some(Placed { id: (p.id.0, have), len: p.len - cut, under, ..p.clone() })
        })
        .collect();
    let items = items.as_slice();
    let index: BTreeMap<(u64, u32), usize> = items.iter().enumerate().map(|(i, p)| (p.id, i)).collect();
    let at = Local { items, index, sv: &sv, seen: &*seen };
    if cyclic(&at) {
        return Verdict::Refused;
    }
    let mut memo = HashMap::new();
    let mut depths = HashMap::new();
    let mut waiting = None;
    for (i, item) in items.iter().enumerate() {
        // Whether everything it names is here (cheap), before where it lands (a walk of its
        // parent's list, so only when the answer matters: content a list cannot hold, or two
        // neighbours that could sit in two parents).
        if let Some(id) = missing(&sv, &at, item) {
            waiting = waiting.or(Some(id));
            continue;
        }
        let both = matches!(item.under, Under::Beside(Some(_), Some(_)) | Under::Within(..));
        let nests = item.kind == 7;
        if !both && !nests && item.kind == 1 {
            continue;
        }
        match parent_of(txn, &sv, &at, i, &mut memo, 0) {
            Resolved::Waiting(id) => waiting = waiting.or(Some(id)),
            Resolved::Split => return Verdict::Refused,
            Resolved::Known(p) if !allowed(p.class, item.kind, item.sub) => return Verdict::Refused,
            // A text reads a string embed back as text (its diff cannot tell them apart), so an
            // app's offsets, taken from the characters, would count it as characters.
            Resolved::Known(p) if p.class == Class::Text && item.string_embed => return Verdict::Refused,
            Resolved::Known(p) if nests && depth_of(txn, &sv, &at, &p, &mut memo, &mut depths) >= MAX_NESTING => return Verdict::Refused,
            Resolved::Known(_) => {}
        }
    }
    let verdict = waiting.map_or(Verdict::Fits, Verdict::Waiting);
    seen.staged.clear();
    if verdict == Verdict::Fits {
        for (i, p) in memo {
            if let Resolved::Known(p) = p {
                seen.staged.push((items[i].id, items[i].len, p));
            }
        }
    }
    verdict
}

/// The ids an item names: its parent, origin and right origin, as far as the update says.
fn named(item: &Placed) -> [Option<(u64, u32)>; 2] {
    match &item.under {
        Under::Root(_) => [None, None],
        Under::Parent(id) => [Some(*id), None],
        Under::Beside(o, r) => [*o, *r],
        Under::Within(named, last) => [if let Under::Parent(id) = **named { Some(id) } else { None }, Some(*last)],
    }
}

/// Whether the update's items can never all integrate: yrs takes a client's blocks in clock
/// order, and an item only once what it names is in, so an item that names itself, a later
/// clock of its own client, or anything that leads back to it waits in yrs for ever. yrs then
/// integrates it whenever another update brings those ids, with whatever content that one
/// gives them (found by the fuzz: a hang, and a format landing in a map). An update made from
/// a document never does this.
fn cyclic(at: &Local<'_>) -> bool {
    let n = at.items.len();
    let deps = |i: usize| {
        let p = &at.items[i];
        // The item before it in its client's clocks, if the update has it.
        let before = at.index.range(..p.id).next_back().filter(|((c, _), _)| *c == p.id.0).map(|(_, &j)| j);
        let names = named(p).into_iter().flatten().filter_map(|id| at.find(id));
        before.into_iter().chain(names).collect::<Vec<usize>>()
    };
    // Depth-first, without recursion: 0 unseen, 1 on the path, 2 done.
    let mut state = vec![0_u8; n];
    for root in 0..n {
        if state[root] != 0 {
            continue;
        }
        let mut stack = vec![(root, deps(root), 0_usize)];
        state[root] = 1;
        while let Some((node, next, k)) = stack.last_mut() {
            if let Some(&j) = next.get(*k) {
                *k += 1;
                match state[j] {
                    1 => return true,
                    0 => {
                        state[j] = 1;
                        let d = deps(j);
                        stack.push((j, d, 0));
                    }
                    _ => {}
                }
            } else {
                state[*node] = 2;
                stack.pop();
            }
        }
    }
    false
}

/// An id `item` names that neither the document nor the update holds.
fn missing(sv: &StateVector, at: &Local<'_>, item: &Placed) -> Option<(u64, u32)> {
    named(item).into_iter().flatten().find(|id| !present(sv, *id) && at.find(*id).is_none())
}

fn present(sv: &StateVector, id: (u64, u32)) -> bool {
    sv.get(&ClientID::new(id.0)) > id.1
}

fn yid(id: (u64, u32)) -> ID {
    ID::new(ClientID::new(id.0), id.1)
}

fn known(r: &Resolved) -> Option<Parent> {
    match r {
        Resolved::Known(p) => Some(p.clone()),
        _ => None,
    }
}

/// The parent of whatever item covers `id`, as yrs would give it to an item beside it.
fn beside<T: ReadTxn>(
    txn: &T,
    sv: &StateVector,
    at: &Local<'_>,
    i: usize,
    id: (u64, u32),
    memo: &mut HashMap<usize, Resolved>,
    depth: usize,
) -> Resolved {
    match at.find(id) {
        Some(j) if j != i => parent_of(txn, sv, at, j, memo, depth + 1),
        // It names itself: yrs holds it for ever as missing a dependency, and integrates it once
        // another update brings that id with other content (found by the fuzz: a format then
        // lands in a map and the map event unwraps `None`).
        Some(_) => Resolved::Split,
        None if present(sv, id) => Resolved::Known(
            at.seen.find(id).unwrap_or_else(|| StickyIndex::from_id(yid(id), Assoc::After).get_offset(txn).map_or(OPEN, |o| of_branch(&o.branch))),
        ),
        None => Resolved::Waiting(id),
    }
}

/// Where item `i` goes.
fn parent_of<T: ReadTxn>(txn: &T, sv: &StateVector, at: &Local<'_>, i: usize, memo: &mut HashMap<usize, Resolved>, depth: usize) -> Resolved {
    if let Some(known) = memo.get(&i) {
        return known.clone();
    }
    if depth > 64 {
        // A chain this long inside one update: wait on the item itself, which never comes.
        return Resolved::Waiting(at.items[i].id);
    }
    // Marked first, so a cycle of items beside each other ends here as waiting (it never lands).
    memo.insert(i, Resolved::Waiting(at.items[i].id));
    let resolved = resolve(txn, sv, at, i, &at.items[i].under, memo, depth);
    memo.insert(i, resolved.clone());
    resolved
}

/// Where `under` puts item `i`.
fn resolve<T: ReadTxn>(
    txn: &T,
    sv: &StateVector,
    at: &Local<'_>,
    i: usize,
    under: &Under,
    memo: &mut HashMap<usize, Resolved>,
    depth: usize,
) -> Resolved {
    let items = at.items;
    match under {
        Under::Root(name) => Resolved::Known(
            BranchID::Root(Arc::from(name.as_str()))
                .get_branch(txn)
                .map_or_else(|| Parent { key: Some(Key::Root(name.clone())), class: Class::Open }, |b| of_branch(&b)),
        ),
        Under::Parent(id) => match at.find(*id) {
            Some(j) if j == i => Resolved::Split,
            // A parent in the same update: its own type, if it is one.
            Some(j) => Resolved::Known(match (items[j].kind, items[j].type_ref) {
                (7, Some(b)) => Parent { key: Some(Key::Nested(items[j].id)), class: class_of_byte(b) },
                _ => OPEN,
            }),
            None if present(sv, *id) => Resolved::Known(BranchID::Nested(yid(*id)).get_branch(txn).map_or(OPEN, |b| of_branch(&b))),
            None => Resolved::Waiting(*id),
        },
        Under::Beside(origin, right) => {
            let left = origin.map(|o| beside(txn, sv, at, i, o, memo, depth));
            let right = right.map(|r| beside(txn, sv, at, i, r, memo, depth));
            match (left, right) {
                (Some(Resolved::Split), _) | (_, Some(Resolved::Split)) => Resolved::Split,
                (Some(l), Some(r)) => match (known(&l), known(&r)) {
                    (Some(a), Some(b)) if a.key.is_some() && b.key.is_some() && a.key != b.key => Resolved::Split,
                    // An origin with no parent the doc can name (collected, so yrs finds no item
                    // there): yrs takes the right origin's parent instead.
                    (Some(a), Some(b)) if a.key.is_none() && b.key.is_some() => r,
                    // Otherwise yrs takes the origin's parent.
                    _ => l,
                },
                (Some(one), None) | (None, Some(one)) => one,
                (None, None) => Resolved::Known(OPEN),
            }
        }
        Under::Within(named, last) => {
            let named = resolve(txn, sv, at, i, named, memo, depth);
            let beside = beside(txn, sv, at, i, *last, memo, depth);
            match (known(&named), known(&beside)) {
                (Some(a), Some(b)) if a.key.is_some() && b.key.is_some() && a.key != b.key => Resolved::Split,
                _ => named,
            }
        }
    }
}
