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

/// Whether content of kind `kind` may go under `class`.
fn allowed(class: Class, kind: u8) -> bool {
    !(class == Class::List && matches!(kind, 4..=6))
}

/// Parents of items checked before, so the next keystroke (whose origin is the last one) does
/// not walk its parent's list again. Item parents never change, so an entry never goes stale.
#[derive(Default)]
pub(crate) struct Seen {
    map: BTreeMap<(u64, u32), (u32, Parent)>,
}

/// How many items [`Seen`] keeps before it starts over.
const SEEN_CAP: usize = 4096;

impl Seen {
    fn find(&self, id: (u64, u32)) -> Option<Parent> {
        let (start, (len, p)) = self.map.range(..=id).next_back()?;
        (start.0 == id.0 && id.1 < start.1.saturating_add(*len)).then(|| p.clone())
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
enum Resolved {
    Known(Parent),
    /// A dependency is missing: this id.
    Waiting((u64, u32)),
    /// Origin and right origin sit in different parents.
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
            let right = match &p.under {
                Under::Beside(_, r) => *r,
                _ => None,
            };
            let cut = have - p.id.1;
            Some(Placed { id: (p.id.0, have), len: p.len - cut, kind: p.kind, type_ref: p.type_ref, under: Under::Beside(Some((p.id.0, have - 1)), right) })
        })
        .collect();
    let items = items.as_slice();
    let index: BTreeMap<(u64, u32), usize> = items.iter().enumerate().map(|(i, p)| (p.id, i)).collect();
    let at = Local { items, index, sv: &sv, seen: &*seen };
    let mut memo = HashMap::new();
    let mut waiting = None;
    for i in 0..items.len() {
        // Whether everything it names is here (cheap), before where it lands (a walk of its
        // parent's list, so only when the answer matters: content a list cannot hold, or two
        // neighbours that could sit in two parents).
        if let Some(id) = missing(&sv, &at, &items[i]) {
            waiting = waiting.or(Some(id));
            continue;
        }
        let both = matches!(items[i].under, Under::Beside(Some(_), Some(_)));
        if !both && allowed(Class::List, items[i].kind) {
            continue;
        }
        match parent_of(txn, &sv, &at, i, &mut memo, 0) {
            Resolved::Waiting(id) => waiting = waiting.or(Some(id)),
            Resolved::Split => return Verdict::Refused,
            Resolved::Known(p) if !allowed(p.class, items[i].kind) => return Verdict::Refused,
            Resolved::Known(_) => {}
        }
    }
    let verdict = waiting.map_or(Verdict::Fits, Verdict::Waiting);
    if verdict == Verdict::Fits {
        for (i, p) in memo {
            if let Ok(p) = p {
                seen.keep(items[i].id, items[i].len, p);
            }
        }
    }
    verdict
}

/// An id `item` names that neither the document nor the update holds.
fn missing(sv: &StateVector, at: &Local<'_>, item: &Placed) -> Option<(u64, u32)> {
    let named = match &item.under {
        Under::Root(_) => [None, None],
        Under::Parent(id) => [Some(*id), None],
        Under::Beside(o, r) => [*o, *r],
    };
    named.into_iter().flatten().find(|id| !present(sv, *id) && at.find(*id).is_none())
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
fn beside<T: ReadTxn>(txn: &T, sv: &StateVector, at: &Local<'_>, i: usize, id: (u64, u32), memo: &mut HashMap<usize, Result<Parent, (u64, u32)>>, depth: usize) -> Resolved {
    match at.find(id) {
        Some(j) if j != i => parent_of(txn, sv, at, j, memo, depth + 1),
        Some(_) => Resolved::Known(OPEN),
        None if present(sv, id) => Resolved::Known(at.seen.find(id).unwrap_or_else(|| StickyIndex::from_id(yid(id), Assoc::After).get_offset(txn).map_or(OPEN, |o| of_branch(&o.branch)))),
        None => Resolved::Waiting(id),
    }
}

/// Where item `i` goes.
fn parent_of<T: ReadTxn>(txn: &T, sv: &StateVector, at: &Local<'_>, i: usize, memo: &mut HashMap<usize, Result<Parent, (u64, u32)>>, depth: usize) -> Resolved {
    if let Some(known) = memo.get(&i) {
        return known.clone().map_or_else(Resolved::Waiting, Resolved::Known);
    }
    if depth > 64 {
        // A chain this long inside one update: wait on the item itself, which never comes.
        return Resolved::Waiting(at.items[i].id);
    }
    let items = at.items;
    let resolved = match &items[i].under {
        Under::Root(name) => Resolved::Known(BranchID::Root(Arc::from(name.as_str())).get_branch(txn).map_or_else(
            || Parent { key: Some(Key::Root(name.clone())), class: Class::Open },
            |b| of_branch(&b),
        )),
        Under::Parent(id) => match at.find(*id) {
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
                    // yrs takes the origin's parent.
                    _ => l,
                },
                (Some(one), None) | (None, Some(one)) => one,
                (None, None) => Resolved::Known(OPEN),
            }
        }
    };
    match &resolved {
        Resolved::Known(p) => {
            memo.insert(i, Ok(p.clone()));
        }
        Resolved::Waiting(id) => {
            memo.insert(i, Err(*id));
        }
        Resolved::Split => {}
    }
    resolved
}
