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
//! Where an item lands is only known against the document: an item with an origin takes its
//! origin's parent and its map key (or, without an origin, its right origin's). So this check
//! runs in the replica before yrs integrates an update, and on the relay's shadow document.
//!
//! The rules: a list-like parent (array, map, xml element or fragment) takes no string, embed or
//! format content; a text takes no plain value, binary or sub-document in its list, no string,
//! embed or format as a map entry, and no string embed; an item's origin and right origin must
//! share a parent and a key; types nest at most [`MAX_NESTING`] deep; and the update's items
//! must be able to integrate in some order (no cycles). A parent whose type is not known yet (a
//! root no app has opened), and a parent that is not a type at all, take anything.
//!
//! Cost: every item of the document that a check may need is in an index ([`Seen`]) of where
//! it sits, kept as updates land and local edits are made, so a check is a few ordered-map
//! lookups per item: O(n log m) for n items against a document of m runs, with no walk of any
//! list. An id the index misses (a document built before the replica, say) rebuilds the index
//! from the document's state once per check. Updates of more than [`MAX_ITEMS`] items are
//! refused.

use std::collections::BTreeMap;
use std::sync::Arc;

use yrs::branch::BranchID;
use yrs::types::TypeRef;
use yrs::{ClientID, ReadTxn, StateVector};

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

/// Which type a parent is.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Key {
    Root(Arc<str>),
    Nested((u64, u32)),
}

/// Where an item sits, as yrs integrates it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Place {
    /// Its parent type; `None` when yrs gives it none (it names a collected or a non-type item),
    /// in which case yrs collects it or refuses the update.
    parent: Option<Key>,
    /// The map key it sits under, if it is a map entry rather than in its parent's list.
    sub: Option<Arc<str>>,
    /// How many types its parent sits under, counting the parent (an item under a root: 0).
    nest: u16,
    /// For a type item: what it holds.
    holds: Option<Class>,
}

const NOWHERE: Place = Place { parent: None, sub: None, nest: 0, holds: None };

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

/// How deep shared types may nest. yrs deletes, reads and encodes nested types recursively:
/// 5,000 levels overflow a 1 MiB stack (a browser's) natively in release, which aborts (yrs
/// #749). No Craft app nests more than a few levels.
pub(crate) const MAX_NESTING: usize = 64;

/// The most items one check takes. A 1 MiB document's full state has tens of thousands; past
/// this, an update is refused rather than checked (each item costs a few map lookups).
pub(crate) const MAX_ITEMS: usize = 1 << 20;

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

/// Where every item of the document sits, by id range: what a check looks up instead of
/// walking a list. Kept as updates land ([`Seen::commit`]) and local edits are made
/// ([`learn`]); rebuilt from the document's state when a check misses (see the module docs).
/// Item places never change, and a root's class is read from the document at each lookup, so
/// an entry does not go stale.
#[derive(Default)]
pub(crate) struct Seen {
    map: BTreeMap<(u64, u32), (u32, Place)>,
    /// What the last [`check`] that fit found, kept by [`Seen::commit`] once the update lands.
    /// Not before: an update that waits, or that yrs holds back, may never land, and another
    /// can bring the same ids with other content (found by the fuzz: a stale entry hid a split).
    staged: Vec<((u64, u32), u32, Place)>,
}

impl Seen {
    fn find(&self, id: (u64, u32)) -> Option<&Place> {
        let (start, (len, p)) = self.map.range(..=id).next_back()?;
        (start.0 == id.0 && id.1 < start.1.saturating_add(*len)).then_some(p)
    }

    /// How many runs the index holds (a measure of its memory).
    pub(crate) fn len(&self) -> usize {
        self.map.len()
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

    fn keep(&mut self, id: (u64, u32), len: u32, p: Place) {
        // Joined to the run before it when it continues it in the same place (typing).
        if let Some((start, (n, q))) = self.map.range_mut(..id).next_back()
            && start.0 == id.0
            && start.1.saturating_add(*n) == id.1
            && *q == p
            && p.holds.is_none()
        {
            *n += len;
            return;
        }
        self.map.insert(id, (len, p));
    }

    /// Start again from the document's whole state.
    fn rebuild<T: ReadTxn>(&mut self, txn: &T) {
        self.map.clear();
        let state = txn.encode_state_as_update_v1(&StateVector::default());
        let Some((_, items)) = crate::walk::items(&state) else {
            log::warn!("collab: the document's own state does not read; its index stays empty");
            return;
        };
        let mut scratch = Seen::default();
        let mut ctx = Ctx { txn, sv: StateVector::default(), seen: &mut scratch, rebuilt: true };
        if let Some(places) = place_all(&mut ctx, &items) {
            for (i, p) in places.into_iter().enumerate() {
                if let Some(Resolved::Known(p)) = p {
                    self.keep(items[i].id, items[i].len, p);
                }
            }
        }
    }
}

/// Where an item may be resolved: what yrs makes of it.
#[derive(Clone, Debug)]
enum Resolved {
    Known(Place),
    /// A dependency is missing: this id.
    Waiting((u64, u32)),
    /// Origin and right origin sit in different parents or under different keys, or the item
    /// names itself.
    Split,
}

/// What a check reads: the document (`sv` is what it counts as held), and the index.
struct Ctx<'a, T: ReadTxn> {
    txn: &'a T,
    sv: StateVector,
    seen: &'a mut Seen,
    /// The index was rebuilt during this check already.
    rebuilt: bool,
}

impl<T: ReadTxn> Ctx<'_, T> {
    /// Where the document's item covering `id` sits. One the index lacks rebuilds it (once per
    /// check); still missing, the clocks hold nothing yrs can name (collected).
    fn doc_place(&mut self, id: (u64, u32)) -> Place {
        if let Some(p) = self.seen.find(id) {
            return p.clone();
        }
        if !self.rebuilt {
            self.rebuilt = true;
            self.seen.rebuild(self.txn);
            if let Some(p) = self.seen.find(id) {
                return p.clone();
            }
        }
        NOWHERE
    }

    fn class(&mut self, key: &Key, places: &[Option<Resolved>], at: &Local<'_>) -> Class {
        match key {
            Key::Root(name) => BranchID::Root(name.clone()).get_branch(self.txn).map_or(Class::Open, |b| class_of(b.type_ref())),
            Key::Nested(id) => match at.find(*id, &self.sv) {
                Some(j) => match &places[j] {
                    Some(Resolved::Known(p)) => p.holds.unwrap_or(Class::Open),
                    _ => Class::Open,
                },
                None => self.doc_place(*id).holds.unwrap_or(Class::Open),
            },
        }
    }
}

/// The items of the update, by first id.
struct Local<'a> {
    items: &'a [Placed],
    index: BTreeMap<(u64, u32), usize>,
}

impl Local<'_> {
    /// The item of the update that covers `id`, unless the document already holds that id: yrs
    /// skips what it already has, so the document's item is the one that counts (an update can
    /// carry other content under a clock the document already used).
    fn find(&self, id: (u64, u32), sv: &StateVector) -> Option<usize> {
        if present(sv, id) {
            return None;
        }
        let (_, &i) = self.index.range(..=id).next_back()?;
        let p = &self.items[i];
        (p.id.0 == id.0 && id.1 < p.id.1.saturating_add(p.len)).then_some(i)
    }
}

/// What yrs keeps of `items` against a document holding `sv`: it skips what the document
/// already holds, and integrates the rest of an item it partly holds beside the last clock it
/// holds (keeping its right origin, or its named parent).
fn trimmed(items: &[Placed], sv: &StateVector) -> Vec<Placed> {
    items
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
        .collect()
}

/// Check `items` (one update, or an update and what yrs holds, in written order) against the
/// document `txn` reads, looking places up in `seen` (and filling it on a miss).
pub(crate) fn check<T: ReadTxn>(txn: &T, items: &[Placed], seen: &mut Seen) -> Verdict {
    let sv = txn.state_vector();
    let items = trimmed(items, &sv);
    seen.staged.clear();
    if items.len() > MAX_ITEMS {
        log::warn!("collab: an update of {} items is past the {MAX_ITEMS} a check takes; refused", items.len());
        return Verdict::Refused;
    }
    let mut ctx = Ctx { txn, sv, seen, rebuilt: false };
    let Some(places) = place_all(&mut ctx, &items) else { return Verdict::Refused };
    let at = Local { items: &items, index: items.iter().enumerate().map(|(i, p)| (p.id, i)).collect() };
    let mut waiting = None;
    for (i, item) in items.iter().enumerate() {
        let place = match &places[i] {
            Some(Resolved::Split) => return Verdict::Refused,
            Some(Resolved::Waiting(id)) => {
                waiting = waiting.or(Some(*id));
                continue;
            }
            Some(Resolved::Known(p)) => p.clone(),
            None => continue,
        };
        let Some(key) = &place.parent else { continue };
        let class = ctx.class(key, &places, &at);
        if !allowed(class, item.kind, place.sub.is_some()) {
            return Verdict::Refused;
        }
        // A text reads a string embed back as text (its diff cannot tell them apart), so an
        // app's offsets, taken from the characters, would count it as characters.
        if class == Class::Text && item.string_embed {
            return Verdict::Refused;
        }
        if item.kind == 7 && usize::from(place.nest) >= MAX_NESTING {
            return Verdict::Refused;
        }
    }
    let verdict = waiting.map_or(Verdict::Fits, Verdict::Waiting);
    if verdict == Verdict::Fits {
        for (i, p) in places.into_iter().enumerate() {
            if let Some(Resolved::Known(p)) = p {
                ctx.seen.staged.push((items[i].id, items[i].len, p));
            }
        }
    }
    verdict
}

/// Index a local edit's update: where each of its items sits, against the document as it was
/// before the edit (`before`), which `txn` now holds with the edit applied.
pub(crate) fn learn<T: ReadTxn>(txn: &T, update: &[u8], before: &StateVector, seen: &mut Seen) {
    let Some((_, items)) = crate::walk::items(update) else { return };
    let items = trimmed(&items, before);
    let mut ctx = Ctx { txn, sv: before.clone(), seen, rebuilt: false };
    if let Some(places) = place_all(&mut ctx, &items) {
        let sv = txn.state_vector();
        for (i, p) in places.into_iter().enumerate() {
            if let Some(Resolved::Known(p)) = p
                && present(&sv, (items[i].id.0, items[i].id.1 + (items[i].len - 1)))
            {
                ctx.seen.keep(items[i].id, items[i].len, p);
            }
        }
    }
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

/// An order in which the update's items can integrate: every item after the item before it in
/// its client's clocks and after every item of the update it names. `None` when there is none:
/// yrs takes a client's blocks in clock order, and an item only once what it names is in, so an
/// item that names itself, a later clock of its own client, or anything that leads back to it
/// waits in yrs for ever. yrs then integrates it whenever another update brings those ids, with
/// whatever content that one gives them (found by the fuzz: a hang, and a format landing in a
/// map). An update made from a document never does this.
fn order(at: &Local<'_>, sv: &StateVector) -> Option<Vec<usize>> {
    let n = at.items.len();
    let deps = |i: usize| {
        let p = &at.items[i];
        // The item before it in its client's clocks, if the update has it.
        let before = at.index.range(..p.id).next_back().filter(|((c, _), _)| *c == p.id.0).map(|(_, &j)| j);
        let names = named(p).into_iter().flatten().filter_map(|id| at.find(id, sv));
        before.into_iter().chain(names).collect::<Vec<usize>>()
    };
    // Depth-first, without recursion: 0 unseen, 1 on the path, 2 done. Done items are pushed
    // in post-order, dependencies first.
    let mut state = vec![0_u8; n];
    let mut out = Vec::with_capacity(n);
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
                    1 => return None,
                    0 => {
                        state[j] = 1;
                        let d = deps(j);
                        stack.push((j, d, 0));
                    }
                    _ => {}
                }
            } else {
                state[*node] = 2;
                out.push(*node);
                stack.pop();
            }
        }
    }
    Some(out)
}

/// Where each item goes, resolved in an order where what it names comes first, so no item is
/// resolved twice and nothing recurses. `None` when the items cannot all integrate (see
/// [`order`]).
fn place_all<T: ReadTxn>(ctx: &mut Ctx<'_, T>, items: &[Placed]) -> Option<Vec<Option<Resolved>>> {
    let at = Local { items, index: items.iter().enumerate().map(|(i, p)| (p.id, i)).collect() };
    let order = order(&at, &ctx.sv)?;
    let mut places: Vec<Option<Resolved>> = vec![None; items.len()];
    for i in order {
        let resolved = resolve(ctx, &at, &places, i);
        places[i] = Some(resolved);
    }
    Some(places)
}

/// What sits at `id` for item `i` beside it: an item of the update (already resolved), the
/// document's item, or nothing yet.
enum Near {
    Place(Place),
    Waiting((u64, u32)),
    Split,
}

fn near<T: ReadTxn>(ctx: &mut Ctx<'_, T>, at: &Local<'_>, places: &[Option<Resolved>], i: usize, id: (u64, u32)) -> Near {
    match at.find(id, &ctx.sv) {
        // It names itself: yrs holds it for ever (and `order` refuses that before here).
        Some(j) if j == i => Near::Split,
        Some(j) => match &places[j] {
            Some(Resolved::Known(p)) => Near::Place(p.clone()),
            Some(Resolved::Waiting(w)) => Near::Waiting(*w),
            Some(Resolved::Split) => Near::Split,
            // Not resolved before it: only possible in a cycle, which `order` refuses.
            None => Near::Split,
        },
        None if present(&ctx.sv, id) => Near::Place(ctx.doc_place(id)),
        None => Near::Waiting(id),
    }
}

/// The place of the type item `id` names as a parent: under it, one level deeper.
fn under_type<T: ReadTxn>(ctx: &mut Ctx<'_, T>, at: &Local<'_>, places: &[Option<Resolved>], i: usize, id: (u64, u32)) -> Result<Place, Resolved> {
    let item = match at.find(id, &ctx.sv) {
        Some(j) if j == i => return Err(Resolved::Split),
        Some(j) => match &places[j] {
            Some(Resolved::Known(p)) => p.clone(),
            Some(other) => return Err(other.clone()),
            None => return Err(Resolved::Split),
        },
        None if present(&ctx.sv, id) => ctx.doc_place(id),
        None => return Err(Resolved::Waiting(id)),
    };
    // A parent that is not a type item: yrs refuses the update or collects the item.
    if item.holds.is_none() {
        return Ok(NOWHERE);
    }
    Ok(Place { parent: Some(Key::Nested(id)), sub: None, nest: item.nest.saturating_add(1), holds: None })
}

/// Where item `i` goes.
fn resolve<T: ReadTxn>(ctx: &mut Ctx<'_, T>, at: &Local<'_>, places: &[Option<Resolved>], i: usize) -> Resolved {
    let item = &at.items[i];
    let holds = match (item.kind, item.type_ref) {
        (7, Some(b)) => Some(class_of_byte(b)),
        (7, None) => Some(Class::Open),
        _ => None,
    };
    let own_sub = item.sub.as_deref().map(Arc::from);
    // A collected range: nothing sits there.
    if item.kind == 0 {
        return Resolved::Known(NOWHERE);
    }
    let named = |ctx: &mut Ctx<'_, T>, under: &Under| -> Result<Place, Resolved> {
        match under {
            Under::Root(name) => Ok(Place { parent: Some(Key::Root(Arc::from(name.as_str()))), sub: None, nest: 0, holds: None }),
            Under::Parent(id) => under_type(ctx, at, places, i, *id),
            _ => Ok(NOWHERE),
        }
    };
    let placed = match &item.under {
        Under::Root(_) | Under::Parent(_) => match named(ctx, &item.under) {
            Ok(p) if p.parent.is_some() => Place { sub: own_sub, ..p },
            Ok(p) => p,
            Err(r) => return r,
        },
        Under::Beside(origin, right) => {
            let mut side = |id: Option<(u64, u32)>| match id.map(|id| near(ctx, at, places, i, id)) {
                None => Ok(None),
                Some(Near::Place(p)) => Ok(p.parent.is_some().then_some(p)),
                Some(Near::Waiting(w)) => Err(Resolved::Waiting(w)),
                Some(Near::Split) => Err(Resolved::Split),
            };
            // yrs finds no item at a collected origin and goes by the right origin then.
            let left = match side(*origin) {
                Ok(p) => p,
                Err(r) => return r,
            };
            let right = match side(*right) {
                Ok(p) => p,
                Err(r) => return r,
            };
            match (left, right) {
                (Some(l), Some(r)) if l.parent != r.parent || l.sub != r.sub => return Resolved::Split,
                // The parent and the key both come from the origin (or the right origin).
                (Some(n), _) | (None, Some(n)) => Place { holds: None, ..n },
                (None, None) => NOWHERE,
            }
        }
        Under::Within(under, last) => {
            let base = match named(ctx, under) {
                Ok(p) => p,
                Err(r) => return r,
            };
            match near(ctx, at, places, i, *last) {
                Near::Place(l) if l.parent.is_some() => {
                    // yrs keeps the named parent but links it after `last` and takes its key.
                    if base.parent.is_some() && l.parent != base.parent {
                        return Resolved::Split;
                    }
                    Place { sub: l.sub.or(own_sub), ..base }
                }
                Near::Place(_) => Place { sub: own_sub, ..base },
                Near::Waiting(w) => return Resolved::Waiting(w),
                Near::Split => return Resolved::Split,
            }
        }
    };
    Resolved::Known(Place { holds, ..placed })
}

fn present(sv: &StateVector, id: (u64, u32)) -> bool {
    sv.get(&ClientID::new(id.0)) > id.1
}
