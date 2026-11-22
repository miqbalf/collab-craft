//! A stable-toolchain fuzz harness for panics inside yrs that an update passing the relay gate
//! ([`update_is_sound`](crate::update_is_sound)) can still trigger in a replica.
//!
//! A browser replica (wasm32, `panic = "abort"`) dies on any panic, caught or not, and the relay
//! replays an ordered op to every joiner, so one such op would crash every co-editor on every
//! reload. The harness:
//!
//! 1. builds honest edit histories on three peers (text with formatting and embeds, maps,
//!    arrays, nested types, deletes; byte and UTF-16 offsets; and LiveDoc-shaped histories made
//!    through [`LiveDoc`]'s own API),
//! 2. takes an honest update, parses it into blocks ([`Upd`]) and mutates it structurally
//!    (origins and parents pointed at other, missing, deleted or other-type items, content of the
//!    wrong kind, delete ranges over embeds and types, other clients, reordered, dropped or
//!    duplicated blocks, skips and GC blocks, sections from other updates),
//! 3. keeps only what the gate accepts, and applies it to a replica that already holds the
//!    history up to that update, under a panic hook that records every panic, caught or not,
//! 4. then reads the replica, encodes it (yrs #720 panics at encode, not apply), round-trips the
//!    state into a fresh document, and makes local edits on it as an app would.
//!
//! Every distinct panic location is reported with a minimised reproducer (base state and op
//! bytes, hex), which becomes a regression test in [`fixtures`].
//!
//! The default run (`fuzz_gate_accepted_updates_never_panic`) is bounded and deterministic. The
//! long run is `#[ignore]`d: `C7_FUZZ_SECS` (wall time, default 600), `C7_FUZZ_SEED`,
//! `C7_FUZZ_THREADS` (default 3), and `C7_FUZZ_OUT` (a file the findings are appended to).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Once};

use yrs::types::text::YChange;
use yrs::types::{Attrs, Event, ToJson};
use yrs::updates::decoder::Decode;
use yrs::{
    Any, Array, ClientID, DeepObservable, Doc, GetString, Map, OffsetKind, Options, Out, ReadTxn, StateVector, Text, TextRef, Transact,
    TransactionMut, Update,
};

use crate::replica::YReplica;
use crate::ydoc::{Attributes, LiveDoc, SeedBlock};

pub(crate) mod fixtures;

// ---------------------------------------------------------------------------------------------
// Randomness

/// SplitMix64: small, deterministic, good enough to steer mutations.
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub(crate) fn below(&mut self, n: usize) -> usize {
        if n == 0 { 0 } else { (self.next() % n as u64) as usize }
    }

    fn one_in(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() { None } else { items.get(self.below(items.len())) }
    }
}

// ---------------------------------------------------------------------------------------------
// A v1 update as blocks

pub(crate) type Id = (u64, u32);

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Parent {
    /// Taken from the origin or right origin (none is written).
    Inherit,
    Root(String),
    Item(Id),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Item {
    pub(crate) origin: Option<Id>,
    pub(crate) right: Option<Id>,
    pub(crate) parent: Parent,
    pub(crate) sub: Option<String>,
    /// The parent-sub bit on an item that has an origin (yrs writes it; nothing is read).
    pub(crate) sub_bit: bool,
    pub(crate) kind: u8,
    pub(crate) content: Vec<u8>,
    pub(crate) len: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Blk {
    Gc(u32),
    Skip(u32),
    Item(Item),
}

impl Blk {
    fn len(&self) -> u32 {
        match self {
            Blk::Gc(n) | Blk::Skip(n) => *n,
            Blk::Item(i) => i.len,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Sec {
    pub(crate) client: u64,
    pub(crate) clock: u32,
    pub(crate) blocks: Vec<Blk>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Upd {
    pub(crate) secs: Vec<Sec>,
    pub(crate) ds: Vec<(u64, Vec<(u32, u32)>)>,
}

struct Rd<'a> {
    b: &'a [u8],
    at: usize,
}

impl Rd<'_> {
    fn byte(&mut self) -> Option<u8> {
        let v = *self.b.get(self.at)?;
        self.at += 1;
        Some(v)
    }

    fn var(&mut self) -> Option<u64> {
        let (mut out, mut shift) = (0_u64, 0);
        loop {
            let b = self.byte()?;
            out |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some(out);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    }

    fn u32(&mut self) -> Option<u32> {
        u32::try_from(self.var()?).ok()
    }

    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let out = self.b.get(self.at..self.at.checked_add(n)?)?;
        self.at += n;
        Some(out)
    }

    fn string(&mut self) -> Option<String> {
        let n = self.u32()? as usize;
        String::from_utf8(self.take(n)?.to_vec()).ok()
    }

    fn id(&mut self) -> Option<Id> {
        Some((self.var()?, self.u32()?))
    }

    fn any(&mut self, depth: usize) -> Option<()> {
        if depth > 64 {
            return None;
        }
        match self.byte()? {
            126 | 127 | 120 | 121 => Some(()),
            125 => {
                while self.byte()? & 0x80 != 0 {}
                Some(())
            }
            124 => self.take(4).map(|_| ()),
            123 | 122 => self.take(8).map(|_| ()),
            119 => self.string().map(|_| ()),
            118 => {
                for _ in 0..self.u32()? {
                    self.string()?;
                    self.any(depth + 1)?;
                }
                Some(())
            }
            117 => {
                for _ in 0..self.u32()? {
                    self.any(depth + 1)?;
                }
                Some(())
            }
            116 => {
                let n = self.u32()? as usize;
                self.take(n).map(|_| ())
            }
            _ => None,
        }
    }

    /// The content of kind `kind`: its raw bytes and its length in clocks.
    fn content(&mut self, kind: u8) -> Option<(Vec<u8>, u32)> {
        let start = self.at;
        let len = match kind {
            1 => self.u32()?,
            3 => {
                let n = self.u32()? as usize;
                self.take(n)?;
                1
            }
            4 => u32::try_from(self.string()?.encode_utf16().count()).ok()?,
            5 => {
                self.string()?;
                1
            }
            6 => {
                self.string()?;
                self.string()?;
                1
            }
            7 => {
                if self.byte()? == 3 {
                    self.string()?;
                }
                1
            }
            8 => {
                let n = self.u32()?;
                for _ in 0..n {
                    self.any(0)?;
                }
                n
            }
            9 => {
                self.string()?;
                self.any(0)?;
                1
            }
            _ => return None,
        };
        Some((self.b[start..self.at].to_vec(), len))
    }
}

impl Upd {
    /// Parse v1 bytes; `None` for anything the harness does not model (JSON content and the like).
    pub(crate) fn parse(bytes: &[u8]) -> Option<Upd> {
        let mut r = Rd { b: bytes, at: 0 };
        let mut u = Upd::default();
        for _ in 0..r.u32()? {
            let n = r.u32()?;
            let client = r.var()?;
            let clock = r.u32()?;
            let mut blocks = Vec::new();
            for _ in 0..n {
                let info = r.byte()?;
                blocks.push(match info {
                    0 => Blk::Gc(r.u32()?),
                    10 => Blk::Skip(r.u32()?),
                    _ => {
                        let origin = if info & 0x80 != 0 { Some(r.id()?) } else { None };
                        let right = if info & 0x40 != 0 { Some(r.id()?) } else { None };
                        let (mut parent, mut sub) = (Parent::Inherit, None);
                        if origin.is_none() && right.is_none() {
                            parent = if r.u32()? == 1 { Parent::Root(r.string()?) } else { Parent::Item(r.id()?) };
                            if info & 0x20 != 0 {
                                sub = Some(r.string()?);
                            }
                        }
                        let kind = info & 0x0f;
                        let (content, len) = r.content(kind)?;
                        let sub_bit = info & 0x20 != 0 && (origin.is_some() || right.is_some());
                        Blk::Item(Item { origin, right, parent, sub, sub_bit, kind, content, len })
                    }
                });
            }
            u.secs.push(Sec { client, clock, blocks });
        }
        for _ in 0..r.u32()? {
            let client = r.var()?;
            let mut ranges = Vec::new();
            for _ in 0..r.u32()? {
                ranges.push((r.u32()?, r.u32()?));
            }
            u.ds.push((client, ranges));
        }
        Some(u)
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut o = Vec::new();
        var(&mut o, self.secs.len() as u64);
        for s in &self.secs {
            var(&mut o, s.blocks.len() as u64);
            var(&mut o, s.client);
            var(&mut o, u64::from(s.clock));
            for b in &s.blocks {
                match b {
                    Blk::Gc(n) => {
                        o.push(0);
                        var(&mut o, u64::from(*n));
                    }
                    Blk::Skip(n) => {
                        o.push(10);
                        var(&mut o, u64::from(*n));
                    }
                    Blk::Item(i) => {
                        let inherit = i.origin.is_some() || i.right.is_some();
                        let sub = if inherit { i.sub_bit } else { i.sub.is_some() };
                        let info = i.kind
                            | if i.origin.is_some() { 0x80 } else { 0 }
                            | if i.right.is_some() { 0x40 } else { 0 }
                            | if sub { 0x20 } else { 0 };
                        o.push(info);
                        for id in [i.origin, i.right].into_iter().flatten() {
                            var(&mut o, id.0);
                            var(&mut o, u64::from(id.1));
                        }
                        if !inherit {
                            match &i.parent {
                                Parent::Item((c, k)) => {
                                    o.push(0);
                                    var(&mut o, *c);
                                    var(&mut o, u64::from(*k));
                                }
                                Parent::Root(name) => {
                                    o.push(1);
                                    string(&mut o, name);
                                }
                                Parent::Inherit => {
                                    o.push(1);
                                    string(&mut o, "t");
                                }
                            }
                            if let Some(s) = &i.sub {
                                string(&mut o, s);
                            }
                        }
                        o.extend_from_slice(&i.content);
                    }
                }
            }
        }
        var(&mut o, self.ds.len() as u64);
        for (c, ranges) in &self.ds {
            var(&mut o, *c);
            var(&mut o, ranges.len() as u64);
            for (s, l) in ranges {
                var(&mut o, u64::from(*s));
                var(&mut o, u64::from(*l));
            }
        }
        o
    }

    /// Every item id in the update, with its kind, parent-sub and (for types) its type byte.
    fn items(&self) -> Vec<(Id, &Item)> {
        let mut out = Vec::new();
        for s in &self.secs {
            let mut clock = s.clock;
            for b in &s.blocks {
                if let Blk::Item(i) = b {
                    out.push(((s.client, clock), i));
                }
                clock = clock.saturating_add(b.len());
            }
        }
        out
    }
}

pub(crate) fn var(o: &mut Vec<u8>, mut n: u64) {
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            o.push(b);
            return;
        }
        o.push(b | 0x80);
    }
}

fn string(o: &mut Vec<u8>, s: &str) {
    var(o, s.len() as u64);
    o.extend_from_slice(s.as_bytes());
}

/// Content that yrs can carry, of every kind the gate admits.
fn templates() -> Vec<(u8, Vec<u8>, u32)> {
    let mut t = Vec::new();
    let s = |x: &str| {
        let mut o = Vec::new();
        string(&mut o, x);
        o
    };
    for n in [1_u32, 2, 5] {
        let mut o = Vec::new();
        var(&mut o, u64::from(n));
        t.push((1, o, n));
    }
    t.push((3, vec![2, 1, 2], 1));
    t.push((4, s("a"), 1));
    t.push((4, s("h\u{e9}\u{1f600}"), 4));
    t.push((5, s("{\"src\":\"x\"}"), 1));
    t.push((5, s("null"), 1));
    let mut f = s("b");
    f.extend(s("true"));
    t.push((6, f, 1));
    let mut f = s("b");
    f.extend(s("null"));
    t.push((6, f, 1));
    for ty in [0_u8, 1, 2, 4, 6, 9, 15] {
        t.push((7, vec![ty], 1));
    }
    let mut x = vec![3];
    x.extend(s("p"));
    t.push((7, x, 1));
    // any: a string, an int, a map, an array, undefined
    t.push((8, vec![1, 119, 1, b'q'], 1));
    t.push((8, vec![2, 125, 7, 126], 2));
    t.push((8, vec![1, 118, 1, 1, b'k', 120], 1));
    t.push((8, vec![1, 117, 2, 127, 121], 1));
    // doc: guid and an empty map of options
    let mut d = s("sub-guid");
    d.extend([118, 0]);
    t.push((9, d, 1));
    t
}

// ---------------------------------------------------------------------------------------------
// Honest histories

/// Which replica the history is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Schema {
    /// Text "t", map "m", array "a", with nested types: GridCraft's and the generic shapes.
    Generic(Offsets),
    /// WordCraft's blocks, made through [`LiveDoc`].
    Live,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Offsets {
    Bytes,
    Utf16,
}

pub(crate) fn doc_for(client: u64, offsets: Offsets) -> Doc {
    let mut options = Options::with_client_id(ClientID::new(client));
    options.offset_kind = match offsets {
        Offsets::Bytes => OffsetKind::Bytes,
        Offsets::Utf16 => OffsetKind::Utf16,
    };
    options.cleanup_formatting = false;
    Doc::with_options(options)
}

/// A history: honest updates in one causal order, and the state after each prefix.
pub(crate) struct History {
    pub(crate) schema: Schema,
    pub(crate) updates: Vec<Vec<u8>>,
    /// `states[k]` is the state after `updates[..k]` applied in order.
    pub(crate) states: Vec<Vec<u8>>,
    pub(crate) parsed: Vec<Upd>,
}

/// The pieces of a text: byte (or UTF-16) offsets of each character or embed, and whether it is
/// a character. yrs cannot remove an embed by a byte range, so an app's cuts stay on characters.
fn pieces(text: &TextRef, t: &impl ReadTxn, offsets: Offsets) -> Vec<(u32, u32, bool)> {
    let mut out = Vec::new();
    let mut at = 0;
    for d in text.diff(t, YChange::identity) {
        if let Out::Any(Any::String(s)) = d.insert {
            for c in s.chars() {
                let w = match offsets {
                    Offsets::Bytes => c.len_utf8(),
                    Offsets::Utf16 => c.len_utf16(),
                } as u32;
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

fn insert_at(p: &[(u32, u32, bool)], i: usize) -> u32 {
    let i = i % (p.len() + 1);
    p.get(i).map_or_else(|| p.last().map_or(0, |x| x.1), |x| x.0)
}

fn char_run(p: &[(u32, u32, bool)], i: usize, n: usize) -> Option<(u32, u32)> {
    let chars: Vec<usize> = (0..p.len()).filter(|k| p[*k].2).collect();
    let k = *chars.get(i % chars.len().max(1))?;
    let mut j = k;
    while j + 1 < p.len() && p[j + 1].2 && j - k < n % 4 {
        j += 1;
    }
    Some((p[k].0, p[j].1 - p[k].0))
}

fn small_any(r: &mut Rng) -> Any {
    match r.below(6) {
        0 => Any::from(i64::try_from(r.next() >> 12).unwrap_or(0) - (1 << 40)),
        1 => Any::from(["x", "h\u{e9}", "\u{1f600}", ""][r.below(4)]),
        2 => Any::Bool(r.one_in(2)),
        3 => Any::Null,
        4 => Any::from(vec![Any::from(1.5), Any::Undefined]),
        _ => Any::Map(Arc::new(std::collections::HashMap::from([("k".to_owned(), Any::from("v"))]))),
    }
}

/// The nested types reachable from the roots, up to a depth.
fn nested(txn: &impl ReadTxn, _doc: &Doc) -> Vec<Out> {
    let mut out = Vec::new();
    // Through the transaction: `Doc::get_or_insert_*` would wait for the one held here.
    let mut stack: Vec<(Out, usize)> = Vec::new();
    stack.extend(txn.get_text("t").map(|x| (Out::YText(x), 0)));
    stack.extend(txn.get_map("m").map(|x| (Out::YMap(x), 0)));
    stack.extend(txn.get_array("a").map(|x| (Out::YArray(x), 0)));
    while let Some((v, d)) = stack.pop() {
        if d > 8 || out.len() > 64 {
            continue;
        }
        match &v {
            Out::YMap(m) => stack.extend(m.iter(txn).map(|(_, x)| (x, d + 1))),
            Out::YArray(a) => stack.extend(a.iter(txn).map(|x| (x, d + 1))),
            _ => {}
        }
        out.push(v);
    }
    out
}

/// A generic history on three peers.
fn generic_history(seed: u64, offsets: Offsets, steps: usize) -> Vec<Vec<u8>> {
    let mut r = Rng(seed);
    let docs: Vec<Doc> = (1..=3).map(|c| doc_for(c, offsets)).collect();
    for d in &docs {
        d.get_or_insert_text("t");
        d.get_or_insert_map("m");
        d.get_or_insert_array("a");
    }
    let mut out = Vec::new();
    for _ in 0..steps {
        let p = r.below(3);
        let doc = &docs[p];
        let from = doc.transact().state_vector();
        if r.one_in(4) {
            let src = &docs[r.below(3)];
            let diff = src.transact().encode_diff_v1(&from);
            doc.transact_mut().apply_update(Update::decode_v1(&diff).unwrap()).unwrap();
            continue;
        }
        let made = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut t = doc.transact_mut();
            let targets = nested(&t, doc);
            let target = targets[r.below(targets.len())].clone();
            match target {
                Out::YText(text) => {
                    let p = pieces(&text, &t, offsets);
                    match r.below(6) {
                        0 | 1 => {
                            let s = ["a", "na\u{ef}ve ", "\u{1f600}x", "\u{4e2d}"][r.below(4)];
                            let attrs: Attrs = if r.one_in(2) { [("b".into(), Any::Bool(true))].into_iter().collect() } else { Attrs::new() };
                            text.insert_with_attributes(&mut t, insert_at(&p, r.below(64)), s, attrs);
                        }
                        2 => {
                            let img = Any::Map(Arc::new(std::collections::HashMap::from([("src".to_owned(), Any::from("img"))])));
                            text.insert_embed(&mut t, insert_at(&p, r.below(64)), img);
                        }
                        3 => {
                            if let Some((a, n)) = char_run(&p, r.below(64), r.below(8)) {
                                text.remove_range(&mut t, a, n);
                            }
                        }
                        4 => {
                            if let Some((a, n)) = char_run(&p, r.below(64), r.below(8)) {
                                text.format(&mut t, a, n, [("i".into(), if r.one_in(3) { Any::Null } else { Any::Bool(true) })].into_iter().collect());
                            }
                        }
                        _ => {
                            let _ = text.insert_embed(&mut t, insert_at(&p, r.below(64)), yrs::MapPrelim::from([("x", Any::from(1))]));
                        }
                    }
                }
                Out::YMap(m) => {
                    let key = format!("k{}", r.below(5));
                    match r.below(6) {
                        0 | 1 => {
                            m.insert(&mut t, key, small_any(&mut r));
                        }
                        2 => {
                            m.insert(&mut t, key, yrs::MapPrelim::from([("x", small_any(&mut r))]));
                        }
                        3 => {
                            m.insert(&mut t, key, yrs::ArrayPrelim::from([small_any(&mut r)]));
                        }
                        4 => {
                            m.insert(&mut t, key, yrs::TextPrelim::new("in map"));
                        }
                        _ => {
                            m.remove(&mut t, &key);
                        }
                    }
                }
                Out::YArray(a) => {
                    let n = a.len(&t);
                    let i = r.below(n as usize + 1) as u32;
                    match r.below(6) {
                        0 | 1 => {
                            a.insert(&mut t, i, small_any(&mut r));
                        }
                        2 => {
                            a.insert(&mut t, i, yrs::TextPrelim::new("in array"));
                        }
                        3 => {
                            a.insert(&mut t, i, yrs::MapPrelim::from([("y", small_any(&mut r))]));
                        }
                        4 => {
                            a.insert(&mut t, i, yrs::ArrayPrelim::from([small_any(&mut r), small_any(&mut r)]));
                        }
                        _ if n > 0 => {
                            let i = i.min(n - 1);
                            a.remove_range(&mut t, i, (r.below(3) as u32 + 1).min(n - i));
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }));
        if made.is_err() {
            // The honest generator made an edit yrs will not take; skip it.
            continue;
        }
        out.push(doc.transact().encode_diff_v1(&from));
    }
    out
}

fn attrs(pairs: &[(&str, serde_json::Value)]) -> Attributes {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect()
}

/// A WordCraft-shaped history on three [`LiveDoc`] peers seeded from the same file.
fn live_history(seed: u64, steps: usize) -> Vec<Vec<u8>> {
    let mut r = Rng(seed);
    let seed_blocks = vec![
        SeedBlock { id: 1, text: "Hello w\u{f6}rld".into(), attrs: attrs(&[("style", "h1".into())]), runs: vec![(5, attrs(&[("b", true.into())]))] },
        SeedBlock { id: 2, text: "Second \u{1f600} line".into(), attrs: Attributes::new(), runs: vec![] },
        SeedBlock { id: 3, text: String::new(), attrs: Attributes::new(), runs: vec![] },
    ];
    let mut peers: Vec<LiveDoc> = (1..=3_u32)
        .map(|c| {
            let mut d = LiveDoc::seeded(&seed_blocks).unwrap();
            d.set_client(c).unwrap();
            d
        })
        .collect();
    let mut out: Vec<Vec<u8>> = vec![LiveDoc::seeded(&seed_blocks).unwrap().state()];
    let mut seen = [1_usize; 3];
    let mut next_id = 100_u64;
    for _ in 0..steps {
        let p = r.below(3);
        if r.one_in(3) {
            for u in &out[seen[p]..] {
                let _ = peers[p].apply(u);
            }
            seen[p] = out.len();
        }
        let d = &mut peers[p];
        let order = d.order();
        let Some(&id) = r.pick(&order) else { continue };
        let len = d.block(id).map_or(0, |b| b.text.chars().count());
        let at = r.below(len + 1);
        let n = r.below(4) + 1;
        let made = match r.below(11) {
            0..=2 => d.insert_text(id, at, ["x", "\u{e9}t\u{e9} ", "\u{1f600}"][r.below(3)], &attrs(&[("i", true.into())])),
            3 => d.delete_text(id, at.min(len.saturating_sub(1)), n.min(len - at.min(len.saturating_sub(1)))),
            4 => d.format(id, at.min(len), n.min(len - at.min(len)), &attrs(&[("b", if r.one_in(2) { serde_json::Value::Null } else { true.into() })])),
            5 => d.set_attrs(id, &attrs(&[("align", "center".into())])),
            6 => {
                next_id += 1;
                d.split(id, at, next_id)
            }
            7 => match r.pick(&order) {
                Some(&other) => d.merge(id, other),
                None => continue,
            },
            8 => {
                next_id += 1;
                d.insert_block(r.below(order.len() + 1), &SeedBlock { id: next_id, text: "new".into(), ..SeedBlock::default() })
            }
            9 if order.len() > 1 => d.remove_block(id),
            _ => match d.undo() {
                Some(u) => Ok(u),
                None => continue,
            },
        };
        if let Ok(u) = made {
            out.push(u);
        }
    }
    out
}

impl History {
    pub(crate) fn new(schema: Schema, seed: u64, steps: usize) -> History {
        let updates = match schema {
            Schema::Generic(o) => generic_history(seed, o, steps),
            Schema::Live => live_history(seed, steps),
        };
        let doc = match schema {
            Schema::Generic(o) => doc_for(98, o),
            Schema::Live => doc_for(98, Offsets::Bytes),
        };
        let mut states = vec![doc.transact().encode_state_as_update_v1(&StateVector::default())];
        for u in &updates {
            doc.transact_mut().apply_update(Update::decode_v1(u).unwrap()).unwrap();
            states.push(doc.transact().encode_state_as_update_v1(&StateVector::default()));
        }
        let parsed = updates.iter().chain(states.last()).filter_map(|u| Upd::parse(u)).collect();
        History { schema, updates, states, parsed }
    }
}

// ---------------------------------------------------------------------------------------------
// Mutation

/// Ids and names to point mutated fields at.
struct Pool {
    ids: Vec<Id>,
    /// Ids of items of each kind (embed, type, format, deleted, any, string).
    by_kind: BTreeMap<u8, Vec<Id>>,
    /// Ids under a parent-sub (map entries).
    keyed: Vec<Id>,
    deleted: Vec<Id>,
    clients: Vec<u64>,
    clocks: BTreeMap<u64, u32>,
    roots: Vec<String>,
    subs: Vec<String>,
    contents: Vec<(u8, Vec<u8>, u32)>,
}

impl Pool {
    fn new(base: &Upd, others: &[&Upd]) -> Pool {
        let mut p = Pool {
            ids: Vec::new(),
            by_kind: BTreeMap::new(),
            keyed: Vec::new(),
            deleted: Vec::new(),
            clients: vec![0, 1, 2, 3, 98, 99, 777, (1 << 53) - 1],
            clocks: BTreeMap::new(),
            roots: ["t", "m", "a", "body", "blocks", "zz", ""].iter().map(|s| (*s).to_owned()).collect(),
            subs: ["k0", "k1", "t", "a", "1", "x", ""].iter().map(|s| (*s).to_owned()).collect(),
            contents: templates(),
        };
        for u in std::iter::once(base).chain(others.iter().copied()) {
            for s in &u.secs {
                let end = s.clock.saturating_add(s.blocks.iter().map(Blk::len).sum());
                let c = p.clocks.entry(s.client).or_default();
                *c = (*c).max(end);
                p.clients.push(s.client);
            }
            for ((c, k), i) in u.items() {
                for d in 0..i.len.min(4) {
                    p.ids.push((c, k + d));
                }
                if i.len > 1 {
                    p.ids.push((c, k + i.len - 1));
                }
                let kind = if i.kind == 7 { 0x70 + i.content.first().copied().unwrap_or(0) } else { i.kind };
                p.by_kind.entry(kind).or_default().push((c, k));
                if i.sub.is_some() {
                    p.keyed.push((c, k));
                }
                if i.kind == 1 {
                    p.deleted.push((c, k));
                }
                if p.contents.len() < 200 {
                    p.contents.push((i.kind, i.content.clone(), i.len));
                }
            }
            for (c, ranges) in &u.ds {
                for (s, _) in ranges {
                    p.deleted.push((*c, *s));
                }
            }
        }
        p.clients.sort_unstable();
        p.clients.dedup();
        p
    }

    fn id(&self, r: &mut Rng) -> Id {
        match r.below(10) {
            0 => {
                // Just past what a client has.
                let c = *r.pick(&self.clients).unwrap();
                let k = self.clocks.get(&c).copied().unwrap_or(0);
                (c, k + r.below(3) as u32)
            }
            1 => *r.pick(&self.deleted).unwrap_or(&(1, 0)),
            2 => *r.pick(&self.keyed).unwrap_or(&(1, 0)),
            3 | 4 => {
                let kinds: Vec<&Vec<Id>> = self.by_kind.values().collect();
                r.pick(&kinds).and_then(|v| r.pick(v)).copied().unwrap_or((1, 0))
            }
            5 => (*r.pick(&self.clients).unwrap(), r.below(8) as u32),
            _ => *r.pick(&self.ids).unwrap_or(&(1, 0)),
        }
    }

    fn parent(&self, r: &mut Rng) -> Parent {
        if r.one_in(2) { Parent::Root(r.pick(&self.roots).unwrap().clone()) } else { Parent::Item(self.id(r)) }
    }
}

fn item_at(u: &mut Upd, i: usize) -> &mut Item {
    items_mut(u).swap_remove(i)
}

fn items_mut(u: &mut Upd) -> Vec<&mut Item> {
    u.secs
        .iter_mut()
        .flat_map(|s| s.blocks.iter_mut())
        .filter_map(|b| match b {
            Blk::Item(i) => Some(i),
            _ => None,
        })
        .collect()
}

/// One structural mutation.
fn mutate(u: &mut Upd, pool: &Pool, donors: &[Upd], r: &mut Rng) {
    let n_items = items_mut(u).len();
    let pick_item = |_: &mut Upd, r: &mut Rng| -> Option<usize> { if n_items == 0 { None } else { Some(r.below(n_items)) } };
    match r.below(22) {
        0 | 1 => {
            if let Some(i) = pick_item(u, r) {
                let id = pool.id(r);
                let it = item_at(u, i);
                it.origin = Some(id);
            }
        }
        2 | 3 => {
            if let Some(i) = pick_item(u, r) {
                let id = pool.id(r);
                item_at(u, i).right = Some(id);
            }
        }
        4 => {
            if let Some(i) = pick_item(u, r) {
                let parent = pool.parent(r);
                let sub = if r.one_in(2) { Some(r.pick(&pool.subs).unwrap().clone()) } else { None };
                let it = item_at(u, i);
                it.origin = None;
                it.right = None;
                it.parent = parent;
                it.sub = sub;
            }
        }
        5 => {
            if let Some(i) = pick_item(u, r) {
                let it = item_at(u, i);
                if r.one_in(2) {
                    it.origin = None;
                } else {
                    it.right = None;
                }
                if it.origin.is_none() && it.right.is_none() && it.parent == Parent::Inherit {
                    it.parent = pool.parent(r);
                }
            }
        }
        6 | 7 => {
            if let Some(i) = pick_item(u, r) {
                let (kind, content, len) = r.pick(&pool.contents).unwrap().clone();
                let it = item_at(u, i);
                it.kind = kind;
                it.content = content;
                it.len = len;
            }
        }
        8 => {
            if let Some(i) = pick_item(u, r) {
                let it = item_at(u, i);
                it.sub = if it.sub.is_some() && r.one_in(2) { None } else { Some(r.pick(&pool.subs).unwrap().clone()) };
                it.sub_bit = !it.sub_bit;
            }
        }
        9 | 10 => {
            // Delete ranges: over an embed, a type, a format, a key, a missing id, a long run.
            let (c, k) = pool.id(r);
            let len = [1, 1, 2, 3, 7, 50][r.below(6)];
            match u.ds.iter_mut().find(|(x, _)| *x == c) {
                Some((_, ranges)) => ranges.push((k, len)),
                None => u.ds.push((c, vec![(k, len)])),
            }
        }
        11 => {
            let n = u.ds.len().max(1);
            if let Some((_, ranges)) = u.ds.get_mut(r.below(n)) {
                let m = ranges.len().max(1);
                if let Some(rg) = ranges.get_mut(r.below(m)) {
                    rg.1 = rg.1.saturating_add([1, 3, 100][r.below(3)]);
                    if r.one_in(2) {
                        rg.0 = rg.0.saturating_sub(1);
                    }
                }
            }
        }
        12 => {
            if let Some(s) = { let n = u.secs.len().max(1); u.secs.get_mut(r.below(n)) } {
                s.client = *r.pick(&pool.clients).unwrap();
            }
        }
        13 => {
            if let Some(s) = { let n = u.secs.len().max(1); u.secs.get_mut(r.below(n)) } {
                let have = pool.clocks.get(&s.client).copied().unwrap_or(0);
                s.clock = match r.below(4) {
                    0 => 0,
                    1 => have,
                    2 => have.saturating_sub(1),
                    _ => s.clock.saturating_sub(1 + r.below(3) as u32),
                };
            }
        }
        14 => {
            if let Some(s) = { let n = u.secs.len().max(1); u.secs.get_mut(r.below(n)) } {
                if s.blocks.len() > 1 {
                    let (a, b) = (r.below(s.blocks.len()), r.below(s.blocks.len()));
                    s.blocks.swap(a, b);
                }
            }
            if u.secs.len() > 1 && r.one_in(2) {
                u.secs.swap(0, 1);
            }
        }
        15 => {
            if let Some(s) = { let n = u.secs.len().max(1); u.secs.get_mut(r.below(n)) } {
                if !s.blocks.is_empty() {
                    let at = r.below(s.blocks.len());
                    match r.below(3) {
                        0 => s.blocks.truncate(at.max(1)),
                        1 => {
                            s.blocks.remove(at);
                        }
                        _ => {
                            let first = s.blocks.remove(0);
                            s.clock = s.clock.saturating_add(first.len());
                        }
                    }
                }
            }
        }
        16 => {
            if let Some(s) = { let n = u.secs.len().max(1); u.secs.get_mut(r.below(n)) } {
                let at = r.below(s.blocks.len() + 1);
                let n = [1, 1, 2, 5][r.below(4)];
                s.blocks.insert(at, if r.one_in(2) { Blk::Skip(n) } else { Blk::Gc(n) });
            }
        }
        17 => {
            if let Some(s) = { let n = u.secs.len().max(1); u.secs.get_mut(r.below(n)) } {
                if !s.blocks.is_empty() {
                    let at = r.below(s.blocks.len());
                    let b = s.blocks[at].clone();
                    s.blocks.insert(at, b);
                }
            }
        }
        18 | 19 => {
            // A section from another update, appended or merged under an existing client.
            if let Some(d) = r.pick(donors) {
                if let Some(s) = r.pick(&d.secs) {
                    let mut s = s.clone();
                    if let Some(own) = u.secs.iter_mut().find(|x| x.client == s.client) {
                        own.blocks.append(&mut s.blocks);
                    } else {
                        u.secs.push(s);
                    }
                }
                if r.one_in(2) {
                    if let Some(ds) = r.pick(&d.ds) {
                        if !u.ds.iter().any(|(c, _)| *c == ds.0) {
                            u.ds.push(ds.clone());
                        }
                    }
                }
            }
        }
        20 => {
            if let Some(i) = pick_item(u, r) {
                let it = item_at(u, i);
                // An origin of its own neighbour block or the same item.
                if let Some(o) = it.origin {
                    it.right = Some(o);
                } else if let Some(rt) = it.right {
                    it.origin = Some(rt);
                }
            }
        }
        _ => {
            // A new block: an item with a random parent and content.
            let (kind, content, len) = r.pick(&pool.contents).unwrap().clone();
            let item = Item {
                origin: if r.one_in(2) { Some(pool.id(r)) } else { None },
                right: if r.one_in(3) { Some(pool.id(r)) } else { None },
                parent: pool.parent(r),
                sub: if r.one_in(3) { Some(r.pick(&pool.subs).unwrap().clone()) } else { None },
                sub_bit: false,
                kind,
                content,
                len,
            };
            let item = Blk::Item(Item { parent: if item.origin.is_some() || item.right.is_some() { Parent::Inherit } else { item.parent.clone() }, ..item });
            if u.secs.is_empty() || r.one_in(3) {
                let c = *r.pick(&pool.clients).unwrap();
                if !u.secs.iter().any(|s| s.client == c) {
                    u.secs.push(Sec { client: c, clock: pool.clocks.get(&c).copied().unwrap_or(0), blocks: vec![item] });
                }
            } else {
                let at = r.below(u.secs.len());
                u.secs[at].blocks.push(item);
            }
        }
    }
    // Keep an item without inherited parent info readable.
    for it in items_mut(u) {
        if it.origin.is_none() && it.right.is_none() && it.parent == Parent::Inherit {
            it.parent = Parent::Root("t".into());
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Panic capture

thread_local! {
    static CAPTURING: Cell<bool> = const { Cell::new(false) };
    static CAUGHT: RefCell<Option<(String, String)>> = const { RefCell::new(None) };
}

static HOOK: Once = Once::new();

/// The phase the newest case is in, for the hang reports of [`fuzz_case`].
static PHASE: std::sync::Mutex<&str> = std::sync::Mutex::new("");

/// Install a hook that records panics on threads that are capturing and passes every other
/// panic to the previous hook, so other tests still print theirs.
pub(crate) fn install_hook() {
    HOOK.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if CAPTURING.with(Cell::get) {
                let at = info.location().map_or_else(|| "?".to_owned(), |l| format!("{}:{}", short(l.file()), l.line()));
                let msg = info
                    .payload()
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| info.payload().downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                CAUGHT.with(|c| {
                    let mut c = c.borrow_mut();
                    if c.is_none() {
                        *c = Some((at, msg));
                    }
                });
            } else {
                prev(info);
            }
        }));
    });
}

fn short(file: &str) -> String {
    match file.find("yrs-") {
        Some(i) => file[i..].split_once('/').map_or(file, |(_, rest)| rest).to_owned().replace("src/", "yrs/"),
        None => file.to_owned(),
    }
}

/// A panic found: where (`file:line`), its message, and the phase it happened in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Found {
    pub(crate) at: String,
    pub(crate) msg: String,
    pub(crate) phase: &'static str,
}

/// Run `f` capturing any panic; the first one wins.
fn capture<R>(phase: &'static str, found: &mut Option<Found>, f: impl FnOnce() -> R) -> Option<R> {
    *PHASE.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = phase;
    CAPTURING.with(|c| c.set(true));
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    CAPTURING.with(|c| c.set(false));
    if let Some((at, msg)) = CAUGHT.with(|c| c.borrow_mut().take()) {
        if found.is_none() {
            *found = Some(Found { at, msg, phase });
        }
    }
    out.ok()
}

// ---------------------------------------------------------------------------------------------
// Targets

/// Read every root and nested type as an app would.
fn read_all(doc: &Doc) {
    let txn = doc.transact();
    for v in nested(&txn, doc) {
        match v {
            Out::YText(t) => {
                let _ = t.get_string(&txn);
                let _ = t.diff(&txn, YChange::identity);
                let _ = t.len(&txn);
            }
            Out::YMap(m) => {
                let _ = m.to_json(&txn);
            }
            Out::YArray(a) => {
                let _ = a.to_json(&txn);
            }
            Out::YXmlElement(x) => {
                let _ = x.get_string(&txn);
            }
            Out::YXmlFragment(x) => {
                let _ = x.get_string(&txn);
            }
            Out::YXmlText(x) => {
                let _ = x.get_string(&txn);
            }
            Out::YDoc(d) => {
                let _ = d.guid();
            }
            _ => {}
        }
    }
}

/// Encode the document every way a replica or the relay does, and load it again.
fn encode_all(doc: &Doc, before: &StateVector, offsets: Offsets) {
    let state = doc.transact().encode_state_as_update_v1(&StateVector::default());
    let _ = doc.transact().encode_diff_v1(before);
    let _ = doc.transact().state_vector();
    let fresh = doc_for(97, offsets);
    if let Ok(u) = Update::decode_v1(&state) {
        let _ = fresh.transact_mut().apply_update(u);
    }
    let _ = fresh.transact().encode_state_as_update_v1(&StateVector::default());
}

/// Local edits as an app would make them (cuts and formats stay on characters).
fn edit_all(doc: &Doc, offsets: Offsets) {
    let mut t = doc.transact_mut();
    for v in nested(&t, doc) {
        match v {
            Out::YText(text) => {
                let p = pieces(&text, &t, offsets);
                text.insert(&mut t, insert_at(&p, p.len() / 2), "z");
                let p = pieces(&text, &t, offsets);
                if let Some((a, n)) = char_run(&p, 0, 3) {
                    text.format(&mut t, a, n, [("u".into(), Any::Bool(true))].into_iter().collect());
                    text.remove_range(&mut t, a, n);
                }
            }
            Out::YMap(m) => {
                m.insert(&mut t, "k0", Any::from(5));
                m.remove(&mut t, "k1");
            }
            Out::YArray(a) => {
                a.insert(&mut t, 0, Any::from(1));
                if a.len(&t) > 1 {
                    a.remove(&mut t, 1);
                }
            }
            _ => {}
        }
    }
}

/// What an app's observers do with an event: its path, and its delta or keys.
fn observe(txn: &TransactionMut, events: &yrs::types::Events) {
    for e in events.iter() {
        let _ = e.path();
        match e {
            Event::Text(x) => {
                let _ = x.delta(txn);
            }
            Event::Array(x) => {
                let _ = x.delta(txn);
            }
            Event::Map(x) => {
                let _ = x.keys(txn);
            }
            Event::XmlFragment(x) => {
                let _ = x.delta(txn);
                let _ = x.keys(txn);
            }
            Event::XmlText(x) => {
                let _ = x.delta(txn);
                let _ = x.keys(txn);
            }
        }
    }
}

/// Apply `ops` to a replica holding `base`, then read, encode and edit it. Returns the first
/// panic, if any. Only the ops are hostile; `base` is a state an honest history reached.
pub(crate) fn run_case(schema: Schema, base: &[u8], ops: &[Vec<u8>]) -> Option<Found> {
    install_hook();
    let mut found = None;
    match schema {
        Schema::Generic(offsets) => {
            let doc = doc_for(99, offsets);
            let (t, m, a) = (doc.get_or_insert_text("t"), doc.get_or_insert_map("m"), doc.get_or_insert_array("a"));
            t.observe_deep("c7", observe);
            m.observe_deep("c7", observe);
            a.observe_deep("c7", observe);
            if crate::replica::integrate(&doc, base).is_err() {
                return None;
            }
            let before = doc.transact().state_vector();
            let mut y = YReplica::assemble(doc, Some(99), move |doc, undo| {
                undo.expand_scope(doc, &t);
                undo.expand_scope(doc, &m);
                undo.expand_scope(doc, &a);
            });
            for op in ops {
                capture("apply", &mut found, || y.apply(op));
                if found.is_some() {
                    return found;
                }
            }
            capture("read", &mut found, || read_all(y.doc()));
            capture("encode", &mut found, || encode_all(y.doc(), &before, offsets));
            capture("edit", &mut found, || {
                edit_all(y.doc(), offsets);
                let _ = y.undo();
                let _ = y.redo();
            });
        }
        Schema::Live => {
            let Ok(mut d) = LiveDoc::from_state(base) else { return None };
            if d.set_client(99).is_err() {
                return None;
            }
            for op in ops {
                capture("apply", &mut found, || d.apply(op));
                if found.is_some() {
                    return found;
                }
            }
            capture("read", &mut found, || {
                for id in d.order() {
                    let _ = d.block(id);
                }
                for id in 0..200 {
                    let _ = d.block(id);
                }
            });
            capture("encode", &mut found, || {
                let s = d.state();
                let _ = LiveDoc::from_state(&s).map(|x| x.state());
            });
            capture("edit", &mut found, || {
                let order = d.order();
                for (i, id) in order.iter().enumerate().take(6) {
                    let len = d.block(*id).map_or(0, |b| b.text.chars().count());
                    let _ = d.insert_text(*id, len / 2, "z", &Attributes::new());
                    let _ = d.format(*id, 0, len.min(3), &attrs(&[("u", true.into())]));
                    let _ = d.delete_text(*id, 0, len.min(2));
                    let _ = d.set_attrs(*id, &attrs(&[("k", 1.into())]));
                    if i == 0 {
                        let _ = d.split(*id, 1, 9_000);
                        let _ = d.undo();
                    }
                }
                if order.len() > 1 {
                    let _ = d.merge(order[0], order[1]);
                    let _ = d.remove_block(order[1]);
                }
                let _ = d.undo();
                let _ = d.redo();
            });
        }
    }
    found
}

// ---------------------------------------------------------------------------------------------
// The loop

/// One finding with its minimised reproducer.
#[derive(Clone, Debug)]
pub(crate) struct Repro {
    pub(crate) found: Found,
    pub(crate) schema: Schema,
    pub(crate) base: Vec<u8>,
    pub(crate) ops: Vec<Vec<u8>>,
}

/// Shrink an op while it is still accepted by the gate and still panics at the same place.
fn minimise(schema: Schema, base: &[u8], op: &Upd, follow: &[Vec<u8>], at: &str) -> (Vec<u8>, Vec<Vec<u8>>) {
    let still = |u: &Upd, follow: &[Vec<u8>]| -> bool {
        let b = u.encode();
        if !crate::update_is_sound(&b) {
            return false;
        }
        let mut ops = vec![b];
        ops.extend_from_slice(follow);
        run_case(schema, base, &ops).is_some_and(|f| f.at == at)
    };
    let mut follow = follow.to_vec();
    while !follow.is_empty() {
        let shorter = &follow[..follow.len() - 1];
        if still(op, shorter) {
            follow.pop();
        } else {
            break;
        }
    }
    let mut best = op.clone();
    for _ in 0..8 {
        let mut better = false;
        // Candidates, simplest first.
        let mut cands: Vec<Upd> = Vec::new();
        for i in 0..best.ds.len() {
            let mut c = best.clone();
            c.ds.remove(i);
            cands.push(c);
            for j in 0..best.ds[i].1.len() {
                let mut c = best.clone();
                c.ds[i].1.remove(j);
                cands.push(c);
            }
        }
        for i in 0..best.secs.len() {
            let mut c = best.clone();
            c.secs.remove(i);
            cands.push(c);
            for j in (0..best.secs[i].blocks.len()).rev() {
                let mut c = best.clone();
                c.secs[i].blocks.truncate(j);
                if !c.secs[i].blocks.is_empty() {
                    cands.push(c);
                }
                let mut c = best.clone();
                let b = c.secs[i].blocks.remove(j);
                if j == 0 {
                    c.secs[i].clock = c.secs[i].clock.saturating_add(b.len());
                }
                if !c.secs[i].blocks.is_empty() {
                    cands.push(c);
                }
                if let Blk::Item(it) = &best.secs[i].blocks[j] {
                    if it.kind != 4 || it.len > 1 {
                        let mut c = best.clone();
                        if let Blk::Item(x) = &mut c.secs[i].blocks[j] {
                            x.kind = 4;
                            x.content = vec![1, b'a'];
                            x.len = 1;
                        }
                        cands.push(c);
                    }
                }
            }
        }
        for c in cands {
            if c.encode().len() < best.encode().len() && still(&c, &follow) {
                best = c;
                better = true;
                break;
            }
        }
        if !better {
            break;
        }
    }
    (best.encode(), follow)
}

/// Ends the process with the case at hand when one case runs too long (`C7_HANG_SECS`, default
/// 30): an update that makes yrs loop for ever would hang a browser tab as surely as a panic.
struct Watchdog {
    current: Arc<std::sync::Mutex<(std::time::Instant, String)>>,
    done: Arc<std::sync::atomic::AtomicBool>,
}

impl Watchdog {
    fn start() -> Watchdog {
        let current = Arc::new(std::sync::Mutex::new((std::time::Instant::now(), String::new())));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let limit = std::env::var("C7_HANG_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(30);
        let (c, d) = (Arc::clone(&current), Arc::clone(&done));
        std::thread::spawn(move || {
            while !d.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(500));
                let now = c.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                if now.0.elapsed().as_secs() > limit {
                    let text = format!("HANG after {limit} s: {}", now.1);
                    eprintln!("{text}");
                    if let Ok(path) = std::env::var("C7_FUZZ_OUT") {
                        let _ = std::fs::write(format!("{path}.hang"), &text);
                    }
                    std::process::exit(3);
                }
            }
        });
        Watchdog { current, done }
    }

    fn set(&self, schema: Schema, base: &[u8], ops: &[Vec<u8>]) {
        let text = format!("schema={schema:?} base={} ops={}", hex(base), ops.iter().map(|o| hex(o)).collect::<Vec<_>>().join(","));
        *self.current.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = (std::time::Instant::now(), text);
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.done.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Statistics of one run.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    pub(crate) cases: u64,
    pub(crate) accepted: u64,
    pub(crate) found: BTreeMap<String, (u64, Repro)>,
}

/// Run fuzz cases from `seed` until `budget` cases or `until` passes.
pub(crate) fn run(seed: u64, budget: u64, until: Option<std::time::Instant>, histories: usize) -> Stats {
    let mut r = Rng(seed);
    let schemas = [Schema::Generic(Offsets::Bytes), Schema::Generic(Offsets::Utf16), Schema::Live];
    let hist: Vec<History> = (0..histories)
        .map(|i| History::new(schemas[i % schemas.len()], seed.wrapping_mul(31).wrapping_add(i as u64), 12 + r.below(40)))
        .filter(|h| h.updates.len() > 2)
        .collect();
    let mut stats = Stats::default();
    let watch = Watchdog::start();
    while stats.cases < budget && until.is_none_or(|t| std::time::Instant::now() < t) {
        stats.cases += 1;
        let h = &hist[r.below(hist.len())];
        let k = r.below(h.updates.len());
        let base = &h.states[k];
        let base_upd = Upd::parse(base).unwrap_or_default();
        let donors: Vec<&Upd> = h.parsed.iter().collect();
        let pool = Pool::new(&base_upd, &donors);
        let donors: Vec<Upd> = h.parsed.clone();
        // The op to damage: the next honest update, or a merge, or a full state, or an earlier one.
        let source = match r.below(8) {
            0 => yrs::merge_updates_v1(h.updates[k..(k + 3).min(h.updates.len())].iter().map(Vec::as_slice)).unwrap_or_default(),
            1 => h.states[r.below(h.states.len())].clone(),
            2 => h.updates[r.below(h.updates.len())].clone(),
            _ => h.updates[k].clone(),
        };
        let Some(mut u) = Upd::parse(&source) else { continue };
        for _ in 0..=r.below(4) {
            mutate(&mut u, &pool, &donors, &mut r);
        }
        let op = u.encode();
        if !crate::update_is_sound(&op) {
            continue;
        }
        stats.accepted += 1;
        let follow: Vec<Vec<u8>> = h.updates[(k + 1).min(h.updates.len())..(k + 1 + r.below(4)).min(h.updates.len())].to_vec();
        let mut ops = vec![op];
        ops.extend(follow.iter().cloned());
        watch.set(h.schema, base, &ops);
        if let Some(f) = run_case(h.schema, base, &ops) {
            let entry = stats.found.entry(f.at.clone());
            match entry {
                std::collections::btree_map::Entry::Occupied(mut e) => e.get_mut().0 += 1,
                std::collections::btree_map::Entry::Vacant(e) => {
                    watch.set(h.schema, base, &ops);
                    let (op, follow) = minimise(h.schema, base, &u, &follow, &f.at);
                    let mut ops = vec![op];
                    ops.extend(follow);
                    let repro = Repro { found: f, schema: h.schema, base: base.clone(), ops };
                    e.insert((1, repro));
                }
            }
        }
    }
    stats
}

pub(crate) fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub(crate) fn unhex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

fn report(stats: &Stats) -> String {
    let mut out = format!("FUZZ cases {} gate-accepted {} distinct panics {}\n", stats.cases, stats.accepted, stats.found.len());
    for (at, (n, rep)) in &stats.found {
        out.push_str(&format!(
            "PANIC at={at} count={n} phase={} schema={:?} msg={:?}\n  base={}\n  ops={}\n",
            rep.found.phase,
            rep.schema,
            rep.found.msg,
            hex(&rep.base),
            rep.ops.iter().map(|o| hex(o)).collect::<Vec<_>>().join(",")
        ));
    }
    out
}

#[test]
fn the_ast_round_trips_honest_updates() {
    for schema in [Schema::Generic(Offsets::Bytes), Schema::Generic(Offsets::Utf16), Schema::Live] {
        for seed in 0..6 {
            let h = History::new(schema, seed, 40);
            for u in h.updates.iter().chain(&h.states) {
                let p = Upd::parse(u).unwrap_or_else(|| panic!("{schema:?} seed {seed}: did not parse {u:?}"));
                assert_eq!(&p.encode(), u);
                assert!(crate::update_is_sound(u));
            }
        }
    }
}

/// The bounded default run: gate-accepted mutations of honest updates never panic in a replica.
#[test]
fn fuzz_gate_accepted_updates_never_panic() {
    let cases = std::env::var("C7_FUZZ_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(4_000);
    let stats = run(0x00c7_f022, cases, None, 24);
    let text = report(&stats);
    println!("{text}");
    assert!(stats.accepted > cases / 20, "too few mutations pass the gate: {}", stats.accepted);
    assert!(stats.found.is_empty(), "{text}");
}

/// The long run, on several threads, bounded by wall time.
#[test]
#[ignore = "long fuzz run; see the module docs"]
fn fuzz_long() {
    let secs: u64 = std::env::var("C7_FUZZ_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(600);
    let seed: u64 = std::env::var("C7_FUZZ_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let threads: u64 = std::env::var("C7_FUZZ_THREADS").ok().and_then(|s| s.parse().ok()).unwrap_or(3);
    let until = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            std::thread::spawn(move || {
                let mut all = Vec::new();
                let mut round = 0_u64;
                while std::time::Instant::now() < until {
                    let s = run(seed.wrapping_mul(1_000_003).wrapping_add(t * 7919 + round * 104_729), 20_000, Some(until), 30);
                    let text = report(&s);
                    println!("thread {t} round {round}: {text}");
                    if let Ok(path) = std::env::var("C7_FUZZ_OUT") {
                        use std::io::Write as _;
                        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
                            let _ = writeln!(f, "thread {t} round {round}: {text}");
                        }
                    }
                    all.push(s);
                    round += 1;
                }
                all
            })
        })
        .collect();
    let mut sites = BTreeSet::new();
    let (mut cases, mut accepted) = (0, 0);
    for h in handles {
        for s in h.join().unwrap() {
            cases += s.cases;
            accepted += s.accepted;
            sites.extend(s.found.keys().cloned());
        }
    }
    println!("FUZZ LONG total cases {cases} accepted {accepted} sites {sites:?}");
    assert!(sites.is_empty(), "panic sites: {sites:?}");
}

/// Debugging aid: `C7_CASE` holds a `HANG`/`PANIC` line (`schema=... base=<hex> ops=<hex>,...`).
/// Runs every one-step reduction of the first op in its own thread and reports which still hang
/// (after 3 s) or panic, so a reproducer can be cut down by hand.
#[test]
#[ignore = "debugging aid for one case"]
fn fuzz_case() {
    let line = std::env::var("C7_CASE").unwrap();
    let field = |k: &str| line.split_whitespace().find_map(|w| w.strip_prefix(k)).unwrap_or("").to_owned();
    let schema = match field("schema=").as_str() {
        "Generic(Utf16)" => Schema::Generic(Offsets::Utf16),
        "Live" => Schema::Live,
        _ => Schema::Generic(Offsets::Bytes),
    };
    let base = unhex(&field("base="));
    let ops: Vec<Vec<u8>> = field("ops=").split(',').filter(|s| !s.is_empty()).map(unhex).collect();
    let op = Upd::parse(&ops[0]).unwrap();
    println!("schema {schema:?}\nbase {:?}\nop {op:#?}", Upd::parse(&base));
    let rest: Vec<Vec<u8>> = ops[1..].to_vec();
    let try_ops = |label: String, all: Vec<Vec<u8>>| {
        let (tx, rx) = std::sync::mpsc::channel();
        let b = base.clone();
        std::thread::spawn(move || {
            let _ = tx.send(run_case(schema, &b, &all));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(3)) {
            Ok(Some(f)) => println!("{label}: PANIC {f:?}"),
            Ok(None) => println!("{label}: fine"),
            Err(_) => println!("{label}: HANG in {}", PHASE.lock().unwrap_or_else(std::sync::PoisonError::into_inner)),
        }
    };
    try_ops("all ops".into(), ops.clone());
    for k in 1..ops.len() {
        let mut all = ops.clone();
        all.remove(k);
        try_ops(format!("all but follow-up {k}"), all);
    }
    for k in 1..ops.len() {
        println!("follow-up {k}: {:?}", Upd::parse(&ops[k]));
    }
    let try_one = |label: String, u: Upd| {
        let bytes = u.encode();
        if !crate::update_is_sound(&bytes) {
            println!("{label}: refused by the gate");
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let b = base.clone();
        let mut all = vec![bytes];
        all.extend(rest.iter().cloned());
        std::thread::spawn(move || {
            let _ = tx.send(run_case(schema, &b, &all));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(3)) {
            Ok(Some(f)) => println!("{label}: PANIC {f:?}"),
            Ok(None) => println!("{label}: fine"),
            Err(_) => println!("{label}: HANG in {}", PHASE.lock().unwrap_or_else(std::sync::PoisonError::into_inner)),
        }
    };
    try_one("whole".into(), op.clone());
    for i in 0..op.ds.len() {
        let mut c = op.clone();
        c.ds.remove(i);
        try_one(format!("without ds {i}"), c);
    }
    let mut c = op.clone();
    c.ds.clear();
    try_one("without any ds".into(), c);
    for s in 0..op.secs.len() {
        for j in 0..op.secs[s].blocks.len() {
            let mut c = op.clone();
            c.secs[s].blocks.truncate(j + 1);
            try_one(format!("sec {s} first {} blocks", j + 1), c);
        }
    }
}

#[test]
#[ignore = "tmp"]
fn c7_tmp_trace() {
    let line = std::env::var("C7_CASE").unwrap();
    let field = |k: &str| line.split_whitespace().find_map(|w| w.strip_prefix(k)).unwrap_or("").to_owned();
    let base = unhex(&field("base="));
    let ops: Vec<Vec<u8>> = field("ops=").split(',').filter(|s| !s.is_empty()).map(unhex).collect();
    let offsets = if field("schema=").contains("Utf16") { Offsets::Utf16 } else { Offsets::Bytes };
    let doc = doc_for(99, offsets);
    doc.get_or_insert_text("t");
    doc.get_or_insert_map("m");
    doc.get_or_insert_array("a");
    crate::replica::integrate(&doc, &base).unwrap();
    for op in &ops {
        println!("apply {:?}", crate::replica::integrate(&doc, op));
    }
    let mut t = doc.transact_mut();
    for v in nested(&t, &doc) {
        println!("nested {v:?}");
        match v {
            Out::YText(text) => {
                println!("  diff {:?}", text.diff(&t, YChange::identity));
                let p = pieces(&text, &t, offsets);
                println!("  pieces {p:?} len {}", text.len(&t));
                text.insert(&mut t, insert_at(&p, p.len() / 2), "z");
                println!("  inserted");
                let p = pieces(&text, &t, offsets);
                if let Some((a, n)) = char_run(&p, 0, 3) {
                    println!("  format {a} {n}");
                    text.format(&mut t, a, n, [("u".into(), Any::Bool(true))].into_iter().collect());
                    println!("  remove {a} {n}");
                    text.remove_range(&mut t, a, n);
                }
            }
            Out::YMap(m) => {
                m.insert(&mut t, "k0", Any::from(5));
                m.remove(&mut t, "k1");
            }
            Out::YArray(a) => {
                println!("  array len {}", a.len(&t));
                a.insert(&mut t, 0, Any::from(1));
                println!("  inserted");
                if a.len(&t) > 1 {
                    a.remove(&mut t, 1);
                }
                println!("  removed");
            }
            _ => {}
        }
    }
}
