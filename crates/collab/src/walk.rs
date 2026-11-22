//! A bounds check over the raw bytes of a yrs update (v1), made without yrs, and a check that
//! yrs read the same bytes the same way.
//!
//! yrs's own decoder adds each block's length to a running clock without checking, and its id
//! ranges silently drop a range that wrapped, so nothing it exposes can tell an update whose
//! clocks overflow `u32` from one that does not. [`read`] reads the same bytes itself: every
//! client section, every block (garbage-collected, skipped or item), the delete set, and the ids
//! an item names, and refuses any clock past [`MAX_CLOCK`] (2^31: no honest client writes that
//! many characters, and anything below it cannot wrap a `u32`).
//!
//! It is also the resource cap in front of yrs's decoder (y-crdt #675/#750: a few bytes that
//! claim billions of clients, blocks or values abort the process on allocation, which no
//! `catch_unwind` catches). Every count is a loop here that must consume bytes on each turn, so
//! a count the bytes cannot back fails at the end of the input, before yrs sizes anything by it;
//! `Any` values nest at most 64 deep, client ids stay below 2^53 (#743), and the weak-link type
//! (#733) and JSON content (#720's content ref 18) are refused.
//!
//! That only protects a replica if yrs parses the bytes into the same blocks. yrs ignores bytes
//! after its delete set, so where the two readers disagree on one field, the rest of an update
//! can mean one thing here and another to yrs. Two defences:
//!
//! - The reader mirrors yrs 0.28 `Update::decode` field by field: the same integer widths,
//!   canonical varints only (so a value reads the same at any width), GC and Skip only for the
//!   exact info bytes yrs uses, the same content kinds. It refuses the forms the two would read
//!   differently or that no Craft app writes: JSON content (yrs reads one string more than the
//!   count it writes), the XmlHook type (yrs reads no name for it), the Weak type, zero-length
//!   blocks (yrs drops an empty item without moving its clock), flag bits yrs never sets, a
//!   client written twice, and trailing bytes.
//! - [`agrees`] then compares what this reader saw with what yrs decoded, through yrs's public
//!   API (the clock ranges each client's blocks cover, where they start and end, the delete
//!   set), and reads yrs's own encoding of its parse back through [`read`]. Any difference means
//!   the two parses diverged, and the update is refused.

use std::collections::{BTreeMap, HashSet};

use yrs::Update;
use yrs::updates::encoder::Encode;

/// No clock, block end or id may pass this.
pub(crate) const MAX_CLOCK: u64 = 1 << 31;

/// How deep `Any` values may nest.
const MAX_DEPTH: usize = 64;

/// The widest client id yrs takes (a JavaScript safe integer).
const MAX_CLIENT: u64 = (1 << 53) - 1;

/// Clock ranges `[start, end)` in clocks.
type Ranges = Vec<(u32, u32)>;

/// What an update holds, in the terms yrs's public API reports it.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Shape {
    /// Per client with a block that is not a skip: the clock ranges those blocks cover, adjacent
    /// ones joined (yrs's `Update::insertions(true)`). The first range starts at the client's
    /// first such block (`Update::state_vector_lower`).
    pub(crate) blocks: BTreeMap<u64, Ranges>,
    /// Per client whose section starts at clock 0: where its blocks end before the first skip,
    /// when that is not 0 (`Update::state_vector`).
    pub(crate) upper: BTreeMap<u64, u32>,
    /// The delete set, each client's ranges in written order (`Update::delete_set`).
    pub(crate) deletes: BTreeMap<u64, Ranges>,
}

impl Shape {
    /// Each client's total of blocks other than skips, in clocks.
    #[cfg(test)]
    pub(crate) fn totals(&self) -> BTreeMap<u64, u64> {
        self.blocks.iter().map(|(c, r)| (*c, r.iter().map(|(s, e)| u64::from(e - s)).sum())).collect()
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    /// Where each item goes, when the caller asked ([`items`]).
    items: Option<Vec<Placed>>,
}

/// Where an item says it goes: a root by name, a parent item, or beside its origin (or, without
/// one, its right origin), whose parent yrs then gives it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Under {
    Root(String),
    Parent((u64, u32)),
    /// Beside an origin and a right origin (at least one of them).
    Beside(Option<(u64, u32)>, Option<(u64, u32)>),
    /// The rest of an item with a named parent (a root or a parent item) whose first clocks the
    /// document already holds: yrs keeps the named parent and puts it beside the last clock it
    /// holds. Only made by the `fits` check, never read.
    Within(Box<Under>, (u64, u32)),
}

/// One item of an update: its first id, its length, its content kind (`info & 0x0f`), the type
/// byte of a type item, and where it goes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Placed {
    pub(crate) id: (u64, u32),
    pub(crate) len: u32,
    pub(crate) kind: u8,
    pub(crate) type_ref: Option<u8>,
    pub(crate) under: Under,
    /// It names a key in its parent (a map entry), rather than taking its origin's.
    pub(crate) sub: bool,
    /// An embed whose value is a JSON string.
    pub(crate) string_embed: bool,
}

impl Reader<'_> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.bytes.get(self.at)?;
        self.at += 1;
        Some(b)
    }

    fn take(&mut self, n: u64) -> Option<&[u8]> {
        let end = self.at.checked_add(usize::try_from(n).ok()?)?;
        let out = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(out)
    }

    /// An unsigned varint (lib0) of at most `max`, written in its shortest form. yrs reads these
    /// with wrapping shifts at its own width; a shortest form within that width reads the same
    /// there as here.
    fn var(&mut self, max: u64) -> Option<u64> {
        let mut out = 0_u64;
        let mut shift = 0;
        loop {
            let b = self.byte()?;
            out |= u64::from(b & 0x7f) << shift;
            if out > max {
                return None;
            }
            if b & 0x80 == 0 {
                // A last group of zero bits is a longer form of a shorter number.
                return (b != 0 || shift == 0).then_some(out);
            }
            shift += 7;
            if shift > 56 {
                return None;
            }
        }
    }

    /// What yrs reads as a `u32` varint: counts, lengths, clocks, parent info.
    fn u32(&mut self) -> Option<u32> {
        self.var(u64::from(u32::MAX)).map(|v| v as u32)
    }

    fn client(&mut self) -> Option<u64> {
        self.var(MAX_CLIENT)
    }

    fn clock(&mut self) -> Option<u32> {
        self.u32().filter(|c| u64::from(*c) <= MAX_CLOCK)
    }

    /// A signed varint: the bytes are skipped. yrs stops at the same byte (the first without the
    /// continuation bit) and refuses longer ones than this does.
    fn int(&mut self) -> Option<()> {
        for _ in 0..10 {
            if self.byte()? & 0x80 == 0 {
                return Some(());
            }
        }
        None
    }

    /// A string; returns its length in UTF-16 units, which is how yrs counts its clocks.
    fn string(&mut self) -> Option<u32> {
        let n = self.u32()?;
        let s = std::str::from_utf8(self.take(u64::from(n))?).ok()?;
        u32::try_from(s.encode_utf16().count()).ok()
    }

    fn id(&mut self) -> Option<(u64, u32)> {
        Some((self.client()?, self.clock()?))
    }

    /// A string, for a root name.
    fn name(&mut self) -> Option<String> {
        let n = self.u32()?;
        std::str::from_utf8(self.take(u64::from(n))?).ok().map(str::to_owned)
    }

    fn any(&mut self, depth: usize) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        match self.byte()? {
            // undefined, null, false, true
            126 | 127 | 120 | 121 => Some(()),
            125 => self.int(),
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
                let n = self.u32()?;
                self.take(u64::from(n)).map(|_| ())
            }
            _ => None,
        }
    }

    /// One item, after its info byte, at `id`; returns its length in clocks.
    fn item(&mut self, info: u8, id: (u64, u32)) -> Option<u32> {
        // yrs masks the content kind with 0x0f; it never writes 0x10.
        if info & 0x10 != 0 {
            return None;
        }
        let (origin, right, sub) = (info & 0x80 != 0, info & 0x40 != 0, info & 0x20 != 0);
        let origin = if origin { Some(self.id()?) } else { None };
        let right = if right { Some(self.id()?) } else { None };
        let mut keyed = false;
        let under = match (origin, right) {
            (None, None) => {
                // Parent: a root name (1) or the id of an item.
                let under = if self.u32()? == 1 { Under::Root(self.name()?) } else { Under::Parent(self.id()?) };
                // Only named here: an item with an origin takes its key from it.
                if sub {
                    self.string()?;
                    keyed = true;
                }
                under
            }
            _ => Under::Beside(origin, right),
        };
        let kind = info & 0x0f;
        let type_ref = (kind == 7).then(|| self.bytes.get(self.at).copied()).flatten();
        let start = self.at;
        let len = self.content(kind)?;
        if let Some(items) = &mut self.items {
            // An embed's JSON follows its length; a string's starts with a quote.
            let string_embed = kind == 5 && {
                let mut peek = Reader { bytes: self.bytes, at: start, items: None };
                peek.u32().is_some() && self.bytes[peek.at..].iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'"')
            };
            items.push(Placed { id, len, kind, type_ref, under, sub: keyed, string_embed });
        }
        Some(len)
    }

    /// An item's content of kind `kind`; returns its length in clocks.
    fn content(&mut self, kind: u8) -> Option<u32> {
        match kind {
            // deleted
            1 => self.u32(),
            // binary
            3 => {
                let n = self.u32()?;
                self.take(u64::from(n))?;
                Some(1)
            }
            4 => self.string(),
            // embed: a json string
            5 => self.string().map(|_| 1),
            // format: key, json value
            6 => {
                self.string()?;
                self.string().map(|_| 1)
            }
            // type: one byte (yrs reads a u8, not a varint), and a name for xml elements.
            // Array, map, text, xml element, fragment and text, sub-document, undefined. The
            // XmlHook (5) and Weak (7) types are refused.
            7 => match self.byte()? {
                0 | 1 | 2 | 4 | 6 | 9 | 15 => Some(1),
                3 => self.string().map(|_| 1),
                _ => None,
            },
            // any
            8 => {
                let n = self.u32()?;
                for _ in 0..n {
                    self.any(0)?;
                }
                Some(n)
            }
            // doc: guid and options
            9 => {
                self.string()?;
                self.any(0)?;
                Some(1)
            }
            // 2 is JSON: yrs reads one string more than the count it writes, so the bytes after
            // it mean something else to yrs. No Craft app writes it. 0 and 10 are not item kinds.
            _ => None,
        }
    }
}

/// Reads `bytes` (a yrs update, v1) the way yrs 0.28 does, refusing any clock past
/// [`MAX_CLOCK`] and anything outside the forms described in the module docs.
///
/// Refused: a block whose clock or clock + len passes the ceiling, a client's blocks adding up
/// past it, a client with two sections or two delete entries, an id naming a clock past it, a
/// delete range past it, an empty block, trailing bytes, and anything this reader cannot parse.
pub(crate) fn read(bytes: &[u8]) -> Option<Shape> {
    read_into(Reader { bytes, at: 0, items: None }).map(|(shape, _)| shape)
}

/// [`read`], and where every item goes, in the order written.
pub(crate) fn items(bytes: &[u8]) -> Option<(Shape, Vec<Placed>)> {
    read_into(Reader { bytes, at: 0, items: Some(Vec::new()) }).map(|(shape, items)| (shape, items.unwrap_or_default()))
}

fn read_into(mut r: Reader<'_>) -> Option<(Shape, Option<Vec<Placed>>)> {
    let bytes = r.bytes;
    let mut shape = Shape::default();
    let mut seen = HashSet::new();
    for _ in 0..r.u32()? {
        let blocks = r.u32()?;
        let client = r.client()?;
        let mut clock = r.clock()?;
        if !seen.insert(client) {
            return None;
        }
        let from_zero = clock == 0;
        let (mut upper, mut skipped) = (0, false);
        let mut ranges = Ranges::new();
        for _ in 0..blocks {
            // yrs: exactly 0 is garbage-collected and 10 is a skip; any other byte is an item.
            let (len, skip) = match r.byte()? {
                0 => (r.u32()?, false),
                10 => (r.u32()?, true),
                info => (r.item(info, (client, clock))?, false),
            };
            if len == 0 {
                return None;
            }
            let end = u32::try_from(u64::from(clock) + u64::from(len)).ok().filter(|e| u64::from(*e) <= MAX_CLOCK)?;
            if skip {
                skipped = true;
            } else {
                if !skipped {
                    upper = end;
                }
                match ranges.last_mut() {
                    Some(last) if last.1 == clock => last.1 = end,
                    _ => ranges.push((clock, end)),
                }
            }
            clock = end;
        }
        if from_zero && upper != 0 {
            shape.upper.insert(client, upper);
        }
        if !ranges.is_empty() {
            shape.blocks.insert(client, ranges);
        }
    }
    // The delete set.
    for _ in 0..r.u32()? {
        let client = r.client()?;
        let mut ranges = Ranges::new();
        for _ in 0..r.u32()? {
            let clock = r.clock()?;
            let end = u64::from(clock) + u64::from(r.u32()?);
            if end > MAX_CLOCK {
                return None;
            }
            ranges.push((clock, end as u32));
        }
        if shape.deletes.insert(client, ranges).is_some() {
            return None;
        }
    }
    (r.at == bytes.len()).then_some((shape, r.items))
}

/// Whether yrs's parse of an update, `update`, is the one [`read`] made of the same bytes,
/// `shape`: the same clients with the same block ranges, starts and ends, the same delete set,
/// and yrs's own encoding of what it parsed reads back to the same shape. A block yrs parsed
/// with a wrapped clock shows up as a range missing, a different start or end, or an encoding
/// the reader refuses.
pub(crate) fn agrees(shape: &Shape, update: &Update) -> bool {
    let ranges =
        |set: &yrs::IdSet| -> BTreeMap<u64, Ranges> { set.iter().map(|(c, r)| (c.get(), r.iter().map(|r| (r.start, r.end)).collect())).collect() };
    let vector = |sv: &yrs::StateVector| -> BTreeMap<u64, u32> { sv.iter().map(|(c, k)| (c.get(), *k)).collect() };
    let lower: BTreeMap<u64, u32> = shape.blocks.iter().map(|(c, r)| (*c, r[0].0)).collect();
    ranges(&update.insertions(true)) == shape.blocks
        && ranges(update.delete_set()) == shape.deletes
        && vector(&update.state_vector_lower()) == lower
        && vector(&update.state_vector()) == shape.upper
        && read(&update.encode_v1()).as_ref() == Some(shape)
}
