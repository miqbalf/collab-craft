//! A bounds check over the raw bytes of a yrs update (v1), made without yrs.
//!
//! yrs's own decoder adds each block's length to a running clock without checking, and its id
//! ranges silently drop a range that wrapped, so nothing it exposes can tell an update whose
//! clocks overflow `u32` from one that does not. [`blocks_in_range`] reads the same bytes
//! itself: every client section, every block (garbage-collected, skipped or item), the
//! delete set, and the ids an item names.
//!
//! The ceiling is [`MAX_CLOCK`] (2^31): no honest client writes that many characters, and
//! anything below it cannot wrap a `u32`. Reading is fail-closed. A byte pattern this reader
//! does not understand is refused, so the content types it knows are the ones yrs 0.28 writes
//! (the cross-check test compares its lengths with yrs's own for real documents).

use std::collections::HashMap;

/// No clock, block end or id may pass this.
pub(crate) const MAX_CLOCK: u64 = 1 << 31;

/// How deep `Any` values may nest.
const MAX_DEPTH: usize = 64;

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
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

    /// An unsigned varint (lib0), at most 2^53.
    fn uint(&mut self) -> Option<u64> {
        let mut out = 0_u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            out |= u64::from(b & 0x7f).checked_shl(shift)?;
            if out > 1 << 53 {
                return None;
            }
            if b & 0x80 == 0 {
                return Some(out);
            }
        }
        None
    }

    /// A signed varint: the bytes are skipped.
    fn int(&mut self) -> Option<()> {
        for _ in 0..10 {
            if self.byte()? & 0x80 == 0 {
                return Some(());
            }
        }
        None
    }

    /// A string; returns its length in UTF-16 units.
    fn string(&mut self) -> Option<u64> {
        let n = self.uint()?;
        let s = std::str::from_utf8(self.take(n)?).ok()?;
        Some(s.encode_utf16().count() as u64)
    }

    fn clock(&mut self) -> Option<u64> {
        let c = self.uint()?;
        (c <= MAX_CLOCK).then_some(c)
    }

    fn id(&mut self) -> Option<()> {
        self.uint()?;
        self.clock()?;
        Some(())
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
                for _ in 0..self.uint()? {
                    self.string()?;
                    self.any(depth + 1)?;
                }
                Some(())
            }
            117 => {
                for _ in 0..self.uint()? {
                    self.any(depth + 1)?;
                }
                Some(())
            }
            116 => {
                let n = self.uint()?;
                self.take(n).map(|_| ())
            }
            _ => None,
        }
    }

    /// One item, after its info byte; returns its length in clocks.
    fn item(&mut self, info: u8) -> Option<u64> {
        let (origin, right, sub) = (info & 0x80 != 0, info & 0x40 != 0, info & 0x20 != 0);
        if origin {
            self.id()?;
        }
        if right {
            self.id()?;
        }
        if !origin && !right {
            // Parent: a root name (1) or the id of an item.
            if self.uint()? == 1 {
                self.string()?;
            } else {
                self.id()?;
            }
            // Only named here: an item with an origin takes its key from it.
            if sub {
                self.string()?;
            }
        }
        match info & 0x1f {
            // deleted
            1 => self.uint(),
            // json
            2 => {
                let n = self.uint()?;
                for _ in 0..n {
                    self.string()?;
                }
                Some(n)
            }
            // binary
            3 => {
                let n = self.uint()?;
                self.take(n)?;
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
            // type: a type ref, and a name for xml elements and hooks
            7 => {
                let kind = self.uint()?;
                if kind == 3 || kind == 5 {
                    self.string()?;
                }
                Some(1)
            }
            // any
            8 => {
                let n = self.uint()?;
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
            _ => None,
        }
    }
}

/// Whether every clock in `bytes` (a yrs update, v1) stays within [`MAX_CLOCK`], and the update
/// is well formed to the last byte. On success, how many clocks each client's blocks span
/// (skips and garbage-collected blocks included), for the caller to compare with yrs.
///
/// Refused: a block whose clock or clock + len passes the ceiling, a client's blocks adding up
/// past it, a client with two sections, an id naming a clock past it, a delete range past it,
/// and anything this reader cannot parse.
pub(crate) fn blocks_in_range(bytes: &[u8]) -> Option<HashMap<u64, u64>> {
    let mut r = Reader { bytes, at: 0 };
    let mut spans = HashMap::new();
    for _ in 0..r.uint()? {
        let blocks = r.uint()?;
        let client = r.uint()?;
        let mut clock = r.clock()?;
        if spans.contains_key(&client) {
            return None;
        }
        let from = clock;
        for _ in 0..blocks {
            let info = r.byte()?;
            let len = match info & 0x1f {
                // garbage-collected, or skipped
                0 | 10 => r.uint()?,
                _ => r.item(info)?,
            };
            clock = clock.checked_add(len).filter(|c| *c <= MAX_CLOCK)?;
        }
        spans.insert(client, clock - from);
    }
    // The delete set.
    for _ in 0..r.uint()? {
        r.uint()?;
        for _ in 0..r.uint()? {
            let clock = r.clock()?;
            clock.checked_add(r.uint()?).filter(|c| *c <= MAX_CLOCK)?;
        }
    }
    (r.at == bytes.len()).then_some(spans)
}
