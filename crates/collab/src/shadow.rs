//! A relay-side copy of a room's document, to try each op on before it is ordered.
//!
//! The gate ([`update_is_sound`](crate::update_is_sound)) reads an op alone. Whether yrs can
//! integrate it, and read and encode the result, depends on the document it lands in, so the
//! relay keeps one: a [`YReplica`] (native, where a panic unwinds) that takes every accepted op
//! in order. An op is refused when the replica refuses it (it does not fit, see the `fits`
//! module), when yrs panics applying it, or when encoding the result panics (yrs #720 panics at
//! the next encode, not at the apply). After a panic the copy is rebuilt from its last
//! compacted state and the ops accepted since, because yrs leaves its store undefined (#764).
//!
//! What it cannot judge: an op that waits for something the copy lacks. The relay never sees the
//! file a room was seeded from (client 0), so until the room's first checkpoint snapshot every op
//! that names a seeded item waits here, and is accepted unjudged; the browser replicas still run
//! the same checks before yrs sees it. A snapshot brings the seed in, and from then on the copy
//! judges every op.

use yrs::{ReadTxn, StateVector, Transact};

use crate::replica::YReplica;

/// Accepted bytes after which the copy compacts its log into one state.
const COMPACT_BYTES: usize = 4 * 1024 * 1024;

/// Why [`Shadow::admit`] refused an op.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ShadowRefusal {
    /// The replica refused it: it does not read, or does not fit the document.
    #[error("the update does not fit the document")]
    Invalid,
    /// yrs panicked applying it or encoding the result.
    #[error("the update makes the document library panic")]
    Panicked,
    /// It has more items than one check takes (see the `fits` module); not judged.
    #[error("the update is too large to check")]
    TooLarge,
}

/// One room's copy of its document. See the [module docs](self).
pub struct Shadow {
    y: YReplica,
    /// The replica schema whose roots the copy types (`"wordcraft/1"`), if the room named one.
    schema: Option<String>,
    /// The last compacted state, and the ops accepted since.
    base: Vec<u8>,
    log: Vec<Vec<u8>>,
    log_bytes: usize,
    rebuilds: u64,
}

/// Where a [`Shadow`] stood, for [`Shadow::rollback`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mark {
    log: usize,
    log_bytes: usize,
}

/// The roots a replica schema types, as (name, is a map). A root the schema does not name stays
/// untyped in the copy, and the `fits` check then lets anything under it through.
fn roots(schema: Option<&str>) -> &'static [(&'static str, bool)] {
    match schema {
        Some("wordcraft/1") => &[("body", false), ("blocks", true)],
        Some("gridcraft/1") => &[("meta", true), ("sheets", false), ("sheet", true), ("styles", true), ("ops", false)],
        _ => &[],
    }
}

fn fresh(schema: Option<&str>) -> YReplica {
    let doc = YReplica::seed_doc();
    for &(name, map) in roots(schema) {
        if map {
            doc.get_or_insert_map(name);
        } else {
            doc.get_or_insert_array(name);
        }
    }
    YReplica::assemble(doc, None, |_, _| {})
}

impl Shadow {
    /// An empty copy of a document of replica schema `schema` (`"wordcraft/1"`). Its roots
    /// are typed as that schema types them, so an op that puts content where the schema's
    /// replicas cannot read it back is refused; with no schema, or an unknown one, they are not.
    #[must_use]
    pub fn new(schema: Option<&str>) -> Shadow {
        Shadow { y: fresh(schema), schema: schema.map(str::to_owned), base: Vec::new(), log: Vec::new(), log_bytes: 0, rebuilds: 0 }
    }

    /// The schema the copy was made for.
    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Try `update` (one yrs v1 update, or a checkpoint's full state) on the copy and keep it
    /// when it lands or waits. Refused updates leave the copy as it was.
    pub fn admit(&mut self, update: &[u8]) -> Result<(), ShadowRefusal> {
        self.admit_all(std::slice::from_ref(&update))
    }

    /// Try one submit's updates and keep them all or none, because the relay refuses the whole
    /// submit. They are tried one at a time, in order, each under `catch_unwind`, as every
    /// replica applies them: a merged update is not the same thing (yrs's merge writes a
    /// block that overlaps an earlier one with an offset, and so without its parent and key).
    /// When a later one is refused after earlier ones landed, the copy is rolled back, which
    /// is a rebuild; [`rebuilds`](Self::rebuilds) says when that happened.
    pub fn admit_all<U: AsRef<[u8]>>(&mut self, updates: &[U]) -> Result<(), ShadowRefusal> {
        let mark = self.mark();
        for update in updates {
            if let Err(refusal) = self.admit_one(update.as_ref()) {
                self.rollback(mark);
                return Err(refusal);
            }
        }
        Ok(())
    }

    /// Where the copy stands now, to [`rollback`](Self::rollback) to when the relay refuses an
    /// op after the copy took it (it is never ordered, so no peer sees it).
    #[must_use]
    pub fn mark(&self) -> Mark {
        Mark { log: self.log.len(), log_bytes: self.log_bytes }
    }

    /// Take out what was admitted since `mark`. A rebuild, so only for the rare refusal after
    /// the copy (the relay could not number the op); call [`settle`](Self::settle) only once
    /// the op is ordered.
    pub fn rollback(&mut self, mark: Mark) {
        if self.log.len() > mark.log {
            self.log.truncate(mark.log);
            self.log_bytes = mark.log_bytes;
            self.rebuild();
        }
    }

    /// Fold the log into one state once it is past [`COMPACT_BYTES`]. For after an op is
    /// ordered: a [`Mark`] from before does not survive it.
    pub fn settle(&mut self) {
        if self.log_bytes > COMPACT_BYTES {
            self.compact();
        }
    }

    /// Fold the log into one state now, to shrink a copy past its byte cap (see
    /// [`bytes`](Self::bytes)). A [`Mark`] from before does not survive it.
    pub fn compact_now(&mut self) {
        self.compact();
    }

    /// About how many bytes the copy holds: its document (at most its base and log again), its
    /// base, its log and its index.
    #[must_use]
    pub fn bytes(&self) -> usize {
        2 * (self.base.len() + self.log_bytes) + self.y.index_runs() * 96
    }

    /// How many times the copy was rebuilt (after a panic, or a rollback).
    #[must_use]
    pub fn rebuilds(&self) -> u64 {
        self.rebuilds
    }

    /// Type the roots by `schema` from now on, if the copy was made without one.
    pub fn set_schema(&mut self, schema: Option<&str>) {
        if self.schema.is_none() && schema.is_some() {
            self.schema = schema.map(str::to_owned);
            self.rebuild();
        }
    }

    fn admit_one(&mut self, update: &[u8]) -> Result<(), ShadowRefusal> {
        let before: StateVector = self.y.doc().transact().state_vector();
        let applied = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.y.apply(update)));
        match applied {
            Ok(_) if self.y.damaged() => {
                self.rebuild();
                return Err(ShadowRefusal::Panicked);
            }
            Err(_) => {
                self.rebuild();
                return Err(ShadowRefusal::Panicked);
            }
            Ok(Err(crate::LiveError::TooLarge)) => return Err(ShadowRefusal::TooLarge),
            Ok(Err(_)) => {
                // Refused before yrs saw it, as a rule; if yrs took part of it, start again.
                if self.y.doc().transact().state_vector() != before {
                    self.rebuild();
                }
                return Err(ShadowRefusal::Invalid);
            }
            Ok(Ok(())) => {}
        }
        // What changed, encoded as a replica would send or checkpoint it.
        let encoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(self.y.doc().transact().encode_diff_v1(&before));
        }));
        if encoded.is_err() {
            self.rebuild();
            return Err(ShadowRefusal::Panicked);
        }
        self.log.push(update.to_vec());
        self.log_bytes += update.len();
        Ok(())
    }

    /// Whether ops wait in the copy for something it lacks (the seed, before a checkpoint).
    #[must_use]
    pub fn is_waiting(&self) -> bool {
        self.y.is_waiting()
    }

    /// Fold the log into one state. Ops that wait are not in the state, so a later rebuild
    /// loses them (they were accepted unjudged; the copy then judges what needs them less,
    /// never wrongly). Folding anyway keeps the log bounded by [`COMPACT_BYTES`].
    fn compact(&mut self) {
        if let Ok(state) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.y.state())) {
            self.base = state;
            self.log.clear();
            self.log_bytes = 0;
        }
    }

    /// Start again from the last compacted state and the ops accepted since.
    fn rebuild(&mut self) {
        self.rebuilds += 1;
        let rebuilt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut y = fresh(self.schema.as_deref());
            if !self.base.is_empty() {
                let _ = y.apply(&self.base);
            }
            for op in &self.log {
                let _ = y.apply(op);
            }
            y
        }));
        // Every op in the log was accepted on a copy built the same way, so this does not
        // panic; if it ever did, an empty copy judges less but never wrongly refuses.
        self.y = rebuilt.unwrap_or_else(|_| fresh(self.schema.as_deref()));
    }

    /// The copy's full state (a test, or a measurement).
    #[must_use]
    pub fn state(&self) -> Vec<u8> {
        self.y.state()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::time::{Duration, Instant};

    use super::*;
    use crate::ydoc::{Attributes, LiveDoc, SeedBlock};

    /// A WordCraft document of about `target` bytes of state, built by three peers, and the
    /// typing ops of a fourth that follow it.
    fn big_doc(target: usize) -> (Vec<u8>, Vec<Vec<u8>>) {
        let text: String = "Lorem ipsum dolor sit amet, consectetur adipiscing \u{e9}lit. ".repeat(8);
        let blocks: Vec<SeedBlock> = (0..)
            .take_while(|i| i * text.len() < target * 7 / 10)
            .map(|i| SeedBlock { id: i as u64 + 1, text: text.clone(), attrs: Attributes::new(), runs: vec![] })
            .collect();
        let n = blocks.len();
        let mut peers: Vec<LiveDoc> = (1..=3_u32)
            .map(|c| {
                let mut d = LiveDoc::seeded(&blocks).unwrap();
                d.set_client(c).unwrap();
                d
            })
            .collect();
        let mut rng = crate::fuzz::Rng(5);
        let mut all = Vec::new();
        let bold: Attributes = [("b".to_owned(), serde_json::Value::Bool(true))].into_iter().collect();
        while peers[0].state().len() < target {
            for _ in 0..200 {
                let p = rng.below(3);
                let id = rng.below(n) as u64 + 1;
                let at = rng.below(300);
                let u = match rng.below(3) {
                    0 => peers[p].insert_text(id, at, "typed ", &Attributes::new()),
                    1 => peers[p].delete_text(id, at, 3),
                    _ => peers[p].format(id, at, 5, &bold),
                };
                if let Ok(u) = u {
                    all.push(u);
                }
            }
            for p in &mut peers {
                for u in &all {
                    p.apply(u).unwrap();
                }
            }
        }
        let state = peers[0].state();
        let mut typist = LiveDoc::from_state(&state).unwrap();
        typist.set_client(9).unwrap();
        let ops = (0..300).map(|i| typist.insert_text(1 + (i % n) as u64, 10, "k", &Attributes::new()).unwrap()).collect();
        (state, ops)
    }

    fn stats(mut d: Vec<Duration>) -> String {
        d.sort();
        let mean = d.iter().sum::<Duration>() / u32::try_from(d.len()).unwrap();
        format!("mean {:?} p99 {:?} max {:?}", mean, d[d.len() * 99 / 100], d[d.len() - 1])
    }

    /// The gate and the shadow on a 1 MiB document. Prints COST lines; asserts only that
    /// nothing is refused. Run in release for the numbers that matter.
    #[test]
    #[ignore = "measurement; run with --release --ignored --nocapture"]
    fn costs_on_a_one_mib_document() {
        let (state, ops) = big_doc(1024 * 1024);
        println!("COST state {} bytes, {} ops", state.len(), ops.len());
        let t = Instant::now();
        assert!(crate::update_is_sound(&state));
        println!("COST gate on the full state: {:?}", t.elapsed());
        let gate: Vec<Duration> = ops
            .iter()
            .map(|op| {
                let t = Instant::now();
                assert!(crate::update_is_sound(op));
                t.elapsed()
            })
            .collect();
        println!("COST gate per typing op: {}", stats(gate));

        let mut shadow = Shadow::new(Some("wordcraft/1"));
        let t = Instant::now();
        shadow.admit(&state).unwrap();
        println!("COST shadow takes the full state: {:?}", t.elapsed());
        let per: Vec<Duration> = ops
            .iter()
            .map(|op| {
                let t = Instant::now();
                shadow.admit(op).unwrap();
                t.elapsed()
            })
            .collect();
        println!("COST shadow per typing op: {}", stats(per));
        let t = Instant::now();
        let full = shadow.state();
        println!("COST full encode of the copy ({} bytes): {:?}", full.len(), t.elapsed());
        let t = Instant::now();
        shadow.rebuild();
        println!("COST rebuild after a panic: {:?}", t.elapsed());
        assert!(!shadow.is_waiting());
    }

    /// The shadow refuses an op its replica refuses, keeps going, and stays level with a replica
    /// that took the same accepted ops.
    /// Re-review N4: a submit is judged as replicas apply it, one update at a time. Merged,
    /// `[one letter at the end of a text, the same id two long with the text as parent, key
    /// `k`, "ab"]` loses the second update's parent and key (yrs writes the overlap with an
    /// offset) and reads as an unkeyed string beside the letter, which fits; replicas apply the
    /// second as a keyed string in a text and refuse it. The copy refuses the submit and keeps
    /// none of it.
    #[test]
    fn a_submit_is_judged_one_update_at_a_time() {
        use crate::ydoc::tests::{doc, resent_keyed, text_id_and_letter};
        let d = doc();
        let (text, letter) = text_id_and_letter(&d);
        let keyed_string = resent_keyed(text, 0x04, &[2, b'a', b'b']);
        let fresh = || {
            let mut s = Shadow::new(Some("wordcraft/1"));
            s.admit(&d.state()).unwrap();
            s
        };
        let merged = yrs::merge_updates_v1([letter.as_slice(), keyed_string.as_slice()]).unwrap();
        assert!(fresh().admit(&merged).is_ok(), "the merged form is refused too, so this test shows nothing");
        let mut shadow = fresh();
        let before = shadow.state();
        assert_eq!(shadow.admit_all(&[letter.clone(), keyed_string]), Err(ShadowRefusal::Invalid));
        assert_eq!(shadow.state(), before, "part of the submit was kept");
        shadow.admit(&letter).unwrap();
    }

    /// An honest checkpoint of a block split, more than the nesting cap times, by as many
    /// sessions in turn, is judged in full by a fresh copy (it used to wait for ever: the cap
    /// was on origin-chain length, and the body's chain crosses between the sessions' runs).
    #[test]
    fn a_checkpoint_of_a_block_split_many_times_is_judged() {
        let mut state = LiveDoc::seeded(&[SeedBlock { id: 1, text: "x".repeat(100), ..SeedBlock::default() }]).unwrap().state();
        for n in 0..70_u64 {
            let mut peer = LiveDoc::from_state(&state).unwrap();
            peer.set_client(100 + n as u32).unwrap();
            peer.split(1 + n, 1, 2 + n).unwrap();
            state = peer.state();
        }
        let mut shadow = Shadow::new(Some("wordcraft/1"));
        shadow.admit(&state).unwrap();
        assert!(!shadow.is_waiting(), "the checkpoint waits");
        let back = LiveDoc::from_state(&shadow.state()).unwrap();
        assert_eq!(back.block(71).unwrap().text, "x".repeat(30));
    }

    #[test]
    fn the_shadow_refuses_what_does_not_fit_and_keeps_going() {
        let (state, ops) = big_doc(16 * 1024);
        let mut shadow = Shadow::new(Some("wordcraft/1"));
        shadow.admit(&state).unwrap();
        // A string item whose right origin is (0, 0), the seed's first id in the body array:
        // a string in an array, which yrs cannot read back.
        let bad = vec![1, 1, 77, 0, 0x44, 0, 0, 2, 0xc3, 0xa9, 0];
        assert!(crate::update_is_sound(&bad), "the gate alone takes it");
        assert_eq!(shadow.admit(&bad), Err(ShadowRefusal::Invalid));
        for op in &ops[..20] {
            shadow.admit(op).unwrap();
        }
        let mut replica = LiveDoc::from_state(&state).unwrap();
        for op in &ops[..20] {
            replica.apply(op).unwrap();
        }
        assert_eq!(LiveDoc::from_state(&shadow.state()).unwrap().order(), replica.order());
    }
}
