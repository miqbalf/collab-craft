//! Integration tests for the yrs live document and the `crdt.text` path that carries it.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use craft_collab::{BlockChange, ClientMsg, DOC_SCOPE, Hub, Live, LiveDoc, LiveError, SeedBlock, ServerMsg, TextPayload, Work, update_from_wire};
use proptest::prelude::*;
use proptest::test_runner::{Config, RngSeed};
use serde_json::{Map, Value, json};

fn attrs(v: Value) -> Map<String, Value> {
    match v {
        Value::Object(m) => m,
        _ => Map::new(),
    }
}

fn plain(id: u64, text: &str) -> SeedBlock {
    SeedBlock { id, text: text.to_owned(), attrs: Map::new(), runs: Vec::new() }
}

/// A small document with attributes, formatted runs and non-ASCII text.
fn sample() -> Vec<SeedBlock> {
    vec![
        SeedBlock { id: 1, text: "Title".to_owned(), attrs: attrs(json!({"style": "Heading1", "align": "center"})), runs: Vec::new() },
        SeedBlock {
            id: 2,
            text: "naïve 😀 text".to_owned(),
            attrs: attrs(json!({"style": "Normal"})),
            runs: vec![(2, Map::new()), (4, attrs(json!({"b": true}))), (2, attrs(json!({"b": true, "i": true}))), (4, Map::new())],
        },
        plain(3, ""),
        plain(4, "last paragraph"),
    ]
}

fn replica(blocks: &[SeedBlock], space: u32) -> LiveDoc {
    let mut d = LiveDoc::seeded(blocks).unwrap();
    d.set_client(space).unwrap();
    d
}

fn assert_same(a: &LiveDoc, b: &LiveDoc, ids: &BTreeSet<u64>) {
    assert_eq!(a.order(), b.order(), "order differs");
    for id in ids {
        assert_eq!(a.block(*id), b.block(*id), "block {id} differs");
    }
}

fn ids_of(docs: &[&LiveDoc]) -> BTreeSet<u64> {
    docs.iter().flat_map(|d| d.order()).collect()
}

#[test]
fn seeding_is_deterministic() {
    let a = LiveDoc::seeded(&sample()).unwrap();
    let b = LiveDoc::seeded(&sample()).unwrap();
    assert_eq!(a.state(), b.state());
    assert_eq!(a.order(), vec![1, 2, 3, 4]);
}

#[test]
fn seeded_blocks_round_trip() {
    let d = LiveDoc::seeded(&sample()).unwrap();
    for block in sample() {
        assert_eq!(d.block(block.id), Some(block));
    }
    assert_eq!(d.block(99), None);
}

/// A run list that covers a plain text with one empty run reads back as no runs.
#[test]
fn plain_text_has_no_runs() {
    let mut b = plain(1, "abc");
    b.runs = vec![(3, Map::new())];
    let d = LiveDoc::seeded(&[b]).unwrap();
    assert_eq!(d.block(1).unwrap().runs, Vec::new());
}

#[test]
fn offsets_count_unicode_scalars() {
    let mut d = replica(&[plain(1, "naïve 😀")], 1);
    d.insert_text(1, 7, "!", &Map::new()).unwrap();
    assert_eq!(d.block(1).unwrap().text, "naïve 😀!");
    d.delete_text(1, 6, 1).unwrap();
    assert_eq!(d.block(1).unwrap().text, "naïve !");
    d.format(1, 2, 1, &attrs(json!({"b": true}))).unwrap();
    assert_eq!(d.block(1).unwrap().runs, vec![(2, Map::new()), (1, attrs(json!({"b": true}))), (4, Map::new())]);
    assert_eq!(d.insert_text(1, 8, "x", &Map::new()), Err(LiveError::OutOfRange));
    assert_eq!(d.delete_text(1, 6, 2), Err(LiveError::OutOfRange));
    assert_eq!(d.insert_text(9, 0, "x", &Map::new()), Err(LiveError::UnknownBlock));
}

/// Inserted text carries exactly the run attributes given, not the ones around the caret.
#[test]
fn inserted_text_takes_exactly_the_given_attributes() {
    let mut d = replica(&sample(), 1);
    // Position 4 of block 2 is inside the bold run.
    d.insert_text(2, 4, "XY", &attrs(json!({"i": true}))).unwrap();
    let b = d.block(2).unwrap();
    assert_eq!(b.text, "naïvXYe 😀 text");
    assert_eq!(b.runs[0], (2, Map::new()));
    assert_eq!(b.runs[1], (2, attrs(json!({"b": true}))));
    assert_eq!(b.runs[2], (2, attrs(json!({"i": true}))));
}

#[test]
fn local_edits_wait_for_a_client_id() {
    let mut d = LiveDoc::seeded(&sample()).unwrap();
    assert_eq!(d.insert_text(1, 0, "x", &Map::new()), Err(LiveError::NoClient));
    // Space 0 is the seed's, so it is ignored rather than adopted.
    d.set_client(0).unwrap();
    assert_eq!(d.insert_text(1, 0, "x", &Map::new()), Err(LiveError::NoClient));
    assert_eq!(d.state(), LiveDoc::seeded(&sample()).unwrap().state());
    d.set_client(4).unwrap();
    assert!(d.insert_text(1, 0, "x", &Map::new()).is_ok());
}

#[test]
fn garbage_is_a_decode_error() {
    let mut d = replica(&sample(), 1);
    assert_eq!(d.apply(&[0xff, 0xff, 0xff]), Err(LiveError::Decode));
}

#[test]
fn split_moves_the_tail_and_copies_attrs() {
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let up = a.split(2, 4, 100).unwrap();
    assert_eq!(a.order(), vec![1, 2, 100, 3, 4]);
    let head = a.block(2).unwrap();
    let tail = a.block(100).unwrap();
    assert_eq!(head.text, "naïv");
    assert_eq!(head.runs, vec![(2, Map::new()), (2, attrs(json!({"b": true})))]);
    assert_eq!(tail.text, "e 😀 text");
    assert_eq!(tail.attrs, head.attrs);
    assert_eq!(tail.runs, vec![(2, attrs(json!({"b": true}))), (2, attrs(json!({"b": true, "i": true}))), (4, Map::new())]);
    let changes = b.apply(&up).unwrap();
    assert!(changes.contains(&BlockChange::Inserted { at: 2, id: 100 }), "{changes:?}");
    assert_same(&a, &b, &ids_of(&[&a, &b]));
    assert_eq!(a.split(2, 9, 101), Err(LiveError::OutOfRange));
    assert_eq!(a.split(2, 1, 100), Err(LiveError::Exists));
}

#[test]
fn merge_appends_and_removes_the_second() {
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let up = a.merge(1, 2).unwrap();
    assert_eq!(a.order(), vec![1, 3, 4]);
    let merged = a.block(1).unwrap();
    assert_eq!(merged.text, "Titlenaïve 😀 text");
    assert_eq!(merged.attrs, sample()[0].attrs);
    assert_eq!(merged.runs, vec![(7, Map::new()), (4, attrs(json!({"b": true}))), (2, attrs(json!({"b": true, "i": true}))), (4, Map::new())]);
    // The second block stays readable as a tombstone.
    assert_eq!(a.block(2).unwrap().text, "naïve 😀 text");
    let changes = b.apply(&up).unwrap();
    assert!(changes.contains(&BlockChange::Removed { id: 2 }), "{changes:?}");
    assert!(changes.contains(&BlockChange::Text { id: 1 }), "{changes:?}");
    assert_same(&a, &b, &ids_of(&[&a, &b]));
}

#[test]
fn merge_of_removed_block_is_refused() {
    let mut d = replica(&sample(), 1);
    d.remove_block(2).unwrap();
    assert_eq!(d.merge(1, 2), Err(LiveError::Gone));
    assert_eq!(d.merge(2, 3), Err(LiveError::Gone));
    assert_eq!(d.merge(1, 99), Err(LiveError::Gone));
    assert_eq!(d.remove_block(2), Err(LiveError::Gone));
    assert_eq!(d.remove_block(99), Err(LiveError::UnknownBlock));
}

#[test]
fn insert_block_and_set_attrs_replicate() {
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let block = SeedBlock {
        id: 50,
        text: "new".to_owned(),
        attrs: attrs(json!({"style": "Quote"})),
        runs: vec![(1, attrs(json!({"b": true}))), (2, Map::new())],
    };
    let up1 = a.insert_block(1, &block).unwrap();
    let up2 = a.set_attrs(1, &attrs(json!({"style": "Title"}))).unwrap();
    assert_eq!(a.block(50), Some(block.clone()));
    assert_eq!(a.insert_block(9, &block), Err(LiveError::OutOfRange));
    assert_eq!(b.apply(&up1).unwrap(), vec![BlockChange::Inserted { at: 1, id: 50 }]);
    assert_eq!(b.apply(&up2).unwrap(), vec![BlockChange::Attrs { id: 1 }]);
    assert_eq!(b.block(1).unwrap().attrs, attrs(json!({"style": "Title"})));
    assert_same(&a, &b, &ids_of(&[&a, &b]));
}

/// Ruling 1: a removed block is a tombstone, so typing that raced the removal still lands and
/// is reported against an id that is no longer in the order.
#[test]
fn concurrent_edit_to_a_removed_block_survives_as_a_tombstone() {
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let removal = a.remove_block(4).unwrap();
    let typing = b.insert_text(4, 14, "!", &Map::new()).unwrap();

    let at_b = b.apply(&removal).unwrap();
    assert_eq!(at_b, vec![BlockChange::Removed { id: 4 }]);
    let at_a = a.apply(&typing).unwrap();
    assert_eq!(at_a, vec![BlockChange::Text { id: 4 }]);

    for d in [&a, &b] {
        assert!(!d.order().contains(&4));
        assert_eq!(d.block(4).unwrap().text, "last paragraph!");
    }
    assert_same(&a, &b, &[1, 2, 3, 4].into_iter().collect());
}

#[test]
fn undo_only_undoes_local() {
    let mut a = replica(&[plain(1, "hello")], 1);
    let mut b = replica(&[plain(1, "hello")], 2);
    let ua = a.insert_text(1, 5, " A", &Map::new()).unwrap();
    let ub = b.insert_text(1, 0, "B ", &Map::new()).unwrap();
    a.apply(&ub).unwrap();
    b.apply(&ua).unwrap();
    assert_eq!(a.block(1).unwrap().text, "B hello A");

    let undo = a.undo().expect("A has a local change to undo");
    assert_eq!(a.block(1).unwrap().text, "B hello");
    b.apply(&undo).unwrap();
    assert_eq!(b.block(1).unwrap().text, "B hello");
    // B's typing arrived through apply, so it is not A's to undo.
    assert_eq!(a.undo(), None);

    let redo = a.redo().expect("the undo can be redone");
    b.apply(&redo).unwrap();
    assert_eq!(a.block(1).unwrap().text, "B hello A");
    assert_eq!(b.block(1).unwrap().text, "B hello A");
}

#[test]
fn untracked_edits_are_sent_but_never_undone() {
    let mut a = replica(&[plain(1, "hello")], 1);
    let mut b = replica(&[plain(1, "hello")], 2);
    let typed = a.insert_text(1, 5, " A", &Map::new()).unwrap();
    let kept = a.untracked(|d| d.insert_text(1, 0, "U ", &Map::new())).unwrap();
    b.apply(&typed).unwrap();
    b.apply(&kept).unwrap();
    assert_eq!(b.block(1).unwrap().text, "U hello A", "an untracked edit travels like any other");
    // Undo passes over the untracked edit, newer as it is, to the tracked one before it.
    let undo = a.undo().expect("the tracked edit is undone");
    b.apply(&undo).unwrap();
    for d in [&a, &b] {
        assert_eq!(d.block(1).unwrap().text, "U hello");
    }
    assert_eq!(a.undo(), None, "nothing tracked is left");
    // Edits after the closure are tracked again.
    a.insert_text(1, 0, "x", &Map::new()).unwrap();
    assert!(a.undo().is_some());
}

#[test]
fn undo_covers_structure_too() {
    let mut a = replica(&sample(), 1);
    a.split(2, 4, 100).unwrap();
    a.remove_block(4).unwrap();
    a.undo().unwrap();
    assert_eq!(a.order(), vec![1, 2, 100, 3, 4]);
    a.undo().unwrap();
    assert_eq!(a.order(), vec![1, 2, 3, 4]);
    assert_eq!(a.block(2), Some(sample()[1].clone()));
}

/// Undo never takes away another replica's text: an undo that would remove a block someone
/// else has typed in says so before anything changes ([`LiveDoc::undo_takes_others_text`]), so
/// the app can refuse it. Typing elsewhere, or in a block the undo keeps, does not count.
#[test]
fn an_undo_that_would_remove_anothers_typing_is_found_first() {
    let new = (1 << 32) | 7;
    // A inserts a block, B types in it: undoing the insert would take B's typing away.
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let up = a.insert_block(1, &plain(new, "fresh")).unwrap();
    assert!(!a.undo_takes_others_text(1), "nobody else typed yet");
    b.apply(&up).unwrap();
    let up = b.insert_text(new, 2, "BOB", &Map::new()).unwrap();
    a.apply(&up).unwrap();
    assert!(a.undo_takes_others_text(1), "B typed in the block the undo would remove");
    assert!(!b.undo_takes_others_text(1), "B's own undo takes only B's typing");
    assert_eq!(a.order(), b.order(), "asking changes nothing");
    assert_eq!(a.block(new), b.block(new));
    // B typing in a block the undo keeps does not count.
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let up = a.insert_block(1, &plain(new, "fresh")).unwrap();
    b.apply(&up).unwrap();
    let up = b.insert_text(1, 0, "BOB", &Map::new()).unwrap();
    a.apply(&up).unwrap();
    assert!(!a.undo_takes_others_text(1));
    // A split (Enter): B types in the new tail; undoing the split would take it away.
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let up = a.split(4, 4, new).unwrap();
    b.apply(&up).unwrap();
    assert_eq!(b.block(new).unwrap().text, " paragraph");
    let up = b.insert_text(new, 0, "BOB", &Map::new()).unwrap();
    a.apply(&up).unwrap();
    assert!(a.undo_takes_others_text(1));
    // B typing in the head of the split does not count: the undo keeps that block.
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let up = a.split(4, 4, new).unwrap();
    b.apply(&up).unwrap();
    let up = b.insert_text(4, 0, "BOB", &Map::new()).unwrap();
    a.apply(&up).unwrap();
    assert!(!a.undo_takes_others_text(1));
    // Only the steps asked about count: an older step's block is not looked at.
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let up = a.insert_block(1, &plain(new, "fresh")).unwrap();
    b.apply(&up).unwrap();
    let up = b.insert_text(new, 0, "BOB", &Map::new()).unwrap();
    a.apply(&up).unwrap();
    a.insert_text(1, 0, "A", &Map::new()).unwrap();
    assert!(!a.undo_takes_others_text(1), "the newest step is A's own typing");
    assert!(a.undo_takes_others_text(2), "the step before it inserted the block B typed in");
}

/// One peer undoes inserting a block while another undoes removing it: the second undo puts the
/// id back in the body, the first takes the block itself away. An id whose block is gone is not
/// in the order on any replica, and its removal is reported, so every app drops it alike.
#[test]
fn an_id_whose_block_was_undone_away_is_not_in_the_order() {
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let new = (1 << 32) | 7;
    let up = a.insert_block(1, &plain(new, "fresh")).unwrap();
    b.apply(&up).unwrap();
    let up = b.remove_block(new).unwrap();
    a.apply(&up).unwrap();
    let undo_insert = a.undo().unwrap();
    let undo_remove = b.undo().unwrap();
    let on_a = a.apply(&undo_remove).unwrap();
    let on_b = b.apply(&undo_insert).unwrap();
    for (name, d) in [("a", &a), ("b", &b)] {
        assert!(d.order().iter().all(|id| d.block(*id).is_some()), "{name}: every id in the order has its block: {:?}", d.order());
    }
    assert_eq!(a.order(), b.order());
    assert!(!a.order().contains(&new));
    assert!(on_b.contains(&BlockChange::Removed { id: new }), "b, which had the block in its order, is told it left: {on_b:?}");
    assert!(!on_a.iter().any(|c| matches!(c, BlockChange::Inserted { id, .. } if *id == new)), "a is not told it came back: {on_a:?}");
}

/// The relay's retained `crdt.text` log, in order, as `(seq, params)`.
#[derive(Default)]
struct Relay {
    seq: u64,
    ops: Vec<(u64, Value)>,
}

impl Relay {
    fn submit(&mut self, params: Value) -> u64 {
        self.seq += 1;
        self.ops.push((self.seq, params));
        self.seq
    }
    fn edit(&mut self, up: Vec<u8>) -> Vec<u8> {
        self.submit(serde_json::to_value(TextPayload::new(&up)).unwrap());
        up
    }
    /// `Saved{seq}`: drop every op at or below it.
    fn saved(&mut self, seq: u64) {
        self.ops.retain(|(s, _)| *s > seq);
    }
}

fn bytes_of(params: &Value) -> Vec<Vec<u8>> {
    params["ops"].as_array().unwrap().iter().map(|op| update_from_wire(op.as_str().unwrap()).unwrap()).collect()
}

/// The document as the file would hold it: the blocks in order.
fn file_of(d: &LiveDoc) -> Vec<SeedBlock> {
    d.order().into_iter().map(|id| d.block(id).unwrap()).collect()
}

/// A and B edit, the worker checkpoints, the relay drops what the checkpoint covers, and C joins
/// from what is left. Then all three edit inside the text the room was first seeded with, which
/// is where a joiner built from the saved file would hold different item ids.
#[test]
fn late_joiner_after_checkpoint_converges() {
    let file = sample();
    let mut a = replica(&file, 1);
    let mut b = replica(&file, 2);
    let mut worker = replica(&file, 9);
    let mut relay = Relay::default();

    let u1 = relay.edit(a.insert_text(2, 0, "¡", &Map::new()).unwrap());
    let u2 = relay.edit(b.split(4, 4, 200).unwrap());
    b.apply(&u1).unwrap();
    a.apply(&u2).unwrap();
    let u3 = relay.edit(a.format(200, 0, 3, &attrs(json!({"u": true}))).unwrap());
    let u4 = relay.edit(b.remove_block(3).unwrap());
    b.apply(&u3).unwrap();
    a.apply(&u4).unwrap();

    // The worker has applied everything ordered so far, checkpoints at S and saves the file.
    for (_, params) in &relay.ops.clone() {
        for up in bytes_of(params) {
            worker.apply(&up).unwrap();
        }
    }
    let saved_file = file_of(&worker);
    let s = relay.submit(serde_json::to_value(TextPayload::snapshot(&worker.state())).unwrap());
    relay.saved(s - 1);
    assert_eq!(relay.ops.first().map(|(seq, p)| (*seq, p["snap"].clone())), Some((s, Value::Bool(true))));

    // Editing goes on after the checkpoint.
    let u5 = relay.edit(b.merge(1, 2).unwrap());
    a.apply(&u5).unwrap();

    // C joins: the first retained op is the snapshot, so it starts from that, never the file.
    let (first, tail) = relay.ops.split_first().unwrap();
    let mut c = LiveDoc::from_state(&bytes_of(&first.1)[0]).unwrap();
    c.set_client(3).unwrap();
    for (_, params) in tail {
        for up in bytes_of(params) {
            c.apply(&up).unwrap();
        }
    }
    // The saved file is behind the checkpoint's items; a joiner never seeds it.
    drop(saved_file);
    let ids: BTreeSet<u64> = ids_of(&[&a, &b, &c]).into_iter().chain([1, 2, 3, 4, 200]).collect();
    assert_same(&a, &c, &ids);

    // Everyone edits inside the originally seeded text.
    let ua = a.insert_text(4, 2, "A", &Map::new()).unwrap();
    let ub = b.delete_text(1, 1, 2).unwrap();
    let uc = c.insert_text(1, 3, "C", &Map::new()).unwrap();
    let uc2 = c.format(4, 0, 4, &attrs(json!({"b": true}))).unwrap();
    for (d, ups) in [(&mut a, vec![&ub, &uc, &uc2]), (&mut b, vec![&ua, &uc, &uc2]), (&mut c, vec![&ua, &ub])] {
        for up in ups {
            d.apply(up).unwrap();
        }
    }
    assert_same(&a, &b, &ids);
    assert_same(&a, &c, &ids);
    for d in [&a, &b, &c] {
        let rebuilt = LiveDoc::from_state(&d.state()).unwrap();
        assert_same(&a, &rebuilt, &ids);
    }
}

/// A block removed before the checkpoint stays out of the order after a restore, and a late
/// edit to it is still reported against its id.
#[test]
fn snapshot_restores_tombstones() {
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let removal = a.remove_block(3).unwrap();
    // B has not seen the removal and types into the block.
    let typing = b.insert_text(3, 0, "late", &Map::new()).unwrap();

    let mut c = LiveDoc::from_state(&a.state()).unwrap();
    c.set_client(3).unwrap();
    assert_eq!(c.order(), vec![1, 2, 4]);
    assert_eq!(c.block(3), Some(plain(3, "")));
    assert_eq!(c.apply(&typing).unwrap(), vec![BlockChange::Text { id: 3 }]);
    assert_eq!(c.block(3).unwrap().text, "late");
    b.apply(&removal).unwrap();
    assert_same(&b, &c, &[1, 2, 3, 4].into_iter().collect());
}

#[test]
fn from_state_refuses_garbage() {
    assert!(matches!(LiveDoc::from_state(&[0xff, 0x01, 0x02]), Err(LiveError::Decode)));
}

/// Damaged updates never escape as a panic: each is applied or refused as `Decode`.
#[test]
fn damaged_updates_never_panic() {
    let mut source = replica(&sample(), 1);
    let mut updates = vec![source.state()];
    updates.push(source.insert_text(2, 3, "xyz", &attrs(json!({"b": true}))).unwrap());
    updates.push(source.split(2, 5, 300).unwrap());
    updates.push(source.remove_block(4).unwrap());
    let mut mix = Mix(7);
    for round in 0..3000 {
        let mut bytes = updates[round % updates.len()].clone();
        for _ in 0..=mix.below(4) {
            let i = mix.below(bytes.len());
            bytes[i] = (mix.next() & 0xff) as u8;
        }
        if mix.below(5) == 0 {
            bytes.truncate(mix.below(bytes.len() + 1));
        }
        let mut target = replica(&sample(), 2);
        match target.apply(&bytes) {
            Ok(_) | Err(LiveError::Decode) => {}
            Err(e) => panic!("unexpected {e:?}"),
        }
        // Only a caught yrs panic marks a replica damaged, and none of these panicked.
        assert!(!target.damaged(), "round {round} left the replica damaged");
    }
}

// ---- the convergence property ------------------------------------------------------------

/// splitmix64, so the delivery schedule is a pure function of the generated seed.
struct Mix(u64);
impl Mix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 { 0 } else { (self.next() % n as u64) as usize }
    }
}

type Step = (u8, u16, u16, u16);

const ALPHABET: [&str; 6] = ["a", "b", " ", "é", "😀", "xyz"];

/// Run one random operation against a replica, resolved against its current state.
fn act(d: &mut LiveDoc, step: Step, fresh: &mut u64) -> Option<Vec<u8>> {
    let (kind, x, y, z) = step;
    let order = d.order();
    if order.is_empty() || kind == 6 {
        *fresh += 1;
        let at = usize::from(x) % (order.len() + 1);
        let block = SeedBlock { id: *fresh, text: ALPHABET[usize::from(y) % ALPHABET.len()].to_owned(), attrs: Map::new(), runs: Vec::new() };
        return d.insert_block(at, &block).ok();
    }
    let id = order[usize::from(x) % order.len()];
    let len = d.block(id)?.text.chars().count();
    let at = usize::from(y) % (len + 1);
    let span = if len > at { usize::from(z) % (len - at) + 1 } else { 0 };
    let flag = |k: &str| {
        let mut m = Map::new();
        m.insert(k.to_owned(), if z % 3 == 0 { Value::Null } else { Value::Bool(true) });
        m
    };
    match kind {
        0 => d.insert_text(id, at, ALPHABET[usize::from(z) % ALPHABET.len()], &if z % 2 == 0 { Map::new() } else { flag("b") }).ok(),
        1 if span > 0 => d.delete_text(id, at, span.min(3)).ok(),
        2 if span > 0 => d.format(id, at, span, &flag(if y % 2 == 0 { "b" } else { "i" })).ok(),
        3 => {
            *fresh += 1;
            d.split(id, at, *fresh).ok()
        }
        4 => {
            let i = order.iter().position(|o| *o == id)?;
            let next = *order.get(i + 1)?;
            d.merge(id, next).ok()
        }
        5 => d.remove_block(id).ok(),
        7 => d.set_attrs(id, &attrs(json!({"style": format!("S{}", z % 3)}))).ok(),
        8 => d.undo(),
        9 => d.redo(),
        _ => d.insert_text(id, at, "q", &Map::new()).ok(),
    }
}

fn converge(replicas: usize, steps: &[Vec<Step>], seed: u64) {
    let blocks = sample();
    let mut docs: Vec<LiveDoc> = (0..replicas).map(|r| replica(&blocks, r as u32 + 1)).collect();
    let mut fresh: Vec<u64> = (0..replicas).map(|r| (r as u64 + 1) * 1_000_000).collect();
    let mut cursor = vec![0usize; replicas];
    let mut log: Vec<Vec<u8>> = Vec::new();
    let mut mix = Mix(seed);

    loop {
        let live: Vec<usize> = (0..replicas).filter(|r| cursor[*r] < steps[*r].len()).collect();
        if live.is_empty() {
            break;
        }
        let r = live[mix.below(live.len())];
        let step = steps[r][cursor[r]];
        cursor[r] += 1;
        if let Some(up) = act(&mut docs[r], step, &mut fresh[r]) {
            log.push(up);
        }
        // Deliver something already sent, in any order and possibly again.
        if !log.is_empty() && mix.below(3) == 0 {
            let to = mix.below(replicas);
            let up = &log[mix.below(log.len())];
            docs[to].apply(up).unwrap();
        }
    }
    for d in &mut docs {
        let mut all: Vec<usize> = (0..log.len()).collect();
        for i in (1..all.len()).rev() {
            all.swap(i, mix.below(i + 1));
        }
        for i in all {
            d.apply(&log[i]).unwrap();
            if mix.below(8) == 0 {
                d.apply(&log[i]).unwrap();
            }
        }
    }
    let refs: Vec<&LiveDoc> = docs.iter().collect();
    // Every id any replica minted, so removed blocks (tombstones) must agree too.
    let mut ids = ids_of(&refs);
    ids.extend(blocks.iter().map(|b| b.id));
    for (r, last) in fresh.iter().enumerate() {
        ids.extend((r as u64 + 1) * 1_000_000 + 1..=*last);
    }
    for d in &docs[1..] {
        assert_same(&docs[0], d, &ids);
    }
}

fn step() -> impl Strategy<Value = Step> {
    (0u8..10, any::<u16>(), any::<u16>(), any::<u16>())
}

proptest! {
    #![proptest_config(Config { cases: 64, rng_seed: RngSeed::Fixed(0x00c0_ffee), failure_persistence: None, ..Config::default() })]

    #[test]
    fn random_traces_converge(replicas in 2usize..=6, steps in prop::collection::vec(prop::collection::vec(step(), 200), 6), seed in any::<u64>()) {
        converge(replicas, &steps, seed);
    }
}

// ---- the relay path ----------------------------------------------------------------------

#[derive(Default)]
struct Wire {
    sent: Vec<ClientMsg>,
    inbox: Vec<ServerMsg>,
}

#[derive(Clone, Default)]
struct FakeHub(Rc<RefCell<Wire>>);

impl Hub for FakeHub {
    fn send(&mut self, msg: ClientMsg) -> craft_collab::Result<()> {
        self.0.borrow_mut().sent.push(msg);
        Ok(())
    }
    fn poll(&mut self) -> Vec<ServerMsg> {
        std::mem::take(&mut self.0.borrow_mut().inbox)
    }
    fn connected(&self) -> bool {
        true
    }
}

fn joined(peer: &str, space: u32) -> ServerMsg {
    ServerMsg::Joined {
        doc: "d".to_owned(),
        peer: peer.to_owned(),
        seq: 0,
        space,
        roster: Vec::new(),
        locks: Vec::new(),
        replay_until: None,
        fallback: false,
        text_cap: None,
        heartbeat: false,
        history_lost: false,
    }
}

#[test]
fn live_sends_yrs_updates_as_crdt_text() {
    let alice_hub = FakeHub::default();
    let bob_hub = FakeHub::default();
    let mut alice = Live::new(alice_hub.clone(), "d");
    let mut bob = Live::new(bob_hub.clone(), "d");
    alice.seed(&sample()).unwrap();
    assert!(!alice.ready_to_type());
    assert!(alice.edit(|d| d.insert_text(4, 0, "x", &Map::new())).is_err(), "no client id before the relay grants a space");

    alice_hub.0.borrow_mut().inbox.push(joined("alice", 1));
    bob_hub.0.borrow_mut().inbox.push(joined("bob", 2));
    alice.pump();
    bob.pump();
    assert!(alice.ready_to_type());

    alice.edit(|d| d.insert_text(4, 14, "!", &Map::new())).unwrap();
    let sent = alice_hub.0.borrow_mut().sent.pop().unwrap();
    let ClientMsg::Submit { command, params, scope, local, .. } = sent else { panic!("not a submit: {sent:?}") };
    assert_eq!(command, craft_collab::CRDT_TEXT);
    assert_eq!(scope, None);
    assert_eq!(params["scope"], DOC_SCOPE);
    assert_eq!(params["ops"].as_array().unwrap().len(), 1);

    // The relay retains it and bob joins late: the replay arrives before bob has the file.
    bob_hub.0.borrow_mut().inbox.push(ServerMsg::TextOps {
        doc: "d".to_owned(),
        scope: DOC_SCOPE.to_owned(),
        ops: params["ops"].clone(),
        complete: true,
        snap: false,
        more: false,
    });
    let work = bob.pump();
    assert!(matches!(work.as_slice(), [Work::SeedWanted { .. }]), "{work:?}");
    let changes = bob.seed(&sample()).unwrap();
    assert_eq!(changes, vec![BlockChange::Text { id: 4 }]);
    assert_eq!(bob.doc().unwrap().block(4).unwrap().text, "last paragraph!");

    // A live op from alice reaches a seeded bob as block changes.
    alice.edit(|d| d.insert_text(1, 0, "#", &Map::new())).unwrap();
    let Some(ClientMsg::Submit { params, .. }) = alice_hub.0.borrow_mut().sent.pop() else { panic!("no submit") };
    bob_hub.0.borrow_mut().inbox.push(ServerMsg::Op {
        doc: "d".to_owned(),
        seq: 1,
        origin: "alice".to_owned(),
        local: Some(local + 1),
        command: craft_collab::CRDT_TEXT.to_owned(),
        params,
        scope: None,
    });
    let work = bob.pump();
    assert!(matches!(work.as_slice(), [Work::Doc { changes }] if changes == &vec![BlockChange::Text { id: 1 }]), "{work:?}");
    assert_eq!(bob.doc().unwrap().block(1).unwrap().text, "#Title");

    // Undo goes out over the same wire.
    alice.undo().unwrap();
    assert!(matches!(alice_hub.0.borrow_mut().sent.pop(), Some(ClientMsg::Submit { .. })));
    assert_eq!(alice.doc().unwrap().block(1).unwrap().text, "Title");
}

/// The text replica this replaces was `Send`, so an app may own the document on a worker.
#[test]
fn live_doc_is_send() {
    fn send<T: Send>() {}
    send::<LiveDoc>();
}

fn op(seq: u64, origin: &str, local: Option<u64>, params: Value) -> ServerMsg {
    ServerMsg::Op { doc: "d".to_owned(), seq, origin: origin.to_owned(), local, command: craft_collab::CRDT_TEXT.to_owned(), params, scope: None }
}

fn last_submit(hub: &FakeHub) -> (u64, Value) {
    match hub.0.borrow_mut().sent.pop() {
        Some(ClientMsg::Submit { local, params, .. }) => (local, params),
        other => panic!("not a submit: {other:?}"),
    }
}

/// The saver's side: a snapshot submit, then `Saved{S-1}` once it is ordered cleanly, and no
/// save report when a remote edit slipped in ahead of it. Then the joiner's side: a first join
/// whose catch-up starts with the snapshot restores it and ignores the file.
#[test]
fn live_checkpoint_and_snapshot_join() {
    let alice_hub = FakeHub::default();
    let mut alice = Live::new(alice_hub.clone(), "d");
    alice.seed(&sample()).unwrap();
    alice_hub.0.borrow_mut().inbox.push(joined("alice", 1));
    alice.pump();
    let mut bob = replica(&sample(), 2);
    let mut relay: Vec<ServerMsg> = Vec::new();

    alice.edit(|d| d.insert_text(2, 0, "¡", &Map::new())).unwrap();
    let (l1, p1) = last_submit(&alice_hub);
    relay.push(op(1, "alice", Some(l1), p1.clone()));
    for up in bytes_of(&p1) {
        bob.apply(&up).unwrap();
    }

    alice.checkpoint().unwrap();
    assert!(alice.checkpoint().is_err(), "one checkpoint at a time");
    let (l2, p2) = last_submit(&alice_hub);
    assert_eq!(p2["snap"], Value::Bool(true));
    relay.push(op(2, "alice", Some(l2), p2.clone()));
    alice_hub.0.borrow_mut().inbox.extend(relay.clone());
    let work = alice.pump();
    assert!(!work.iter().any(|w| matches!(w, Work::Checkpointed { .. })), "not before the relay's ack: {work:?}");
    assert!(matches!(alice_hub.0.borrow_mut().sent.pop(), Some(ClientMsg::Saved { seq: 1, .. })));
    alice_hub.0.borrow_mut().inbox.push(saved_ack(1));
    let work = alice.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Checkpointed { seq: 2 })), "{work:?}");

    // A race: bob's edit is ordered between the snapshot and its confirmation.
    alice.checkpoint().unwrap();
    let (l3, _) = last_submit(&alice_hub);
    let pb = serde_json::to_value(TextPayload::new(&bob.insert_text(4, 0, "B", &Map::new()).unwrap())).unwrap();
    relay.push(op(3, "bob", Some(1), pb.clone()));
    // The relay orders it after bob's edit (its content does not matter to this test).
    relay.push(op(4, "alice", Some(l3), json!({"scope": "doc", "snap": true, "ops": []})));
    alice_hub.0.borrow_mut().inbox.extend(relay[2..].iter().cloned());
    let work = alice.pump();
    assert!(work.iter().any(|w| matches!(w, Work::CheckpointRaced)), "{work:?}");
    assert!(alice_hub.0.borrow().sent.is_empty(), "a raced checkpoint is not reported as saved");

    // The relay kept ops after seq 1. Carol joins at the saved baseline.
    let saved_file = file_of(alice.doc().unwrap());
    let carol_hub = FakeHub::default();
    let mut carol = Live::new(carol_hub.clone(), "d");
    carol_hub.0.borrow_mut().inbox.push(joined("carol", 3));
    if let ServerMsg::Joined { seq, .. } = &mut carol_hub.0.borrow_mut().inbox[0] {
        *seq = 1;
    }
    carol_hub.0.borrow_mut().inbox.extend(relay[1..].iter().cloned());
    let work = carol.pump();
    assert!(work.iter().any(|w| matches!(w, Work::SeedWanted { .. })), "{work:?}");
    carol.seed(&saved_file).unwrap();
    let ids: BTreeSet<u64> = [1, 2, 3, 4].into_iter().collect();
    assert_same(alice.doc().unwrap(), carol.doc().unwrap(), &ids);

    // Dave seeded the (saved) file before his catch-up arrived: he is reset onto the snapshot.
    let dave_hub = FakeHub::default();
    let mut dave = Live::new(dave_hub.clone(), "d");
    dave.seed(&saved_file).unwrap();
    dave_hub.0.borrow_mut().inbox.push(joined("dave", 4));
    if let ServerMsg::Joined { seq, .. } = &mut dave_hub.0.borrow_mut().inbox[0] {
        *seq = 1;
    }
    dave_hub.0.borrow_mut().inbox.extend(relay[1..].iter().cloned());
    let work = dave.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Reset)), "{work:?}");
    assert_same(alice.doc().unwrap(), dave.doc().unwrap(), &ids);

    // Carol types inside the originally seeded text; alice and dave take it.
    carol.edit(|d| d.insert_text(1, 2, "C", &Map::new())).unwrap();
    let (lc, pc) = last_submit(&carol_hub);
    alice_hub.0.borrow_mut().inbox.push(op(5, "carol", Some(lc), pc.clone()));
    dave_hub.0.borrow_mut().inbox.push(op(5, "carol", Some(lc), pc));
    alice.pump();
    dave.pump();
    assert_eq!(alice.doc().unwrap().block(1).unwrap().text, "TiCtle");
    assert_same(alice.doc().unwrap(), carol.doc().unwrap(), &ids);
    assert_same(alice.doc().unwrap(), dave.doc().unwrap(), &ids);
}

/// A reconnect shares history, so a snapshot in its catch-up is just another update.
#[test]
fn a_snapshot_after_the_first_op_is_an_ordinary_update() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined("eve", 5));
    live.pump();
    let mut other = replica(&sample(), 6);
    let p1 = serde_json::to_value(TextPayload::new(&other.insert_text(1, 0, "x", &Map::new()).unwrap())).unwrap();
    let p2 = serde_json::to_value(TextPayload::snapshot(&other.state())).unwrap();
    live.edit(|d| d.insert_text(4, 0, "mine ", &Map::new())).unwrap();
    hub.0.borrow_mut().inbox.push(op(1, "other", Some(1), p1));
    hub.0.borrow_mut().inbox.push(op(2, "other", Some(2), p2));
    let work = live.pump();
    assert!(!work.iter().any(|w| matches!(w, Work::Reset | Work::TextIncomplete { .. })), "{work:?}");
    assert_eq!(live.doc().unwrap().block(1).unwrap().text, "xTitle");
    assert_eq!(live.doc().unwrap().block(4).unwrap().text, "mine last paragraph");
}

// ---- fix round 1 -------------------------------------------------------------------------

fn joined_at(peer: &str, space: u32, seq: u64, replay_until: Option<u64>) -> ServerMsg {
    ServerMsg::Joined {
        doc: "d".to_owned(),
        peer: peer.to_owned(),
        seq,
        space,
        roster: Vec::new(),
        locks: Vec::new(),
        replay_until,
        fallback: false,
        text_cap: None,
        heartbeat: false,
        history_lost: false,
    }
}

fn wire(up: &[u8]) -> Value {
    Value::String(craft_collab::update_to_wire(up))
}

fn text_ops(ops: Value, snap: bool) -> ServerMsg {
    ServerMsg::TextOps { doc: "d".to_owned(), scope: DOC_SCOPE.to_owned(), ops, complete: true, snap, more: false }
}

fn whole(d: &LiveDoc) -> BTreeSet<u64> {
    ids_of(&[d]).into_iter().chain([1, 2, 3, 4]).collect()
}

/// C1: the fallback catch-up carries the snapshot and the ops after it in one batch. The tail
/// must land whether the file was loaded before it or not.
#[test]
fn text_ops_keep_the_tail_after_a_snapshot() {
    let mut room = replica(&sample(), 1);
    let snap = room.state();
    let u1 = room.insert_text(1, 0, "1", &Map::new()).unwrap();
    let u2 = room.insert_text(1, 0, "2", &Map::new()).unwrap();
    let ops = json!([wire(&snap), wire(&u1), wire(&u2)]);

    // Before the file is loaded.
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    hub.0.borrow_mut().inbox.extend([fallback_join("c", 3, 5), text_ops(ops.clone(), true)]);
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::SeedWanted { .. })), "{work:?}");
    live.seed(&sample()).unwrap();
    assert_eq!(live.doc().unwrap().block(1).unwrap().text, "21Title");
    assert_same(&room, live.doc().unwrap(), &whole(&room));

    // After the file was loaded (and not edited): reset onto the snapshot, then the tail.
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.extend([fallback_join("c", 3, 5), text_ops(ops, true)]);
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Reset)), "{work:?}");
    assert_eq!(live.doc().unwrap().block(1).unwrap().text, "21Title");
    assert_same(&room, live.doc().unwrap(), &whole(&room));
}

/// I1: a live op ordered after the replay can reach the socket before it. The session puts them
/// back in order and hands both over in one batch; the snapshot must still be recognised by its
/// own sequence, and a file seeded early must be reset rather than mixed with it.
#[test]
fn a_live_op_that_overtakes_the_replayed_snapshot() {
    let mut worker = replica(&sample(), 9);
    let mut peer = replica(&sample(), 2);
    let u1 = peer.insert_text(2, 0, "¡", &Map::new()).unwrap();
    worker.apply(&u1).unwrap();
    let saved_file = file_of(&worker);
    let snap = serde_json::to_value(TextPayload::snapshot(&worker.state())).unwrap();
    let live_edit = serde_json::to_value(TextPayload::new(&peer.insert_text(4, 0, ">", &Map::new()).unwrap())).unwrap();
    for up in bytes_of(&live_edit) {
        worker.apply(&up).unwrap();
    }
    // The relay: op 1 saved through, the snapshot at 2, the live edit at 3. Replay is 2..=2.
    for early_seed in [false, true] {
        let hub = FakeHub::default();
        let mut live = Live::new(hub.clone(), "d");
        if early_seed {
            live.seed(&saved_file).unwrap();
        }
        hub.0.borrow_mut().inbox.extend([
            joined_at("c", 3, 1, Some(2)),
            op(3, "peer", Some(2), live_edit.clone()),
            op(2, "worker", Some(1), snap.clone()),
        ]);
        let work = live.pump();
        if early_seed {
            assert!(work.iter().any(|w| matches!(w, Work::Reset)), "{work:?}");
        } else {
            live.seed(&saved_file).unwrap();
        }
        assert_eq!(live.doc().unwrap().block(2).unwrap().text, "¡naïve 😀 text", "early seed {early_seed}");
        assert_same(&worker, live.doc().unwrap(), &whole(&worker));
    }
}

/// I2: a join that replays nothing leaves no catch-up to wait for, so a peer's checkpoint
/// arriving next is an ordinary update, even for a replica that seeded the file and typed.
#[test]
fn a_peer_checkpoint_after_an_empty_replay_is_not_a_restart() {
    let mut peer = replica(&sample(), 2);
    peer.insert_text(1, 0, "p", &Map::new()).unwrap();
    let snap = serde_json::to_value(TextPayload::snapshot(&peer.state())).unwrap();
    for replay_until in [Some(4), None] {
        let hub = FakeHub::default();
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&sample()).unwrap();
        // An older relay (no marker) at baseline 0 has never saved, so it has no snapshot either.
        let joined_seq = if replay_until.is_some() { 4 } else { 0 };
        hub.0.borrow_mut().inbox.push(joined_at("c", 3, joined_seq, replay_until));
        live.pump();
        live.edit(|d| d.insert_text(4, 0, "mine ", &Map::new())).unwrap();
        hub.0.borrow_mut().inbox.push(op(joined_seq + 1, "peer", Some(1), snap.clone()));
        let work = live.pump();
        assert!(!work.iter().any(|w| matches!(w, Work::Reset | Work::TextIncomplete { .. })), "{replay_until:?}: {work:?}");
        assert!(live.ready_to_type());
        assert_eq!(live.doc().unwrap().block(1).unwrap().text, "pTitle");
        assert_eq!(live.doc().unwrap().block(4).unwrap().text, "mine last paragraph");
    }
}

/// I4: a checkpoint whose submit was refused, lost to a reconnect or overtaken by a replace is
/// dropped as raced, and the next one goes through.
#[test]
fn saving_still_works_after_a_refused_checkpoint() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();

    for kind in 0..3 {
        live.checkpoint().unwrap();
        let (local, _) = last_submit(&hub);
        hub.0.borrow_mut().sent.clear();
        let lost = match kind {
            0 => submit_refused(local, "relay could not order that edit"),
            1 => ServerMsg::Snapshot { doc: "d".to_owned(), seq: 0, document: Value::Null },
            _ => joined_at("alice", 1, 0, Some(0)),
        };
        hub.0.borrow_mut().inbox.push(lost.clone());
        let work = live.pump();
        assert!(work.iter().any(|w| matches!(w, Work::CheckpointRaced)), "{lost:?}: {work:?}");
    }

    live.checkpoint().unwrap();
    let (local, params) = last_submit(&hub);
    hub.0.borrow_mut().inbox.push(op(1, "alice", Some(local), params));
    live.pump();
    assert!(matches!(hub.0.borrow_mut().sent.pop(), Some(ClientMsg::Saved { seq: 0, .. })));
    hub.0.borrow_mut().inbox.push(saved_ack(0));
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Checkpointed { seq: 1 })), "{work:?}");
}

/// I5: two peers inserting the same id leave it in the body twice. Every replica shows it once,
/// at its first place, and removing it takes every copy out.
#[test]
fn a_duplicate_id_in_the_body_is_shown_once_and_removed_whole() {
    let mut a = replica(&sample(), 1);
    let mut b = replica(&sample(), 2);
    let ua = a.insert_block(0, &plain(77, "from a")).unwrap();
    let ub = b.insert_block(4, &plain(77, "from b")).unwrap();
    a.apply(&ub).unwrap();
    b.apply(&ua).unwrap();
    assert_eq!(a.order(), vec![77, 1, 2, 3, 4]);
    assert_same(&a, &b, &whole(&a));

    // A position after the duplicate still means the shown order.
    let ins = a.insert_block(5, &plain(78, "end")).unwrap();
    b.apply(&ins).unwrap();
    assert_eq!(a.order(), vec![77, 1, 2, 3, 4, 78]);

    let rm = b.remove_block(77).unwrap();
    a.apply(&rm).unwrap();
    assert_eq!(a.order(), vec![1, 2, 3, 4, 78]);
    assert_same(&a, &b, &whole(&a));
}

#[test]
fn seeding_refuses_duplicate_ids() {
    assert!(matches!(LiveDoc::seeded(&[plain(1, "a"), plain(1, "b")]), Err(LiveError::Exists)));
}

/// I6: once a snapshot was seen, the file is never seeded, even after the restore failed.
#[test]
fn a_failed_snapshot_restore_never_falls_back_to_the_file() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    let not_a_state = craft_collab::update_to_wire(&[0xff, 0x00, 0x13, 0x37]);
    hub.0.borrow_mut().inbox.extend([fallback_join("c", 3, 5), text_ops(json!([not_a_state]), true)]);
    live.pump();
    assert!(live.seed(&sample()).is_err());
    assert!(live.seed(&sample()).is_err(), "a second try does not seed the file");
    assert!(live.doc().is_none());
    assert!(!live.ready_to_type());
    assert!(live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).is_err());
}

/// Verify (a): the state carried across `set_client` includes updates yrs is still holding for
/// a missing dependency, so they land once it arrives.
#[test]
fn set_client_carries_pending_updates() {
    let mut a = replica(&sample(), 1);
    let first = a.insert_text(1, 0, "1", &Map::new()).unwrap();
    let second = a.insert_text(1, 1, "2", &Map::new()).unwrap();
    let mut c = LiveDoc::seeded(&sample()).unwrap();
    c.apply(&second).unwrap();
    assert_eq!(c.block(1).unwrap().text, "Title", "held: its anchor has not arrived");
    c.set_client(3).unwrap();
    c.apply(&first).unwrap();
    assert_eq!(c.block(1).unwrap().text, "12Title");
}

// ---- fix round 2 -------------------------------------------------------------------------

fn fallback_join(peer: &str, space: u32, seq: u64) -> ServerMsg {
    ServerMsg::Joined {
        doc: "d".to_owned(),
        peer: peer.to_owned(),
        seq,
        space,
        roster: Vec::new(),
        locks: Vec::new(),
        replay_until: Some(seq),
        fallback: true,
        text_cap: None,
        heartbeat: false,
        history_lost: false,
    }
}

fn submit_refused(local: u64, detail: &str) -> ServerMsg {
    ServerMsg::Error { doc: "d".to_owned(), detail: detail.to_owned(), local: Some(local), code: None }
}

fn saved_ack(seq: u64) -> ServerMsg {
    ServerMsg::SavedAck { doc: "d".to_owned(), seq }
}

fn relay_error(detail: &str) -> ServerMsg {
    ServerMsg::Error { doc: "d".to_owned(), detail: detail.to_owned(), local: None, code: None }
}

/// 1b: a checkpoint needs a ready replica and waits for the join's catch-up, ordered or not,
/// because the catch-up may still replace the replica it would snapshot.
#[test]
fn a_checkpoint_waits_for_a_ready_replica_and_the_catch_up() {
    let mut other = replica(&sample(), 7);
    let p4 = serde_json::to_value(TextPayload::new(&other.insert_text(1, 0, "o", &Map::new()).unwrap())).unwrap();

    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    assert!(live.checkpoint().is_err(), "no id space yet");

    // An ordered replay of 4..=5: refused until its first op lands.
    hub.0.borrow_mut().inbox.push(joined_at("c", 3, 3, Some(5)));
    live.pump();
    assert!(live.checkpoint().is_err(), "the replay has not arrived");
    hub.0.borrow_mut().inbox.push(op(4, "other", Some(1), p4));
    live.pump();
    live.checkpoint().unwrap();

    // The fallback: refused until the text ops land.
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(fallback_join("c", 3, 5));
    live.pump();
    assert!(live.checkpoint().is_err(), "the text ops have not arrived");
    hub.0.borrow_mut().inbox.push(text_ops(json!([wire(&other.state())]), false));
    live.pump();
    live.checkpoint().unwrap();
}

/// 1a: a checkpoint in flight when the catch-up resets the replica onto the room's snapshot
/// was taken of the file, not of the room. It is dropped as raced and never reported saved.
#[test]
fn a_reset_drops_the_checkpoint_in_flight() {
    let mut room = replica(&sample(), 9);
    room.insert_text(2, 0, "¡", &Map::new()).unwrap();

    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    // An older relay, which does not say whether text ops follow.
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, None));
    live.pump();
    live.checkpoint().unwrap();
    let (local, params) = last_submit(&hub);

    hub.0.borrow_mut().inbox.push(text_ops(json!([wire(&room.state())]), true));
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Reset)), "{work:?}");
    assert!(work.iter().any(|w| matches!(w, Work::CheckpointRaced)), "{work:?}");

    hub.0.borrow_mut().inbox.push(op(1, "alice", Some(local), params));
    let work = live.pump();
    assert!(!work.iter().any(|w| matches!(w, Work::Checkpointed { .. })), "{work:?}");
    assert!(!hub.0.borrow().sent.iter().any(|m| matches!(m, ClientMsg::Saved { .. })), "{:?}", hub.0.borrow().sent);
    assert_same(&room, live.doc().unwrap(), &whole(&room));
}

/// 2: on the fallback path a live op can reach the socket before the text ops. It must not use
/// up their restart, and it must survive the restart.
#[test]
fn a_live_op_before_the_fallback_text_ops_keeps_the_restart() {
    let mut worker = replica(&sample(), 9);
    let mut peer = replica(&sample(), 2);
    let u1 = peer.insert_text(2, 0, "¡", &Map::new()).unwrap();
    worker.apply(&u1).unwrap();
    let saved_file = file_of(&worker);
    let snap = worker.state();
    let live_edit = serde_json::to_value(TextPayload::new(&peer.insert_text(4, 0, ">", &Map::new()).unwrap())).unwrap();
    for up in bytes_of(&live_edit) {
        worker.apply(&up).unwrap();
    }
    for early_seed in [true, false] {
        let hub = FakeHub::default();
        let mut live = Live::new(hub.clone(), "d");
        if early_seed {
            live.seed(&saved_file).unwrap();
        }
        hub.0.borrow_mut().inbox.extend([fallback_join("c", 3, 5), op(6, "peer", Some(2), live_edit.clone()), text_ops(json!([wire(&snap)]), true)]);
        let work = live.pump();
        if early_seed {
            assert!(work.iter().any(|w| matches!(w, Work::Reset)), "{work:?}");
        } else {
            live.seed(&saved_file).unwrap();
        }
        assert_eq!(live.doc().unwrap().block(2).unwrap().text, "¡naïve 😀 text", "early seed {early_seed}");
        assert_eq!(live.doc().unwrap().block(4).unwrap().text, ">last paragraph", "early seed {early_seed}");
        assert_same(&worker, live.doc().unwrap(), &whole(&worker));
    }
}

/// 3: a refusal a retry cannot change is reported apart from a race, so a retrying app stops.
/// Only a refusal tied to the checkpoint counts: by the snapshot submit's counter, or by the
/// save report's sequence.
#[test]
fn a_correlated_refusal_maps_to_refused_or_raced() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();
    let mut seq = 0;
    for (detail, permanent) in [
        ("that change is 99999999 bytes, more than this relay will carry", true),
        ("read-only access to this file", true),
        ("relay could not order that edit", false),
    ] {
        for at_save in [false, true] {
            live.checkpoint().unwrap();
            let (local, params) = last_submit(&hub);
            let refusal = if at_save {
                seq += 1;
                hub.0.borrow_mut().inbox.push(op(seq, "alice", Some(local), params));
                live.pump();
                ServerMsg::SavedRefused { doc: "d".to_owned(), seq: seq - 1, reason: detail.to_owned() }
            } else {
                submit_refused(local, detail)
            };
            hub.0.borrow_mut().inbox.push(refusal);
            let work = live.pump();
            let refused = work.iter().any(|w| matches!(w, Work::CheckpointRefused { reason } if reason == detail));
            let raced = work.iter().any(|w| matches!(w, Work::CheckpointRaced));
            assert_eq!((refused, raced), (permanent, !permanent), "{detail}, at save {at_save}: {work:?}");
        }
    }
}

/// Round 2b: `Checkpointed` means the relay moved its baseline, so it waits for `saved_ack`
/// for this save's own sequence.
#[test]
fn checkpointed_comes_only_after_the_ack() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 3, Some(3)));
    live.pump();
    live.checkpoint().unwrap();
    let (local, params) = last_submit(&hub);
    hub.0.borrow_mut().inbox.push(op(4, "alice", Some(local), params));
    let work = live.pump();
    assert!(!work.iter().any(|w| matches!(w, Work::Checkpointed { .. })), "{work:?}");
    assert!(matches!(hub.0.borrow_mut().sent.pop(), Some(ClientMsg::Saved { seq: 3, .. })));
    assert!(live.checkpoint().is_err(), "still in flight until the relay answers");
    hub.0.borrow_mut().inbox.push(saved_ack(2));
    let work = live.pump();
    assert!(work.is_empty(), "an ack for another save is not this one's: {work:?}");
    hub.0.borrow_mut().inbox.push(saved_ack(3));
    let work = live.pump();
    assert!(matches!(work.as_slice(), [Work::Checkpointed { seq: 4 }]), "{work:?}");
}

/// Round 2b: relay errors that do not name the checkpoint leave it pending, even when their
/// wording is permanent.
#[test]
fn an_unrelated_relay_error_leaves_the_checkpoint_pending() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();
    live.checkpoint().unwrap();
    let (local, params) = last_submit(&hub);
    hub.0.borrow_mut().inbox.extend([
        relay_error("read-only access to this file"),
        submit_refused(local + 7, "that change is 5 bytes, more than this relay will carry"),
        ServerMsg::SavedRefused { doc: "d".to_owned(), seq: 41, reason: "read-only access to this file".to_owned() },
    ]);
    let work = live.pump();
    assert!(!work.iter().any(|w| matches!(w, Work::CheckpointRaced | Work::CheckpointRefused { .. })), "{work:?}");
    hub.0.borrow_mut().inbox.extend([op(1, "alice", Some(local), params), saved_ack(0)]);
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Checkpointed { seq: 1 })), "{work:?}");
}

/// Round 2b: a relay too old to acknowledge a save never answers, so the checkpoint times out
/// as raced instead of blocking the next one for good.
#[test]
fn a_checkpoint_with_no_answer_times_out_as_raced() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();
    let state = live.checkpoint().unwrap();
    assert!(live.pump().is_empty(), "the default timeout has not passed");
    live.set_checkpoint_timeout(std::time::Duration::ZERO);
    // The snapshot's own transfer time still applies, so this waits that out.
    std::thread::sleep(craft_collab::transfer_time(state.len() * 2 + 1024));
    let work = live.pump();
    assert!(matches!(work.as_slice(), [Work::CheckpointRaced]), "{work:?}");
    live.checkpoint().unwrap();
    assert_eq!(craft_collab::CHECKPOINT_TIMEOUT, std::time::Duration::from_secs(10));
}

/// Reconnect round 6: a checkpoint's deadline grows with its snapshot, at the slow-link rate,
/// so a large one on a slow link is not reported as unanswered while it is still moving.
#[test]
fn a_large_checkpoint_is_given_its_transfer_time() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&[craft_collab::SeedBlock { id: 0, text: "ab".repeat(64 * 1024), attrs: Default::default(), runs: Vec::new() }]).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();
    live.set_checkpoint_timeout(std::time::Duration::ZERO);
    let state = live.checkpoint().unwrap();
    assert!(craft_collab::transfer_time(state.len()) >= std::time::Duration::from_secs(2), "{} bytes", state.len());
    assert!(live.pump().is_empty(), "timed out before its 128 KiB could move at the slow-link rate");
    assert_eq!(craft_collab::transfer_time(64 * 1024), std::time::Duration::from_secs(1));
}

/// 5: the relay can broadcast a live op to a socket before telling it that it joined. The op
/// is kept and handed over once, after the replay it follows.
#[test]
fn an_op_that_reaches_the_socket_before_joined_lands_once() {
    let mut s = craft_collab::Session::new("d");
    assert!(s.handle(op(3, "peer", Some(1), json!({}))).is_empty(), "nothing is applied before the join");
    let applies = |fx: &[craft_collab::Effect]| {
        fx.iter().filter_map(|e| if let craft_collab::Effect::Apply { seq, .. } = e { Some(*seq) } else { None }).collect::<Vec<_>>()
    };
    // The join is at 1 and replays 2; the early op 3 waits for it.
    assert!(applies(&s.handle(joined_at("me", 4, 1, Some(2)))).is_empty());
    assert_eq!(applies(&s.handle(op(2, "peer", Some(9), json!({})))), vec![2, 3]);
    // The broadcast copy of 3 arriving again is dropped.
    assert!(applies(&s.handle(op(3, "peer", Some(1), json!({})))).is_empty());

    // A join past the early op drops it: it is the replay's, or already in the file.
    let mut s = craft_collab::Session::new("d");
    s.handle(op(3, "peer", Some(1), json!({})));
    s.handle(op(5, "peer", Some(2), json!({})));
    assert_eq!(applies(&s.handle(joined_at("me", 4, 4, Some(4)))), vec![5]);
}

// ---- fix round 3 -------------------------------------------------------------------------

/// Round 3, 2: an older relay (no `replay_until`) that falls back to text ops does not say so.
/// The text ops end its catch-up: a checkpoint is allowed after them, and a peer's snapshot
/// next is an ordinary update even for a replica that typed.
#[test]
fn an_old_relay_fallback_ends_its_catch_up_with_the_text_ops() {
    let mut room = replica(&sample(), 7);
    let u = room.insert_text(1, 0, "r", &Map::new()).unwrap();
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.extend([joined_at("c", 3, 5, None), text_ops(json!([wire(&u)]), false)]);
    live.pump();
    live.checkpoint().unwrap();
    live.edit(|d| d.insert_text(4, 0, "mine ", &Map::new())).unwrap();
    let mut peer = replica(&sample(), 2);
    peer.apply(&u).unwrap();
    peer.insert_text(1, 0, "p", &Map::new()).unwrap();
    let snap = serde_json::to_value(TextPayload::snapshot(&peer.state())).unwrap();
    hub.0.borrow_mut().inbox.push(op(6, "peer", Some(1), snap));
    let work = live.pump();
    assert!(!work.iter().any(|w| matches!(w, Work::Reset | Work::TextIncomplete { .. })), "{work:?}");
    assert!(live.ready_to_type());
    assert_eq!(live.doc().unwrap().block(1).unwrap().text, "prTitle");
    assert_eq!(live.doc().unwrap().block(4).unwrap().text, "mine last paragraph");
}

/// Round 3, 3: a relay that says how large a snapshot it retains lets the saver refuse a
/// larger one itself, permanently, instead of sending what the transport would drop.
#[test]
fn a_snapshot_over_the_advertised_cap_is_refused_before_sending() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    let mut join = joined_at("alice", 1, 0, Some(0));
    if let ServerMsg::Joined { text_cap, .. } = &mut join {
        *text_cap = Some(64);
    }
    hub.0.borrow_mut().inbox.push(join);
    live.pump();
    let refused = live.checkpoint();
    assert!(matches!(&refused, Err(craft_collab::Error::Refused(reason)) if craft_collab::refusal_is_permanent(reason)), "{refused:?}");
    assert!(hub.0.borrow().sent.is_empty(), "nothing was sent");
    assert!(live.pump().is_empty(), "and nothing is pending");
}

/// Round 3, 4: the checkpoint hands back the state it snapshotted, and a document restored
/// from it is what the saver writes even when it typed since.
#[test]
fn checkpoint_returns_the_state_it_sent() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();
    live.edit(|d| d.insert_text(1, 0, "saved ", &Map::new())).unwrap();
    let state = live.checkpoint().unwrap();
    let (_, params) = last_submit(&hub);
    assert_eq!(bytes_of(&params), vec![state.clone()], "the bytes returned are the ones sent");
    live.edit(|d| d.insert_text(1, 0, "later ", &Map::new())).unwrap();
    let file = LiveDoc::from_state(&state).unwrap();
    assert_eq!(file.block(1).unwrap().text, "saved Title");
    assert_eq!(live.doc().unwrap().block(1).unwrap().text, "later saved Title");
}

/// Round 3, 5: the timeout is checked after the pump's messages, so an answer that arrived
/// is never beaten by the timer.
#[test]
fn an_answer_in_the_same_pump_beats_the_timeout() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();
    live.checkpoint().unwrap();
    let (local, params) = last_submit(&hub);
    live.set_checkpoint_timeout(std::time::Duration::ZERO);
    hub.0.borrow_mut().inbox.extend([op(1, "alice", Some(local), params), saved_ack(0)]);
    let work = live.pump();
    assert!(matches!(work.as_slice(), [Work::Checkpointed { seq: 1 }]), "{work:?}");
}

/// Round 3, 7: on the marked fallback a live op can arrive before the file is loaded and
/// before the text ops. Seeding applies it to the file replica the text ops then replace, so
/// it must survive the restart too.
#[test]
fn a_held_op_survives_a_restart_after_seeding() {
    let mut worker = replica(&sample(), 9);
    let mut peer = replica(&sample(), 2);
    let u1 = peer.insert_text(2, 0, "¡", &Map::new()).unwrap();
    worker.apply(&u1).unwrap();
    let saved_file = file_of(&worker);
    let snap = worker.state();
    let live_edit = serde_json::to_value(TextPayload::new(&peer.insert_text(4, 0, ">", &Map::new()).unwrap())).unwrap();
    for up in bytes_of(&live_edit) {
        worker.apply(&up).unwrap();
    }
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    hub.0.borrow_mut().inbox.extend([fallback_join("c", 3, 5), op(6, "peer", Some(2), live_edit)]);
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::SeedWanted { .. })), "{work:?}");
    live.seed(&saved_file).unwrap();
    hub.0.borrow_mut().inbox.push(text_ops(json!([wire(&snap)]), true));
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Reset)), "{work:?}");
    assert_eq!(live.doc().unwrap().block(4).unwrap().text, ">last paragraph");
    assert_same(&worker, live.doc().unwrap(), &whole(&worker));
}

// ---- fix round 4 -------------------------------------------------------------------------

/// Round 4, 2: an edit past the advertised cap would be dropped with the socket and resent on
/// every reconnect. It is refused once, permanently, and nothing goes out.
#[test]
fn an_edit_over_the_advertised_cap_is_refused_for_good() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    let mut join = joined_at("alice", 1, 0, Some(0));
    if let ServerMsg::Joined { text_cap, .. } = &mut join {
        *text_cap = Some(4096);
    }
    hub.0.borrow_mut().inbox.push(join);
    live.pump();
    live.edit(|d| d.insert_text(1, 0, "small", &Map::new())).unwrap();
    hub.0.borrow_mut().sent.clear();
    let big = "x".repeat(8192);
    let refused = live.edit(|d| d.insert_text(1, 0, &big, &Map::new()));
    assert!(matches!(&refused, Err(craft_collab::Error::Refused(reason)) if craft_collab::refusal_is_permanent(reason)), "{refused:?}");
    assert!(hub.0.borrow().sent.is_empty(), "nothing was sent");
    assert!(!live.ready_to_type(), "the replica holds an edit the room never saw");
    assert!(live.edit(|d| d.insert_text(1, 0, "y", &Map::new())).is_err());
}

// ---- fix round 5 -------------------------------------------------------------------------

fn text_piece(ops: Value, snap: bool, more: bool) -> ServerMsg {
    ServerMsg::TextOps { doc: "d".to_owned(), scope: DOC_SCOPE.to_owned(), ops, complete: true, snap, more }
}

fn with_cap(mut join: ServerMsg, cap: u64) -> ServerMsg {
    if let ServerMsg::Joined { text_cap, .. } = &mut join {
        *text_cap = Some(cap);
    }
    join
}

/// Round 5, 1: a catch-up in pieces is over only with its last piece. A link lost midway
/// leaves it unfinished, so the rejoin re-arms it: no checkpoint goes out until the whole
/// catch-up has arrived again, and the replica ends with every piece.
#[test]
fn a_catch_up_in_pieces_ends_only_with_the_last() {
    let mut worker = replica(&sample(), 9);
    let saved_file = file_of(&worker);
    let u1 = worker.insert_text(2, 0, "¡", &Map::new()).unwrap();
    let snap = worker.state();
    let u2 = worker.insert_text(4, 0, ">", &Map::new()).unwrap();
    let _ = u1;

    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&saved_file).unwrap();
    hub.0.borrow_mut().inbox.extend([fallback_join("c", 3, 5), text_piece(json!([wire(&snap)]), true, true)]);
    live.pump();
    assert!(live.checkpoint().is_err(), "the catch-up is not over");
    // The link drops before the last piece; the relay sends it all again on the rejoin.
    hub.0.borrow_mut().inbox.push(fallback_join("c", 3, 5));
    live.pump();
    assert!(live.checkpoint().is_err(), "the rejoin re-armed the unfinished catch-up");
    hub.0.borrow_mut().inbox.extend([text_piece(json!([wire(&snap)]), true, true), text_piece(json!([wire(&u2)]), false, false)]);
    live.pump();
    assert_same(&worker, live.doc().unwrap(), &whole(&worker));
    assert!(live.checkpoint().is_ok(), "the last piece ended the catch-up");
}

/// Round 5, 2: an incomplete scope is reported once per catch-up, not once per piece.
#[test]
fn an_incomplete_catch_up_is_reported_once() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    let incomplete =
        |more| ServerMsg::TextOps { doc: "d".to_owned(), scope: DOC_SCOPE.to_owned(), ops: json!([]), complete: false, snap: false, more };
    hub.0.borrow_mut().inbox.extend([fallback_join("c", 3, 5), incomplete(true), incomplete(true), incomplete(false)]);
    let work = live.pump();
    assert_eq!(work.iter().filter(|w| matches!(w, Work::TextIncomplete { .. })).count(), 1, "{work:?}");
}

/// Round 5, 4: the guard measures the serialized params, as the relay does, so an edit whose
/// ops fit the cap but whose submit does not is refused here rather than by the relay.
#[test]
fn the_edit_guard_measures_the_params_as_the_relay_does() {
    let measure = |cap: Option<u64>| {
        let hub = FakeHub::default();
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&sample()).unwrap();
        let join = joined_at("alice", 1, 0, Some(0));
        hub.0.borrow_mut().inbox.push(match cap {
            Some(cap) => with_cap(join, cap),
            None => join,
        });
        live.pump();
        let sent = live.edit(|d| d.insert_text(1, 0, &"x".repeat(300), &Map::new()));
        (sent, hub)
    };
    let (sent, hub) = measure(None);
    sent.unwrap();
    let (_, params) = last_submit(&hub);
    let params_bytes = serde_json::to_vec(&params).unwrap().len() as u64;
    let ops_bytes: u64 = params["ops"].as_array().unwrap().iter().map(|op| serde_json::to_vec(op).unwrap().len() as u64).sum();
    assert!(ops_bytes < params_bytes - 1);
    let (refused, hub) = measure(Some(params_bytes - 1));
    assert!(matches!(&refused, Err(craft_collab::Error::Refused(_))), "{refused:?}");
    assert!(hub.0.borrow().sent.iter().all(|m| !matches!(m, ClientMsg::Submit { .. })), "nothing was submitted");
    let (fits, _) = measure(Some(params_bytes));
    assert!(fits.is_ok(), "a submit at exactly the cap goes out");
}

/// Round 5, 4: a refused edit is not ignored. It already landed here and the room will never
/// have it, so the replica is broken and the app is told. A refusal of a checkpoint snapshot,
/// even a late one, is the checkpoint's and breaks nothing.
#[test]
fn a_refused_edit_breaks_the_replica_and_says_so() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();
    live.checkpoint().unwrap();
    let (snap_local, _) = last_submit(&hub);
    hub.0
        .borrow_mut()
        .inbox
        .extend([submit_refused(snap_local, "read-only access to this file"), submit_refused(snap_local, "read-only access to this file")]);
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::CheckpointRefused { .. })), "{work:?}");
    assert!(live.ready_to_type(), "a refused snapshot is not a lost edit");

    live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).unwrap();
    let (local, _) = last_submit(&hub);
    live.notices.clear();
    // A refusal a retry cannot change. A transient one keeps the edit instead (see
    // `a_transient_refusal_keeps_the_edit_and_sends_it_again`).
    hub.0.borrow_mut().inbox.push(submit_refused(local, "that change is 99 bytes, more than this relay will carry"));
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::TextIncomplete { .. })), "{work:?}");
    assert!(!live.ready_to_type());
    assert!(!live.notices.is_empty(), "the app is told");
    assert!(live.edit(|d| d.insert_text(1, 0, "y", &Map::new())).is_err());
}

// ---- reconnect ---------------------------------------------------------------------------

/// A rejoin sends again, as one merged update, every edit the relay never confirmed: a link
/// that drops can take submits with it. A confirmed edit is not sent again.
#[test]
fn a_rejoin_sends_the_unconfirmed_edits_again_merged() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
    live.pump();
    live.edit(|d| d.insert_text(1, 0, "kept ", &Map::new())).unwrap();
    let (confirmed_local, confirmed) = last_submit(&hub);
    live.edit(|d| d.insert_text(1, 0, "lost ", &Map::new())).unwrap();
    live.edit(|d| d.insert_text(1, 0, "also ", &Map::new())).unwrap();
    hub.0.borrow_mut().sent.clear();
    hub.0.borrow_mut().inbox.push(op(1, "alice", Some(confirmed_local), confirmed.clone()));
    live.pump();

    hub.0.borrow_mut().inbox.push(joined_at("alice", 4, 1, Some(1)));
    live.pump();
    let submits: Vec<Value> = hub
        .0
        .borrow()
        .sent
        .iter()
        .filter_map(|m| match m {
            ClientMsg::Submit { params, .. } => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(submits.len(), 1, "one merged resend: {submits:?}");
    // A replica that has only the confirmed edit gets the rest from the resend alone.
    let mut other = replica(&sample(), 7);
    for up in bytes_of(&confirmed).into_iter().chain(bytes_of(&submits[0])) {
        other.apply(&up).unwrap();
    }
    assert_eq!(other.block(1).unwrap().text, live.doc().unwrap().block(1).unwrap().text);
}

/// The relay refusing the connection itself reaches the app as a typed event.
#[test]
fn a_refused_connection_is_a_typed_event() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    hub.0.borrow_mut().inbox.push(ServerMsg::Error {
        doc: String::new(),
        detail: "token rejected".to_owned(),
        local: None,
        code: Some(craft_collab::REFUSED_AUTH.to_owned()),
    });
    let work = live.pump();
    assert!(matches!(work.as_slice(), [Work::ConnectionRefused { code, .. }] if code == craft_collab::REFUSED_AUTH), "{work:?}");
    hub.0.borrow_mut().inbox.push(ServerMsg::Error { doc: String::new(), detail: "busy".to_owned(), local: None, code: None });
    assert!(live.pump().is_empty(), "an ordinary error is not one");
}

/// The merged resend stays under the relay's cap: edits that fit one by one but not merged go
/// in more than one update, and the replica is not broken.
#[test]
fn a_resend_past_the_cap_goes_in_pieces() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(with_cap(joined_at("alice", 1, 0, Some(0)), 4096));
    live.pump();
    // A large paste, split so each piece fits the cap.
    for piece in ["a", "b", "c"] {
        live.edit(|d| d.insert_text(1, 0, &piece.repeat(2000), &Map::new())).unwrap();
    }
    hub.0.borrow_mut().sent.clear();
    hub.0.borrow_mut().inbox.push(with_cap(joined_at("alice", 4, 0, Some(0)), 4096));
    live.pump();
    assert!(live.ready_to_type(), "the resend broke the replica");
    let submits: Vec<Value> = hub
        .0
        .borrow()
        .sent
        .iter()
        .filter_map(|m| match m {
            ClientMsg::Submit { params, .. } => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert!(submits.len() >= 2, "{} submits", submits.len());
    for params in &submits {
        assert!(serde_json::to_vec(params).unwrap().len() <= 4096);
    }
    let mut other = replica(&sample(), 7);
    for params in &submits {
        for up in bytes_of(params) {
            other.apply(&up).unwrap();
        }
    }
    assert_eq!(other.block(1).unwrap().text, live.doc().unwrap().block(1).unwrap().text);
}

#[test]
fn applied_counts_only_ops_in_order() {
    let alice_hub = FakeHub::default();
    let bob_hub = FakeHub::default();
    let mut alice = Live::new(alice_hub.clone(), "d");
    let mut bob = Live::new(bob_hub.clone(), "d");
    alice.seed(&sample()).unwrap();
    bob.seed(&sample()).unwrap();
    alice_hub.0.borrow_mut().inbox.push(joined("alice", 1));
    bob_hub.0.borrow_mut().inbox.push(joined("bob", 2));
    alice.pump();
    bob.pump();
    let mut edits = Vec::new();
    for at in [0, 1] {
        alice.edit(|d| d.insert_text(4, at, "x", &Map::new())).unwrap();
        let Some(ClientMsg::Submit { params, .. }) = alice_hub.0.borrow_mut().sent.pop() else { panic!("no submit") };
        edits.push(params);
    }
    // The relay broadcast them out of order: 2 first, alone.
    bob_hub.0.borrow_mut().inbox.push(op(2, "alice", None, edits[1].clone()));
    bob.pump();
    assert_eq!(bob.applied(), 0, "op 2 is held until op 1 is in");
    bob_hub.0.borrow_mut().inbox.push(op(1, "alice", None, edits[0].clone()));
    bob.pump();
    assert_eq!(bob.applied(), 2);
}

// ---- final fix wave ------------------------------------------------------------------------

fn retry_refused(local: u64) -> ServerMsg {
    ServerMsg::Error {
        doc: "d".to_owned(),
        detail: "relay could not order that edit".to_owned(),
        local: Some(local),
        code: Some(craft_collab::RETRY.to_owned()),
    }
}

fn lost_join(peer: &str, space: u32, seq: u64) -> ServerMsg {
    let mut join = joined_at(peer, space, seq, Some(seq));
    if let ServerMsg::Joined { history_lost, .. } = &mut join {
        *history_lost = true;
    }
    join
}

fn submits(hub: &FakeHub) -> Vec<(u64, Value)> {
    hub.0
        .borrow()
        .sent
        .iter()
        .filter_map(|m| match m {
            ClientMsg::Submit { local, params, .. } => Some((*local, params.clone())),
            _ => None,
        })
        .collect()
}

/// I3: a submit the relay refused for now (`code: "retry"`, its bus did not answer) is kept,
/// not taken for a lost edit: the replica stays sound, and after a backoff the same update goes
/// out again under a new counter. Once that one is confirmed nothing more is sent. An older
/// relay's wording without a code is treated the same, because it is not one of the refusals a
/// retry cannot change.
#[test]
fn a_transient_refusal_keeps_the_edit_and_sends_it_again() {
    for coded in [true, false] {
        let hub = FakeHub::default();
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&sample()).unwrap();
        hub.0.borrow_mut().inbox.push(joined_at("alice", 1, 0, Some(0)));
        live.pump();
        live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).unwrap();
        let (local, params) = last_submit(&hub);
        let refusal = if coded { retry_refused(local) } else { submit_refused(local, "relay could not order that edit") };
        hub.0.borrow_mut().inbox.push(refusal);
        let work = live.pump();
        assert!(!work.iter().any(|w| matches!(w, Work::TextIncomplete { .. })), "{coded}: {work:?}");
        assert!(live.ready_to_type(), "{coded}: a transient refusal broke the replica");
        assert!(submits(&hub).is_empty(), "{coded}: sent again before the backoff");

        std::thread::sleep(std::time::Duration::from_millis(300));
        live.pump();
        let again = submits(&hub);
        assert_eq!(again.len(), 1, "{coded}: the refused edit was not sent again: {again:?}");
        let (resent, resent_params) = again[0].clone();
        assert_ne!(resent, local, "a new counter");
        assert_eq!(resent_params["ops"], params["ops"], "{coded}: the same update");
        hub.0.borrow_mut().sent.clear();

        hub.0.borrow_mut().inbox.push(op(1, "alice", Some(resent), resent_params));
        live.pump();
        std::thread::sleep(std::time::Duration::from_millis(600));
        live.pump();
        assert!(submits(&hub).is_empty(), "{coded}: a confirmed edit went out again");
        assert!(live.ready_to_type());
    }
}

/// C1: a replica that joins a room whose history the relay lost, holding nothing from before,
/// is told once and does not take the file as current: no edit, undo or checkpoint, and the
/// room's ordinary updates are not laid over the file. The room's next snapshot restarts it,
/// and it edits from there.
#[test]
fn a_fresh_replica_waits_for_the_snapshot_after_a_lost_history() {
    // The room's real state: the file plus edits the joiner's file does not have, made by a
    // replica whose items the file's seed does not share.
    let mut room = replica(&sample(), 2);
    room.insert_text(1, 0, "Live ", &Map::new()).unwrap();
    let saved_file = file_of(&room);
    let edit = room.insert_text(4, 0, "unsaved ", &Map::new()).unwrap();
    let snap = serde_json::to_value(TextPayload::snapshot(&room.state())).unwrap();

    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    hub.0.borrow_mut().inbox.push(lost_join("fresh", 9, 40));
    let work = live.pump();
    assert_eq!(work.iter().filter(|w| matches!(w, Work::HistoryLost)).count(), 1, "{work:?}");
    assert!(live.awaiting_snapshot());
    live.seed(&saved_file).unwrap();
    assert!(!live.ready_to_type());
    let refused = live.edit(|d| d.insert_text(1, 0, "x", &Map::new()));
    assert!(refused.is_err_and(|e| e.to_string().contains("lost its history")), "the reason is clear");
    assert!(live.undo().is_err());
    assert!(live.checkpoint().is_err());

    // An ordinary update, ordered before the snapshot, is left to it.
    hub.0.borrow_mut().inbox.push(op(41, "worker", Some(7), serde_json::to_value(TextPayload::new(&edit)).unwrap()));
    let work = live.pump();
    assert!(!work.iter().any(|w| matches!(w, Work::Doc { .. } | Work::HistoryLost)), "{work:?}");
    assert!(submits(&hub).is_empty(), "nothing went out while waiting");

    hub.0.borrow_mut().inbox.push(op(42, "worker", Some(8), snap));
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Reset)), "{work:?}");
    assert!(!live.awaiting_snapshot());
    assert!(live.ready_to_type());
    assert_same(live.doc().unwrap(), &room, &whole(&room));

    live.edit(|d| d.insert_text(1, 0, ">", &Map::new())).unwrap();
    let (_, params) = last_submit(&hub);
    room.apply(&bytes_of(&params)[0]).unwrap();
    assert_same(live.doc().unwrap(), &room, &whole(&room));
    assert_eq!(room.block(1).unwrap().text, ">Live Title");
}

/// C1: the snapshot can arrive before the app loaded the file. Then the file is never seeded:
/// the seed restores the snapshot.
#[test]
fn a_lost_history_snapshot_before_the_seed_is_what_the_seed_restores() {
    let mut room = replica(&sample(), 2);
    room.insert_text(1, 0, "Live ", &Map::new()).unwrap();
    let snap = serde_json::to_value(TextPayload::snapshot(&room.state())).unwrap();
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    hub.0.borrow_mut().inbox.extend([lost_join("fresh", 9, 40), op(41, "worker", Some(1), snap)]);
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::SeedWanted { .. })), "{work:?}");
    live.seed(&sample()).unwrap();
    assert!(!live.awaiting_snapshot());
    assert!(live.ready_to_type());
    assert_same(live.doc().unwrap(), &room, &whole(&room));
}

/// C1: a replica that settled on the room's history before the relay lost it keeps working
/// when it rejoins: it is told (so the service saver checkpoints at once) and waits for
/// nothing.
#[test]
fn a_settled_replica_is_told_of_a_lost_history_and_keeps_working() {
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.push(joined_at("worker", 1, 0, Some(0)));
    live.pump();
    live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).unwrap();
    let (local, params) = last_submit(&hub);
    hub.0.borrow_mut().inbox.push(op(1, "worker", Some(local), params));
    live.pump();

    hub.0.borrow_mut().inbox.push(lost_join("worker", 5, 1));
    let work = live.pump();
    assert_eq!(work.iter().filter(|w| matches!(w, Work::HistoryLost)).count(), 1, "{work:?}");
    assert!(!live.awaiting_snapshot());
    assert!(live.ready_to_type());
    live.checkpoint().expect("the saver checkpoints at once");

    // Told once per join that lost it, not per message.
    let work = live.pump();
    assert!(!work.iter().any(|w| matches!(w, Work::HistoryLost)), "{work:?}");
}
