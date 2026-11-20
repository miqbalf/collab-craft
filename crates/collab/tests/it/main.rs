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
    ServerMsg::Joined { doc: "d".to_owned(), peer: peer.to_owned(), seq: 0, space, roster: Vec::new(), locks: Vec::new(), replay_until: None }
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
    assert!(work.iter().any(|w| matches!(w, Work::Checkpointed { seq: 2 })), "{work:?}");
    assert!(matches!(alice_hub.0.borrow_mut().sent.pop(), Some(ClientMsg::Saved { seq: 1, .. })));

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
    ServerMsg::Joined { doc: "d".to_owned(), peer: peer.to_owned(), seq, space, roster: Vec::new(), locks: Vec::new(), replay_until }
}

fn wire(up: &[u8]) -> Value {
    Value::String(craft_collab::update_to_wire(up))
}

fn text_ops(ops: Value, snap: bool) -> ServerMsg {
    ServerMsg::TextOps { doc: "d".to_owned(), scope: DOC_SCOPE.to_owned(), ops, complete: true, snap }
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
    hub.0.borrow_mut().inbox.extend([joined_at("c", 3, 5, Some(5)), text_ops(ops.clone(), true)]);
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::SeedWanted { .. })), "{work:?}");
    live.seed(&sample()).unwrap();
    assert_eq!(live.doc().unwrap().block(1).unwrap().text, "21Title");
    assert_same(&room, live.doc().unwrap(), &whole(&room));

    // After the file was loaded (and not edited): reset onto the snapshot, then the tail.
    let hub = FakeHub::default();
    let mut live = Live::new(hub.clone(), "d");
    live.seed(&sample()).unwrap();
    hub.0.borrow_mut().inbox.extend([joined_at("c", 3, 5, Some(5)), text_ops(ops, true)]);
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

    for lost in [
        ServerMsg::Error { doc: "d".to_owned(), detail: "that change is too large".to_owned() },
        ServerMsg::Snapshot { doc: "d".to_owned(), seq: 0, document: Value::Null },
        joined_at("alice", 1, 0, Some(0)),
    ] {
        live.checkpoint().unwrap();
        hub.0.borrow_mut().sent.clear();
        hub.0.borrow_mut().inbox.push(lost.clone());
        let work = live.pump();
        assert!(work.iter().any(|w| matches!(w, Work::CheckpointRaced)), "{lost:?}: {work:?}");
    }

    live.checkpoint().unwrap();
    let (local, params) = last_submit(&hub);
    hub.0.borrow_mut().inbox.push(op(1, "alice", Some(local), params));
    let work = live.pump();
    assert!(work.iter().any(|w| matches!(w, Work::Checkpointed { seq: 1 })), "{work:?}");
    assert!(matches!(hub.0.borrow_mut().sent.pop(), Some(ClientMsg::Saved { seq: 0, .. })));
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
    hub.0.borrow_mut().inbox.extend([joined_at("c", 3, 5, Some(5)), text_ops(json!([not_a_state]), true)]);
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
