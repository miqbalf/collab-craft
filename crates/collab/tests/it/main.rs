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
    let mut d = LiveDoc::seeded(blocks);
    d.set_client(space);
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
    let a = LiveDoc::seeded(&sample());
    let b = LiveDoc::seeded(&sample());
    assert_eq!(a.state(), b.state());
    assert_eq!(a.order(), vec![1, 2, 3, 4]);
}

#[test]
fn seeded_blocks_round_trip() {
    let d = LiveDoc::seeded(&sample());
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
    let d = LiveDoc::seeded(&[b]);
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
    let mut d = LiveDoc::seeded(&sample());
    assert_eq!(d.insert_text(1, 0, "x", &Map::new()), Err(LiveError::NoClient));
    // Space 0 is the seed's, so it is ignored rather than adopted.
    d.set_client(0);
    assert_eq!(d.insert_text(1, 0, "x", &Map::new()), Err(LiveError::NoClient));
    assert_eq!(d.state(), LiveDoc::seeded(&sample()).state());
    d.set_client(4);
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

/// The relay keeps every `crdt.text` update since the file the room opened. A peer that seeds
/// that same file later and applies the retained updates reaches the live replicas.
#[test]
fn late_joiner_after_checkpoint_converges() {
    let checkpoint = sample();
    let mut a = replica(&checkpoint, 1);
    let mut b = replica(&checkpoint, 2);
    let mut retained: Vec<Value> = Vec::new();
    let mut keep = |up: Vec<u8>| -> Vec<u8> {
        let params = serde_json::to_value(TextPayload::new(&up)).unwrap();
        retained.extend(params["ops"].as_array().unwrap().iter().cloned());
        up
    };

    let u1 = keep(a.insert_text(2, 0, "¡", &Map::new()).unwrap());
    let u2 = keep(b.split(4, 4, 200).unwrap());
    b.apply(&u1).unwrap();
    a.apply(&u2).unwrap();
    let u3 = keep(a.format(200, 0, 3, &attrs(json!({"u": true}))).unwrap());
    let u4 = keep(b.remove_block(3).unwrap());
    b.apply(&u3).unwrap();
    a.apply(&u4).unwrap();
    let u5 = keep(b.merge(1, 2).unwrap());
    a.apply(&u5).unwrap();

    let mut c = replica(&checkpoint, 3);
    for op in &retained {
        c.apply(&update_from_wire(op.as_str().unwrap()).unwrap()).unwrap();
    }
    let ids = ids_of(&[&a, &b, &c]);
    assert_same(&a, &b, &ids);
    assert_same(&a, &c, &ids);

    // And the joiner writes in its own space without colliding with anybody.
    let u6 = c.insert_text(1, 0, ">", &Map::new()).unwrap();
    a.apply(&u6).unwrap();
    b.apply(&u6).unwrap();
    assert_same(&a, &c, &ids);
    assert_same(&b, &c, &ids);
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
    (0u8..8, any::<u16>(), any::<u16>(), any::<u16>())
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
    ServerMsg::Joined { doc: "d".to_owned(), peer: peer.to_owned(), seq: 0, space, roster: Vec::new(), locks: Vec::new() }
}

#[test]
fn live_sends_yrs_updates_as_crdt_text() {
    let alice_hub = FakeHub::default();
    let bob_hub = FakeHub::default();
    let mut alice = Live::new(alice_hub.clone(), "d");
    let mut bob = Live::new(bob_hub.clone(), "d");
    alice.seed(&sample());
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
    });
    let work = bob.pump();
    assert!(matches!(work.as_slice(), [Work::SeedWanted { .. }]), "{work:?}");
    let changes = bob.seed(&sample());
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
