//! End-to-end tests of the replica schema pin: the room keeps the schema its first join named,
//! a join naming another is refused with `code: "schema"`, and a join naming none (a client from
//! before the field) is let in and pins nothing. Ignored by default; they need a throwaway
//! Valkey in `REVIEW_VALKEY_URL`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use craft_collab::native::NativeHub;
use craft_collab::{ClientMsg, Hub, ServerMsg};

use crate::e2e_reconnect::{now, start, ticket, unique_room};

/// What a hub hears until it is joined or refused, within ten seconds.
async fn answer(hub: &mut NativeHub) -> Vec<ServerMsg> {
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        seen.extend(hub.poll());
        if seen.iter().any(|m| matches!(m, ServerMsg::Joined { .. }) || m.refuses_connection().is_some()) {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("neither joined nor refused: {seen:?}");
}

fn joined(seen: &[ServerMsg]) -> bool {
    seen.iter().any(|m| matches!(m, ServerMsg::Joined { .. }))
}

fn refused_for_schema(seen: &[ServerMsg]) -> bool {
    seen.iter().any(|m| m.refuses_connection() == Some(craft_collab::REFUSED_SCHEMA))
}

async fn connect(url: &str, room: &str, uid: &str, schema: Option<&str>) -> NativeHub {
    NativeHub::connect_as(url, &ticket(room, uid, now()), None, schema).await.expect("connects")
}

/// The first join pins the room; another schema is refused and its hub stops; the same schema
/// and no schema are let in; the pin does not leak into another room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_room_keeps_the_schema_its_first_join_named() {
    let Some(h) = start().await else { return };
    let room = unique_room("schema-pin");
    let mut grid = connect(&h.url, &room, "alice", Some("gridcraft/1")).await;
    assert!(joined(&answer(&mut grid).await), "the first join was not let in");

    let mut word = connect(&h.url, &room, "bob", Some("wordcraft/1")).await;
    let seen = answer(&mut word).await;
    assert!(refused_for_schema(&seen) && !joined(&seen), "another schema was let into a pinned room: {seen:?}");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!word.connected(), "a refused hub kept its link");

    let mut again = connect(&h.url, &room, "carol", Some("gridcraft/1")).await;
    assert!(joined(&answer(&mut again).await), "the pinned schema was refused");
    let mut legacy = connect(&h.url, &room, "dora", None).await;
    assert!(joined(&answer(&mut legacy).await), "a join naming no schema was refused");
    assert_eq!(h.relay.rooms.lock().await.get(&room).and_then(crate::room::Room::schema), Some("gridcraft/1"));

    let other = unique_room("schema-other");
    let mut elsewhere = connect(&h.url, &other, "bob", Some("wordcraft/1")).await;
    assert!(joined(&answer(&mut elsewhere).await), "a pin reached another room");
}

/// A room that a join naming no schema opened is not pinned by it: the first join that names
/// one pins it, and the next naming another is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_join_without_a_schema_pins_nothing() {
    let Some(h) = start().await else { return };
    let room = unique_room("schema-legacy");
    let mut legacy = connect(&h.url, &room, "alice", None).await;
    assert!(joined(&answer(&mut legacy).await));
    assert_eq!(h.relay.rooms.lock().await.get(&room).and_then(crate::room::Room::schema), None);

    let mut deck = connect(&h.url, &room, "bob", Some("deckcraft/1")).await;
    assert!(joined(&answer(&mut deck).await), "the first named schema was refused");
    let mut grid = connect(&h.url, &room, "carol", Some("gridcraft/1")).await;
    assert!(refused_for_schema(&answer(&mut grid).await), "a second schema was let in");
}

/// A malformed schema name is refused before the ticket is even read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_malformed_schema_is_refused() {
    let Some(h) = start().await else { return };
    let room = unique_room("schema-bad");
    let long = "x".repeat(65);
    for bad in ["", "grid craft", long.as_str()] {
        let mut hub = connect(&h.url, &room, "alice", Some(bad)).await;
        let seen = answer(&mut hub).await;
        assert!(refused_for_schema(&seen) && !joined(&seen), "{bad:?} was let in: {seen:?}");
    }
    assert!(h.relay.rooms.lock().await.get(&room).is_none(), "a refused join made a room");
}

fn text_op(doc: &str, local: u64, op: &str) -> ClientMsg {
    ClientMsg::Submit {
        doc: doc.to_owned(),
        local,
        command: craft_collab::CRDT_TEXT.to_owned(),
        params: serde_json::json!({ "scope": craft_collab::DOC_SCOPE, "ops": [op] }),
        scope: None,
        fence: None,
    }
}

/// What a hub hears in `ms` milliseconds.
async fn listen(hub: &mut NativeHub, ms: u64) -> Vec<ServerMsg> {
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    while tokio::time::Instant::now() < deadline {
        seen.extend(hub.poll());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    seen
}

fn ops(seen: &[ServerMsg]) -> usize {
    seen.iter().filter(|m| matches!(m, ServerMsg::Op { command, .. } if command == craft_collab::CRDT_TEXT)).count()
}

/// In a room pinned to a yrs schema a junk op is refused with `code: "invalid"`, takes no
/// sequence number, reaches nobody and is not in a later joiner's replay; a valid one is
/// ordered as before. A room with no schema is not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_junk_op_is_refused_before_it_is_ordered() {
    junk_op_round(Some("gridcraft/1")).await;
}

/// The same with a room nobody named a schema for, which is how WordCraft-web joins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_junk_op_is_refused_in_a_room_with_no_schema() {
    junk_op_round(None).await;
}

async fn junk_op_round(schema: Option<&str>) {
    use base64::Engine as _;
    let wire = |b: &[u8]| base64::engine::general_purpose::STANDARD.encode(b);
    let Some(h) = start().await else { return };
    let room = unique_room("junk-op-round");
    let mut alice = connect(&h.url, &room, "alice", schema).await;
    let seen = answer(&mut alice).await;
    let doc = seen.iter().find_map(|m| matches!(m, ServerMsg::Joined { .. }).then(|| m.doc().to_owned())).expect("joined");
    let mut bob = connect(&h.url, &room, "bob", schema).await;
    answer(&mut bob).await;
    let before = h.relay.rooms.lock().await.get(&room).map_or(0, crate::room::Room::seq);

    // An empty update: [0 clients, 0 delete-set clients] is valid.
    let valid = wire(&[0, 0]);
    let overflow = wire(&[0, 1, 1, 1, 0xff, 0xff, 0xff, 0xff, 0x0f, 2]);
    alice.send(text_op(&doc, 1, &valid)).unwrap();
    alice.send(text_op(&doc, 2, &wire(&[0xff, 0x01, 0x02]))).unwrap();
    alice.send(text_op(&doc, 3, &overflow)).unwrap();
    let heard_a = listen(&mut alice, 1500).await;
    let heard_b = listen(&mut bob, 100).await;

    let mut refused: Vec<u64> = heard_a
        .iter()
        .filter_map(|m| match m {
            ServerMsg::Error { local: Some(l), code: Some(c), .. } if c == "invalid" => Some(*l),
            _ => None,
        })
        .collect();
    refused.sort_unstable();
    assert_eq!(refused, vec![2, 3], "{heard_a:?}");
    assert_eq!(ops(&heard_a), 1, "only the valid op was ordered: {heard_a:?}");
    assert_eq!(ops(&heard_b), 1, "bob heard only the valid op: {heard_b:?}");
    let after = h.relay.rooms.lock().await.get(&room).map_or(0, crate::room::Room::seq);
    assert_eq!(after - before, 1, "junk burned no sequence number");

    let mut carol = connect(&h.url, &room, "carol", schema).await;
    let mut replay = answer(&mut carol).await;
    replay.extend(listen(&mut carol, 500).await);
    let replayed: Vec<String> = replay
        .iter()
        .filter_map(|m| match m {
            ServerMsg::TextOps { ops, .. } => Some(ops.to_string()),
            _ => None,
        })
        .collect();
    let joined_text = format!("{replayed:?} {replay:?}");
    assert!(!joined_text.contains("/wEC") && !joined_text.contains(&overflow), "junk is in the replay: {joined_text}");
}
