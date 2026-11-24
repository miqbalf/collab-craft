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

/// An op the gate takes alone but that breaks yrs in the document it lands in is refused by the
/// relay's shadow copy, takes no sequence number and reaches nobody. The ops before and after
/// it are ordered. With a schema the copy types the roots too, so a string straight in the
/// WordCraft body (an array) is refused; with none, only what the ops themselves type is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_the_shadow_refuses_an_op_that_does_not_fit_the_document() {
    shadow_round(Some("wordcraft/1")).await;
    shadow_round(None).await;
}

async fn shadow_round(schema: Option<&str>) {
    use base64::Engine as _;
    let wire = |b: &[u8]| base64::engine::general_purpose::STANDARD.encode(b);
    let Some(h) = start().await else { return };
    let room = unique_room("shadow-round");
    let mut alice = connect(&h.url, &room, "alice", schema).await;
    let seen = answer(&mut alice).await;
    let doc = seen.iter().find_map(|m| matches!(m, ServerMsg::Joined { .. }).then(|| m.doc().to_owned())).expect("joined");
    let mut bob = connect(&h.url, &room, "bob", schema).await;
    answer(&mut bob).await;
    let before = h.relay.rooms.lock().await.get(&room).map_or(0, crate::room::Room::seq);

    // Client 50 puts an array in the body, then a string in that array (yrs cannot read it back:
    // its array iterator spins for ever), then a string straight in the body.
    let array = [1, 1, 50, 0, 0x07, 1, 4, b'b', b'o', b'd', b'y', 0, 0];
    let string_in_array = [1, 1, 50, 1, 0x04, 0, 50, 0, 2, 0xc3, 0xa9, 0];
    let string_in_body = [1, 1, 50, 1, 0x04, 1, 4, b'b', b'o', b'd', b'y', 2, 0xc3, 0xa9, 0];
    for op in [&array[..], &string_in_array, &string_in_body] {
        assert!(craft_collab::update_is_sound(op), "the gate alone takes {op:?}");
    }
    // One at a time: submits sent together are ordered by whichever handler takes the gate
    // first, and an op that arrives before the one it names waits at the copy, unjudged.
    let mut heard_a = Vec::new();
    for (local, op) in [(1, &array[..]), (2, &string_in_array), (3, &string_in_body)] {
        alice.send(text_op(&doc, local, &wire(op))).unwrap();
        heard_a.extend(listen(&mut alice, 500).await);
    }
    let heard_b = listen(&mut bob, 100).await;

    let refused: Vec<u64> = heard_a
        .iter()
        .filter_map(|m| match m {
            ServerMsg::Error { local: Some(l), code: Some(c), .. } if c == "invalid" => Some(*l),
            _ => None,
        })
        .collect();
    let (want_refused, want_ops) = if schema.is_some() { (vec![2, 3], 1) } else { (vec![2], 2) };
    assert_eq!(refused, want_refused, "{heard_a:?}");
    assert_eq!(ops(&heard_a), want_ops, "{heard_a:?}");
    assert_eq!(ops(&heard_b), want_ops, "{heard_b:?}");
    let after = h.relay.rooms.lock().await.get(&room).map_or(0, crate::room::Room::seq);
    assert_eq!(after - before, want_ops as u64, "a refused op burned a sequence number");
}

/// C14: a room joined as a GridCraft build (`"gridcraft/1_abc"`, GridCraft ruling R1) gets the
/// typed GridCraft copy: a string straight in the `sheets` array, which GridCraft's replicas
/// cannot read back, is refused as invalid and orders nothing. A room of another version
/// (`"gridcraft/10_x"`) gets the untyped copy, which takes it, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_gridcraft_build_gets_the_typed_copy() {
    assert!(sheets_round(Some("gridcraft/1_abc")).await, "gridcraft/1_abc took a string in sheets");
    assert!(sheets_round(Some("gridcraft/1")).await, "gridcraft/1 took a string in sheets");
    assert!(!sheets_round(Some("gridcraft/10_x")).await, "gridcraft/10_x was typed as gridcraft/1");
}

/// Join a fresh room as `schema` and send a string straight in `sheets`; whether the relay
/// refused it as invalid (and ordered nothing), or ordered it.
async fn sheets_round(schema: Option<&str>) -> bool {
    use base64::Engine as _;
    let Some(h) = start().await else { return false };
    let room = unique_room("grid-build");
    let mut alice = connect(&h.url, &room, "alice", schema).await;
    let seen = answer(&mut alice).await;
    let doc = seen.iter().find_map(|m| matches!(m, ServerMsg::Joined { .. }).then(|| m.doc().to_owned())).expect("joined");
    let string_in_sheets = [1, 1, 50, 0, 0x04, 1, 6, b's', b'h', b'e', b'e', b't', b's', 2, 0xc3, 0xa9, 0];
    assert!(craft_collab::update_is_sound(&string_in_sheets), "the gate alone takes it");
    let before = h.relay.rooms.lock().await.get(&room).map_or(0, crate::room::Room::seq);
    alice.send(text_op(&doc, 1, &base64::engine::general_purpose::STANDARD.encode(string_in_sheets))).unwrap();
    let heard = listen(&mut alice, 1500).await;
    let after = h.relay.rooms.lock().await.get(&room).map_or(0, crate::room::Room::seq);
    let refused = heard.iter().any(|m| matches!(m, ServerMsg::Error { local: Some(1), code: Some(c), .. } if c == "invalid"));
    if refused {
        assert_eq!(after, before, "a refused op burned a sequence number: {heard:?}");
    } else {
        assert_eq!(after, before + 1, "neither refused nor ordered: {heard:?}");
    }
    refused
}

/// The copy holds only what the room records, and judges the document scope whatever other
/// scopes a peer invents. An editor's op that the room refuses after the copy (here: past the
/// room's size limit, which only the room reads, with the padding outside the ops) once stayed
/// in the copy, so a second op with the same ids and content that does not fit was taken as
/// already there and ordered; and 64 invented scope names once filled the copy slots and turned
/// the shadow off for the room.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_the_shadow_holds_only_what_the_room_records() {
    use base64::Engine as _;
    let wire = |b: &[u8]| base64::engine::general_purpose::STANDARD.encode(b);
    // A text cap of 8 MiB: the room refuses params past 8 MiB, and the transport carries 64 KiB
    // more, so an op can reach the copy and then be refused by the room.
    let cap = crate::room::HISTORY_BYTES;
    let Some(h) = crate::e2e_reconnect::start_capped(None, cap).await else { return };
    let room = unique_room("shadow-holds");
    let schema = Some("wordcraft/1");
    let mut alice = connect(&h.url, &room, "alice", schema).await;
    let seen = answer(&mut alice).await;
    let doc = seen.iter().find_map(|m| matches!(m, ServerMsg::Joined { .. }).then(|| m.doc().to_owned())).expect("joined");

    // The same id (client 50, clock 0): an array in the body, then a string in the body.
    let array = [1, 1, 50, 0, 0x07, 1, 4, b'b', b'o', b'd', b'y', 0, 0];
    let string_in_body = [1, 1, 50, 0, 0x04, 1, 4, b'b', b'o', b'd', b'y', 2, 0xc3, 0xa9, 0];
    let padded = craft_collab::ClientMsg::Submit {
        doc: doc.clone(),
        local: 1,
        command: craft_collab::CRDT_TEXT.to_owned(),
        params: serde_json::json!({ "scope": craft_collab::DOC_SCOPE, "ops": [wire(&array)], "pad": "x".repeat(cap + 1024) }),
        scope: None,
        fence: None,
    };
    alice.send(padded).unwrap();
    let heard = listen(&mut alice, 3000).await;
    let too_large =
        heard.iter().any(|m| matches!(m, ServerMsg::Error { local: Some(1), detail, .. } if detail.contains("more than this relay will carry")));
    assert!(too_large, "the padded op was not refused by the room: {heard:?}");

    // Ops in 70 invented scopes (each its own client), then the string on the document scope.
    let mut heard_a = Vec::new();
    for n in 0..70_u8 {
        let op = [1, 1, 20 + n, 0, 0x07, 1, 4, b'b', b'o', b'd', b'y', 0, 0];
        let params = serde_json::json!({ "scope": format!("junk-{n}"), "ops": [wire(&op)] });
        let submit = craft_collab::ClientMsg::Submit {
            doc: doc.clone(),
            local: 100 + u64::from(n),
            command: craft_collab::CRDT_TEXT.to_owned(),
            params,
            scope: None,
            fence: None,
        };
        alice.send(submit).unwrap();
    }
    heard_a.extend(listen(&mut alice, 1500).await);
    alice.send(text_op(&doc, 2, &wire(&string_in_body))).unwrap();
    heard_a.extend(listen(&mut alice, 500).await);
    let refused = heard_a.iter().any(|m| matches!(m, ServerMsg::Error { local: Some(2), code: Some(c), .. } if c == "invalid"));
    assert!(refused, "a string in the body was ordered: {heard_a:?}");
    // The 70 junk ops are ordered; the string is not.
    assert_eq!(ops(&heard_a), 70, "{heard_a:?}");
}
