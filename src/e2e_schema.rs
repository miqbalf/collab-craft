//! End-to-end tests of the replica schema pin: the room keeps the schema its first join named,
//! a join naming another is refused with `code: "schema"`, and a join naming none (a client from
//! before the field) is let in and pins nothing. Ignored by default; they need a throwaway
//! Valkey in `REVIEW_VALKEY_URL`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use craft_collab::native::NativeHub;
use craft_collab::{Hub, ServerMsg};

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
