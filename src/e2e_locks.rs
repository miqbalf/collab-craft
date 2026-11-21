//! End-to-end tests of the room `seq` on `lock_granted`: a grant says how far the room had
//! been ordered when the lock was given, so a holder can wait until it has applied that far
//! before it edits. Read off the raw wire, because the event carries a field the client
//! library of this branch does not yet name. Ignored by default; they need a throwaway Valkey
//! in `REVIEW_VALKEY_URL`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use futures_util::FutureExt;
use rust_socketio::asynchronous::{Client, ClientBuilder};
use rust_socketio::{Payload, TransportType};
use serde_json::{Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::e2e_reconnect::{Harness, now, seq_of, start, ticket, unique_room};
use crate::room::Submission;
use crate::test_text::Replica;

const STRUCT: &str = "grid/struct";

/// One socket speaking the wire directly, hearing the events a lock test cares about.
struct Wire {
    client: Client,
    heard: UnboundedReceiver<(String, Value)>,
}

impl Wire {
    async fn connect(url: &str, room: &str, pid: &str) -> Wire {
        let (tx, heard) = unbounded_channel();
        let mut builder = ClientBuilder::new(url.to_owned()).reconnect(false).transport_type(TransportType::Websocket);
        for event in ["op", "lock_granted", "lock_denied", "lock_released", "joined"] {
            let tx = tx.clone();
            builder = builder.on(event, move |payload, _| {
                let tx = tx.clone();
                async move {
                    if let Payload::Text(values) = payload {
                        for v in values {
                            let _ = tx.send((event.to_owned(), v));
                        }
                    }
                }
                .boxed()
            });
        }
        let auth = json!({ "token": ticket(room, pid, now()), "proto": craft_collab::PROTO });
        let client = builder.auth(auth).connect().await.expect("connects");
        let mut wire = Wire { client, heard };
        wire.next("joined").await;
        wire
    }

    async fn send(&self, event: &str, body: Value) {
        self.client.emit(event.to_owned(), body).await.expect("emits");
    }

    /// The next event named `event`, skipping others, within ten seconds.
    async fn next(&mut self, event: &str) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(left, self.heard.recv()).await {
                Ok(Some((name, body))) if name == event => return body,
                Ok(Some(_)) => {}
                _ => panic!("no {event} within ten seconds"),
            }
        }
    }

    async fn submit(&self, local: u64, params: Value) {
        self.send("submit", json!({ "local": local, "command": craft_collab::CRDT_TEXT, "params": params })).await;
    }
}

fn seq_in(v: &Value) -> u64 {
    v["seq"].as_u64().expect("the event carries a seq")
}

async fn pair(h: &Harness, tag: &str) -> (String, Wire, Wire) {
    let room = unique_room(tag);
    let a = Wire::connect(&h.url, &room, "alice").await;
    let b = Wire::connect(&h.url, &room, "bob").await;
    (room, a, b)
}

/// The grant carries the room's seq: not before A's last op, and the room's own at that moment.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn lock_grant_carries_the_room_seq() {
    let Some(h) = start().await else { return };
    let (room, mut a, mut b) = pair(&h, "lock-seq").await;
    let mut text = Replica::seeded(1, "hello");
    let mut last = 0;
    for (i, word) in ["a", "b", "c"].iter().enumerate() {
        a.submit(i as u64 + 1, text.insert(0, word)).await;
        last = seq_in(&a.next("op").await);
    }
    b.send("lock", json!({ "scope": STRUCT })).await;
    let granted = b.next("lock_granted").await;
    assert_eq!(granted["scope"], STRUCT);
    let seq = seq_in(&granted);
    assert!(seq >= last, "grant seq {seq} is before the last op {last}");
    assert_eq!(seq, seq_of(&h, &room).await, "grant seq is not the room's seq");
}

/// A locks, submits and unlocks at once; B, once the lock is released, is granted a seq that
/// includes A's op. This covers the case where the relay handles A's submit before B's grant.
/// A submit still on its way to the room when the unlock lands is C3's job: the holder unlocks
/// only after its own submit is ordered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn submit_then_unlock_orders_the_submit_first() {
    let Some(h) = start().await else { return };
    let (_room, mut a, mut b) = pair(&h, "lock-order").await;
    a.send("lock", json!({ "scope": STRUCT })).await;
    a.next("lock_granted").await;
    let mut text = Replica::seeded(1, "hello");
    a.submit(1, text.insert(0, "x")).await;
    a.send("unlock", json!({ "scope": STRUCT })).await;

    // B waits for the release, then locks once: the relay's own order, within ten seconds.
    b.next("lock_released").await;
    b.send("lock", json!({ "scope": STRUCT })).await;
    let granted = b.next("lock_granted").await;
    let op = seq_in(&a.next("op").await);
    assert!(seq_in(&granted) >= op, "grant seq {} is before A's op {op}", seq_in(&granted));
}

/// Whoever holds the lease for `STRUCT` on the bus.
async fn lease(h: &Harness, room: &str) -> Option<String> {
    h.relay.bus.lock().await.current_holder(room, STRUCT).await.expect("the bus answers")
}

/// Poll `done` every 20 ms until it holds or `d` passes. Whether it held.
async fn within(d: Duration, mut done: impl AsyncFnMut() -> bool) -> bool {
    let end = tokio::time::Instant::now() + d;
    loop {
        if done().await {
            return true;
        }
        if tokio::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A requester that disconnects while its grant waits on the room's gate is never recorded as
/// the holder, and its lease on the bus is given back. The test holds the gate itself, so the
/// request is parked after it took the lease and before it recorded anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_requester_that_left_while_waiting_holds_nothing() {
    let Some(h) = start().await else { return };
    let (room, a, _b) = pair(&h, "lock-left").await;
    let gate = h.relay.gate(&room);
    let parked = gate.lock().await;

    a.send("lock", json!({ "scope": STRUCT })).await;
    assert!(within(Duration::from_secs(10), async || lease(&h, &room).await.as_deref() == Some("alice")).await, "the lease was never taken");
    a.client.disconnect().await.expect("disconnects");
    let gone =
        within(Duration::from_secs(10), async || h.relay.rooms.lock().await.get(&room).is_some_and(|r| !r.peers().iter().any(|p| p.peer == "alice")))
            .await;
    assert!(gone, "alice never left the room");
    drop(parked);

    assert!(within(Duration::from_secs(5), async || lease(&h, &room).await.is_none()).await, "a departed peer still holds the lease");
    assert_eq!(h.relay.rooms.lock().await.get(&room).and_then(|r| r.holder_of(STRUCT)), None, "a departed peer is the recorded holder");
}

/// A grant is not given while an op is between its number and the room's record: the test is
/// that op (it holds the gate), and the grant must wait for it and then count it. Without the
/// gate the grant arrives at once, with the seq from before the op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_grant_waits_for_an_op_that_is_being_ordered() {
    let Some(h) = start().await else { return };
    let (room, _a, mut b) = pair(&h, "lock-gate").await;
    let before = seq_of(&h, &room).await;
    let gate = h.relay.gate(&room);
    let ordering = gate.lock().await;

    b.send("lock", json!({ "scope": STRUCT })).await;
    assert!(within(Duration::from_secs(10), async || lease(&h, &room).await.as_deref() == Some("bob")).await, "the lease was never taken");
    // Numbered under the gate, as `on_submit` does; recorded below, still under it.
    let seq = {
        let floor = seq_of(&h, &room).await;
        h.relay.bus.lock().await.next_seq(&room, floor).await.expect("a seq")
    };
    let early = tokio::time::timeout(Duration::from_millis(400), b.next("lock_granted")).await;
    assert!(early.is_err(), "the grant did not wait for the op being ordered: {early:?}");
    {
        let mut rooms = h.relay.rooms.lock().await;
        let submission = Submission {
            peer_id: "alice".to_owned(),
            may_edit: true,
            command: craft_collab::CRDT_TEXT.to_owned(),
            params: Replica::seeded(1, "hello").insert(0, "x"),
            scope: None,
            local: Some(1),
        };
        h.relay.room(&mut rooms, &room).submit(seq, submission).expect("the op is recorded");
    }
    drop(ordering);

    let granted = seq_in(&b.next("lock_granted").await);
    assert!(granted >= seq && seq > before, "grant seq {granted} does not count the op {seq}");
}
