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
use crate::test_text::{Replica, SCOPE};

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
    let _ = SCOPE;
}

/// A locks, submits and unlocks at once; B, once the lock is released, is granted a seq that
/// includes A's op.
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

    // B asks until it is granted; a denial means A still holds it.
    let granted = loop {
        b.send("lock", json!({ "scope": STRUCT })).await;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut got = None;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(100), b.heard.recv()).await {
                Ok(Some((name, body))) if name == "lock_granted" => {
                    got = Some(body);
                    break;
                }
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => panic!("socket gone"),
            }
        }
        if let Some(body) = got {
            break body;
        }
    };
    let op = seq_in(&a.next("op").await);
    assert!(seq_in(&granted) >= op, "grant seq {} is before A's op {op}", seq_in(&granted));
}
