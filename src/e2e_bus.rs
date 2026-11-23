//! End-to-end tests of the bus across a Valkey outage (R1) and of the sequence floor a rejoin
//! carries (R2).
//!
//! R1: the relay keeps running while Valkey drops its connections or restarts. The health
//! route answers 503 while the bus is down and 200 again once it is back, and the next submit
//! is ordered right after the last one, with no gap.
//!
//! R2: the sequence counter is lost (a wiped volume, the `everysec` window) and the relay
//! restarts. A replica that rejoins says how far it has applied, and the relay numbers the next
//! op past that, so no replica drops it as a replay.
//!
//! Ignored by default; they need a throwaway Valkey in `REVIEW_VALKEY_URL`. The restart test
//! also needs that Valkey's container name in `REVIEW_VALKEY_CONTAINER`, and stops and starts
//! it.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use axum::http::StatusCode;
use futures_util::FutureExt;
use rust_socketio::asynchronous::{Client, ClientBuilder};
use rust_socketio::{Payload, TransportType};
use serde_json::{Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::bus::MAX_SEQ_FLOOR;
use crate::e2e_reconnect::{Harness, now, start, ticket, unique_room};
use crate::test_text::Replica;

/// One socket speaking the wire directly.
struct Wire {
    client: Client,
    heard: UnboundedReceiver<(String, Value)>,
    joined: Value,
}

impl Wire {
    /// Join `room` as `pid`, resuming at `applied` when it is given.
    async fn join(url: &str, room: &str, pid: &str, applied: Option<u64>) -> Wire {
        let (tx, heard) = unbounded_channel();
        let mut builder = ClientBuilder::new(url.to_owned()).reconnect(false).transport_type(TransportType::Websocket);
        for event in ["op", "error", "joined"] {
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
        let mut auth = json!({ "token": ticket(room, pid, now()), "proto": craft_collab::PROTO });
        if let Some(applied) = applied {
            auth["applied"] = json!(applied);
        }
        let client = builder.auth(auth).connect().await.expect("connects");
        let mut wire = Wire { client, heard, joined: Value::Null };
        wire.joined = wire.next("joined").await;
        wire
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
        self.client.emit("submit", json!({ "local": local, "command": craft_collab::CRDT_TEXT, "params": params })).await.expect("emits");
    }

    /// Submit one edit and wait for the room to order it: the `op` this socket hears back.
    async fn ordered(&mut self, local: u64, params: Value) -> u64 {
        self.submit(local, params).await;
        let op = self.next("op").await;
        op["seq"].as_u64().expect("the op carries a seq")
    }

    async fn close(self) {
        let _ = self.client.disconnect().await;
    }
}

/// Poll the health route until it answers `want`, for at most `within`.
async fn health_turns(h: &Harness, want: StatusCode, within: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        if crate::health(&h.relay).await.0 == want {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Drop every client connection Valkey holds except the caller's, as a Valkey restart does to
/// the relay's connection.
async fn kill_bus_connections() {
    let url = std::env::var("REVIEW_VALKEY_URL").expect("REVIEW_VALKEY_URL");
    let client = redis::Client::open(url).expect("url");
    let mut admin = client.get_multiplexed_async_connection().await.expect("connects");
    let killed: i64 =
        redis::cmd("CLIENT").arg("KILL").arg("TYPE").arg("normal").arg("SKIPME").arg("yes").query_async(&mut admin).await.expect("kills");
    assert!(killed > 0, "no connection was dropped");
}

fn docker(args: &[&str]) {
    let status = std::process::Command::new("docker").args(args).status().expect("docker runs");
    assert!(status.success(), "docker {args:?} failed");
}

/// Order three edits in a fresh room and return the room, the socket and the last sequence.
async fn three_edits(h: &Harness, tag: &str) -> (String, Wire, Replica, u64) {
    let room = unique_room(tag);
    let mut a = Wire::join(&h.url, &room, "alice", None).await;
    let mut text = Replica::seeded(1, "hello");
    let mut last = 0;
    for i in 1..=3u64 {
        let seq = a.ordered(i, text.insert(0, "x")).await;
        assert_eq!(seq, last + 1, "the edits were not ordered one after the other");
        last = seq;
    }
    (room, a, text, last)
}

/// R1: Valkey drops the relay's connection (as a restart does). The relay reconnects without
/// being restarted: health is 200 again within seconds, and the next submit is ordered right
/// after the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn the_bus_reconnects_after_valkey_drops_its_connection() {
    let Some(h) = start().await else { return };
    assert_eq!(crate::health(&h.relay).await.0, StatusCode::OK);
    let (_room, mut a, mut text, last) = three_edits(&h, "bus-drop").await;

    kill_bus_connections().await;

    assert!(health_turns(&h, StatusCode::OK, Duration::from_secs(5)).await, "health did not come back after the connection dropped");
    let seq = a.ordered(4, text.insert(0, "y")).await;
    assert_eq!(seq, last + 1, "the first edit after the reconnect left a gap");
    a.close().await;
}

/// R1, against a real restart: Valkey is stopped under a running relay, then started again.
/// Health answers 503 while it is down and 200 within a few seconds of it coming back, and the
/// next submit is ordered right after the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL and REVIEW_VALKEY_CONTAINER"]
async fn health_and_ordering_survive_a_valkey_restart() {
    let Ok(container) = std::env::var("REVIEW_VALKEY_CONTAINER") else { return };
    let Some(h) = start().await else { return };
    assert_eq!(crate::health(&h.relay).await.0, StatusCode::OK);
    let (_room, mut a, mut text, last) = three_edits(&h, "bus-restart").await;

    docker(&["stop", "-t", "5", &container]);
    assert_eq!(crate::health(&h.relay).await.0, StatusCode::SERVICE_UNAVAILABLE, "health was 200 with Valkey stopped");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(crate::health(&h.relay).await.0, StatusCode::SERVICE_UNAVAILABLE, "health was 200 with Valkey stopped");
    docker(&["start", &container]);

    assert!(health_turns(&h, StatusCode::OK, Duration::from_secs(10)).await, "health stayed 503 after Valkey came back");
    let seq = a.ordered(4, text.insert(0, "y")).await;
    assert_eq!(seq, last + 1, "the first edit after the restart left a gap");
    a.close().await;
}

/// R2: the sequence counter is lost and the relay restarts. A replica that rejoins at `applied`
/// raises the sequence past it, so the next op is numbered after what every replica applied and
/// another replica that was in the session does not drop it as a replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_rejoin_raises_the_sequence_past_what_it_applied() {
    let Some(h) = start().await else { return };
    let (room, a, mut text, applied) = three_edits(&h, "seq-floor").await;
    let b = Wire::join(&h.url, &room, "bob", Some(applied)).await;
    a.close().await;

    // Valkey lost the counter, and the relay restarted: every room gone, every socket closed.
    h.relay.bus.lock().await.rewind_seq(&room, 0).await;
    h.relay.rooms.lock().await.clear();
    h.io.disconnect().await.expect("the relay closes every socket");
    let _ = b.client.disconnect().await;

    let mut a = Wire::join(&h.url, &room, "alice", Some(applied)).await;
    assert_eq!(a.joined["seq"].as_u64(), Some(applied), "the rejoin was not caught up at what it applied: {}", a.joined);
    assert_eq!(a.joined["history_lost"].as_bool(), Some(true), "the lost counter was not reported as a lost history: {}", a.joined);
    let mut b = Wire::join(&h.url, &room, "bob", Some(applied)).await;
    assert_eq!(b.joined["seq"].as_u64(), Some(applied));

    let seq = a.ordered(4, text.insert(0, "y")).await;
    assert_eq!(seq, applied + 1, "the next op was numbered {seq}; the replicas had applied {applied}");
    let heard = b.next("op").await;
    assert_eq!(heard["seq"].as_u64(), Some(applied + 1), "the other replica heard {heard}");
    assert!(h.relay.bus.lock().await.current_seq(&room).await.expect("reads") > applied, "the Valkey counter was not raised");
    a.close().await;
    b.close().await;
}

/// R2: a claimed `applied` past [`MAX_SEQ_FLOOR`] (a buggy client) is not honoured, so the
/// counter is not pushed towards its limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_huge_applied_does_not_move_the_sequence() {
    let Some(h) = start().await else { return };
    let (room, a, mut text, last) = three_edits(&h, "seq-huge").await;
    a.close().await;
    let mut a = Wire::join(&h.url, &room, "alice", Some(MAX_SEQ_FLOOR + 1)).await;
    let seq = a.ordered(4, text.insert(0, "y")).await;
    assert_eq!(seq, last + 1, "a huge applied moved the sequence to {seq}");
    a.close().await;
}

/// The counter went backwards under a live room (a restart inside the `everysec` window): the
/// next op is still numbered after the room's own, never one the replicas already have.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_counter_behind_the_room_never_hands_out_a_used_number() {
    let Some(h) = start().await else { return };
    let (room, mut a, mut text, last) = three_edits(&h, "seq-behind").await;
    h.relay.bus.lock().await.rewind_seq(&room, last - 2).await;
    let seq = a.ordered(4, text.insert(0, "y")).await;
    assert_eq!(seq, last + 1, "a counter behind the room handed out {seq}");
    a.close().await;
}
