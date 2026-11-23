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

use crate::bus::{MAX_SEQ_CLAIM, MAX_SEQ_FLOOR, SEQ_RAISE_MARGIN};
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
        Wire::join_with(url, ticket(room, pid, now()), applied).await
    }

    /// Join with a read ticket: it may watch, never edit.
    async fn join_reader(url: &str, room: &str, pid: &str, applied: Option<u64>) -> Wire {
        let token = crate::token::sign_at(crate::e2e_reconnect::SECRET, room, pid, pid, false, pid, now()).unwrap();
        Wire::join_with(url, token, applied).await
    }

    async fn join_with(url: &str, token: String, applied: Option<u64>) -> Wire {
        let (tx, heard) = unbounded_channel();
        let mut builder = ClientBuilder::new(url.to_owned()).reconnect(false).transport_type(TransportType::Websocket);
        for event in ["op", "error", "joined", "text_ops"] {
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
        let mut auth = json!({ "token": token, "proto": craft_collab::PROTO });
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

    /// Submit one edit and wait for the room to order it: the `op` this socket hears back with
    /// that `local` (a catch-up replay may come first).
    async fn ordered(&mut self, local: u64, params: Value) -> u64 {
        self.submit(local, params).await;
        loop {
            let op = self.next("op").await;
            if op["local"].as_u64() == Some(local) {
                return op["seq"].as_u64().expect("the op carries a seq");
            }
        }
    }

    async fn close(self) {
        let _ = self.client.disconnect().await;
    }
}

fn joined_seq(w: &Wire) -> u64 {
    w.joined["seq"].as_u64().expect("joined carries a seq")
}

/// Wait until `peer` is no longer in the room: the relay closed it.
async fn left_the_room(h: &Harness, room: &str, peer: &str) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let still = h.relay.rooms.lock().await.get(room).is_some_and(|r| r.peers().iter().any(|p| p.peer == peer));
        if !still {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The relay restarts after Valkey lost the latest `INCR`s (a power cut inside the `everysec`
/// window): the counter is `to` (0: gone), every room is gone and every socket closed.
async fn crash(h: &Harness, room: &str, to: u64) {
    h.relay.bus.lock().await.rewind_seq(room, to).await;
    h.relay.rooms.lock().await.clear();
    h.io.disconnect().await.expect("the relay closes every socket");
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
    let start = joined_seq(&a);
    assert!(start >= applied + SEQ_RAISE_MARGIN, "the raise kept no margin past what was applied: {}", a.joined);
    assert_eq!(a.joined["history_lost"].as_bool(), Some(true), "the lost counter was not reported as a lost history: {}", a.joined);
    let mut b = Wire::join(&h.url, &room, "bob", Some(applied)).await;
    assert_eq!(joined_seq(&b), start);

    let seq = a.ordered(4, text.insert(0, "y")).await;
    assert_eq!(seq, start + 1, "the next op was numbered {seq}; the room starts at {start}");
    let heard = b.next("op").await;
    assert_eq!(heard["seq"].as_u64(), Some(start + 1), "the other replica heard {heard}");
    assert!(h.relay.bus.lock().await.current_seq(&room).await.expect("reads") > applied, "the Valkey counter was not raised");
    a.close().await;
    b.close().await;
}

/// R2: a claimed `applied` past [`MAX_SEQ_CLAIM`] (a buggy client) is not honoured, so the
/// counter is not pushed towards its limit, margin included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_huge_applied_does_not_move_the_sequence() {
    let Some(h) = start().await else { return };
    let (room, a, mut text, mut last) = three_edits(&h, "seq-huge").await;
    a.close().await;
    for (i, huge) in [MAX_SEQ_FLOOR + 1, MAX_SEQ_CLAIM + 1].into_iter().enumerate() {
        let mut a = Wire::join(&h.url, &room, "alice", Some(huge)).await;
        let seq = a.ordered(4 + i as u64, text.insert(0, "y")).await;
        assert_eq!(seq, last + 1, "an applied of {huge} moved the sequence to {seq}");
        last = seq;
        a.close().await;
    }
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

/// I1: a read-only peer that claims to have applied far more than the document holds does not
/// move the sequence. It is caught up as a fresh load instead, and hears the next op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_read_only_rejoin_does_not_raise_the_sequence() {
    let Some(h) = start().await else { return };
    let (room, mut a, mut text, last) = three_edits(&h, "seq-reader").await;
    let mut r = Wire::join_reader(&h.url, &room, "rita", Some(last + 100)).await;
    assert_eq!(h.relay.bus.lock().await.current_seq(&room).await.expect("reads"), last, "a read-only claim raised the counter");
    let seq = a.ordered(4, text.insert(0, "y")).await;
    assert_eq!(seq, last + 1, "a read-only claim moved the sequence to {seq}");
    let heard = loop {
        let op = r.next("op").await;
        if op["seq"].as_u64() == Some(last + 1) {
            break op;
        }
    };
    assert_eq!(heard["seq"].as_u64(), Some(last + 1));
    a.close().await;
    r.close().await;
}

/// I2: the counter is wiped and the relay restarts; a fresh loader joins first and orders edits
/// of its own; then a replica rejoins with a higher `applied` and raises the sequence past
/// them. No edit is lost in silence: the rejoiner is told the history is lost and is sent the
/// room's retained text (the loader's edits), and the loader is reconnected at once, not left
/// holding ops behind a gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_raise_past_a_loaders_own_edits_loses_nothing_in_silence() {
    let Some(h) = start().await else { return };
    let (room, a, _, applied) = three_edits(&h, "seq-lift").await;
    a.close().await;

    // Valkey lost the counter, and the relay restarted.
    h.relay.bus.lock().await.rewind_seq(&room, 0).await;
    h.relay.rooms.lock().await.clear();
    h.io.disconnect().await.expect("the relay closes every socket");

    // A fresh loader is first, and edits on the wiped counter.
    let mut c = Wire::join(&h.url, &room, "carol", None).await;
    let mut loader = Replica::seeded(2, "hello");
    assert_eq!(c.ordered(1, loader.insert(0, "c")).await, 1);
    assert_eq!(c.ordered(2, loader.insert(0, "c")).await, 2);
    assert!(applied > 2);

    // Alice's replica rejoins, ahead of the room.
    let mut a = Wire::join(&h.url, &room, "alice", Some(applied)).await;
    assert_eq!(a.joined["history_lost"].as_bool(), Some(true), "the rejoin past the loader's edits was not told: {}", a.joined);
    assert_eq!(a.joined["fallback"].as_bool(), Some(true), "the rejoin was not sent the retained text: {}", a.joined);
    let texts = a.next("text_ops").await;
    assert!(texts["ops"].as_array().is_some_and(|ops| ops.len() >= 2), "the loader's edits are not in the catch-up: {texts}");
    let lost = h.relay.rooms.lock().await.get(&room).map(crate::room::Room::history_lost);
    assert_eq!(lost, Some(true));

    // The loader is closed now, so it rejoins through the lost history instead of waiting.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let still = h.relay.rooms.lock().await.get(&room).is_some_and(|r| r.peers().iter().any(|p| p.peer == "carol"));
        if !still {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "the loader was left in the room behind the jump");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Ordering carries on past what the rejoiner applied.
    let mut text = Replica::seeded(1, "hello");
    let start = joined_seq(&a);
    assert!(start >= applied + SEQ_RAISE_MARGIN);
    assert_eq!(a.ordered(9, text.insert(0, "z")).await, start + 1);
    a.close().await;
    c.close().await;
}

/// M6: a sequence counter that is not a whole number (a stray write) neither refuses every join
/// nor every submit. It is treated as lost, like a wiped one: the rejoin raises past it, and
/// ordering carries on right after the rejoiner's `joined` sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_counter_that_is_not_a_number_does_not_wedge_the_document() {
    let Some(h) = start().await else { return };
    for garbage in ["not-a-number", "99999999.5"] {
        let (room, a, _, last) = three_edits(&h, "seq-garbage").await;
        a.close().await;
        h.relay.bus.lock().await.corrupt_seq(&room, garbage).await;
        let mut b = Wire::join(&h.url, &room, "bob", Some(last)).await;
        let seq = b.ordered(1, Replica::seeded(4, "hello").insert(0, "b")).await;
        assert_eq!(seq, joined_seq(&b) + 1, "a counter of {garbage:?} handed out {seq} after joined {}", b.joined);
        assert!(seq > last, "a counter of {garbage:?} handed out a used number {seq}");
        b.close().await;
    }
}

/// N1/N2 (a): after a power cut, A rejoins at what it applied and B at one more (an op that
/// reached B before the crash). Neither is left behind a gap: each hears the next op right
/// after its own `joined` sequence, and B, which holds an op numbered past the raise point, is
/// told the history is lost and gets the retained text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn two_rejoins_after_a_power_cut_both_carry_on() {
    let Some(h) = start().await else { return };
    let (room, a, mut text, applied) = three_edits(&h, "seq-skew").await;
    a.close().await;
    crash(&h, &room, applied - 2).await;

    let mut a = Wire::join(&h.url, &room, "alice", Some(applied)).await;
    let mut b = Wire::join(&h.url, &room, "bob", Some(applied + 1)).await;
    assert_eq!(b.joined["history_lost"].as_bool(), Some(true), "{}", b.joined);
    // Bob's join does not raise again (the margin already covers him): no second lift, so
    // Alice stays connected.
    assert!(!left_the_room(&h, &room, "alice").await, "bob's join lifted the room again and closed alice");
    let seq = a.ordered(4, text.insert(0, "y")).await;
    assert_eq!(seq, joined_seq(&a) + 1, "alice was left behind a gap: joined at {}, next op {seq}", joined_seq(&a));
    let heard = b.next("op").await;
    assert_eq!(heard["seq"].as_u64(), Some(joined_seq(&b) + 1), "bob was left behind a gap: joined {}, heard {heard}", b.joined);
    a.close().await;
    b.close().await;
}

/// N2: after a power cut, A rejoins and orders two ops; then B rejoins with one more than A had
/// applied before the crash. B's op at that number is not A's new one, so B must not be caught
/// up by a replay that skips it: it is told the history is lost and gets the retained text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_rejoin_past_the_raise_point_is_not_replayed_over() {
    let Some(h) = start().await else { return };
    let (room, a, mut text, applied) = three_edits(&h, "seq-past-raise").await;
    a.close().await;
    crash(&h, &room, applied - 2).await;

    let mut a = Wire::join(&h.url, &room, "alice", Some(applied)).await;
    a.ordered(4, text.insert(0, "y")).await;
    a.ordered(5, text.insert(0, "y")).await;
    let b = Wire::join(&h.url, &room, "bob", Some(applied + 1)).await;
    assert_eq!(b.joined["history_lost"].as_bool(), Some(true), "{}", b.joined);
    assert_eq!(b.joined["fallback"].as_bool(), Some(true), "bob was replayed over the op he holds: {}", b.joined);
    a.close().await;
    b.close().await;
}

/// N1 (b): after a wipe, a fresh loader joins and has not edited; then a rejoin raises the
/// sequence. The loader is not left at the old sequence behind the gap: it is closed and
/// rejoins through the lost history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_raise_does_not_strand_a_loader_that_has_not_edited() {
    let Some(h) = start().await else { return };
    let (room, a, _, applied) = three_edits(&h, "seq-idle-loader").await;
    a.close().await;
    crash(&h, &room, 0).await;
    let c = Wire::join(&h.url, &room, "carol", None).await;
    let a = Wire::join(&h.url, &room, "alice", Some(applied)).await;
    assert!(left_the_room(&h, &room, "carol").await, "the loader was left behind the raise");
    a.close().await;
    c.close().await;
}

/// N1 (c): after a wipe, only a read-only viewer is in the room; then a rejoin raises the
/// sequence. The viewer is not left behind the gap either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_raise_does_not_strand_a_read_only_viewer() {
    let Some(h) = start().await else { return };
    let (room, a, _, applied) = three_edits(&h, "seq-viewer").await;
    a.close().await;
    crash(&h, &room, 0).await;
    let r = Wire::join_reader(&h.url, &room, "rita", None).await;
    let a = Wire::join(&h.url, &room, "alice", Some(applied)).await;
    assert!(left_the_room(&h, &room, "rita").await, "the viewer was left behind the raise");
    a.close().await;
    r.close().await;
}

/// N5: a power cut leaves the counter at 1 with A at 1 and B at 3 (B was a little ahead). The
/// relay restarts. A rejoins first with nothing to raise past, and the rebuilt room numbers A's
/// next ops 2 and 3 again; B then rejoins at 3. B holds different ops under 2 and 3, so it must
/// not be replayed over them: it gets the lost history with the retained text (A's new ops).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_rebuild_after_a_power_cut_never_reuses_a_number() {
    let Some(h) = start().await else { return };
    let (room, a, mut text, applied) = three_edits(&h, "seq-rebuild").await;
    assert_eq!(applied, 3);
    a.close().await;
    // Valkey restarted (so what it holds may trail) and lost the last two INCRs.
    h.relay.bus.lock().await.forget_epoch(&room).await;
    crash(&h, &room, 1).await;

    let mut a = Wire::join(&h.url, &room, "alice", Some(1)).await;
    let first = a.ordered(4, text.insert(0, "y")).await;
    let second = a.ordered(5, text.insert(0, "y")).await;
    assert!(first > applied && second > applied, "the rebuilt room reused pre-crash numbers: {first}, {second}");
    let b = Wire::join(&h.url, &room, "bob", Some(applied)).await;
    assert_eq!(b.joined["history_lost"].as_bool(), Some(true), "{}", b.joined);
    assert_eq!(b.joined["fallback"].as_bool(), Some(true), "bob was replayed over the ops he holds: {}", b.joined);
    a.close().await;
    b.close().await;
}

/// N5's guard: a healthy room (Valkey never restarted) reopened after its idle close is an
/// ordinary rejoin. Nothing is raised, nobody is told the history is lost by a lift, and the
/// next op follows straight on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_healthy_room_reopened_after_idle_is_not_lifted() {
    let Some(h) = start().await else { return };
    let (room, a, mut text, applied) = three_edits(&h, "seq-idle").await;
    a.close().await;
    // The room is dropped after its grace period; the relay and Valkey both kept running.
    tokio::time::sleep(Duration::from_millis(200)).await;
    h.relay.rooms.lock().await.remove(&room);
    let mut a = Wire::join(&h.url, &room, "alice", Some(applied)).await;
    assert_eq!(joined_seq(&a), applied, "a healthy reopen moved the sequence: {}", a.joined);
    assert_eq!(h.relay.bus.lock().await.current_seq(&room).await.expect("reads"), applied, "a healthy reopen raised the counter");
    assert_eq!(a.ordered(4, text.insert(0, "y")).await, applied + 1);
    a.close().await;
}

/// N6: Valkey is hard-killed under a live relay and comes back a little behind the live room.
/// An editor that reconnects with what it applied (not past the live room) does not lift the
/// room: nobody is disconnected and the next op follows the room's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_counter_trailing_a_live_room_does_not_lift_it() {
    let Some(h) = start().await else { return };
    let (room, mut a, mut text, last) = three_edits(&h, "seq-trailing").await;
    h.relay.bus.lock().await.rewind_seq(&room, last - 2).await;
    let b = Wire::join(&h.url, &room, "bob", Some(last)).await;
    assert_eq!(joined_seq(&b), last, "the trailing counter lifted the live room: {}", b.joined);
    assert!(!left_the_room(&h, &room, "alice").await, "alice was disconnected by a needless lift");
    assert_eq!(a.ordered(4, text.insert(0, "y")).await, last + 1);
    a.close().await;
    b.close().await;
}
