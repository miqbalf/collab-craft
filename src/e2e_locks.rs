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
        Wire::connect_with(url, ticket(room, pid, now())).await
    }

    /// Connect with a read ticket: it may watch, never edit or lock.
    async fn connect_reader(url: &str, room: &str, pid: &str) -> Wire {
        Wire::connect_with(url, crate::token::sign_at(crate::e2e_reconnect::SECRET, room, pid, pid, false, pid, now()).unwrap()).await
    }

    async fn connect_with(url: &str, token: String) -> Wire {
        let (tx, heard) = unbounded_channel();
        let mut builder = ClientBuilder::new(url.to_owned()).reconnect(false).transport_type(TransportType::Websocket);
        for event in ["op", "lock_granted", "lock_denied", "lock_released", "lock_lost", "error", "joined"] {
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
        let auth = json!({ "token": token, "proto": craft_collab::PROTO });
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

    /// An ordered edit under the grant `token` of `STRUCT`.
    async fn submit_fenced(&self, local: u64, params: Value, token: u64) {
        let fence = json!({ "scope": STRUCT, "token": token });
        self.send("submit", json!({ "local": local, "command": craft_collab::CRDT_TEXT, "params": params, "fence": fence })).await;
    }

    /// Ask for `STRUCT` and return the grant's fencing token.
    async fn take(&mut self) -> u64 {
        self.send("lock", json!({ "scope": STRUCT })).await;
        let granted = self.next("lock_granted").await;
        assert!(granted["lease_ms"].as_u64().is_some_and(|ms| ms > 0), "the grant names no lease: {granted}");
        granted["token"].as_u64().expect("the grant carries a fencing token")
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

/// The fencing tests' lease: short, so a test can let one lapse.
const SHORT_LEASE: Duration = Duration::from_millis(1000);

/// A holder frozen past its lease, whose scope was granted to another peer since, is refused
/// when it wakes and submits under its old grant: code `fenced`, its counter echoed, nothing
/// numbered. The new holder's edit under the new grant is ordered, and is the only op anyone
/// sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_paused_holder_is_fenced_out_once_its_lease_went_to_another() {
    let Some(h) = start().await else { return };
    h.relay.bus.lock().await.set_lock_ttl(SHORT_LEASE);
    let (room, mut a, mut b) = pair(&h, "fence-paused").await;
    let old = a.take().await;
    // A sends a fenced edit while it holds the grant: ordered.
    let mut text = Replica::seeded(1, "hello");
    a.submit_fenced(1, text.insert(0, "a"), old).await;
    let first = seq_in(&b.next("op").await);

    // A is paused past its lease; B is granted the scope under a newer token.
    tokio::time::sleep(SHORT_LEASE + Duration::from_millis(300)).await;
    let new = b.take().await;
    assert!(new > old, "the token did not grow: {old} then {new}");

    let before = seq_of(&h, &room).await;
    a.submit_fenced(2, text.insert(0, "b"), old).await;
    let refused = a.next("error").await;
    assert_eq!(refused["code"], "fenced", "{refused}");
    assert_eq!(refused["local"], 2, "{refused}");
    assert_eq!(seq_of(&h, &room).await, before, "the stale holder's edit was numbered");

    let mut theirs = Replica::seeded(2, "hello");
    b.submit_fenced(1, theirs.insert(5, "!"), new).await;
    let op = a.next("op").await;
    assert_eq!(op["origin"], "bob", "the next op anyone sees is not the new holder's: {op}");
    assert!(seq_in(&op) > first);
    assert_eq!(seq_of(&h, &room).await, before + 1);
    // Nothing of A's refused edit was kept for a joiner either.
    let rooms = h.relay.rooms.lock().await;
    let since = rooms.get(&room).and_then(|r| r.since(0)).expect("the history reaches back");
    assert!(since.iter().all(|op| op.origin == "bob" || op.local == Some(1)), "the refused edit was recorded");
}

/// A fenced unlock under a grant that is no longer current releases nothing, even from the peer
/// that holds the scope now under a newer grant; the current grant's own unlock does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_stale_unlock_releases_nothing() {
    let Some(h) = start().await else { return };
    h.relay.bus.lock().await.set_lock_ttl(SHORT_LEASE);
    let (room, mut a, mut b) = pair(&h, "fence-unlock").await;
    let old = a.take().await;
    tokio::time::sleep(SHORT_LEASE + Duration::from_millis(300)).await;
    let new = b.take().await;

    a.send("unlock", json!({ "scope": STRUCT, "token": old })).await;
    // The same peer, after taking the scope again: its old grant's unlock is stale too.
    b.send("unlock", json!({ "scope": STRUCT, "token": old })).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(lease(&h, &room).await.as_deref(), Some("bob"), "a stale unlock released the current grant");
    assert_eq!(h.relay.rooms.lock().await.get(&room).and_then(|r| r.holder_of(STRUCT)), Some("bob"));

    b.send("unlock", json!({ "scope": STRUCT, "token": new })).await;
    b.next("lock_released").await;
    assert_eq!(lease(&h, &room).await, None);
}

/// A holder that refreshes its lease keeps the scope past the lease's length (a long run), and
/// its edit is ordered at the end of it; a refresh under a grant that is not current is
/// answered with `lock_lost`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_refreshed_lease_outlives_its_length() {
    let Some(h) = start().await else { return };
    h.relay.bus.lock().await.set_lock_ttl(SHORT_LEASE);
    let (room, mut a, mut b) = pair(&h, "fence-refresh").await;
    let token = a.take().await;
    for _ in 0..6 {
        tokio::time::sleep(SHORT_LEASE / 3).await;
        a.send("refresh", json!({ "scope": STRUCT, "token": token })).await;
    }
    // Twice the lease has passed.
    b.send("lock", json!({ "scope": STRUCT })).await;
    let denied = b.next("lock_denied").await;
    assert_eq!(denied["holder"], "alice");
    let before = seq_of(&h, &room).await;
    a.submit_fenced(1, Replica::seeded(1, "hello").insert(0, "x"), token).await;
    assert_eq!(seq_in(&a.next("op").await), before + 1);

    a.send("refresh", json!({ "scope": STRUCT, "token": token + 1000 })).await;
    let lost = a.next("lock_lost").await;
    assert_eq!(lost["scope"], STRUCT);
    assert_eq!(lost["token"], token + 1000);
    assert_eq!(lease(&h, &room).await.as_deref(), Some("alice"), "a stale refresh changed the lease");

    // Without refreshes the lease lapses, and a refresh after that is answered too.
    tokio::time::sleep(SHORT_LEASE + Duration::from_millis(300)).await;
    a.send("refresh", json!({ "scope": STRUCT, "token": token })).await;
    a.next("lock_lost").await;
    assert_eq!(lease(&h, &room).await, None, "a refresh revived a lapsed lease");
}

/// A read ticket never holds a lock, so its refresh is not even checked: no bus call, no
/// answer, and the holder's lease is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_reader_cannot_refresh_a_lease() {
    let Some(h) = start().await else { return };
    let room = unique_room("fence-reader");
    let mut a = Wire::connect(&h.url, &room, "alice").await;
    let mut r = Wire::connect_reader(&h.url, &room, "rita").await;
    let token = a.take().await;
    r.send("refresh", json!({ "scope": STRUCT, "token": token })).await;
    let answer = tokio::time::timeout(Duration::from_millis(500), r.next("lock_lost")).await;
    assert!(answer.is_err(), "a reader's refresh was answered: {answer:?}");
    assert_eq!(lease(&h, &room).await.as_deref(), Some("alice"));
    a.send("refresh", json!({ "scope": STRUCT, "token": token })).await;
    let answer = tokio::time::timeout(Duration::from_millis(500), a.next("lock_lost")).await;
    assert!(answer.is_err(), "the holder's own refresh lost its lock: {answer:?}");
}
