//! End-to-end tests of the final fix wave: a room that lost its history (C1), the grace period
//! an emptied room is kept for, a transient ordering failure (I3), the scope cap (I4), the
//! engine.io ping timeout (I1), the heartbeat after heavy sends (I2), and the saved report and
//! heartbeat of a socket that may not make them (Minors 4 and 10). Ignored by default; they need
//! a throwaway Valkey in `REVIEW_VALKEY_URL`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::Ordering;
use std::time::Duration;

use craft_collab::native::NativeHub;
use craft_collab::{ClientMsg, Hub, ServerMsg, TextPayload, Work};

use crate::e2e_reconnect::{
    Harness, SECRET, SLOW_LINK, black_hole, large_file, link_is, now, relay_addr, saver, seq_of, start, start_capped, start_with, throttled, ticket,
    unique_room, until_both,
};
use crate::test_text::{LivePeer, para};

fn lost(work: &[Work]) -> bool {
    work.iter().any(|w| matches!(w, Work::HistoryLost))
}

async fn history_lost_in_room(h: &Harness, room: &str) -> Option<bool> {
    h.relay.rooms.lock().await.get(room).map(crate::room::Room::history_lost)
}

/// The document service's saver `w` opens the room, types, saves and types again, so the
/// saved file is behind the room and the room is behind nothing. Returns the saved file's text.
async fn session_saves(h: &Harness, room: &str, doc: &str, url: &str) -> (LivePeer, String) {
    let mut w = LivePeer::open(url, &saver(room), doc, "hello").await;
    w.insert(5, " world");
    assert!(w.save().await, "the first save was not acknowledged: {:?}", w.work);
    let file = w.text();
    w.insert(0, ">");
    let before = seq_of(h, room).await;
    assert!(w.until(Duration::from_secs(5), move |live, _| live.applied() > before).await, "the unsaved edit was not ordered");
    (w, file)
}

/// C1, after the room's history is gone: the saver `w` (still holding the settled document)
/// rejoins and is told; a fresh joiner seeded from the saved file is told, refuses edits and
/// does not take the file as current, not even when the saver types meanwhile. Then the saver
/// checkpoints at once, the fresh joiner restarts from that snapshot, and the two converge both
/// ways. A joiner after that save joins the ordinary way.
async fn recover_from_a_lost_history(h: &Harness, room: &str, doc: &str, mut w: LivePeer, file: &str) {
    assert!(w.until(Duration::from_secs(20), |_, work| lost(work)).await, "the saver was not told: {:?}", w.work);
    assert!(!w.live.awaiting_snapshot(), "the saver held the document before and waits for nothing");
    assert!(w.live.ready_to_type());
    assert_eq!(history_lost_in_room(h, room).await, Some(true));
    let room_text = w.text();
    assert_ne!(room_text, file, "the room is past the file");

    let mut f = LivePeer::connect(&h.url, &ticket(room, "bob", now()), doc).await;
    f.live.seed(&para(file)).expect("the file seeds");
    assert!(f.until(Duration::from_secs(10), |_, work| lost(work)).await, "the fresh joiner was not told: {:?}", f.work);
    assert!(f.live.awaiting_snapshot());
    assert!(!f.live.ready_to_type());
    let refused = f.live.edit(|d| d.insert_text(0, 0, "x", &serde_json::Map::new()));
    assert!(refused.is_err(), "a fresh joiner edited on a file the room is not on");
    assert_eq!(f.work.iter().filter(|w| matches!(w, Work::HistoryLost)).count(), 1, "told once");

    // The saver keeps typing; the fresh joiner does not lay that over the file.
    let before = seq_of(h, room).await;
    let at = room_text.chars().count();
    w.insert(at, "!");
    assert!(w.until(Duration::from_secs(5), move |live, _| live.applied() > before).await, "the saver's edit was not ordered");
    f.until(Duration::from_millis(500), |_, _| false).await;
    assert_eq!(seq_of(h, room).await, before + 1, "only the saver's edit was ordered");
    assert_eq!(f.text(), file, "the fresh joiner showed text the room does not have");

    // The saver checkpoints at once; the fresh joiner restarts from it.
    assert!(w.save().await, "the recovery checkpoint was not acknowledged: {:?}", w.work);
    assert_eq!(history_lost_in_room(h, room).await, Some(false));
    assert!(
        f.until(Duration::from_secs(10), |live, work| !live.awaiting_snapshot() && work.iter().any(|w| matches!(w, Work::Reset))).await,
        "{:?}",
        f.work
    );
    assert_eq!(f.text(), w.text(), "the fresh joiner did not converge on the snapshot");
    assert!(f.live.ready_to_type());

    f.insert(0, "[f]");
    w.insert(0, "[w]");
    assert!(
        until_both(&mut w, &mut f, Duration::from_secs(10), |w, f| w.text() == f.text() && w.text().contains("[f]") && w.text().contains("[w]"))
            .await,
        "diverged: saver {:?}, joiner {:?}",
        w.text(),
        f.text()
    );

    // A joiner after the recovery save is caught up the ordinary way.
    let saved = {
        let rooms = h.relay.rooms.lock().await;
        rooms.get(room).map(crate::room::Room::saved_seq).unwrap_or(0)
    };
    assert!(saved > 0);
    let mut g = LivePeer::connect(&h.url, &ticket(room, "carol", now()), doc).await;
    g.until(Duration::from_millis(800), |_, _| false).await;
    g.live.seed(&para(file)).expect("seeds");
    assert!(g.until(Duration::from_secs(10), |live, _| live.ready_to_type()).await);
    assert!(!lost(&g.work) && !g.live.awaiting_snapshot());
    let want = w.text();
    assert!(
        g.until(Duration::from_secs(10), |live, _| live.doc().and_then(|d| d.block(0)).is_some_and(|b| b.text == want)).await,
        "carol has {:?}",
        g.text()
    );
}

/// C1: the relay restarts (its rooms are gone; the sequence stays in Valkey) under a session
/// that saved. The saver's replica survives and reconnects; a fresh joiner seeded from the
/// saved file must not diverge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_relay_restart_loses_the_history_and_the_saver_recovers_it() {
    let Some(h) = start().await else { return };
    let room = unique_room("restart");
    let doc = format!("doc:{room}");
    let (w, file) = session_saves(&h, &room, &doc, &h.url).await;
    // A restart, as the clients and Valkey see it: every room gone, every socket closed.
    h.relay.rooms.lock().await.clear();
    h.io.disconnect().await.expect("the relay closes every socket");
    recover_from_a_lost_history(&h, &room, &doc, w, &file).await;
}

/// N1: after a restart and before any snapshot, a replica that held the document and rejoins
/// late still gets what the room ordered since the restart. Alice (settled) is back first and
/// types; bob (settled, his link down a while longer) rejoins after that and must converge
/// with her, as the fallback text brought a late rejoiner before the restart was detected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_survivor_rejoining_late_after_a_restart_gets_the_rooms_own_edits() {
    let Some(h) = start().await else { return };
    let room = unique_room("late-survivor");
    let doc = format!("doc:{room}");
    let p = crate::e2e_reconnect::proxy(relay_addr(&h), Duration::from_secs(3)).await;
    let mut a = LivePeer::open(&h.url, &ticket(&room, "alice", now()), &doc, "hello").await;
    let mut b = LivePeer::open(&p.url, &ticket(&room, "bob", now()), &doc, "hello").await;
    a.insert(5, " world");
    assert!(until_both(&mut a, &mut b, Duration::from_secs(10), |a, b| a.text() == "hello world" && b.text() == a.text()).await);

    // The restart: every room gone, every socket closed, and bob's link down for 3 s.
    h.relay.rooms.lock().await.clear();
    p.cut.notify_one();
    h.io.disconnect().await.expect("the relay closes every socket");
    let bob_joins = p.accepted.load(Ordering::SeqCst);

    assert!(a.until(Duration::from_secs(10), |live, work| lost(work) && live.ready_to_type()).await, "{:?}", a.work);
    assert!(!a.live.awaiting_snapshot());
    let before = seq_of(&h, &room).await;
    a.insert(0, ">");
    assert!(a.until(Duration::from_secs(5), move |live, _| live.applied() > before).await, "alice's edit was not ordered");
    assert_eq!(p.accepted.load(Ordering::SeqCst), bob_joins, "bob rejoined before alice typed; the test proves nothing");
    assert_eq!(history_lost_in_room(&h, &room).await, Some(true), "no snapshot yet");

    assert!(b.until(Duration::from_secs(20), |_, work| lost(work)).await, "bob was not told: {:?}", b.work);
    assert!(!b.live.awaiting_snapshot(), "bob held the document before");
    assert!(
        until_both(&mut a, &mut b, Duration::from_secs(10), |a, b| b.text() == a.text()).await,
        "bob missed the room's own edits: alice {:?}, bob {:?}",
        a.text(),
        b.text()
    );
    assert_eq!(b.text(), ">hello world");
    b.insert(0, "[b]");
    assert!(
        until_both(&mut a, &mut b, Duration::from_secs(10), |a, b| a.text() == b.text() && a.text().starts_with("[b]")).await,
        "alice {:?}, bob {:?}",
        a.text(),
        b.text()
    );
}

/// C1: the room is dropped after its grace period while its only peer is away. The same loss,
/// without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_room_dropped_after_its_grace_loses_the_history_and_the_saver_recovers_it() {
    // A grace shorter than the hub's first reconnect backoff (250 ms).
    let Some(h) = start_with(None, crate::room::TEXT_BYTES, Duration::from_millis(50)).await else { return };
    let room = unique_room("grace-lapsed");
    let doc = format!("doc:{room}");
    let (w, file) = session_saves(&h, &room, &doc, &h.url).await;
    h.io.disconnect().await.expect("the relay closes every socket");
    recover_from_a_lost_history(&h, &room, &doc, w, &file).await;
}

/// The grace period: a room whose last peer left is kept, so the peer coming back within it
/// (a reload, a worker restart, a dropped link) finds the history whole, and so does a fresh
/// joiner. Nobody is told the history was lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_an_emptied_room_is_kept_for_its_grace_period() {
    let Some(h) = start().await else { return };
    let room = unique_room("grace-kept");
    let doc = format!("doc:{room}");
    let (mut w, file) = session_saves(&h, &room, &doc, &h.url).await;
    // Every socket closes, so the room empties; the hub is back after its backoff.
    h.io.disconnect().await.expect("the relay closes every socket");
    assert!(link_is(&mut w, true).await, "the saver never came back");
    w.until(Duration::from_millis(800), |_, _| false).await;
    assert!(!lost(&w.work), "told of a loss within the grace period: {:?}", w.work);
    {
        let rooms = h.relay.rooms.lock().await;
        let kept = rooms.get(&room).expect("the room");
        // A room dropped and made again would have no save and an inherited history.
        assert!(kept.saved_seq() > 0 && kept.inherited() == 0, "the room was not kept: saved {}, inherited {}", kept.saved_seq(), kept.inherited());
    }

    let mut f = LivePeer::connect(&h.url, &ticket(&room, "bob", now()), &doc).await;
    f.until(Duration::from_millis(800), |_, _| false).await;
    f.live.seed(&para(&file)).expect("seeds");
    let want = w.text();
    assert!(
        f.until(Duration::from_secs(10), |live, _| live.ready_to_type() && live.doc().and_then(|d| d.block(0)).is_some_and(|b| b.text == want)).await
    );
    assert!(!lost(&f.work) && !f.live.awaiting_snapshot());
}

/// I3: the bus failing one sequence is a transient refusal. The edit is kept and sent again,
/// lands exactly once, and the replica stays sound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_an_edit_refused_for_a_transient_bus_failure_is_sent_again() {
    let Some(h) = start().await else { return };
    let room = unique_room("retry");
    let doc = format!("doc:{room}");
    let mut a = LivePeer::open(&h.url, &ticket(&room, "alice", now()), &doc, "hello").await;
    let mut b = LivePeer::open(&h.url, &ticket(&room, "bob", now()), &doc, "hello").await;
    let before = seq_of(&h, &room).await;
    h.relay.bus.lock().await.fail_next_seq = 1;
    a.insert(5, "!");
    assert!(until_both(&mut a, &mut b, Duration::from_secs(10), |_, b| b.text() == "hello!").await, "bob has {:?}; alice {:?}", b.text(), a.work);
    assert!(a.live.ready_to_type(), "a transient refusal broke the replica");
    assert!(!a.work.iter().any(|w| matches!(w, Work::TextIncomplete { .. })), "{:?}", a.work);
    a.until(Duration::from_millis(500), |_, _| false).await;
    assert_eq!(seq_of(&h, &room).await - before, 1, "ordered exactly once");
    a.insert(0, ">");
    assert!(b.until(Duration::from_secs(5), |live, _| live.doc().and_then(|d| d.block(0)).is_some_and(|p| p.text == ">hello!")).await);
}

/// I4: an editor naming a fresh scope per edit used to make the catch-up larger than the
/// socket's buffer, and every join was cut and retried for ever. With the scope cap the
/// fresh joiner gets in on its first connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_many_scopes_do_not_make_a_room_unjoinable() {
    let Some(h) = start().await else { return };
    let room = unique_room("scopes");
    let doc = format!("doc:{room}");
    let mut a = LivePeer::open(&h.url, &ticket(&room, "alice", now()), &doc, "hello").await;
    // More scopes than the old cap's catch-up fit the socket's buffer (2560 packets), each
    // large enough that a slow link cannot drain them as fast as a join queues them.
    let scopes = 4000u64;
    // A valid yrs update of about the size of one frame's worth of text: one string item of
    // 1000 characters (the relay refuses ops that are not valid updates).
    let op = {
        use base64::Engine as _;
        let mut bytes = vec![1, 1, 1, 0, 0x04, 1, 1, b't', 0xe8, 0x07];
        bytes.extend(std::iter::repeat_n(b'a', 1000));
        bytes.push(0);
        base64::engine::general_purpose::STANDARD.encode(bytes)
    };
    let before = seq_of(&h, &room).await;
    for i in 0..scopes {
        let params = serde_json::json!({ "scope": format!("s{i}"), "ops": [op] });
        a.live.submit(craft_collab::CRDT_TEXT, params, None).expect("goes out");
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while seq_of(&h, &room).await < before + scopes && tokio::time::Instant::now() < deadline {
        a.until(Duration::from_millis(100), |_, _| false).await;
    }
    assert_eq!(seq_of(&h, &room).await, before + scopes, "every submit was ordered");
    let retained = h.relay.rooms.lock().await.get(&room).map(|r| r.texts().len()).unwrap_or(0);
    assert!(retained <= crate::room::MAX_SCOPES, "{retained} scopes retained");

    let (url, accepted) = throttled(relay_addr(&h), SLOW_LINK).await;
    let mut b = LivePeer::connect(&url, &ticket(&room, "bob", now()), &doc).await;
    b.until(Duration::from_millis(800), |_, _| false).await;
    b.live.seed(&para("hello")).expect("seeds");
    assert!(b.until(Duration::from_secs(15), |live, _| live.ready_to_type()).await, "the joiner never got in");
    b.until(Duration::from_secs(5), |_, _| false).await;
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "the join was cut and retried");
    assert!(b.live.hub().connected());
}

/// I1: a checkpoint whose upload takes longer than engineioxide's default ping timeout (20 s)
/// plus its ping interval: the pong waits behind the upload, and the relay used to close the
/// socket mid transfer, so every retry died the same way. With 90 s it is acknowledged on the
/// first connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_checkpoint_slower_than_the_old_ping_timeout_is_acked() {
    let Some(h) = start_capped(None, 8 * 1024 * 1024).await else { return };
    let rate = SLOW_LINK / 2;
    let (url, accepted) = throttled(relay_addr(&h), rate).await;
    let room = unique_room("slow-ping");
    let file = "ab".repeat(2_600_000);
    let mut a = LivePeer::open(&url, &saver(&room), &format!("doc:{room}"), &file).await;
    // The snapshot goes up and its echo comes back at this rate, which is the hub's own
    // slow-link allowance for the pair: give the checkpoint more room than that, so only the
    // relay's ping timeout is under test.
    a.live.set_checkpoint_timeout(Duration::from_secs(120));
    let started = tokio::time::Instant::now();
    a.live.checkpoint().expect("a checkpoint goes out");
    let done = a
        .until(Duration::from_secs(240), |_, work| {
            work.iter().any(|w| matches!(w, Work::Checkpointed { .. } | Work::CheckpointRaced | Work::CheckpointRefused { .. }))
        })
        .await;
    assert!(
        done && a.work.iter().any(|w| matches!(w, Work::Checkpointed { .. })),
        "the checkpoint was not acknowledged after {:?}, {} connections: {:?}",
        started.elapsed(),
        accepted.load(Ordering::SeqCst),
        a.work
    );
    assert!(started.elapsed() > Duration::from_secs(45), "the upload did not span a ping and the old timeout: {:?}", started.elapsed());
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "the relay closed the socket mid transfer");
    assert!(a.live.hub().connected());
}

/// I2 (R6-3 style): heavy sends on a fast link used to leave the hub's transfer allowance
/// (`busy_until`) a minute ahead, so a link that died after them was noticed only that much
/// later. An acknowledged heartbeat now clamps it, and the dead link is noticed in about one
/// heartbeat timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_link_that_dies_after_heavy_sends_is_noticed_in_time() {
    let Some(h) = start_capped(None, 8 * 1024 * 1024).await else { return };
    let (url, hole) = black_hole(relay_addr(&h)).await;
    let room = unique_room("heavy-then-dead");
    // About 3.5 MiB on the wire: some 56 s at the hub's slow-link rate, on a link that takes
    // it in well under a second.
    let mut a = LivePeer::open(&url, &saver(&room), &format!("doc:{room}"), &large_file()).await;
    assert!(a.save().await, "the checkpoint was not acknowledged: {:?}", a.work);
    // Two heartbeats, both acknowledged at once on the fast link.
    a.until(Duration::from_secs(12), |_, _| false).await;
    assert!(a.live.hub().connected());

    hole.notify_one();
    let went = tokio::time::Instant::now();
    assert!(a.until(Duration::from_secs(40), |live, _| !live.hub().connected()).await, "the dead link was never noticed");
    assert!(went.elapsed() <= Duration::from_secs(20), "noticed only after {:?}", went.elapsed());
}

/// Wait for a hub's first `Joined`, keeping what else arrived.
async fn hub_joined(hub: &mut NativeHub) -> Vec<ServerMsg> {
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        seen.extend(hub.poll());
        if seen.iter().any(|m| matches!(m, ServerMsg::Joined { .. })) {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("never joined: {seen:?}");
}

/// Minor 4: only the document service may move the join baseline. An editor reporting a save
/// is refused, even one naming the sequence the service itself saved through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_an_editor_may_not_report_a_save() {
    let Some(h) = start().await else { return };
    let room = unique_room("editor-save");
    let doc = format!("doc:{room}");
    let mut w = LivePeer::open(&h.url, &saver(&room), &doc, "hello").await;
    w.insert(5, "!");
    assert!(w.save().await, "{:?}", w.work);
    let saved = h.relay.rooms.lock().await.get(&room).map(crate::room::Room::saved_seq).unwrap();

    let mut editor = NativeHub::connect(&h.url, &ticket(&room, "bob", now()), None).await.expect("connects");
    hub_joined(&mut editor).await;
    editor.send(ClientMsg::Saved { doc: doc.clone(), seq: saved }).expect("sent");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut answer = None;
    while answer.is_none() && tokio::time::Instant::now() < deadline {
        answer = editor.poll().into_iter().find(|m| matches!(m, ServerMsg::SavedAck { .. } | ServerMsg::SavedRefused { .. }));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    match answer {
        Some(ServerMsg::SavedRefused { reason, .. }) => assert!(craft_collab::refusal_is_permanent(&reason), "{reason}"),
        other => panic!("an editor's save report was not refused: {other:?}"),
    }

    // A checkpoint snapshot from the editor is refused the same way, as before.
    let snapshot = serde_json::to_value(TextPayload::snapshot(&w.live.doc().unwrap().state())).unwrap();
    editor
        .send(ClientMsg::Submit { doc, local: 1, command: craft_collab::CRDT_TEXT.to_owned(), params: snapshot, scope: None, fence: None })
        .expect("sent");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(editor.poll().iter().any(|m| matches!(m, ServerMsg::Error { local: Some(1), .. })));
}

/// Minor 10: a socket that only listens and beats cannot outlive its ticket. Its heartbeat is
/// not acknowledged once the ticket lapsed: the relay drops it, and the reconnect with the same
/// ticket is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_heartbeat_does_not_outlive_its_ticket() {
    let Some(h) = start().await else { return };
    let room = unique_room("beat-expiry");
    let expiring = crate::token::sign_at(SECRET, &room, "dora", "dora", false, "dora", now() - crate::token::LIFETIME_SECS + 3).unwrap();
    let mut listener = NativeHub::connect(&h.url, &expiring, None).await.expect("connects");
    hub_joined(&mut listener).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    let mut refused = false;
    while !refused && tokio::time::Instant::now() < deadline {
        refused = listener.poll().iter().any(|m| m.refuses_connection() == Some(craft_collab::REFUSED_AUTH));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(refused, "a beating socket outlived its ticket");
    assert!(!listener.connected());
}
