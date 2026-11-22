//! The session pump against the in-memory relay, with the smallest app there is: a list of
//! lines, one [`LiveDoc`] block each, whose edits replace a line's whole text. These guard what
//! every app inherits; WordCraft's own pump tests guard its mapping on top.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use craft_collab::{BlockChange, Hub, LiveDoc, LockEvent, SeedBlock, ServerMsg};
use serde_json::{Map, Value, json};

use crate::ordered::{Action, GIVE_UP_MS, Ordered, Polled};
use crate::testing::{LEASE_MS, MemHub, MemRelay, ROOM};
use crate::{
    Access, CONNECTING, Change, ENDED_KIND, Landed, LiveApp, LiveEditing, LiveKind, LiveStatus, LocalError, NOTICE_KIND, SAVED_KIND, SERVICE_UID,
};

/// The app's types: lines by block id, an edit is the lines before and after.
struct Lines;

type Doc = Vec<(u64, String)>;

impl LiveKind for Lines {
    type Replica = LiveDoc;
    type Edit = (Doc, Doc);
    type Caret = (u64, usize);
    type Pos = (u64, usize);
    fn pos(caret: &(u64, usize)) -> (u64, usize) {
        *caret
    }
}

/// The engine: its lines.
#[derive(Default)]
struct Engine {
    doc: Doc,
}

impl LiveApp<Lines> for Engine {
    fn local(&mut self, replica: &mut LiveDoc, edits: &[(Doc, Doc)]) -> Result<Vec<Vec<u8>>, LocalError> {
        let (Some((before, _)), Some((_, after))) = (edits.first(), edits.last()) else { return Ok(Vec::new()) };
        if before.len() != after.len() {
            return Err(LocalError::Refused("Lines can't be added while others are editing.".to_owned()));
        }
        let mut out = Vec::new();
        for ((id, was), (_, now)) in before.iter().zip(after) {
            if was != now {
                out.push(replica.delete_text(*id, 0, was.chars().count()).map_err(|e| LocalError::Failed(e.to_string()))?);
                out.push(replica.insert_text(*id, 0, now, &Map::new()).map_err(|e| LocalError::Failed(e.to_string()))?);
            }
        }
        Ok(out)
    }
    fn landed(&mut self) -> Landed {
        Landed::Done
    }
    fn put_back(&mut self, edits: Vec<(Doc, Doc)>) -> Result<(), String> {
        let (before, _) = edits.into_iter().next().ok_or("nothing to put back")?;
        self.doc = before;
        Ok(())
    }
    fn received(&mut self, _: &mut LiveDoc, changes: Vec<BlockChange>) -> (Vec<BlockChange>, Vec<Vec<u8>>) {
        (changes, Vec::new())
    }
    fn apply(&mut self, replica: &LiveDoc, _: &[Change<Lines>]) -> Result<(), String> {
        self.resync(replica)
    }
    fn resync(&mut self, replica: &LiveDoc) -> Result<(), String> {
        self.doc = replica.order().into_iter().map(|id| (id, replica.block(id).map(|b| b.text).unwrap_or_default())).collect();
        Ok(())
    }
    fn reset(&mut self, _: u32) {}
    fn set_space(&mut self, _: u32) {}
    fn caret(&self, payload: &Value) -> Option<(u64, usize)> {
        let id = payload.get("id")?.as_u64()?;
        let at = usize::try_from(payload.get("at")?.as_u64()?).ok()?;
        self.doc.iter().any(|(i, _)| *i == id).then_some((id, at))
    }
    fn follow(&self, carets: &mut HashMap<String, (u64, usize)>) {
        carets.retain(|_, (id, at)| match self.doc.iter().find(|(i, _)| i == id) {
            Some((_, text)) => {
                *at = (*at).min(text.chars().count());
                true
            }
            None => false,
        });
    }
}

fn seed() -> Vec<SeedBlock> {
    vec![SeedBlock { id: 1, text: "one".to_owned(), ..SeedBlock::default() }, SeedBlock { id: 2, text: "two".to_owned(), ..SeedBlock::default() }]
}

/// One window: its engine and its live session.
struct Window {
    engine: Engine,
    le: LiveEditing<MemHub, Lines>,
}

impl Window {
    fn join(relay: &MemRelay, uid: &str, access: Access) -> Window {
        let hub = relay.connect(uid, uid, access == Access::Edit);
        let le = LiveEditing::start(hub, ROOM, seed().as_slice(), access).unwrap();
        let engine = Engine { doc: seed().into_iter().map(|b| (b.id, b.text)).collect() };
        Window { engine, le }
    }

    fn pump(&mut self) -> Vec<crate::Peer<(u64, usize)>> {
        self.le.pump(&mut self.engine)
    }

    /// The user sets line `id` to `text`, as an engine command reports it.
    fn set(&mut self, id: u64, text: &str) {
        let before = self.engine.doc.clone();
        if let Some((_, t)) = self.engine.doc.iter_mut().find(|(i, _)| *i == id) {
            text.clone_into(t);
        }
        self.le.queue().push((before, self.engine.doc.clone()));
        self.le.flush(&mut self.engine);
    }

    fn line(&self, id: u64) -> &str {
        self.engine.doc.iter().find(|(i, _)| *i == id).map_or("", |(_, t)| t.as_str())
    }
}

fn settle(windows: &mut [&mut Window]) {
    for _ in 0..4 {
        for w in windows.iter_mut() {
            w.pump();
        }
    }
}

/// An edit on one window reaches the other, and both say they are live.
#[test]
fn two_windows_converge() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    assert!(a.le.editable() && b.le.editable(), "{:?} {:?}", a.le.status(), b.le.status());
    a.set(1, "uno");
    b.set(2, "dos");
    settle(&mut [&mut a, &mut b]);
    assert_eq!((a.line(1), a.line(2)), (b.line(1), b.line(2)));
    assert_eq!(b.line(1), "uno");
    assert_eq!(a.line(2), "dos");
}

/// An edit made before the link is live is put back, and the user is told why.
#[test]
fn an_edit_before_live_is_put_back() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    assert!(!a.le.editable());
    a.set(1, "early");
    assert_eq!(a.line(1), "one", "the edit stayed");
    assert!(a.le.take_notices().iter().any(|n| n == CONNECTING));
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    assert_eq!(b.line(1), "one", "the put-back edit reached the room");
}

/// The mapping's refusal puts the edit back with its own line, and nothing is sent.
#[test]
fn a_refused_edit_is_put_back_with_the_apps_reason() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    settle(&mut [&mut a]);
    let seq = relay.seq();
    let before = a.engine.doc.clone();
    a.engine.doc.push((9, "new".to_owned()));
    a.le.queue().push((before, a.engine.doc.clone()));
    a.le.flush(&mut a.engine);
    assert_eq!(a.engine.doc.len(), 2, "the refused edit stayed");
    assert!(a.le.take_notices().iter().any(|n| n.starts_with("Lines can't")));
    assert_eq!(relay.seq(), seq, "a refused edit was sent");
}

/// A read-only window follows and never sends.
#[test]
fn a_read_only_window_follows_and_never_sends() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut r = Window::join(&relay, "rita", Access::Read);
    settle(&mut [&mut a, &mut r]);
    assert_eq!(r.le.status(), &LiveStatus::ReadOnly);
    a.set(1, "seen");
    settle(&mut [&mut a, &mut r]);
    assert_eq!(r.line(1), "seen");
    let seq = relay.seq();
    r.set(2, "never");
    settle(&mut [&mut a, &mut r]);
    assert_eq!(relay.seq(), seq);
    assert_eq!(a.line(2), "two");
}

/// Only the service's `saved` counts, and its sequence never goes back; `closed` ends the link.
#[test]
fn saved_markers_count_from_the_service_only() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut s = Window::join(&relay, SERVICE_UID, Access::Edit);
    let mut m = Window::join(&relay, "mallory", Access::Edit);
    settle(&mut [&mut a, &mut s, &mut m]);
    m.le.hub_mut()
        .send(craft_collab::ClientMsg::Ephemeral { doc: ROOM.into(), kind: SAVED_KIND.into(), payload: json!({"seq": 9, "etag": "fake"}) })
        .unwrap();
    settle(&mut [&mut a, &mut s, &mut m]);
    assert_eq!(a.le.saved(), None, "an editor's saved marker counted");
    s.le.announce_saved(5, "e5").unwrap();
    settle(&mut [&mut a, &mut s, &mut m]);
    assert_eq!(a.le.status(), &LiveStatus::Live { saved_at: Some("e5".to_owned()) });
    s.le.announce_saved(3, "e3").unwrap();
    settle(&mut [&mut a, &mut s, &mut m]);
    assert_eq!(a.le.saved(), Some((5, "e5")), "the saved sequence went back");
    assert!(a.pump().iter().all(|p| p.id.split('~').next() != Some(SERVICE_UID)), "the service is listed as an editor");
    s.le.announce_closed(5, "e5").unwrap();
    settle(&mut [&mut a, &mut s, &mut m]);
    assert!(matches!(a.le.status(), LiveStatus::Broken(_)), "{:?}", a.le.status());
}

/// The service's `notice` reaches the editors once; its `ended` ends their link with its reason.
#[test]
fn the_service_tells_and_ends() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut s = Window::join(&relay, SERVICE_UID, Access::Edit);
    settle(&mut [&mut a, &mut s]);
    let say = |s: &mut Window, kind: &str, payload: Value| {
        s.le.hub_mut().send(craft_collab::ClientMsg::Ephemeral { doc: ROOM.into(), kind: kind.into(), payload }).unwrap();
    };
    say(&mut s, NOTICE_KIND, json!({"text": "Hello from the service."}));
    settle(&mut [&mut a, &mut s]);
    assert_eq!(a.le.take_notices().iter().filter(|n| *n == "Hello from the service.").count(), 1);
    say(&mut s, ENDED_KIND, json!({"reason": "Saving stopped."}));
    settle(&mut [&mut a, &mut s]);
    assert_eq!(a.le.status(), &LiveStatus::Broken("Saving stopped.".to_owned()));
    assert!(!a.le.editable());
}

/// Carets go out at most five a second on the host's clock, and the newest one wins.
#[test]
fn carets_are_held_to_five_a_second() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    let now = std::sync::Arc::new(std::sync::Mutex::new(0.0_f64));
    let read = now.clone();
    a.le.set_clock(Box::new(move || *read.lock().unwrap()));
    settle(&mut [&mut a, &mut b]);
    a.le.send_presence(json!({"id": 1, "at": 1}));
    a.le.send_presence(json!({"id": 1, "at": 2}));
    a.le.send_presence(json!({"id": 1, "at": 3}));
    settle(&mut [&mut a, &mut b]);
    let caret = |b: &mut Window| b.pump().iter().find_map(|p| p.caret);
    assert_eq!(caret(&mut b), Some((1, 1)), "only the first caret went out at once");
    *now.lock().unwrap() += 250.0;
    settle(&mut [&mut a, &mut b]);
    assert_eq!(caret(&mut b), Some((1, 3)), "the newest held caret went out when due");
}

/// A caret on a line another edit shortened is clamped to it.
#[test]
fn carets_follow_edits() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.send_presence(json!({"id": 2, "at": 3}));
    settle(&mut [&mut a, &mut b]);
    b.set(2, "t");
    settle(&mut [&mut a, &mut b]);
    let caret = b.pump().iter().find_map(|p| p.caret);
    assert_eq!(caret, Some((2, 1)), "the caret was not clamped to the shortened line");
}

/// A catch-up that does not arrive is told once when late and gives up when twice as late.
#[test]
fn a_late_catch_up_is_told_then_given_up() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    settle(&mut [&mut a]);
    a.set(1, "x");
    settle(&mut [&mut a]);
    // A later joiner whose replay never finishes arriving: its inbox is held from the start.
    let hub = relay.connect("bob", "bob", true);
    let mut late = LiveEditing::<MemHub, Lines>::start(hub.clone(), ROOM, seed().as_slice(), Access::Edit).unwrap();
    let now = std::sync::Arc::new(std::sync::Mutex::new(0.0_f64));
    let read = now.clone();
    late.set_clock(Box::new(move || *read.lock().unwrap()));
    let mut engine = Engine { doc: seed().into_iter().map(|b| (b.id, b.text)).collect() };
    relay.cut_replay(&hub);
    late.pump(&mut engine);
    assert_eq!(late.status(), &LiveStatus::Connecting);
    *now.lock().unwrap() += 10_500.0;
    late.pump(&mut engine);
    late.pump(&mut engine);
    assert_eq!(late.take_notices().iter().filter(|n| n.starts_with("Still catching up")).count(), 1, "told once");
    *now.lock().unwrap() += 10_000.0;
    late.pump(&mut engine);
    assert!(matches!(late.status(), LiveStatus::Broken(_)), "{:?}", late.status());
}

const SCOPE: &str = "grid/struct";

fn granted(seq: u64) -> LockEvent {
    LockEvent::Granted { scope: SCOPE.into(), seq: Some(seq), token: None }
}

fn denied(holder: &str) -> LockEvent {
    LockEvent::Denied { scope: SCOPE.into(), holder: holder.into() }
}

fn released() -> LockEvent {
    LockEvent::Released { scope: SCOPE.into() }
}

/// With a grant at seq 5 the item waits for the replica to apply up to it.
#[test]
fn a_free_lock_runs_after_the_grant_seq_is_applied() {
    let mut q = Ordered::<&str>::new(SCOPE);
    assert_eq!(q.request("insert", 0.0), Ok(vec![Action::SendLock]));
    assert!(q.on_lock(&granted(5), 1.0).is_empty());
    assert_eq!(q.poll(4, 2.0), Polled::Nothing);
    assert_eq!(q.poll(5, 3.0), Polled::Run("insert"));
    assert_eq!(q.poll(9, 4.0), Polled::Nothing, "the item ran twice");
}

/// A grant from an older relay carries no seq: nothing to wait for.
#[test]
fn a_grant_without_seq_runs_at_once() {
    let mut q = Ordered::<u8>::new(SCOPE);
    q.request(1, 0.0).unwrap();
    q.on_lock(&LockEvent::Granted { scope: SCOPE.into(), seq: None, token: None }, 1.0);
    assert_eq!(q.poll(0, 2.0), Polled::Run(1));
}

/// A refused request names the holder, asks again when it is released, and runs when granted.
#[test]
fn a_held_lock_waits_and_retries_on_release() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("delete", 0.0).unwrap();
    assert!(q.on_lock(&denied("alice~1"), 1.0).is_empty());
    assert_eq!(q.waiting_for(), Some("alice~1"));
    assert_eq!(q.poll(100, 2.0), Polled::Nothing);
    assert_eq!(q.on_lock(&released(), 3.0), vec![Action::SendLock]);
    assert_eq!(q.waiting_for(), None);
    assert!(q.on_lock(&granted(7), 4.0).is_empty());
    assert_eq!(q.poll(7, 5.0), Polled::Run("delete"));
}

/// Somebody else's take of the scope updates who is waited for; another scope changes nothing.
#[test]
fn held_by_other_updates_the_holder_and_other_scopes_are_ignored() {
    let mut q = Ordered::<u8>::new(SCOPE);
    q.request(1, 0.0).unwrap();
    q.on_lock(&denied("alice~1"), 1.0);
    q.on_lock(&LockEvent::HeldByOther { scope: SCOPE.into(), holder: "carol~3".into() }, 2.0);
    assert_eq!(q.waiting_for(), Some("carol~3"));
    let other = "grid/other".to_owned();
    assert!(q.on_lock(&LockEvent::Released { scope: other.clone() }, 3.0).is_empty());
    assert!(q.on_lock(&LockEvent::Granted { scope: other, seq: Some(1), token: None }, 3.0).is_empty());
    assert_eq!(q.waiting_for(), Some("carol~3"));
    assert_eq!(q.poll(99, 4.0), Polled::Nothing);
}

#[test]
fn gives_up_after_ten_seconds() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("insert", 100.0).unwrap();
    q.on_lock(&denied("alice~1"), 101.0);
    assert_eq!(q.poll(0, 100.0 + GIVE_UP_MS), Polled::Nothing, "gave up early");
    assert_eq!(q.poll(0, 100.0 + GIVE_UP_MS + 1.0), Polled::GaveUp { item: "insert", holder: "alice~1".into() });
    // Free again: the next request goes out.
    assert_eq!(q.request("again", 20_000.0), Ok(vec![Action::SendLock]));
}

/// A relay that never answers also ends the wait; a grant that comes late is given back.
#[test]
fn an_unanswered_request_gives_up_and_a_late_grant_is_given_back() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("insert", 0.0).unwrap();
    assert_eq!(q.poll(0, GIVE_UP_MS + 1.0), Polled::GaveUp { item: "insert", holder: String::new() });
    assert_eq!(q.on_lock(&granted(3), GIVE_UP_MS + 2.0), vec![Action::SendUnlock]);
}

#[test]
fn one_pending_at_a_time() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("first", 0.0).unwrap();
    assert_eq!(q.request("second", 1.0), Err("second"));
    q.on_lock(&granted(0), 2.0);
    assert_eq!(q.request("third", 3.0), Err("third"), "granted but not run");
    assert_eq!(q.poll(0, 4.0), Polled::Run("first"));
    assert_eq!(q.request("fourth", 5.0), Err("fourth"), "running");
    assert_eq!(q.finish(true), vec![Action::SendUnlock]);
    assert_eq!(q.request("fifth", 6.0), Ok(vec![Action::SendLock]));
}

#[test]
fn unlock_waits_for_the_submit_to_be_ordered() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("insert", 0.0).unwrap();
    q.on_lock(&granted(1), 1.0);
    assert_eq!(q.poll(1, 2.0), Polled::Run("insert"));
    assert!(q.finish(false).is_empty());
    assert!(q.finish(false).is_empty());
    assert_eq!(q.finish(true), vec![Action::SendUnlock]);
    assert!(q.finish(true).is_empty(), "unlocked twice");
}

/// The room-wide `Released` that echoes this queue's own unlock must not disturb it.
#[test]
fn the_echo_of_our_own_release_changes_nothing() {
    let mut q = Ordered::<&str>::new(SCOPE);
    assert!(q.on_lock(&released(), 0.0).is_empty(), "idle");
    q.request("a", 1.0).unwrap();
    assert!(q.on_lock(&released(), 2.0).is_empty(), "request out, nothing refused yet");
    q.on_lock(&granted(2), 3.0);
    assert!(q.on_lock(&released(), 4.0).is_empty(), "granted");
    assert_eq!(q.poll(2, 5.0), Polled::Run("a"));
    assert!(q.on_lock(&released(), 6.0).is_empty(), "running");
    assert_eq!(q.finish(true), vec![Action::SendUnlock]);
}

/// After a rejoin the relay holds nothing for us: a waiting item asks again; a run that lost
/// its grant owes no unlock.
#[test]
fn a_rejoin_asks_again_or_owes_no_unlock() {
    let mut q = Ordered::<&str>::new(SCOPE);
    assert!(q.rejoined(0.0).is_empty(), "idle");
    q.request("x", 0.0).unwrap();
    q.on_lock(&granted(9), 1.0);
    assert_eq!(q.rejoined(2.0), vec![Action::SendLock]);
    assert_eq!(q.poll(9, 3.0), Polled::Nothing, "the dropped grant still ran the item");
    q.on_lock(&granted(10), 4.0);
    assert_eq!(q.poll(10, 5.0), Polled::Run("x"));
    assert!(q.rejoined(6.0).is_empty());
    assert!(q.finish(true).is_empty(), "unlocked a lock the relay had already released");
    assert_eq!(q.request("y", 7.0), Ok(vec![Action::SendLock]));
    // A refused request asks again too, with a fresh wait.
    q.on_lock(&denied("a~1"), 8.0);
    assert_eq!(q.rejoined(9_000.0), vec![Action::SendLock]);
    assert_eq!(q.waiting_for(), None);
    assert_eq!(q.poll(0, 9_000.0 + GIVE_UP_MS), Polled::Nothing);
}

fn lock_both(a: &mut Window, b: &mut Window) {
    a.le.lock(SCOPE).unwrap();
    settle(&mut [a, b]);
}

fn peer_of(relay: &MemRelay, w: &Window) -> String {
    relay.peer_id(w.le.hub())
}

#[test]
fn mem_relay_serialises_two_holders() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    let a_peer = peer_of(&relay, &a);
    lock_both(&mut a, &mut b);
    let (_, token) = relay.holder(SCOPE).expect("A holds it");
    assert_eq!(a.le.take_lock_events(), vec![LockEvent::Granted { scope: SCOPE.into(), seq: Some(relay.seq()), token: Some(token) }]);
    assert!(b.le.take_lock_events().contains(&LockEvent::HeldByOther { scope: SCOPE.into(), holder: a_peer.clone() }));
    // B asks while A holds: refused with A's peer id.
    b.le.lock(SCOPE).unwrap();
    settle(&mut [&mut a, &mut b]);
    assert_eq!(b.le.take_lock_events(), vec![denied(&a_peer)]);
    assert!(a.le.take_lock_events().is_empty(), "A heard B's refusal");
    // A edits under the lock and unlocks once the relay ordered the edit.
    a.set(1, "uno");
    let edit_seq = relay.seq();
    assert!(!a.le.local_settled());
    for _ in 0..4 {
        if a.le.local_settled() {
            break;
        }
        a.pump();
    }
    assert!(a.le.local_settled());
    a.le.unlock(SCOPE).unwrap();
    settle(&mut [&mut a, &mut b]);
    // Everyone is told, A too; B asks again and is granted at or after A's edit.
    assert_eq!(b.le.take_lock_events(), vec![released()]);
    assert_eq!(a.le.take_lock_events(), vec![released()]);
    b.le.lock(SCOPE).unwrap();
    settle(&mut [&mut a, &mut b]);
    let events = b.le.take_lock_events();
    let Some(LockEvent::Granted { seq: Some(seq), .. }) = events.first().cloned() else { panic!("{events:?}") };
    assert!(seq >= edit_seq, "granted at {seq} before the edit's {edit_seq}");
    assert!(b.le.newest_seq() >= seq && b.line(1) == "uno", "B saw the edit by the time it held the lock");
}

/// The lock a peer holds goes with its link, so nobody waits on a dead holder.
#[test]
fn a_dropped_link_releases_its_lock() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    lock_both(&mut a, &mut b);
    b.le.take_lock_events();
    relay.drop_link(a.le.hub());
    settle(&mut [&mut b]);
    assert_eq!(b.le.take_lock_events(), vec![released()]);
    b.le.lock(SCOPE).unwrap();
    settle(&mut [&mut b]);
    assert!(matches!(b.le.take_lock_events().first(), Some(LockEvent::Granted { .. })));
}

#[test]
fn local_settled_is_false_until_the_relay_orders_the_edit() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    assert!(a.le.local_settled(), "nothing sent yet");
    a.set(1, "uno");
    assert!(!a.le.local_settled(), "sent, not yet ordered back to the sender");
    assert!(!a.le.local_settled(), "asking twice changed it");
    a.pump();
    assert!(a.le.local_settled(), "the relay's order reached the sender");
    // A second burst unsettles it again, and every edit must come back.
    a.set(2, "dos");
    a.set(1, "eins");
    assert!(!a.le.local_settled());
    a.pump();
    assert!(a.le.local_settled());
    settle(&mut [&mut a, &mut b]);
    assert_eq!((b.line(1), b.line(2)), ("eins", "dos"));
}

/// A link that drops with an edit in flight is not settled until the rejoin's resend is
/// ordered, and the edit still reaches the room.
#[test]
fn local_settled_is_false_across_a_rejoin_until_the_resend_is_ordered() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.set(1, "uno");
    relay.drop_link(a.le.hub());
    assert!(!a.le.local_settled(), "dropped with the edit unconfirmed");
    relay.restore_link(a.le.hub());
    assert!(!a.le.local_settled(), "the join has not been read yet");
    settle(&mut [&mut a, &mut b]);
    assert!(a.le.local_settled());
    assert_eq!((a.line(1), b.line(1)), ("uno", "uno"));
}

/// The rejoin's resend never left (the link failed again): the edit is not ordered, so the
/// replica is not settled, even though nothing is waiting for an answer any more.
#[test]
fn local_settled_is_false_while_the_rejoin_resend_has_not_gone_out() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.set(1, "uno");
    relay.drop_link(a.le.hub());
    relay.restore_link(a.le.hub());
    relay.fail_sends(a.le.hub(), true);
    a.pump();
    assert!(!a.le.local_settled(), "an edit that never reached the relay counted as ordered");
    relay.fail_sends(a.le.hub(), false);
}

/// A submit the relay refuses for now is sent again under a new counter: the first counter must
/// not keep the replica unsettled for good (the structure lock would never be released).
#[test]
fn a_submit_refused_for_now_settles_once_its_resend_is_ordered() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.lock(SCOPE).unwrap();
    settle(&mut [&mut a, &mut b]);
    relay.refuse_next_submits(1);
    a.set(1, "uno");
    a.pump();
    assert!(!a.le.local_settled(), "refused, so not ordered");
    let mut settled = false;
    for _ in 0..40 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        settle(&mut [&mut a, &mut b]);
        if a.le.local_settled() {
            settled = true;
            break;
        }
    }
    assert!(settled, "the refused counter kept the replica unsettled");
    assert_eq!(b.line(1), "uno", "the resend was ordered");
}

/// A submit refused for now leaves no stale count behind: once its resend is ordered and a save
/// covers it, the window reports its edits saved, with no rejoin needed.
#[test]
fn edits_saved_after_a_submit_refused_for_now() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut s = Window::join(&relay, SERVICE_UID, Access::Edit);
    settle(&mut [&mut a, &mut s]);
    relay.refuse_next_submits(1);
    a.set(1, "uno");
    a.pump();
    assert!(!a.le.local_settled(), "refused, so not ordered");
    let mut settled = false;
    for _ in 0..40 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        settle(&mut [&mut a, &mut s]);
        if a.le.local_settled() {
            settled = true;
            break;
        }
    }
    assert!(settled, "the resend was never ordered");
    s.le.announce_saved(relay.seq(), "e1").unwrap();
    settle(&mut [&mut a, &mut s]);
    assert!(a.le.edits_saved(), "the refused counter still counts as an unsaved edit");
}

/// An edit the relay refused for good breaks the link and was never ordered: the window still
/// holds an edit the file does not, so it is not saved (the dropped update stays counted).
#[test]
fn an_edit_refused_for_good_is_not_saved() {
    let relay = MemRelay::new();
    let hub = relay.connect("alice", "alice", false);
    let le = LiveEditing::start(hub, ROOM, seed().as_slice(), Access::Edit).unwrap();
    let engine = Engine { doc: seed().into_iter().map(|b| (b.id, b.text)).collect() };
    let mut a = Window { engine, le };
    settle(&mut [&mut a]);
    a.set(1, "uno");
    settle(&mut [&mut a]);
    assert!(matches!(a.le.status(), LiveStatus::Broken(_)), "{:?}", a.le.status());
    assert!(!a.le.edits_saved(), "the refused edit is not in the file");
}

/// A refused-for-now submit's old counter must not make a later rejoin, with nothing to resend,
/// leave the window owing a resend for good.
#[test]
fn a_rejoin_after_a_refused_for_now_submit_leaves_nothing_owed() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut s = Window::join(&relay, SERVICE_UID, Access::Edit);
    settle(&mut [&mut a, &mut s]);
    relay.refuse_next_submits(1);
    a.set(1, "uno");
    a.pump();
    let mut settled = false;
    for _ in 0..40 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        settle(&mut [&mut a, &mut s]);
        if a.le.local_settled() {
            settled = true;
            break;
        }
    }
    assert!(settled, "the resend was never ordered");
    relay.drop_link(a.le.hub());
    relay.restore_link(a.le.hub());
    settle(&mut [&mut a, &mut s]);
    s.le.announce_saved(relay.seq(), "e1").unwrap();
    settle(&mut [&mut a, &mut s]);
    assert!(a.le.local_settled(), "a resend is owed that nobody holds");
    assert!(a.le.edits_saved(), "the old counter still reads as an unsaved edit");
}

/// An edit the relay's snapshot replaces was never ordered: it must not read as saved.
#[test]
fn an_edit_dropped_by_a_snapshot_is_not_saved() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut s = Window::join(&relay, SERVICE_UID, Access::Edit);
    settle(&mut [&mut a, &mut s]);
    s.le.announce_saved(relay.seq(), "e0").unwrap();
    settle(&mut [&mut a, &mut s]);
    relay.refuse_next_submits(5);
    a.set(1, "uno");
    a.pump();
    assert!(!a.le.local_settled(), "the edit is refused, so unordered when the snapshot comes");
    relay.fall_behind(a.le.hub());
    a.pump();
    assert!(matches!(a.le.status(), LiveStatus::Broken(_)), "{:?}", a.le.status());
    assert!(!a.le.edits_saved(), "the dropped edit reads as saved");
}

/// A run whose replica breaks ends as LockLost, owes an unlock sent best effort, and does not
/// wait for `local_settled`.
#[test]
fn a_run_on_a_broken_replica_reports_lock_lost_and_does_not_wait() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("insert", 0.0).unwrap();
    q.on_lock(&granted(1), 1.0);
    assert_eq!(q.poll(1, 2.0), Polled::Run("insert"));
    assert!(q.finish(false).is_empty(), "waits while the replica is sound");
    assert_eq!(q.replica_broken(), vec![Action::SendUnlock]);
    assert_eq!(q.poll(1, 3.0), Polled::LockLost { scope: SCOPE.to_owned() });
    assert!(q.finish(false).is_empty(), "no unlock owed twice");
    assert_eq!(q.poll(1, 4.0), Polled::Nothing);
    assert!(q.request("next", 5.0).is_ok(), "the run ended without local_settled");
}

/// The relay's order for the run's edit has been read but the edit is still in the engine's
/// queue (not flushed): not settled, whatever the order the app calls things in.
#[test]
fn an_unflushed_edit_is_not_settled() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    settle(&mut [&mut a]);
    let before = a.engine.doc.clone();
    a.engine.doc[0].1 = "uno".to_owned();
    a.le.queue().push((before, a.engine.doc.clone()));
    assert!(!a.le.local_settled(), "queued, not sent");
    a.le.flush(&mut a.engine);
    assert!(!a.le.local_settled(), "sent, not ordered");
    a.pump();
    assert!(a.le.local_settled());
}

/// A grant from the old link never survives a rejoin, whichever side of the join it is on in
/// the same poll; one from the new link does.
#[test]
fn a_grant_before_the_join_in_one_poll_is_dropped() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    settle(&mut [&mut a]);
    // Old link's grant, then the rejoin, all unread: one poll gets both.
    relay.inject(a.le.hub(), ServerMsg::LockGranted { doc: ROOM.into(), scope: SCOPE.into(), seq: Some(1), token: None, lease_ms: None });
    relay.restore_link(a.le.hub());
    a.pump();
    assert!(a.le.take_lock_events().is_empty(), "a stale grant survived the rejoin");
    // The new link's grant comes after its join and is kept, in order with what follows.
    relay.restore_link(a.le.hub());
    relay.inject(a.le.hub(), ServerMsg::LockGranted { doc: ROOM.into(), scope: SCOPE.into(), seq: Some(2), token: None, lease_ms: None });
    relay.inject(a.le.hub(), ServerMsg::LockReleased { doc: ROOM.into(), scope: SCOPE.into() });
    a.pump();
    assert_eq!(a.le.take_lock_events(), vec![granted(2), released()]);
}

/// The relay's `lock_released` can overtake its `lock_denied`: the request is asked again.
#[test]
fn a_release_heard_before_the_denial_asks_again() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("x", 0.0).unwrap();
    assert!(q.on_lock(&released(), 1.0).is_empty());
    assert_eq!(q.on_lock(&denied("a~1"), 2.0), vec![Action::SendLock], "the holder had already let go");
    assert_eq!(q.waiting_for(), None);
    assert!(q.on_lock(&denied("a~1"), 3.0).is_empty(), "a second denial is a real one");
    assert_eq!(q.waiting_for(), Some("a~1"));
}

/// A rejoin during a run tells the app once, so it can treat the edit as unprotected.
#[test]
fn a_rejoin_during_a_run_is_reported_once() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("x", 0.0).unwrap();
    q.on_lock(&granted(1), 1.0);
    assert_eq!(q.poll(1, 2.0), Polled::Run("x"));
    assert_eq!(q.poll(1, 3.0), Polled::Nothing);
    assert!(q.rejoined(4.0).is_empty());
    assert_eq!(q.poll(1, 5.0), Polled::LockLost { scope: SCOPE.into() });
    assert_eq!(q.poll(1, 6.0), Polled::Nothing, "told twice");
    assert!(q.finish(true).is_empty(), "no lock to give back");
}

/// Over a real `LiveEditing`: the unlock stays held back while the relay's order for the run's
/// own edit sits unread, whatever else has been heard since the grant.
#[test]
fn unlock_is_held_back_until_the_own_op_is_ordered() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("insert", 0.0).unwrap();
    a.le.lock(SCOPE).unwrap();
    a.pump();
    for e in a.le.take_lock_events() {
        q.on_lock(&e, 1.0);
    }
    let Polled::Run(_) = q.poll(a.le.newest_seq(), 2.0) else { panic!("not granted") };
    a.set(1, "uno");
    // The relay's order for the edit sits unread in A's inbox: not settled, no unlock. Edits
    // under the lock go one submit at a time, so it takes a pump per submit.
    assert!(q.finish(a.le.local_settled()).is_empty());
    let mut pumps = 0;
    while !a.le.local_settled() {
        assert!(q.finish(false).is_empty());
        a.pump();
        pumps += 1;
        assert!(pumps < 8, "the run never settled");
    }
    assert_eq!(q.finish(a.le.local_settled()), vec![Action::SendUnlock]);
}

/// Another peer's `PresenceLock` is `HeldByOther`; this peer's own never is.
#[test]
fn own_presence_lock_is_never_held_by_other() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    let me = peer_of(&relay, &a);
    let them = peer_of(&relay, &b);
    let presence = |holder: &str| ServerMsg::PresenceLock { doc: ROOM.into(), scope: SCOPE.into(), holder: holder.into() };
    relay.inject(a.le.hub(), presence(&me));
    a.pump();
    assert!(a.le.take_lock_events().is_empty(), "this peer waited on itself");
    relay.inject(a.le.hub(), presence(&them));
    a.pump();
    assert_eq!(a.le.take_lock_events(), vec![LockEvent::HeldByOther { scope: SCOPE.into(), holder: them }]);
}

/// Lock events not yet taken belong to the link that said them: a rejoin drops them.
#[test]
fn a_rejoin_drops_untaken_lock_events() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    relay.drop_link(a.le.hub());
    relay.restore_link(a.le.hub());
    settle(&mut [&mut a, &mut b]);
    assert!(a.le.take_lock_events().is_empty(), "a stale grant survived the rejoin");
    // The relay released it with the old link: B can take it now.
    b.le.lock(SCOPE).unwrap();
    settle(&mut [&mut a, &mut b]);
    assert!(matches!(b.le.take_lock_events().last(), Some(LockEvent::Granted { .. })));
}

/// A lock events buffer is emptied by taking it.
#[test]
fn taking_lock_events_empties_them() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    settle(&mut [&mut a]);
    a.le.lock(SCOPE).unwrap();
    settle(&mut [&mut a]);
    assert_eq!(a.le.take_lock_events().len(), 1);
    assert!(a.le.take_lock_events().is_empty());
}

// Fencing: edits made under a fenced grant are ordered only while the grant is current.

fn fenced(seq: u64, token: u64) -> LockEvent {
    LockEvent::Granted { scope: SCOPE.into(), seq: Some(seq), token: Some(token) }
}

fn lost(token: u64) -> LockEvent {
    LockEvent::Lost { scope: SCOPE.into(), token }
}

/// The window's replica, as the lines it holds, read from the replica itself.
fn replica_lines(w: &Window) -> Vec<(u64, String)> {
    let doc = w.le.live().doc().expect("seeded");
    doc.order().into_iter().map(|id| (id, doc.block(id).map(|b| b.text).unwrap_or_default())).collect()
}

/// The fencing token of the grant `w` was just answered with (what came before it, such as
/// somebody else's earlier take, is dropped).
fn take_grant(w: &mut Window) -> u64 {
    let events = w.le.take_lock_events();
    match events.last() {
        Some(LockEvent::Granted { token: Some(token), .. }) => *token,
        _ => panic!("not a fenced grant last: {events:?}"),
    }
}

/// The paused holder: A holds the lock, freezes past its lease, the lease goes to B. A's edit
/// under its old grant is refused by the relay and taken back out of A's replica; B's edit
/// under the new grant is ordered; both windows end on the same document.
#[test]
fn a_paused_holder_whose_lease_went_to_another_is_refused_and_taken_back() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("rename", 0.0).unwrap();
    a.le.lock(SCOPE).unwrap();
    a.pump();
    for e in a.le.take_lock_events() {
        q.on_lock(&e, 1.0);
    }
    let Polled::Run(_) = q.poll(a.le.newest_seq(), 2.0) else { panic!("not granted") };
    let old = relay.holder(SCOPE).expect("A holds it").1;

    // A is frozen: its lease lapses, and B is granted the scope under a newer token.
    relay.lapse(SCOPE);
    b.le.lock(SCOPE).unwrap();
    b.pump();
    let new = take_grant(&mut b);
    assert!(new > old, "the token did not grow: {old} then {new}");

    // A wakes up and makes its run's edit under the old grant: refused, never ordered.
    let seq = relay.seq();
    a.set(1, "alice was here");
    assert_eq!(relay.seq(), seq, "the stale holder's edit was ordered");
    // B's edit under the current grant is ordered.
    b.set(2, "bob was here");
    assert!(relay.seq() > seq, "the holder's edit was not ordered");
    settle(&mut [&mut a, &mut b]);

    let events = a.le.take_lock_events();
    assert!(events.contains(&lost(old)), "A was not told it lost the lock: {events:?}");
    for e in &events {
        q.on_lock(e, 3.0);
    }
    assert_eq!(q.poll(a.le.newest_seq(), 4.0), Polled::LockLost { scope: SCOPE.into() });
    // The unlock is still owed: it ends A's dead fence, and releases nothing at the relay.
    assert_eq!(q.finish(a.le.local_settled()), vec![Action::SendUnlock]);
    a.le.unlock(SCOPE).unwrap();
    settle(&mut [&mut a, &mut b]);
    assert_eq!(relay.holder(SCOPE), Some((peer_of(&relay, &b), new)), "the stale unlock released B's grant");

    // A's replica and engine are back at the room's state, with B's edit and without its own.
    assert_eq!(a.line(1), "one");
    assert_eq!(a.line(2), "bob was here");
    assert_eq!(replica_lines(&a), replica_lines(&b));
    assert!(a.le.local_settled() && a.le.editable());

    // And A edits on from there, free: both windows still agree.
    a.set(1, "uno");
    settle(&mut [&mut a, &mut b]);
    assert_eq!(b.line(1), "uno");
    assert_eq!(replica_lines(&a), replica_lines(&b));
}

/// An edit under the lock that the relay ordered before the grant was lost stays: only what the
/// relay refused is taken back.
#[test]
fn a_refusal_takes_back_only_what_the_relay_did_not_order() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    take_grant(&mut a);
    a.set(1, "first");
    settle(&mut [&mut a, &mut b]);
    relay.lapse(SCOPE);
    b.le.lock(SCOPE).unwrap();
    b.pump();
    take_grant(&mut b);
    a.set(2, "second");
    settle(&mut [&mut a, &mut b]);
    assert_eq!((a.line(1), a.line(2)), ("first", "two"));
    assert_eq!((b.line(1), b.line(2)), ("first", "two"));
    assert_eq!(replica_lines(&a), replica_lines(&b));
}

/// A late unlock under a grant that lapsed and went to somebody else releases nothing.
#[test]
fn a_stale_unlock_is_ignored() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    take_grant(&mut a);
    relay.lapse(SCOPE);
    b.le.lock(SCOPE).unwrap();
    b.pump();
    let token = take_grant(&mut b);
    a.le.unlock(SCOPE).unwrap();
    settle(&mut [&mut a, &mut b]);
    assert_eq!(relay.holder(SCOPE), Some((peer_of(&relay, &b), token)), "a stale unlock released the new holder's lock");
    assert!(!b.le.take_lock_events().contains(&released()), "B was told its lock was released");
    // The holder's own unlock, under its own token, does release it.
    b.le.unlock(SCOPE).unwrap();
    settle(&mut [&mut a, &mut b]);
    assert_eq!(relay.holder(SCOPE), None);
}

/// A run that is still going refreshes its lease at half the lease's length, so a long run does
/// not lose its lock; once the grant is gone the refresh is answered and the run is lost.
#[test]
fn a_long_run_refreshes_its_lease() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    let now = std::sync::Arc::new(std::sync::Mutex::new(0.0_f64));
    let read = now.clone();
    a.le.set_clock(Box::new(move || *read.lock().unwrap()));
    settle(&mut [&mut a, &mut b]);
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("macro", 0.0).unwrap();
    a.le.lock(SCOPE).unwrap();
    a.pump();
    let token = relay.holder(SCOPE).expect("A holds it").1;
    for e in a.le.take_lock_events() {
        q.on_lock(&e, 0.0);
    }
    let Polled::Run(_) = q.poll(a.le.newest_seq(), 0.0) else { panic!("not granted") };

    let half = LEASE_MS as f64 / 2.0;
    *now.lock().unwrap() += half - 1.0;
    a.pump();
    assert!(relay.refreshes().is_empty(), "refreshed before half the lease");
    *now.lock().unwrap() += 2.0;
    a.pump();
    assert_eq!(relay.refreshes(), vec![(SCOPE.to_owned(), token)]);
    *now.lock().unwrap() += half;
    a.pump();
    assert_eq!(relay.refreshes().len(), 2, "the run went on and the lease was not refreshed again");
    assert!(a.le.take_lock_events().is_empty(), "the refresh lost a lock that was held");
    a.set(1, "after a long run");
    settle(&mut [&mut a, &mut b]);
    assert_eq!(b.line(1), "after a long run", "the run's edit under a refreshed lease was not ordered");

    // The lease goes after all: the next refresh says so, and the run is lost.
    relay.lapse(SCOPE);
    *now.lock().unwrap() += half;
    a.pump();
    a.pump();
    let events = a.le.take_lock_events();
    assert_eq!(events, vec![lost(token)]);
    q.on_lock(&events[0], 1.0);
    assert_eq!(q.poll(a.le.newest_seq(), 2.0), Polled::LockLost { scope: SCOPE.into() });
    assert!(a.le.live().fence().is_none(), "a lost grant still fences as if it were held");
    // The run goes on a moment longer (its last frame): its edit never goes out free. It
    // stays under the lost grant and is taken back, so the fence fails closed.
    let seq = relay.seq();
    a.set(2, "the run's last edit");
    settle(&mut [&mut a, &mut b]);
    assert_eq!(relay.seq(), seq, "an edit of a lost run went out");
    assert_eq!((a.line(2), b.line(2)), ("two", "two"));
    assert_eq!(replica_lines(&a), replica_lines(&b));
    // The run's unlock ends the dead fence: edits after it are free again.
    assert_eq!(q.finish(a.le.local_settled()), vec![Action::SendUnlock]);
    a.le.unlock(SCOPE).unwrap();
    a.set(2, "free again");
    settle(&mut [&mut a, &mut b]);
    assert_eq!((b.line(1), b.line(2)), ("after a long run", "free again"));
    assert_eq!(replica_lines(&a), replica_lines(&b));
}

/// Edits without a lock are not fenced, even while another window holds one.
#[test]
fn free_edits_are_not_fenced() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    b.le.lock(SCOPE).unwrap();
    b.pump();
    take_grant(&mut b);
    assert_eq!(a.le.live().fence(), None);
    a.set(1, "free");
    settle(&mut [&mut a, &mut b]);
    assert_eq!(b.line(1), "free");
    assert!(b.le.live().fence().is_some(), "B's grant fences B's edits");
}

/// A fenced run that rejoins keeps going: a rejoin alone does not lose it, the relay's answer
/// to its edits does. What it had ordered before the drop stays; here the relay released the
/// grant with the old link and the scope went to B, so its next edit is refused and taken back.
#[test]
fn a_rejoin_during_a_fenced_run_is_decided_by_the_relay() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("x", 0.0).unwrap();
    a.le.lock(SCOPE).unwrap();
    a.pump();
    for e in a.le.take_lock_events() {
        q.on_lock(&e, 1.0);
    }
    let Polled::Run(_) = q.poll(a.le.newest_seq(), 2.0) else { panic!("not granted") };
    a.set(1, "before the drop");
    settle(&mut [&mut a, &mut b]);
    relay.drop_link(a.le.hub());
    b.pump();
    b.le.lock(SCOPE).unwrap();
    b.pump();
    take_grant(&mut b);
    relay.restore_link(a.le.hub());
    settle(&mut [&mut a, &mut b]);
    assert!(q.rejoined(3.0).is_empty());
    assert_eq!(q.poll(a.le.newest_seq(), 3.0), Polled::Nothing, "a rejoin alone lost a fenced run");
    a.set(2, "after the rejoin");
    settle(&mut [&mut a, &mut b]);
    for e in a.le.take_lock_events() {
        q.on_lock(&e, 4.0);
    }
    assert_eq!(q.poll(a.le.newest_seq(), 5.0), Polled::LockLost { scope: SCOPE.into() });
    assert_eq!((a.line(1), a.line(2)), ("before the drop", "two"));
    assert_eq!((b.line(1), b.line(2)), ("before the drop", "two"));
    assert_eq!(replica_lines(&a), replica_lines(&b));
}

/// `Ordered` and a lost grant: before the run it asks again, during the run the run is lost,
/// and a loss for another grant is ignored.
#[test]
fn a_lost_grant_asks_again_or_loses_the_run() {
    let mut q = Ordered::<&str>::new(SCOPE);
    q.request("x", 0.0).unwrap();
    q.on_lock(&fenced(5, 3), 1.0);
    assert!(q.on_lock(&lost(2), 2.0).is_empty(), "an older grant's loss is not ours");
    assert_eq!(q.on_lock(&lost(3), 2.0), vec![Action::SendUnlock, Action::SendLock], "lost before the run: end the fence, ask again");
    assert_eq!(q.poll(5, 3.0), Polled::Nothing);
    q.on_lock(&fenced(6, 4), 4.0);
    assert_eq!(q.poll(6, 5.0), Polled::Run("x"));
    assert!(q.rejoined(6.0).is_empty());
    assert_eq!(q.poll(6, 6.0), Polled::Nothing, "a rejoin lost a fenced run");
    assert!(q.on_lock(&lost(4), 7.0).is_empty());
    assert_eq!(q.poll(6, 8.0), Polled::LockLost { scope: SCOPE.into() });
    assert_eq!(q.poll(6, 9.0), Polled::Nothing, "told twice");
    assert_eq!(q.finish(true), vec![Action::SendUnlock], "a lost fenced run must still end its fence");
    // A fenced run that was not lost gives its lock back as before.
    q.request("y", 10.0).unwrap();
    q.on_lock(&fenced(7, 5), 11.0);
    assert_eq!(q.poll(7, 12.0), Polled::Run("y"));
    assert_eq!(q.finish(true), vec![Action::SendUnlock]);
}

/// The review's C1: a fenced edit the relay ordered just before the link dropped, its answer
/// lost with the link. The rejoin sends it again under a new counter, which the relay refuses
/// (the drop released the grant and B took it), but the catch-up carries the order of the old
/// counter, which counts: the edit is the room's, and A keeps it. A's replica ends equal to B's.
#[test]
fn a_fenced_edit_ordered_before_the_link_dropped_is_kept() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    take_grant(&mut a);
    // The edit goes out (the relay orders its first submit) and A never hears the answer.
    let seq = relay.seq();
    a.set(1, "ordered");
    assert_eq!(relay.seq(), seq + 1, "not exactly one fenced submit in flight");
    relay.drop_link(a.le.hub());
    b.pump();
    b.le.lock(SCOPE).unwrap();
    b.pump();
    take_grant(&mut b);
    relay.restore_link(a.le.hub());
    settle(&mut [&mut a, &mut b]);
    // The ordered part (the delete) is everybody's, A's included; the rest was taken back.
    assert_eq!(b.line(1), "", "the relay did not order the first submit");
    assert_eq!(a.line(1), "");
    assert_eq!(replica_lines(&a), replica_lines(&b), "A's replica parted from the room's");
    let events = a.le.take_lock_events();
    assert!(events.iter().any(|e| matches!(e, LockEvent::Lost { .. })), "A was not told: {events:?}");
    // And the two stay together from there.
    a.le.unlock(SCOPE).unwrap();
    a.set(2, "after");
    settle(&mut [&mut a, &mut b]);
    assert_eq!(b.line(2), "after");
    assert_eq!(replica_lines(&a), replica_lines(&b));
}

/// The review's I1: an edit refused for now under one grant is never sent under a newer one.
/// A asks again (a new token); the old fence dies, its edit is taken back, and only edits made
/// under the new grant go out under it.
#[test]
fn an_edit_refused_for_now_is_never_sent_under_a_newer_grant() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    let old = take_grant(&mut a);
    relay.refuse_next_submits(1);
    let seq = relay.seq();
    a.set(1, "under the old grant");
    a.pump();
    assert_eq!(relay.seq(), seq);
    // The lease lapses and A asks again before the retry: a newer grant.
    relay.lapse(SCOPE);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    let events = a.le.take_lock_events();
    assert!(events.contains(&lost(old)), "the old grant's loss was not told: {events:?}");
    let new = relay.holder(SCOPE).expect("A holds it again").1;
    assert!(new > old);
    assert_eq!(a.le.live().fence().map(|f| f.token), Some(new));
    settle(&mut [&mut a, &mut b]);
    assert_eq!(relay.seq(), seq, "the old grant's edit was sent under the new one");
    assert_eq!((a.line(1), b.line(1)), ("one", "one"));
    assert_eq!(replica_lines(&a), replica_lines(&b));
    // An edit made under the new grant is ordered.
    a.set(1, "under the new grant");
    settle(&mut [&mut a, &mut b]);
    assert_eq!(b.line(1), "under the new grant");
    assert_eq!(replica_lines(&a), replica_lines(&b));
}

/// The review's I3: one fenced submit at a time. While the first is unanswered (here refused
/// for now) nothing after it goes out, so a refusal can never leave a later edit ordered behind
/// a hole. When the grant is gone by the retry, the whole edit is taken back, and A's next free
/// edits (which reuse the refused clock range) do not bring any of it back.
#[test]
fn only_one_fenced_submit_is_in_flight() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    take_grant(&mut a);
    relay.refuse_next_submits(1);
    let seq = relay.seq();
    a.set(1, "refused");
    a.set(2, "queued behind it");
    a.pump();
    assert_eq!(relay.seq(), seq, "an edit went out behind an unanswered one");
    relay.lapse(SCOPE);
    b.le.lock(SCOPE).unwrap();
    b.pump();
    take_grant(&mut b);
    // The backoff runs out: the retry goes under the old grant and is refused.
    std::thread::sleep(std::time::Duration::from_millis(300));
    settle(&mut [&mut a, &mut b]);
    assert_eq!(relay.seq(), seq, "an edit of the lost run was ordered");
    assert_eq!((a.line(1), a.line(2)), ("one", "two"));
    assert_eq!(replica_lines(&a), replica_lines(&b));
    a.le.unlock(SCOPE).unwrap();
    a.set(1, "x");
    a.set(2, "y");
    settle(&mut [&mut a, &mut b]);
    assert_eq!((b.line(1), b.line(2)), ("x", "y"));
    assert_eq!(replica_lines(&a), replica_lines(&b));
}

/// The review's M3: an unlock while an edit under the lock is unanswered waits for it, so the
/// holder's own last edit is never refused because the unlock overtook it.
#[test]
fn an_unlock_waits_for_the_edits_under_the_lock() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    take_grant(&mut a);
    a.set(1, "last edit");
    a.le.unlock(SCOPE).unwrap();
    assert!(relay.holder(SCOPE).is_some(), "the unlock overtook an unanswered edit");
    // An edit after the unlock waits for the fence to end, then goes free.
    a.set(2, "after the unlock");
    settle(&mut [&mut a, &mut b]);
    assert_eq!(relay.holder(SCOPE), None);
    assert_eq!((b.line(1), b.line(2)), ("last edit", "after the unlock"));
    assert!(a.le.take_lock_events().iter().all(|e| !matches!(e, LockEvent::Lost { .. })));
    assert_eq!(replica_lines(&a), replica_lines(&b));
}

/// The review's M4: a replaced document ends the fence with [`LockEvent::Lost`].
#[test]
fn a_replaced_document_loses_the_fence() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    settle(&mut [&mut a]);
    a.le.lock(SCOPE).unwrap();
    a.pump();
    let token = take_grant(&mut a);
    relay.fall_behind(a.le.hub());
    a.pump();
    assert!(a.le.take_lock_events().contains(&lost(token)));
    assert!(a.le.live().fence().is_none());
}
