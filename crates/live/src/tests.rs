//! The session pump against the in-memory relay, with the smallest app there is: a list of
//! lines, one [`LiveDoc`] block each, whose edits replace a line's whole text. These guard what
//! every app inherits; WordCraft's own pump tests guard its mapping on top.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use craft_collab::{BlockChange, Hub, LiveDoc, SeedBlock};
use serde_json::{Map, Value, json};

use crate::testing::{MemHub, MemRelay, ROOM};
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
struct Window<H: Hub = MemHub> {
    engine: Engine,
    le: LiveEditing<H, Lines>,
}

impl Window {
    fn join(relay: &MemRelay, uid: &str, access: Access) -> Window {
        let hub = relay.connect(uid, uid, access == Access::Edit);
        let le = LiveEditing::start(hub, ROOM, seed().as_slice(), access).unwrap();
        let engine = Engine { doc: seed().into_iter().map(|b| (b.id, b.text)).collect() };
        Window { engine, le }
    }
}

impl<H: Hub> Window<H> {
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

fn settle<H: Hub>(windows: &mut [&mut Window<H>]) {
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

/// LP2: what is typed while the link is down, and while it rejoins, stays in the window and
/// reaches the room after the rejoin, in order, and the two windows converge.
#[test]
fn typing_while_the_link_is_down_or_rejoining_is_kept_and_shared() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    let mut b = Window::join(&relay, "bob", Access::Edit);
    settle(&mut [&mut a, &mut b]);
    a.set(1, "E2x8");
    settle(&mut [&mut a, &mut b]);
    assert_eq!(b.line(1), "E2x8");

    relay.drop_link(a.le.hub());
    a.pump();
    assert_eq!(a.le.status(), &LiveStatus::Reconnecting);
    assert!(a.le.editable(), "a window that was live takes typing while it reconnects");
    let mut typed = String::from("E2x8");
    for c in "2046".chars() {
        typed.push(c);
        a.set(1, &typed);
        a.pump();
        assert_eq!(a.line(1), typed, "each keystroke stays in the window");
    }
    assert!(a.le.take_notices().iter().all(|n| !n.contains("paused")), "no keystroke was refused");
    assert!(!a.le.edits_saved(), "the queued edits are not held by any save");

    // The socket is back but the relay has not answered the join yet (a real relay answers
    // after its bus calls): typing still lands.
    relay.restore_link(a.le.hub());
    typed.push('0');
    a.set(1, &typed);
    assert_eq!(a.line(1), typed);
    settle(&mut [&mut a, &mut b]);
    assert!(matches!(a.le.status(), LiveStatus::Live { .. }), "{:?}", a.le.status());
    assert_eq!(a.line(1), "E2x820460");
    assert_eq!(b.line(1), "E2x820460", "every keystroke reached the other window");
    assert_eq!((a.line(1), a.line(2)), (b.line(1), b.line(2)));
}

/// LP2: a window that never was live still refuses typing while it connects (its first
/// catch-up may replace the replica), and a refused or broken link takes none.
#[test]
fn typing_is_still_refused_before_the_first_join() {
    let relay = MemRelay::new();
    let mut a = Window::join(&relay, "alice", Access::Edit);
    relay.drop_link(a.le.hub());
    a.pump();
    assert_eq!(a.le.status(), &LiveStatus::Connecting);
    assert!(!a.le.editable());
    a.set(1, "early");
    assert_eq!(a.line(1), "one");
}

/// LP2 goal 2: a renewed ticket opens a second link and the window switches to it once it
/// has joined. The window never leaves Live, what is typed before, during and after the
/// switch reaches the other window, and the old connection leaves the room.
#[test]
fn a_renewed_ticket_switches_links_without_a_drop() {
    use craft_collab::Handover;
    let relay = MemRelay::new();
    let opener = relay.clone();
    let first = relay.connect("alice", "Alice", true);
    let hub = Handover::new(first.clone(), move |_ticket: &str| Ok(opener.connect("alice", "Alice", true)));
    let le = LiveEditing::start(hub, ROOM, seed().as_slice(), Access::Edit).unwrap();
    let mut a = Window { engine: Engine { doc: seed().into_iter().map(|b| (b.id, b.text)).collect() }, le };
    let mut b = Window::join(&relay, "bob", Access::Edit);
    let step = |a: &mut Window<Handover<MemHub>>, b: &mut Window| {
        a.pump();
        b.pump();
        assert!(matches!(a.le.status(), LiveStatus::Live { .. }), "never reconnecting: {:?}", a.le.status());
    };
    for _ in 0..4 {
        a.pump();
        b.pump();
    }
    a.set(1, "G1");
    step(&mut a, &mut b);

    a.le.hub_mut().set_token("renewed".into());
    assert_eq!(relay.tokens(&first), ["renewed"], "the old link reconnects with it if it drops first");
    assert!(a.le.hub().switching());
    a.set(1, "G15");
    let mut typed = String::from("G15");
    for c in "80933.".chars() {
        step(&mut a, &mut b);
        typed.push(c);
        a.set(1, &typed);
    }
    for _ in 0..4 {
        step(&mut a, &mut b);
    }
    assert_eq!(a.le.hub().switches(), 1);
    assert!(!first.connected(), "the old link was closed");
    assert_eq!(a.line(1), "G1580933.");
    assert_eq!(b.line(1), "G1580933.", "nothing typed around the switch was lost");
    let peers = b.pump();
    assert_eq!(peers.len(), 1, "one Alice in the room after the switch: {peers:?}");
    assert!(a.le.take_notices().iter().all(|n| !n.contains("econnect")), "the user saw no reconnect");
}
