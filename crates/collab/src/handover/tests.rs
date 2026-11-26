#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::cell::RefCell;
use std::rc::Rc;

use super::*;

/// A scripted link: what it sent, what it will deliver, whether it is up or closed.
#[derive(Default)]
struct Link {
    ticket: String,
    applied: Option<u64>,
    sent: Vec<ClientMsg>,
    inbox: Vec<ServerMsg>,
    up: bool,
    closed: bool,
    tickets: Vec<String>,
    /// Every space it was told to name, oldest first.
    spaces: Vec<u32>,
    /// What it fires when something arrives (`Hub::set_wake`).
    wake: Option<crate::Wake>,
}

#[derive(Clone)]
struct Fake(Rc<RefCell<Link>>);

impl Hub for Fake {
    fn send(&mut self, msg: ClientMsg) -> Result<()> {
        self.0.borrow_mut().sent.push(msg);
        Ok(())
    }
    fn poll(&mut self) -> Vec<ServerMsg> {
        std::mem::take(&mut self.0.borrow_mut().inbox)
    }
    fn connected(&self) -> bool {
        let l = self.0.borrow();
        l.up && !l.closed
    }
    fn set_token(&mut self, token: String) {
        self.0.borrow_mut().tickets.push(token);
    }
    fn set_space(&mut self, space: u32) {
        self.0.borrow_mut().spaces.push(space);
    }
    fn close(&mut self) {
        self.0.borrow_mut().closed = true;
    }
    fn set_wake(&mut self, wake: &crate::Wake) {
        self.0.borrow_mut().wake = Some(wake.clone());
    }
}

impl Fake {
    /// Queue `msgs` and, like the web hub's handlers, fire the wake it was given.
    fn deliver(&self, msgs: impl IntoIterator<Item = ServerMsg>) {
        self.0.borrow_mut().inbox.extend(msgs);
        let wake = self.0.borrow().wake.clone();
        if let Some(wake) = wake {
            wake.fire();
        }
    }
}

fn link(ticket: &str, applied: Option<u64>) -> Fake {
    Fake(Rc::new(RefCell::new(Link { ticket: ticket.to_owned(), applied, up: true, ..Link::default() })))
}

/// A join of room `doc` at baseline `seq`, its replay ending at `until`.
fn joined_at(doc: &str, peer: &str, seq: u64, until: u64) -> ServerMsg {
    ServerMsg::Joined {
        doc: doc.into(),
        peer: peer.into(),
        seq,
        space: 2,
        roster: Vec::new(),
        locks: Vec::new(),
        replay_until: Some(until),
        fallback: false,
        text_cap: None,
        heartbeat: false,
        history_lost: false,
    }
}

fn joined(peer: &str) -> ServerMsg {
    joined_at("d", peer, 0, 0)
}

fn op(seq: u64) -> ServerMsg {
    ServerMsg::Op { doc: "d".into(), seq, origin: "o".into(), local: None, command: "c".into(), params: serde_json::Value::Null, scope: None }
}

fn presence() -> ServerMsg {
    ServerMsg::Presence { doc: "d".into(), roster: Vec::new() }
}

fn submit() -> ClientMsg {
    ClientMsg::Ephemeral { doc: "d".into(), kind: "cursor".into(), payload: serde_json::Value::Null }
}

/// The opener hands out scripted links and remembers them.
fn hub(first: &Fake) -> (Handover<Fake>, Rc<RefCell<Vec<Fake>>>) {
    let opened: Rc<RefCell<Vec<Fake>>> = Rc::default();
    let made = Rc::clone(&opened);
    let h = Handover::new(first.clone(), move |r: &Reopen<'_>| {
        let l = link(r.ticket, r.applied);
        made.borrow_mut().push(l.clone());
        Ok(l)
    });
    (h, opened)
}

/// A hub whose current link joined room `d` and delivered its catch-up.
fn joined_hub() -> (Fake, Handover<Fake>, Rc<RefCell<Vec<Fake>>>) {
    let first = link("t1", None);
    let (mut h, opened) = hub(&first);
    first.deliver([joined("p1")]);
    h.poll();
    (first, h, opened)
}

/// A renewed ticket opens a second link; the first stays in use, sending included, until the
/// second has joined; then the hub switches and closes the first. It never reads as
/// disconnected.
#[test]
fn a_renewed_ticket_switches_links_once_the_new_one_joined() {
    let (first, mut h, opened) = joined_hub();
    h.set_token("t2".into());
    assert_eq!(first.0.borrow().tickets, ["t2"], "the current link reconnects with it if it drops first");
    let second = opened.borrow()[0].clone();
    assert_eq!(second.0.borrow().ticket, "t2");
    assert!(h.switching() && h.connected());

    h.send(submit()).unwrap();
    assert_eq!(first.0.borrow().sent.len(), 1, "sent on the current link while the new one joins");
    first.deliver([presence()]);
    second.deliver([presence()]);
    assert_eq!(h.poll().len(), 1, "nothing of the new link before its join");
    assert!(!first.0.borrow().closed && h.switching());

    first.deliver([presence()]);
    second.deliver([joined("p2"), presence()]);
    let got = h.poll();
    assert_eq!(got.len(), 4, "the old link's last message, then the new link's join and what it held: {got:?}");
    assert!(matches!(got[1], ServerMsg::Joined { .. }));
    assert!(first.0.borrow().closed, "the old link is closed after the switch");
    assert!(!h.switching() && h.connected());
    assert_eq!(h.switches(), 1);
    h.send(submit()).unwrap();
    assert_eq!(second.0.borrow().sent.len(), 1, "sent on the new link after the switch");
    assert_eq!(first.0.borrow().sent.len(), 1);
}

/// LP2 fix 1, M1: what the new link received before its `joined` is delivered after it, never
/// ahead of it (the session's join resets its position and would forget it).
#[test]
fn the_new_links_join_comes_before_what_it_received_earlier() {
    let (_first, mut h, opened) = joined_hub();
    h.set_token("t2".into());
    let second = opened.borrow()[0].clone();
    second.deliver([op(7), presence(), joined_at("d", "p2", 5, 6)]);
    let got = h.poll();
    assert!(matches!(got.first(), Some(ServerMsg::Joined { .. })), "{got:?}");
    assert_eq!(got.len(), 3);
    assert!(matches!(got[1], ServerMsg::Op { seq: 7, .. }));
}

/// A refused new link is closed and its refusal kept from the session; the current link
/// carries on.
#[test]
fn a_refused_new_link_is_dropped_and_the_current_one_kept() {
    let (first, mut h, opened) = joined_hub();
    h.set_token("bad".into());
    let second = opened.borrow()[0].clone();
    second.deliver([ServerMsg::Error { doc: String::new(), detail: "token rejected".into(), local: None, code: Some(crate::REFUSED_AUTH.into()) }]);
    assert!(h.poll().is_empty(), "the refusal is not the current link's");
    assert!(second.0.borrow().closed && !h.switching());
    assert!(h.connected() && !first.0.borrow().closed);
}

/// LP2 fix 1, M4: a new link that joined another room is closed; the current link carries on.
#[test]
fn a_new_link_in_another_room_is_dropped() {
    let (first, mut h, opened) = joined_hub();
    h.set_token("t2".into());
    let second = opened.borrow()[0].clone();
    second.deliver([joined_at("other", "p2", 0, 0)]);
    assert!(h.poll().is_empty(), "nothing of the other room reaches the session");
    assert!(second.0.borrow().closed && !h.switching() && h.switches() == 0);
    assert!(h.connected() && !first.0.borrow().closed);
}

/// LP2 fix 1, M8: the new link's own errors (a join the relay retries) never reach the session,
/// so they never pile up as notices after the switch.
#[test]
fn the_new_links_errors_are_not_delivered() {
    let (_first, mut h, opened) = joined_hub();
    h.set_token("t2".into());
    let second = opened.borrow()[0].clone();
    for _ in 0..3 {
        second.deliver([ServerMsg::Error {
            doc: "d".into(),
            detail: "the relay could not start a session".into(),
            local: None,
            code: Some(crate::RETRY.into()),
        }]);
        assert!(h.poll().is_empty());
    }
    second.deliver([joined("p2")]);
    let got = h.poll();
    assert_eq!(got.len(), 1, "only the join: {got:?}");
    assert_eq!(h.switches(), 1);
}

/// While not connected the current link only takes the ticket for its reconnect; a second
/// renewal replaces a link that has not joined yet.
#[test]
fn a_ticket_while_down_or_switching() {
    let (first, mut h, opened) = joined_hub();
    first.0.borrow_mut().up = false;
    h.set_token("t2".into());
    assert!(opened.borrow().is_empty() && !h.switching());
    assert_eq!(first.0.borrow().tickets, ["t2"]);
    first.0.borrow_mut().up = true;
    h.set_token("t3".into());
    h.set_token("t4".into());
    assert_eq!(opened.borrow().len(), 2);
    assert!(opened.borrow()[0].0.borrow().closed, "the link opened with t3 was given up");
    assert_eq!(opened.borrow()[1].0.borrow().ticket, "t4");
}

/// LP2 fix 1, I1: the kept id space reaches the link in use, the link being opened, and every
/// link opened later, so each rejoin names it.
#[test]
fn the_kept_space_reaches_every_link() {
    let (first, mut h, opened) = joined_hub();
    h.set_space(9);
    assert_eq!(first.0.borrow().spaces, [9]);
    h.set_token("t2".into());
    let second = opened.borrow()[0].clone();
    assert_eq!(second.0.borrow().spaces, [9], "a link opened later names it");
    h.set_space(11);
    assert_eq!(second.0.borrow().spaces, [9, 11], "the link being opened names a new one");
    assert_eq!(first.0.borrow().spaces, [9, 11]);
    second.deliver([joined("p2")]);
    h.poll();
    h.set_token("t3".into());
    assert_eq!(opened.borrow()[1].0.borrow().spaces, [11]);
}

/// LP2 fix 1, M2: the new link resumes at the sequence the current one delivered in order, so
/// its catch-up is only what that one has not delivered. Not while the current link is still
/// catching up, nor after a text catch-up: it joins at the saved sequence then.
#[test]
fn the_new_link_resumes_where_the_current_one_stands() {
    let first = link("t1", None);
    let (mut h, opened) = hub(&first);
    first.deliver([joined_at("d", "p1", 3, 5), op(4)]);
    h.poll();
    h.set_token("mid".into());
    assert_eq!(opened.borrow()[0].0.borrow().applied, None, "mid catch-up");
    first.deliver([op(5), op(7)]);
    h.poll();
    h.set_token("t2".into());
    assert_eq!(opened.borrow()[1].0.borrow().applied, Some(5), "in order only: 7 is past a gap");
    first.deliver([op(6)]);
    h.poll();
    h.set_token("t3".into());
    assert_eq!(opened.borrow()[2].0.borrow().applied, Some(7));

    // After the switch the position is the new link's own.
    let third = opened.borrow()[2].clone();
    third.deliver([joined_at("d", "p3", 7, 8), op(8)]);
    h.poll();
    h.set_token("t4".into());
    assert_eq!(opened.borrow()[3].0.borrow().applied, Some(8));

    // A text catch-up claims nothing until the next join.
    let fourth = opened.borrow()[3].clone();
    let ServerMsg::Joined { doc, peer, seq, space, roster, locks, replay_until, text_cap, heartbeat, history_lost, .. } = joined_at("d", "p4", 8, 8)
    else {
        panic!("a join")
    };
    fourth.deliver([
        ServerMsg::Joined { doc, peer, seq, space, roster, locks, replay_until, fallback: true, text_cap, heartbeat, history_lost },
        op(9),
    ]);
    h.poll();
    h.set_token("t5".into());
    assert_eq!(opened.borrow()[4].0.borrow().applied, None);
}

/// LI2 (PERF2 on the combined line): the app is woken by the link in use, by a link opened with
/// a renewed ticket (before and after it joined), and by the link after a switch.
#[test]
fn the_wake_reaches_every_link() {
    let (first, mut h, opened) = joined_hub();
    let fired = Rc::new(std::cell::Cell::new(0));
    let count = Rc::clone(&fired);
    let wake = crate::Wake::default();
    wake.set(move || count.set(count.get() + 1));
    h.set_wake(&wake);
    first.deliver([op(1)]);
    assert_eq!(fired.get(), 1, "the link in use wakes the app");
    h.set_token("t2".into());
    let second = opened.borrow()[0].clone();
    second.deliver([presence()]);
    assert_eq!(fired.get(), 2, "the link being opened wakes the app before it joined");
    second.deliver([joined("p2")]);
    assert_eq!(fired.get(), 3, "and with its join, so the switch is not left to a timer");
    h.poll();
    assert_eq!(h.switches(), 1);
    second.deliver([op(2)]);
    assert_eq!(fired.get(), 4, "the new link in use wakes the app");
    h.set_token("t3".into());
    let third = opened.borrow()[1].clone();
    third.deliver([presence()]);
    assert_eq!(fired.get(), 5, "a link opened after the wake was set gets it too");
}
