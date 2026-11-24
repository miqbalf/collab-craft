//! A hub that takes a renewed ticket without dropping the link.
//!
//! The relay reads a socket's ticket once, at connect, and drops the socket when that ticket
//! expires. A fresh ticket given to a hub with [`Hub::set_token`] is only used for the next
//! connect, so a connected hub still loses its link at the old ticket's expiry and rejoins a
//! moment later. [`Handover`] opens a second link with the fresh ticket instead, keeps the first
//! one in use while the second joins, and switches over on the second one's `joined`: the
//! first is closed only then. The relay already lets one user hold two connections to a room
//! for that moment (a reconnect does the same), and the switch is an ordinary rejoin to
//! [`crate::Live`], which sends its unconfirmed edits again. Nothing on the wire changes.
//!
//! When the second link is refused (a bad ticket, a full room) it is closed and the first one
//! carries on; it reconnects with the fresh ticket when it drops, as before.

use crate::error::Result;
use crate::hub::Hub;
use crate::proto::{ClientMsg, ServerMsg};

/// Opens a link to the same relay and room with a ticket.
pub type Opener<H> = Box<dyn FnMut(&str) -> Result<H>>;

/// The link being opened with a renewed ticket.
struct Next<H> {
    hub: H,
    /// What it received before its `joined`, delivered after the switch.
    held: Vec<ServerMsg>,
}

/// A [`Hub`] that switches to a new link when given a new ticket while connected.
pub struct Handover<H: Hub> {
    active: H,
    next: Option<Next<H>>,
    open: Opener<H>,
    switches: u64,
}

impl<H: Hub> Handover<H> {
    /// `active` is the connected hub; `open` connects another to the same relay and room with
    /// a ticket (the same call that made `active`, without `applied`).
    pub fn new(active: H, open: impl FnMut(&str) -> Result<H> + 'static) -> Handover<H> {
        Handover { active, next: None, open: Box::new(open), switches: 0 }
    }

    /// Whether a link opened with a renewed ticket has not joined yet.
    pub fn switching(&self) -> bool {
        self.next.is_some()
    }

    /// How many times the hub switched to a new link.
    pub fn switches(&self) -> u64 {
        self.switches
    }

    /// The link in use.
    pub fn active(&self) -> &H {
        &self.active
    }

    /// Give up on the link being opened.
    fn abandon(&mut self) {
        if let Some(mut next) = self.next.take() {
            next.hub.close();
        }
    }
}

impl<H: Hub> Hub for Handover<H> {
    fn send(&mut self, msg: ClientMsg) -> Result<()> {
        self.active.send(msg)
    }

    fn poll(&mut self) -> Vec<ServerMsg> {
        let mut out = self.active.poll();
        let Some(next) = self.next.as_mut() else { return out };
        let mut joined = false;
        let mut refused = None;
        for msg in next.hub.poll() {
            if let Some(code) = msg.refuses_connection() {
                refused = Some(code.to_owned());
                break;
            }
            joined |= matches!(msg, ServerMsg::Joined { .. });
            next.held.push(msg);
        }
        if let Some(code) = refused {
            log::warn!("collab: the relay refused the link opened with a renewed ticket ({code}); keeping the current one");
            self.abandon();
            return out;
        }
        if joined && let Some(next) = self.next.take() {
            log::info!("collab: switched to the link opened with a renewed ticket");
            let mut old = std::mem::replace(&mut self.active, next.hub);
            old.close();
            drop(old);
            self.switches += 1;
            // Everything the old link delivered is in already; the new link's join replays
            // from the saved baseline, so nothing between the two is missed.
            out.extend(next.held);
        }
        out
    }

    fn connected(&self) -> bool {
        self.active.connected()
    }

    /// Use `token` from now on. While connected, a second link is opened with it and the hub
    /// switches to it once it has joined; the current link also takes it for its own
    /// reconnect, in case it drops first. While not connected, the current link reconnects
    /// with it.
    fn set_token(&mut self, token: String) {
        self.active.set_token(token.clone());
        self.abandon();
        if !self.active.connected() {
            return;
        }
        match (self.open)(&token) {
            Ok(hub) => self.next = Some(Next { hub, held: Vec::new() }),
            Err(e) => log::warn!("collab: a link with the renewed ticket could not be opened: {e}"),
        }
    }

    fn close(&mut self) {
        self.abandon();
        self.active.close();
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    /// A scripted link: what it sent, what it will deliver, whether it is up or closed.
    #[derive(Default)]
    struct Link {
        ticket: String,
        sent: Vec<ClientMsg>,
        inbox: Vec<ServerMsg>,
        up: bool,
        closed: bool,
        tickets: Vec<String>,
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
        fn close(&mut self) {
            self.0.borrow_mut().closed = true;
        }
    }

    fn link(ticket: &str) -> Fake {
        Fake(Rc::new(RefCell::new(Link { ticket: ticket.to_owned(), up: true, ..Link::default() })))
    }

    fn joined(peer: &str) -> ServerMsg {
        ServerMsg::Joined {
            doc: "d".into(),
            peer: peer.into(),
            seq: 0,
            space: 2,
            roster: Vec::new(),
            locks: Vec::new(),
            replay_until: None,
            fallback: false,
            text_cap: None,
            heartbeat: false,
            history_lost: false,
        }
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
        let h = Handover::new(first.clone(), move |ticket: &str| {
            let l = link(ticket);
            made.borrow_mut().push(l.clone());
            Ok(l)
        });
        (h, opened)
    }

    /// A renewed ticket opens a second link; the first stays in use, sending included, until
    /// the second has joined; then the hub switches and closes the first. It never reads as
    /// disconnected.
    #[test]
    fn a_renewed_ticket_switches_links_once_the_new_one_joined() {
        let first = link("t1");
        let (mut h, opened) = hub(&first);
        h.set_token("t2".into());
        assert_eq!(first.0.borrow().tickets, ["t2"], "the current link reconnects with it if it drops first");
        let second = opened.borrow()[0].clone();
        assert_eq!(second.0.borrow().ticket, "t2");
        assert!(h.switching() && h.connected());

        h.send(submit()).unwrap();
        assert_eq!(first.0.borrow().sent.len(), 1, "sent on the current link while the new one joins");
        first.0.borrow_mut().inbox.push(presence());
        assert_eq!(h.poll().len(), 1);
        assert!(!first.0.borrow().closed);

        first.0.borrow_mut().inbox.push(presence());
        second.0.borrow_mut().inbox.extend([joined("p2"), presence()]);
        let got = h.poll();
        assert_eq!(got.len(), 3, "the old link's last messages, then the new link's join and what followed");
        assert!(matches!(got[1], ServerMsg::Joined { .. }));
        assert!(first.0.borrow().closed, "the old link is closed after the switch");
        assert!(!h.switching() && h.connected());
        assert_eq!(h.switches(), 1);
        h.send(submit()).unwrap();
        assert_eq!(second.0.borrow().sent.len(), 1, "sent on the new link after the switch");
        assert_eq!(first.0.borrow().sent.len(), 1);
    }

    /// A refused new link is closed and its refusal kept from the session; the current link
    /// carries on.
    #[test]
    fn a_refused_new_link_is_dropped_and_the_current_one_kept() {
        let first = link("t1");
        let (mut h, opened) = hub(&first);
        h.set_token("bad".into());
        let second = opened.borrow()[0].clone();
        second.0.borrow_mut().inbox.push(ServerMsg::Error {
            doc: String::new(),
            detail: "token rejected".into(),
            local: None,
            code: Some(crate::REFUSED_AUTH.into()),
        });
        assert!(h.poll().is_empty(), "the refusal is not the current link's");
        assert!(second.0.borrow().closed && !h.switching());
        assert!(h.connected() && !first.0.borrow().closed);
    }

    /// While not connected the current link only takes the ticket for its reconnect; a second
    /// renewal replaces a link that has not joined yet.
    #[test]
    fn a_ticket_while_down_or_switching() {
        let first = link("t1");
        let (mut h, opened) = hub(&first);
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
}
