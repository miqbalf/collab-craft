//! A hub that takes a renewed ticket without dropping the link.
//!
//! The relay reads a socket's ticket once, at connect, and drops the socket when that ticket
//! expires. A fresh ticket given to a hub with [`Hub::set_token`] is only used for the next
//! connect, so a connected hub still loses its link at the old ticket's expiry and rejoins a
//! moment later. [`Handover`] opens a second link with the fresh ticket instead, keeps the first
//! one in use while the second joins, and switches over on the second one's `joined`: the
//! first is closed only then. The relay already lets one user hold two connections to a room
//! for that moment (a reconnect does the same), and the switch is an ordinary rejoin to
//! [`crate::Live`], which keeps its id space and sends its unconfirmed edits again. Nothing on
//! the wire changes.
//!
//! The new link names the id space the replica keeps ([`Hub::set_space`]), as every rejoin
//! must, and the sequence the current link has applied in order (handshake `applied`), so its
//! catch-up is only what the current link has not delivered yet rather than everything since
//! the last save. It joins at the saved sequence instead while the current link is still
//! catching up, or caught up from text: what it applied is not yet a complete position then.
//!
//! When the second link is refused (a bad ticket, a full room) or joins another room, it is
//! closed and the first one carries on; it reconnects with the fresh ticket when it drops, as
//! before.

use std::collections::BTreeSet;

use crate::error::Result;
use crate::hub::Hub;
use crate::proto::{ClientMsg, DocId, ServerMsg};

/// What a new link is opened with: the renewed ticket, the sequence to resume at (handshake
/// `applied`, `None` to join at the saved sequence) and the id space the replica keeps (the
/// [`Handover`] also sets it on the new hub with [`Hub::set_space`] right after opening it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reopen<'a> {
    pub ticket: &'a str,
    pub applied: Option<u64>,
    pub space: Option<u32>,
}

/// Opens a link to the same relay and room.
pub type Opener<H> = Box<dyn FnMut(&Reopen<'_>) -> Result<H>>;

/// The link being opened with a renewed ticket.
struct Next<H> {
    hub: H,
    /// Its `joined`, delivered first at the switch.
    joined: Option<ServerMsg>,
    /// What it received besides, delivered after its `joined`.
    held: Vec<ServerMsg>,
}

/// Most ops held ahead of a gap while the in-order position is worked out; past it no position
/// is claimed until the next join.
const AHEAD_MAX: usize = 4096;

/// Where the link in use stands: its room, and the highest sequence it delivered in order since
/// its last `joined`, once its catch-up is complete.
#[derive(Default)]
struct Position {
    room: Option<DocId>,
    /// The in-order sequence delivered; `None` when no position can be claimed.
    applied: Option<u64>,
    /// The last sequence of the join's ordered catch-up.
    until: u64,
    /// Ops past a gap, by sequence.
    ahead: BTreeSet<u64>,
}

impl Position {
    fn see(&mut self, msg: &ServerMsg) {
        if let ServerMsg::Joined { doc, seq, replay_until, fallback, .. } = msg {
            // A catch-up as text (or from a relay that names no end) carries no sequence:
            // no position is claimed until the next join.
            *self = Position {
                room: Some(doc.clone()),
                applied: (!fallback && replay_until.is_some()).then_some(*seq),
                until: replay_until.unwrap_or(u64::MAX),
                ahead: BTreeSet::new(),
            };
            return;
        }
        if self.room.as_deref() != Some(msg.doc()) {
            return;
        }
        let Some(applied) = self.applied else { return };
        match msg {
            ServerMsg::Op { seq, .. } if *seq > applied => {
                self.ahead.insert(*seq);
                let mut at = applied;
                while self.ahead.remove(&(at + 1)) {
                    at += 1;
                }
                self.applied = Some(at);
                if self.ahead.len() > AHEAD_MAX {
                    self.applied = None;
                }
            }
            ServerMsg::Snapshot { seq, .. } => {
                self.ahead.retain(|&s| s > *seq);
                self.applied = Some(applied.max(*seq));
            }
            _ => {}
        }
    }

    /// The sequence a new link may resume at: the in-order one, once the catch-up is in.
    fn claim(&self) -> Option<u64> {
        self.applied.filter(|&a| a >= self.until)
    }
}

/// A [`Hub`] that switches to a new link when given a new ticket while connected.
pub struct Handover<H: Hub> {
    active: H,
    next: Option<Next<H>>,
    open: Opener<H>,
    switches: u64,
    /// The id space the replica keeps, named by every link from now on.
    space: Option<u32>,
    at: Position,
}

impl<H: Hub> Handover<H> {
    /// `active` is the connected hub; `open` connects another to the same relay and room (the
    /// same call that made `active`, with the [`Reopen`] values).
    pub fn new(active: H, open: impl FnMut(&Reopen<'_>) -> Result<H> + 'static) -> Handover<H> {
        Handover { active, next: None, open: Box::new(open), switches: 0, space: None, at: Position::default() }
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
        let out = self.active.poll();
        for msg in &out {
            self.at.see(msg);
        }
        let mut out = out;
        let Some(next) = self.next.as_mut() else { return out };
        let mut give_up = None;
        for msg in next.hub.poll() {
            if let Some(code) = msg.refuses_connection() {
                give_up = Some(format!("the relay refused it ({code})"));
                break;
            }
            match msg {
                ServerMsg::Joined { ref doc, .. } if self.at.room.as_ref().is_some_and(|room| room != doc) => {
                    give_up = Some(format!("it joined {doc}, not {}", self.at.room.as_deref().unwrap_or_default()));
                    break;
                }
                ServerMsg::Joined { .. } if next.joined.is_none() => next.joined = Some(msg),
                // The relay's answer to a message of this link's own, which sends none before
                // the switch: a transient failure to join that it retries. Not the person's.
                ServerMsg::Error { local: None, ref detail, ref code, .. } => {
                    log::debug!("collab: the link opened with a renewed ticket reported {code:?}: {detail}");
                }
                msg => next.held.push(msg),
            }
        }
        if let Some(why) = give_up {
            log::warn!("collab: dropped the link opened with a renewed ticket, {why}; keeping the current one");
            self.abandon();
            return out;
        }
        if next.joined.is_some()
            && let Some(Next { hub, joined: Some(joined), held }) = self.next.take()
        {
            log::info!("collab: switched to the link opened with a renewed ticket");
            let mut old = std::mem::replace(&mut self.active, hub);
            old.close();
            drop(old);
            self.switches += 1;
            // Everything the old link delivered is in already. The new link's `joined` comes
            // first, as on any rejoin: what it received before it is past the join's sequence.
            for msg in std::iter::once(joined).chain(held) {
                self.at.see(&msg);
                out.push(msg);
            }
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
        let reopen = Reopen { ticket: &token, applied: self.at.claim(), space: self.space };
        match (self.open)(&reopen) {
            Ok(mut hub) => {
                if let Some(space) = self.space {
                    hub.set_space(space);
                }
                self.next = Some(Next { hub, joined: None, held: Vec::new() });
            }
            Err(e) => log::warn!("collab: a link with the renewed ticket could not be opened: {e}"),
        }
    }

    /// Every link names `space` from now on: the one in use, the one being opened, and each
    /// one opened later.
    fn set_space(&mut self, space: u32) {
        self.space = Some(space);
        self.active.set_space(space);
        if let Some(next) = self.next.as_mut() {
            next.hub.set_space(space);
        }
    }

    fn close(&mut self) {
        self.abandon();
        self.active.close();
    }
}

#[cfg(test)]
mod tests;
