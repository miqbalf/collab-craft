//! An in-memory relay for host tests (feature `test-util`): the interface's tests join a real
//! [`LiveEditing`](crate::LiveEditing) through it, with no socket and no server.
//!
//! It does what a host test needs and no more: numbers every submit and broadcasts it to the
//! room (sender included), refuses edits from a read-only connection with the relay's wording,
//! grants each join a fresh client space, holds one holder per lock scope (grant at the room's
//! sequence, denial naming the holder, release on unlock or a dropped link), fences ordered edits
//! as the relay does (a fencing token per grant, a fenced submit or a refresh refused once the
//! grant is gone, a stale unlock ignored; [`MemRelay::lapse`] lets a lease run out), replays the ops so far to a joiner, keeps the newest
//! ephemeral (caret) per peer and kind, and can drop and restore a connection. It does not save:
//! the crate's own pump tests cover the saver. Not for production use.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use craft_collab::{ClientMsg, Hub, LockState, Presence, ServerMsg};
use serde_json::Value;

/// The room every connection of a [`MemRelay`] is in.
pub const ROOM: &str = "test:room:1";

/// The lease a [`MemRelay`] grant names, as the relay's own default.
pub const LEASE_MS: u64 = 60_000;

struct Conn {
    pid: String,
    display: String,
    may_edit: bool,
    connected: bool,
    inbox: VecDeque<ServerMsg>,
    /// The tickets the host handed this connection with [`Hub::set_token`], oldest first.
    tokens: Vec<String>,
    /// Sends from this connection fail, as a link that broke under the sender.
    send_fails: bool,
    /// The id space its replica keeps, named on every rejoin ([`Hub::set_space`]).
    space: u32,
}

#[derive(Default)]
struct Relay {
    seq: u64,
    log: Vec<ServerMsg>,
    conns: Vec<Conn>,
    next_space: u32,
    /// The space each connection was granted at its last join.
    granted: std::collections::HashMap<usize, u32>,
    ephemeral: Vec<(String, String, Value)>,
    /// The holder (connection index) of each locked scope, and its grant's fencing token.
    locks: Vec<(String, usize, u64)>,
    /// The last fencing token handed out for each scope, as the real relay counts them.
    fences: std::collections::HashMap<String, u64>,
    /// Lease refreshes heard, `(scope, token)`, oldest first.
    refreshes: Vec<(String, u64)>,
    /// How many of the next submits the relay refuses for now (the retry code).
    refuse_submits: usize,
}

/// The relay, shared by its hubs.
#[derive(Clone, Default)]
pub struct MemRelay(Rc<RefCell<Relay>>);

/// One connection to a [`MemRelay`].
#[derive(Clone)]
pub struct MemHub {
    relay: MemRelay,
    at: usize,
}

impl MemRelay {
    pub fn new() -> MemRelay {
        MemRelay::default()
    }

    /// Connect `uid` (shown as `display`); `may_edit` false is a read ticket. The relay joins
    /// it at once.
    pub fn connect(&self, uid: &str, display: &str, may_edit: bool) -> MemHub {
        let mut r = self.0.borrow_mut();
        let at = r.conns.len();
        r.conns.push(Conn {
            pid: format!("{uid}~{at:08x}"),
            display: display.to_owned(),
            may_edit,
            connected: true,
            inbox: VecDeque::new(),
            tokens: Vec::new(),
            send_fails: false,
            space: 0,
        });
        r.join(at);
        MemHub { relay: self.clone(), at }
    }

    /// The link of `hub` drops, as a network outage: nothing reaches it or comes from it.
    pub fn drop_link(&self, hub: &MemHub) {
        let mut r = self.0.borrow_mut();
        if let Some(c) = r.conns.get_mut(hub.at) {
            c.connected = false;
            c.inbox.clear();
        }
        r.release_locks_of(hub.at);
        let presence = ServerMsg::Presence { doc: ROOM.into(), roster: r.roster() };
        r.broadcast(&presence, None);
    }

    /// The next `count` submits are refused for now (the relay's bus did not answer): the client
    /// sends them again later.
    pub fn refuse_next_submits(&self, count: usize) {
        self.0.borrow_mut().refuse_submits = count;
    }

    /// While `fail`, every send from `hub` returns an error and reaches nobody.
    pub fn fail_sends(&self, hub: &MemHub, fail: bool) {
        if let Some(c) = self.0.borrow_mut().conns.get_mut(hub.at) {
            c.send_fails = fail;
        }
    }

    /// The lease on `scope` runs out without a word to anyone, as a holder paused past its TTL
    /// sees it: its grant is no longer current, and the scope is free to grant again.
    pub fn lapse(&self, scope: &str) {
        self.0.borrow_mut().locks.retain(|(s, _, _)| s != scope);
    }

    /// The lease refreshes heard so far, `(scope, token)`, oldest first.
    pub fn refreshes(&self) -> Vec<(String, u64)> {
        self.0.borrow().refreshes.clone()
    }

    /// Who holds `scope` (a peer id) and under which token.
    pub fn holder(&self, scope: &str) -> Option<(String, u64)> {
        let r = self.0.borrow();
        r.locks.iter().find(|(s, _, _)| s == scope).and_then(|(_, h, t)| Some((r.conns.get(*h)?.pid.clone(), *t)))
    }

    /// `hub`'s relay peer id.
    pub fn peer_id(&self, hub: &MemHub) -> String {
        self.0.borrow().conns.get(hub.at).map(|c| c.pid.clone()).unwrap_or_default()
    }

    /// Put `msg` in `hub`'s inbox as if the relay had sent it (for a message the relay would not
    /// send on its own, such as a lock event naming the receiver).
    pub fn inject(&self, hub: &MemHub, msg: ServerMsg) {
        if let Some(c) = self.0.borrow_mut().conns.get_mut(hub.at) {
            c.inbox.push_back(msg);
        }
    }

    /// The link of `hub` comes back and rejoins, as the hub's own reconnect does.
    pub fn restore_link(&self, hub: &MemHub) {
        let mut r = self.0.borrow_mut();
        if let Some(c) = r.conns.get_mut(hub.at) {
            c.connected = true;
        }
        r.join(hub.at);
    }

    /// The relay refuses the connection of `hub` itself (`code` as `auth` for an expired
    /// ticket), with `detail` in its words. The link stays up until [`MemRelay::drop_link`].
    pub fn refuse(&self, hub: &MemHub, code: &str, detail: &str) {
        if let Some(c) = self.0.borrow_mut().conns.get_mut(hub.at) {
            c.inbox.push_back(ServerMsg::Error { doc: ROOM.into(), detail: detail.into(), local: None, code: Some(code.into()) });
        }
    }

    /// The space counter goes back to 0, as a Valkey flush loses it while the room's history
    /// stays: later joins are granted 1, 2, … again, unless a rejoin names a higher floor.
    pub fn forget_spaces(&self) {
        self.0.borrow_mut().next_space = 0;
    }

    /// The id space `hub`'s connection was granted at its last join.
    pub fn granted_space(&self, hub: &MemHub) -> Option<u32> {
        self.0.borrow().granted.get(&hub.at).copied()
    }

    /// The relay's sequence: the last op it ordered.
    pub fn seq(&self) -> u64 {
        self.0.borrow().seq
    }

    /// The relay gives up catching `hub` up and sends the room's snapshot instead (a client
    /// that fell too far behind), which breaks a window's link.
    pub fn fall_behind(&self, hub: &MemHub) {
        let mut r = self.0.borrow_mut();
        let seq = r.seq;
        if let Some(c) = r.conns.get_mut(hub.at) {
            c.inbox.push_back(ServerMsg::Snapshot { doc: ROOM.into(), seq, document: Value::Null });
        }
    }

    /// The ordered replay `hub`'s join is waiting for never arrives: its ops are taken out of
    /// the inbox, as a catch-up cut short.
    pub fn cut_replay(&self, hub: &MemHub) {
        if let Some(c) = self.0.borrow_mut().conns.get_mut(hub.at) {
            c.inbox.retain(|m| !matches!(m, ServerMsg::Op { .. }));
        }
    }

    /// The tickets the host handed `hub` with [`Hub::set_token`], oldest first.
    pub fn tokens(&self, hub: &MemHub) -> Vec<String> {
        self.0.borrow().conns.get(hub.at).map(|c| c.tokens.clone()).unwrap_or_default()
    }

    /// Nothing is waiting for any connected peer.
    pub fn quiet(&self) -> bool {
        self.0.borrow().conns.iter().all(|c| c.inbox.is_empty() || !c.connected)
    }
}

impl Hub for MemHub {
    fn send(&mut self, msg: ClientMsg) -> craft_collab::Result<()> {
        if self.relay.0.borrow().conns.get(self.at).is_some_and(|c| c.send_fails) {
            return Err(craft_collab::Error::Refused("the link failed".into()));
        }
        self.relay.0.borrow_mut().handle(self.at, msg);
        Ok(())
    }

    fn poll(&mut self) -> Vec<ServerMsg> {
        let mut r = self.relay.0.borrow_mut();
        match r.conns.get_mut(self.at) {
            Some(c) if c.connected => c.inbox.drain(..).collect(),
            _ => Vec::new(),
        }
    }

    fn connected(&self) -> bool {
        self.relay.0.borrow().conns.get(self.at).is_some_and(|c| c.connected)
    }

    fn set_token(&mut self, token: String) {
        if let Some(c) = self.relay.0.borrow_mut().conns.get_mut(self.at) {
            c.tokens.push(token);
        }
    }

    fn set_space(&mut self, space: u32) {
        if let Some(c) = self.relay.0.borrow_mut().conns.get_mut(self.at) {
            c.space = space;
        }
    }
}

impl Relay {
    fn roster(&self) -> Vec<Presence> {
        self.conns.iter().filter(|c| c.connected).map(|c| Presence { peer: c.pid.clone(), display: c.display.clone() }).collect()
    }

    fn join(&mut self, at: usize) {
        // As the relay does: the counter is first raised to the space a rejoining replica keeps.
        let floor = self.conns.get(at).map_or(0, |c| c.space);
        self.next_space = self.next_space.max(floor) + 1;
        let space = self.next_space;
        self.granted.insert(at, space);
        let roster = self.roster();
        let locks: Vec<LockState> = self
            .locks
            .iter()
            .filter(|(_, h, _)| *h != at)
            .filter_map(|(scope, h, _)| Some(LockState { scope: scope.clone(), holder: self.conns.get(*h)?.pid.clone() }))
            .collect();
        let Some(pid) = self.conns.get(at).map(|c| c.pid.clone()) else { return };
        let joined = ServerMsg::Joined {
            doc: ROOM.into(),
            peer: pid.clone(),
            seq: 0,
            space,
            roster: roster.clone(),
            locks,
            replay_until: Some(self.seq),
            fallback: false,
            text_cap: None,
            heartbeat: false,
            history_lost: false,
        };
        let replay = self.log.clone();
        let others: Vec<ServerMsg> = self
            .ephemeral
            .iter()
            .filter(|(o, _, _)| *o != pid)
            .map(|(origin, kind, payload)| ServerMsg::Ephemeral {
                doc: ROOM.into(),
                origin: origin.clone(),
                kind: kind.clone(),
                payload: payload.clone(),
            })
            .collect();
        if let Some(conn) = self.conns.get_mut(at) {
            conn.inbox.push_back(joined);
            conn.inbox.extend(replay);
            conn.inbox.extend(others);
        }
        let presence = ServerMsg::Presence { doc: ROOM.into(), roster };
        self.broadcast(&presence, Some(at));
    }

    /// A connection's locks end with its link (or its unlock): everyone is told.
    fn release_locks_of(&mut self, at: usize) {
        let held: Vec<String> = self.locks.iter().filter(|(_, h, _)| *h == at).map(|(s, _, _)| s.clone()).collect();
        self.locks.retain(|(_, h, _)| *h != at);
        for scope in held {
            self.broadcast(&ServerMsg::LockReleased { doc: ROOM.into(), scope }, Some(at));
        }
    }

    fn broadcast(&mut self, msg: &ServerMsg, except: Option<usize>) {
        for (j, c) in self.conns.iter_mut().enumerate() {
            if Some(j) != except && c.connected {
                c.inbox.push_back(msg.clone());
            }
        }
    }

    fn handle(&mut self, at: usize, msg: ClientMsg) {
        let Some(conn) = self.conns.get_mut(at).filter(|c| c.connected) else { return };
        match msg {
            ClientMsg::Submit { local, command, params, fence, .. } => {
                if !conn.may_edit {
                    conn.inbox.push_back(ServerMsg::Error {
                        doc: ROOM.into(),
                        detail: "read-only access to this file".into(),
                        local: Some(local),
                        code: None,
                    });
                    return;
                }
                if self.refuse_submits > 0 {
                    self.refuse_submits -= 1;
                    conn.inbox.push_back(ServerMsg::Error {
                        doc: ROOM.into(),
                        detail: "the relay's bus did not answer".into(),
                        local: Some(local),
                        code: Some(craft_collab::RETRY.into()),
                    });
                    return;
                }
                if let Some(fence) = fence
                    && !self.locks.iter().any(|(s, h, t)| *s == fence.scope && *h == at && *t == fence.token)
                {
                    if let Some(c) = self.conns.get_mut(at) {
                        c.inbox.push_back(ServerMsg::Error {
                            doc: ROOM.into(),
                            detail: format!("you no longer hold {}; that change was not shared", fence.scope),
                            local: Some(local),
                            code: Some(craft_collab::FENCED.into()),
                        });
                    }
                    return;
                }
                let Some(conn) = self.conns.get_mut(at) else { return };
                let origin = conn.pid.clone();
                self.seq += 1;
                let op = ServerMsg::Op { doc: ROOM.into(), seq: self.seq, origin, local: Some(local), command, params, scope: None };
                self.log.push(op.clone());
                self.broadcast(&op, None);
            }
            ClientMsg::Ephemeral { kind, payload, .. } => {
                let origin = conn.pid.clone();
                self.ephemeral.retain(|(o, k, _)| !(*o == origin && *k == kind));
                self.ephemeral.push((origin.clone(), kind.clone(), payload.clone()));
                self.broadcast(&ServerMsg::Ephemeral { doc: ROOM.into(), origin, kind, payload }, Some(at));
            }
            ClientMsg::Lock { scope, .. } => {
                if !conn.may_edit {
                    conn.inbox.push_back(ServerMsg::Error {
                        doc: ROOM.into(),
                        detail: "read-only access to this file".into(),
                        local: None,
                        code: None,
                    });
                    return;
                }
                let me = conn.pid.clone();
                let holder = self.locks.iter().find(|(s, _, _)| *s == scope).map(|(_, h, _)| *h);
                match holder {
                    Some(h) if h != at => {
                        let holder = self.conns.get(h).map(|c| c.pid.clone()).unwrap_or_default();
                        if let Some(c) = self.conns.get_mut(at) {
                            c.inbox.push_back(ServerMsg::LockDenied { doc: ROOM.into(), scope, holder });
                        }
                    }
                    _ => {
                        // Every grant, a re-grant to the holder too, takes a new token.
                        let token = {
                            let last = self.fences.entry(scope.clone()).or_default();
                            *last += 1;
                            *last
                        };
                        if holder.is_none() {
                            self.broadcast(&ServerMsg::PresenceLock { doc: ROOM.into(), scope: scope.clone(), holder: me }, Some(at));
                        }
                        self.locks.retain(|(s, _, _)| *s != scope);
                        self.locks.push((scope.clone(), at, token));
                        let seq = self.seq;
                        if let Some(c) = self.conns.get_mut(at) {
                            c.inbox.push_back(ServerMsg::LockGranted {
                                doc: ROOM.into(),
                                scope,
                                seq: Some(seq),
                                token: Some(token),
                                lease_ms: Some(LEASE_MS),
                            });
                        }
                    }
                }
            }
            // The real relay answers a save report to the reporter alone.
            ClientMsg::Saved { doc, seq } => {
                conn.inbox.push_back(ServerMsg::SavedAck { doc, seq });
            }
            // The whole room hears the release, the releaser too, as the real relay does. A peer
            // that does not hold the scope, or holds it under another grant than the token
            // names, releases nothing.
            ClientMsg::Unlock { scope, token, .. }
                if self.locks.iter().any(|(s, h, t)| *s == scope && *h == at && token.is_none_or(|token| token == *t)) =>
            {
                self.locks.retain(|(s, _, _)| *s != scope);
                self.broadcast(&ServerMsg::LockReleased { doc: ROOM.into(), scope }, None);
            }
            ClientMsg::Refresh { scope, token, .. } => {
                self.refreshes.push((scope.clone(), token));
                if !self.locks.iter().any(|(s, h, t)| *s == scope && *h == at && *t == token)
                    && let Some(c) = self.conns.get_mut(at)
                {
                    c.inbox.push_back(ServerMsg::LockLost { doc: ROOM.into(), scope, token });
                }
            }
            _ => {}
        }
    }
}
