//! The queue behind an ordered edit: ask the relay for a scope, wait until the replica has
//! caught up with the grant, run, and give the scope back once the run's edits are ordered.
//!
//! Pure: it holds no hub and no clock. The app feeds it [`LockEvent`]s, polls it each frame
//! with how far the replica has applied and what time it is, and sends the [`Action`]s it
//! returns through `LiveEditing::lock` and `unlock`.
//!
//! Against a relay that fences (its grants carry a token), the run's edits go out as ordered
//! edits under the grant, and the relay refuses them once the grant is no longer this peer's.
//! The run then ends as [`Polled::LockLost`], with the replica already back at the room's state.
//! A rejoin alone no longer loses the run: the relay's answer to its edits decides.

use craft_collab::LockEvent;

/// How long a request may wait for its lock (denied, or unanswered) before it gives up.
pub const GIVE_UP_MS: f64 = 10_000.0;

/// What the app must send for the queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// `LiveEditing::lock` the queue's scope.
    SendLock,
    /// `LiveEditing::unlock` the queue's scope.
    SendUnlock,
}

/// What [`Ordered::poll`] found.
#[derive(Debug, PartialEq, Eq)]
pub enum Polled<P> {
    Nothing,
    /// The lock is held and the replica has applied up to its grant: run the item now.
    Run(P),
    /// The lock stayed taken for [`GIVE_UP_MS`]. `holder` is the relay peer id that held it, or
    /// empty when the relay never answered.
    GaveUp {
        item: P,
        holder: String,
    },
    /// The run lost its lock. Told once per run; the app treats the ordered edit as failed and
    /// says so. `finish` still ends the run; under a fenced grant it still sends the unlock,
    /// which releases nothing at the relay but ends the client's dead fence.
    ///
    /// Against a fencing relay: the relay refused an edit made under the grant, or a lease
    /// refresh found it gone ([`LockEvent::Lost`]). Every edit of the run the relay did not
    /// order is already out of the replica (`LiveApp::reset` and `resync` ran), so the app
    /// only tells the user. Against an older relay: a rejoin released the lock while the item
    /// was running, so its edits went out unprotected.
    LockLost {
        scope: String,
    },
}

#[derive(Debug)]
enum State {
    /// Nothing queued.
    Idle,
    /// The lock request is out; no answer yet.
    Asking,
    /// Refused: `holder` has it. Asked again when it is released.
    Denied { holder: String },
    /// Granted at `seq`, under fencing `token` when the relay gave one: the item waits for the
    /// replica to apply up to `seq`.
    Granted { seq: u64, token: Option<u64> },
    /// The item ran; its edits are being flushed. `held` is whether an unlock is owed: under a
    /// fenced grant it stays owed after the loss, because the unlock is what ends the client's
    /// dead fence (until then every edit is held back and taken back). `lost` is whether the
    /// run lost its lock, `told` whether [`Polled::LockLost`] went out; `token` the grant's.
    Running { held: bool, lost: bool, told: bool, token: Option<u64> },
}

/// One ordered edit at a time under one relay scope.
#[derive(Debug)]
pub struct Ordered<P> {
    scope: String,
    item: Option<P>,
    state: State,
    since: f64,
    /// A `Released` came while the request was unanswered: the next `Denied` is stale.
    released_early: bool,
    /// The replica broke: its edits will never be ordered, so nothing is left to wait for.
    broken: bool,
}

impl<P> Ordered<P> {
    pub fn new(scope: impl Into<String>) -> Self {
        Ordered { scope: scope.into(), item: None, state: State::Idle, since: 0.0, released_early: false, broken: false }
    }

    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Queue one item; `Err(item)` when one is already pending or running.
    pub fn request(&mut self, item: P, now_ms: f64) -> Result<Vec<Action>, P> {
        if !matches!(self.state, State::Idle) {
            return Err(item);
        }
        self.item = Some(item);
        self.state = State::Asking;
        self.since = now_ms;
        self.released_early = false;
        Ok(vec![Action::SendLock])
    }

    /// The relay's word on a lock. Events for other scopes are not ours; a release is broadcast
    /// to the whole room, ours included, so it only matters while a refused request waits.
    pub fn on_lock(&mut self, event: &LockEvent, now_ms: f64) -> Vec<Action> {
        match event {
            LockEvent::Granted { scope, seq, token } if *scope == self.scope => match self.state {
                State::Asking | State::Denied { .. } => {
                    self.state = State::Granted { seq: seq.unwrap_or(0), token: *token };
                    Vec::new()
                }
                // A grant nobody waits for any more (we gave up while the request was out):
                // give it straight back, or the room stays locked.
                State::Idle => vec![Action::SendUnlock],
                State::Granted { .. } | State::Running { .. } => Vec::new(),
            },
            LockEvent::Denied { scope, holder } if *scope == self.scope => {
                if matches!(self.state, State::Asking) && std::mem::take(&mut self.released_early) {
                    // The holder let go before the relay's refusal reached us: ask again.
                    return vec![Action::SendLock];
                }
                if matches!(self.state, State::Asking | State::Denied { .. }) {
                    self.state = State::Denied { holder: holder.clone() };
                }
                Vec::new()
            }
            LockEvent::HeldByOther { scope, holder } if *scope == self.scope => {
                // Only a refused request tracks who holds it; an unanswered one hears `Denied`.
                if let State::Denied { holder: was } = &mut self.state {
                    was.clone_from(holder);
                }
                Vec::new()
            }
            // The grant this queue holds is gone. Before the run, ask again; during it, the run
            // is lost. A loss for another grant (an older one) is not ours.
            LockEvent::Lost { scope, token } if *scope == self.scope => match self.state {
                // The unlock ends the dead fence (it releases nothing at the relay); then ask again.
                State::Granted { token: Some(t), .. } if t == *token => {
                    self.state = State::Asking;
                    self.since = now_ms;
                    self.released_early = false;
                    vec![Action::SendUnlock, Action::SendLock]
                }
                State::Running { held, told, token: Some(t), .. } if t == *token => {
                    self.state = State::Running { held, lost: true, told, token: Some(t) };
                    Vec::new()
                }
                _ => Vec::new(),
            },
            LockEvent::Released { scope } if *scope == self.scope => {
                match self.state {
                    State::Denied { .. } => {
                        self.state = State::Asking;
                        self.released_early = false;
                        vec![Action::SendLock]
                    }
                    // Either the echo of our own unlock or a release that overtook our refusal.
                    State::Asking => {
                        self.released_early = true;
                        Vec::new()
                    }
                    _ => Vec::new(),
                }
            }
            _ => Vec::new(),
        }
    }

    /// `Run` once granted and `applied >= grant seq`; `GaveUp` after [`GIVE_UP_MS`] waiting.
    pub fn poll(&mut self, applied: u64, now_ms: f64) -> Polled<P> {
        match &self.state {
            State::Granted { seq, token } if applied >= *seq => match self.item.take() {
                Some(item) => {
                    self.state = State::Running { held: true, lost: false, told: false, token: *token };
                    Polled::Run(item)
                }
                None => {
                    self.state = State::Idle;
                    Polled::Nothing
                }
            },
            State::Running { held, lost: true, told: false, token } => {
                self.state = State::Running { held: *held, lost: true, told: true, token: *token };
                Polled::LockLost { scope: self.scope.clone() }
            }
            State::Asking | State::Denied { .. } if now_ms - self.since > GIVE_UP_MS => {
                let holder = match &self.state {
                    State::Denied { holder } => holder.clone(),
                    _ => String::new(),
                };
                self.state = State::Idle;
                match self.item.take() {
                    Some(item) => Polled::GaveUp { item, holder },
                    None => Polled::Nothing,
                }
            }
            _ => Polled::Nothing,
        }
    }

    /// The run's edits were flushed: unlock once `settled` (call every frame until it does).
    pub fn finish(&mut self, settled: bool) -> Vec<Action> {
        match self.state {
            State::Running { held, .. } if settled || self.broken => {
                self.state = State::Idle;
                if held { vec![Action::SendUnlock] } else { Vec::new() }
            }
            _ => Vec::new(),
        }
    }

    /// The replica broke (`LiveEditing::status` turned `Broken`): the run's edits will never be
    /// ordered. A run in progress ends as [`Polled::LockLost`] (told once by [`poll`](Self::poll)),
    /// [`finish`](Self::finish) no longer waits for `settled`, and the returned
    /// [`Action::SendUnlock`] (send it if the link is up; the relay's lease is the backstop)
    /// gives the scope back, which is harmless: nobody can run on stale structure because of
    /// an edit that is never ordered.
    pub fn replica_broken(&mut self) -> Vec<Action> {
        self.broken = true;
        match self.state {
            State::Running { held, told, token, .. } => {
                self.state = State::Running { held: false, lost: true, told, token };
                if held { vec![Action::SendUnlock] } else { Vec::new() }
            }
            _ => Vec::new(),
        }
    }

    /// Who holds the lock while this waits (a relay peer id), for the app's status text.
    pub fn waiting_for(&self) -> Option<&str> {
        match &self.state {
            State::Denied { holder } => Some(holder),
            _ => None,
        }
    }

    /// The link rejoined: drop the grant (the relay released it), keep the item, ask again. A
    /// fenced grant is given back first ([`Action::SendUnlock`], then [`Action::SendLock`]).
    /// The wait for the lock starts over. Feed the lock events taken before the rejoin to
    /// [`on_lock`](Self::on_lock) first (`LiveEditing` already drops the old link's events, so
    /// the order cannot revive a stale grant).
    ///
    /// A run in progress under a fenced grant is left to the relay: its edits are sent again
    /// under the grant's token, ordered if the grant survived the rejoin and refused (the run
    /// lost, [`LockEvent::Lost`]) if not. Under a grant with no token (an older relay) it keeps
    /// going without the lock, and [`poll`](Self::poll) reports [`Polled::LockLost`] once.
    pub fn rejoined(&mut self, now_ms: f64) -> Vec<Action> {
        match self.state {
            State::Idle | State::Running { token: Some(_), .. } => Vec::new(),
            State::Running { told, token: None, .. } => {
                self.state = State::Running { held: false, lost: true, told, token: None };
                Vec::new()
            }
            // A fenced grant the item has not run under yet: give it back before asking again.
            // The relay may still hold it (then this releases it), or it went with the old link
            // (then this releases nothing); either way the unlock ends the client's fence for
            // it, which otherwise would die later with nobody left to end it.
            State::Granted { token: Some(_), .. } => {
                self.state = State::Asking;
                self.since = now_ms;
                self.released_early = false;
                vec![Action::SendUnlock, Action::SendLock]
            }
            State::Asking | State::Denied { .. } | State::Granted { .. } => {
                self.state = State::Asking;
                self.since = now_ms;
                self.released_early = false;
                vec![Action::SendLock]
            }
        }
    }
}
