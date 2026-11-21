//! The queue behind an ordered edit: ask the relay for a scope, wait until the replica has
//! caught up with the grant, run, and give the scope back once the run's edits are ordered.
//!
//! Pure: it holds no hub and no clock. The app feeds it [`LockEvent`]s, polls it each frame
//! with how far the replica has applied and what time it is, and sends the [`Action`]s it
//! returns through `LiveEditing::lock` and `unlock`.

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
    /// A rejoin released the lock while the item was running, so its edits go out unprotected:
    /// another peer may take the scope and order its own edit first. Told once per run; the
    /// app treats the ordered edit as failed and says so. `finish` still ends the run.
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
    /// Granted at `seq`: the item waits for the replica to apply up to it.
    Granted { seq: u64 },
    /// The item ran; its edits are being flushed. `held` is false once a rejoin made the relay
    /// drop the grant, so no unlock is owed; `told` is whether [`Polled::LockLost`] went out.
    Running { held: bool, told: bool },
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
}

impl<P> Ordered<P> {
    pub fn new(scope: impl Into<String>) -> Self {
        Ordered { scope: scope.into(), item: None, state: State::Idle, since: 0.0, released_early: false }
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
    pub fn on_lock(&mut self, event: &LockEvent, _now_ms: f64) -> Vec<Action> {
        match event {
            LockEvent::Granted { scope, seq } if *scope == self.scope => match self.state {
                State::Asking | State::Denied { .. } => {
                    self.state = State::Granted { seq: seq.unwrap_or(0) };
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
            State::Granted { seq } if applied >= *seq => match self.item.take() {
                Some(item) => {
                    self.state = State::Running { held: true, told: false };
                    Polled::Run(item)
                }
                None => {
                    self.state = State::Idle;
                    Polled::Nothing
                }
            },
            State::Running { held: false, told: false } => {
                self.state = State::Running { held: false, told: true };
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
            State::Running { held, .. } if settled => {
                self.state = State::Idle;
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

    /// The link rejoined: drop the grant (the relay released it), keep the item, ask again.
    /// The wait for the lock starts over. Feed the lock events taken before the rejoin to
    /// [`on_lock`](Self::on_lock) first (`LiveEditing` already drops the old link's events, so
    /// the order cannot revive a stale grant). A run in progress keeps going without the lock:
    /// [`poll`](Self::poll) reports [`Polled::LockLost`] once.
    pub fn rejoined(&mut self, now_ms: f64) -> Vec<Action> {
        match self.state {
            State::Idle => Vec::new(),
            State::Running { told, .. } => {
                self.state = State::Running { held: false, told };
                Vec::new()
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
