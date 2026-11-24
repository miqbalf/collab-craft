//! The live session pump every Craft app shares: one app's document kept in step with the other
//! editors of the same file through craft-collab's [`Live`] over any [`Hub`], whatever the
//! replica's schema.
//!
//! This crate knows the session and nothing of an app. What is the app's is behind two traits:
//! [`LiveKind`] names its types (the replica schema, one reported local edit, a caret), and
//! [`LiveApp`] does its part of each step (map local edits onto the replica, bring the engine in
//! line with remote changes, put a refused edit back, follow carets). The app passes its
//! [`LiveApp`] to each call that needs it, so it can borrow its engine session for just that call.
//!
//! # For a host (the browser app, a desktop window)
//!
//! 1. Connect a hub with the ticket (the relay admits the peer from it; never pass
//!    `Session::applied()` as the hub's `applied`: reconnects use the hub's own automatic
//!    reconnect with its original auth), then [`LiveEditing::start`] with the file's seed and the
//!    ticket's [`Access`]. Install the engine's edit observer on [`LiveEditing::queue`]
//!    ([`EditQueue::push`]) and its live undo hook ([`EditQueue::history_hook`]).
//! 2. Every frame, call [`LiveEditing::pump`]. It sends the local edits made since the last
//!    pump, applies the other editors' edits, and returns who else is here with their carets.
//!    Show [`LiveEditing::status`] and [`LiveEditing::take_notices`].
//! 3. Call [`LiveEditing::flush`] after each document-changing command. Each edit is then shared
//!    before the next command. Edits that reach the pump in a batch (several commands between
//!    pumps) are shared as one edit, and an edit made after an Undo in the same batch is dropped
//!    with a notice.
//! 4. Take no edit while [`LiveEditing::editable`] is false, and start no ordered edit while
//!    [`LiveEditing::ordered_editable`] is false. An edit that arrives anyway is put back and the
//!    user is told why: the pump is the backstop, not the gate.
//! 5. Send the caret with [`LiveEditing::send_presence`] whenever it moves; the pump holds it to
//!    at most five a second ([`CURSOR_INTERVAL_MS`]). In a browser, give it the page's clock
//!    with [`LiveEditing::set_clock`]; without a clock it counts frames instead.
//! 6. Implement [`LiveApp::unmerged`]: before the replica drops local edits the room does not
//!    have (an ordered run taken back, a replica given up), it hands over the replica's state
//!    with them, to save as an "unmerged copy" of the file.
//!
//! # The status
//!
//! - [`LiveStatus::Connecting`] from the start until the relay has granted a client space, the
//!   join's catch-up has arrived whole (the ordered replay up to `Joined.replay_until`, or the
//!   last `text_ops` piece of a fallback) and the replica is ready. No edits: the catch-up may
//!   still replace the replica with the room's checkpoint. A catch-up still arriving after 10 s
//!   is told to the user once; after 20 s it ends in [`LiveStatus::Broken`] (reopen).
//! - [`LiveStatus::Live`] while connected, joined and ready: the only state that takes edits.
//!   `saved_at` is the ETag of the newest file the document service saved (the `saved`
//!   marker from the service uid [`SERVICE_UID`], `{seq, etag}`, `seq` never going back). It
//!   is kept across reconnects and peers leaving.
//! - [`LiveStatus::Reconnecting`] when the link drops after it had been live, until the hub's
//!   rejoin is answered and its catch-up has arrived. Free edits go on: they land in the replica
//!   and go out, merged into as few submits as fit, on the rejoin, where they merge with what
//!   the room did meanwhile (yrs merges them; a resend reuses the edits' own clock ranges, so an
//!   update the room already has is a no-op). The replica keeps its client id space across the
//!   rejoin, so its undo history, one step per command, carries on. Ordered (structure-locked)
//!   edits need the link: [`LiveEditing::ordered_editable`] is false (grey them out) and
//!   [`LiveEditing::lock`] refuses. Edits stop with [`BUFFER_FULL`] once
//!   `craft_collab::OFFLINE_MAX_BYTES` or `craft_collab::MAX_UNCONFIRMED_EDITS` wait; nothing
//!   waiting is dropped.
//! - [`LiveStatus::ReadOnly`] for a read-only ticket, from the start and for good (it follows
//!   the other editors and shows its caret, and never sends an edit).
//! - [`LiveStatus::Refused`] when the relay grants no client space, so nothing can be shared.
//! - [`LiveStatus::Broken`] when the replica can no longer be trusted: the relay refused an
//!   edit this replica already holds, an update this replica holds could not be sent, the
//!   catch-up could not be rebuilt (`Work::TextIncomplete`), the room lost its history and no
//!   snapshot came within 20 s to a replica that held nothing (it stays
//!   [`LiveStatus::Connecting`] meanwhile), the relay could no longer catch
//!   this link up (it fell too far behind), a checkpoint did not restore, or yrs failed on an
//!   update. The document service ends it too: an `ended` message (`{reason}`) when it stops
//!   saving or leaves, and a `saved` marker marked `closed` (kept by the relay for an editor
//!   that rejoins after the service left), and the host with [`LiveEditing::end`]. It is final:
//!   the message tells the user to reopen the file. A read-only ticket
//!   started for editing ends here too, when the relay refuses its first edit ("read-only
//!   access"): start a read-only ticket with [`Access::Read`].
//!
//! # For the document service (the saver)
//!
//! The worker connects with its service ticket (uid [`SERVICE_UID`]) and pumps like any host.
//! To save: [`LiveEditing::checkpoint`] returns the state it snapshotted; wait for
//! [`SaveEvent::Checkpointed`] from [`LiveEditing::take_save_events`], build the document from
//! those bytes, write the file, then [`LiveEditing::announce_saved`] with the sequence and the
//! file's new ETag.
//!
//! - One checkpoint at a time: drain [`LiveEditing::take_save_events`] (pumping) until its
//!   outcome arrives before calling [`LiveEditing::checkpoint`] again.
//! - [`SaveEvent::Raced`]: throw the state bytes of that checkpoint away (they may miss a
//!   remote edit) and checkpoint again.
//! - [`SaveEvent::Refused`], or `checkpoint()` failing with `craft_collab::Error::Refused`: a
//!   retry will not help (read-only, too large); stop until something changes.
//! - `checkpoint()` failing with any other error (not ready, still catching up, one in flight,
//!   the hub failed): nothing was sent; keep pumping and try again later.
//!
//! The service's `notice` messages (`{text}`, from [`SERVICE_UID`] only) come out of
//! [`LiveEditing::take_notices`] like the link's own.
//!
//! The lines for the user are plain English, the keys an app's translation system looks up.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unimplemented, clippy::todo, clippy::unreachable)]

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use craft_collab::{ClientMsg, Hub, Live, LiveError, LockEvent, Replica, ServerMsg, Work};
use serde_json::{Value, json};

pub mod ordered;

/// An in-memory relay for host tests; feature `test-util`, never in a release build.
#[cfg(any(test, feature = "test-util"))]
pub mod testing;

#[cfg(test)]
mod tests;

/// The uid of the document service, the only peer whose `saved` marker counts.
pub const SERVICE_UID: &str = "craft_office";
/// The ephemeral kind carrying a caret, in the app's own stable ids.
pub const CURSOR_KIND: &str = "cursor";
/// The ephemeral kind the document service announces a save with: `{seq, etag}`. The last one
/// before the service leaves a room also says `closed: true`: the room has no saver any more.
pub const SAVED_KIND: &str = "saved";
/// The ephemeral kind the document service tells the editors something with: `{text}`.
pub const NOTICE_KIND: &str = "notice";
/// The ephemeral kind the document service ends the session with, when it stops saving or
/// leaves the room: `{reason}`. The editors' link ends ([`LiveStatus::Broken`]) with that reason.
pub const ENDED_KIND: &str = "ended";
/// The least time between two carets sent: at most five a second.
pub const CURSOR_INTERVAL_MS: f64 = 200.0;
/// Without a clock, the frames between two carets sent (five a second at 60 frames).
const CURSOR_FRAMES: u32 = 12;
/// The largest `saved` payload the relay keeps.
const SAVED_MAX_BYTES: usize = 1024;
/// The largest `notice` text or `ended` reason taken from the service.
const SERVICE_TEXT_MAX_BYTES: usize = 2048;

/// The link can no longer be trusted.
pub const BROKEN: &str = "This document fell out of step with the other editors. Reopen the file to keep editing.";
const READ_ONLY_BROKEN: &str = "You can only read this file. Reopen it to keep following the other editors.";
const NO_SPACE: &str = "The live editing server cannot share edits right now. Reopen the file later.";
/// No edit is taken until the link is live.
pub const CONNECTING: &str = "Still connecting to the other editors. You can edit in a moment.";
const RECONNECTING: &str = "Reconnecting to the other editors. Editing is paused until then.";
/// An ordered (structure-locked) change was refused because the link is down: it needs the lock,
/// which only the connected room can grant. Nothing changed.
pub const ORDERED_OFFLINE: &str = "This change needs the connection to the other editors. Try again once you are reconnected.";
/// Too many edits wait for the link (`craft_collab::OFFLINE_MAX_BYTES`, `MAX_UNCONFIRMED_EDITS`):
/// new ones are refused until those are sent. Nothing waiting is dropped.
pub const BUFFER_FULL: &str = "Too many changes are waiting to be sent to the other editors. Editing resumes once they are sent.";
const READ_ONLY: &str = "This file is read-only for you.";
/// Part of a local edit was not shared and the document was rebuilt from the replica.
pub const PARTIAL: &str = "Part of your last edit could not be shared and was taken back.";
const BEHIND: &str = "This window fell too far behind the other editors. Reopen the file to catch up.";
const UNSENT: &str = "Your last edit could not be sent to the other editors.";
const CATCHING_UP: &str = "Still catching up with the other editors. Editing starts as soon as that is done.";
const CATCH_UP_FAILED: &str = "Catching up with the other editors is taking too long. Reopen the file to try again.";
/// How long a join's catch-up may take before the user is told; twice that and it gives up.
const CATCH_UP_LATE_MS: f64 = 10_000.0;
/// The same without a clock, in frames.
const CATCH_UP_LATE_FRAMES: u32 = 600;
/// The room lost its history (the relay restarted with nothing to replay) and this window holds
/// no state of its own from before: nothing can be trusted until the service's snapshot.
const HISTORY_LOST: &str = "The live session lost its history. Download a copy, then reopen the file.";
/// How long a live window waits for the document service to be in the room before it says the
/// file is not being saved (no worker joined: refused at its cap, gave up, or could not start).
const SAVER_WAIT_MS: f64 = 15_000.0;
/// The same without a clock, in frames.
const SAVER_WAIT_FRAMES: u32 = 900;
/// How long a replica that joined a room which lost its history waits for the snapshot.
const HISTORY_WAIT_MS: f64 = 20_000.0;
/// The same without a clock, in frames.
const HISTORY_WAIT_FRAMES: u32 = 1200;
const AFTER_UNDO: &str = "An edit made right after Undo was not kept. Please make it again.";
/// The service closed the room: nothing saves edits made in it any more.
const CLOSED: &str = "The live session ended. Reopen the file to keep editing.";

/// What live editing is doing now; see the [crate documentation](crate).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveStatus {
    Connecting,
    Live { saved_at: Option<String> },
    ReadOnly,
    Reconnecting,
    Refused(String),
    Broken(String),
}

/// What the ticket allows. The relay enforces it; this side never offers an edit it refuses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Edit,
    Read,
}

/// Another editor in the document, with its caret as the app draws it (`P`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer<P> {
    /// The relay's peer id for one connection (`<uid>~<hex>`).
    pub id: String,
    pub display: String,
    /// The same for every connection of the same person, for a stable badge colour.
    pub color_seed: u64,
    /// Where the caret is, kept on the same content through edits.
    pub caret: Option<P>,
    /// What the peer's ticket lets it do, as the relay says: [`Access::Read`] for a member who
    /// only views (a host may label it "viewing"). [`Access::Edit`] when the relay does not say
    /// (an older relay).
    pub access: Access,
}

/// A checkpoint's outcome, for the saver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SaveEvent {
    /// Ordered at `seq` and acknowledged: write the file from the checkpoint's state now.
    Checkpointed { seq: u64 },
    /// Dropped for a reason that can pass: checkpoint again.
    Raced,
    /// Refused for a reason a retry does not change, in the relay's words.
    Refused { reason: String },
}

/// The types of one app's live session.
pub trait LiveKind {
    /// The replica schema the app shares its document through.
    type Replica: Replica;
    /// One local edit the engine reported (WordCraft: the document before and after, and the
    /// command). Queued by the engine's observer, handed back to [`LiveApp`] at the next flush.
    type Edit: Send + 'static;
    /// A peer's caret as the app keeps it between edits.
    type Caret;
    /// A caret as the host draws it, in [`Peer::caret`].
    type Pos: Clone;
    /// Where `caret` is now, for the host.
    fn pos(caret: &Self::Caret) -> Self::Pos;
}

/// What one remote update changed, in the replica schema's own terms.
pub type Change<K> = <<K as LiveKind>::Replica as Replica>::Change;

/// Why the app's mapping refused a batch of local edits. Nothing was sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalError {
    /// The edit changed something live editing does not share, or came before a client space:
    /// it is put back ([`LiveApp::put_back`]) and the user is told this line.
    Refused(String),
    /// The replica refused a step: the engine is rebuilt from the replica and the user told
    /// that part of the edit was taken back. The text is for the log.
    Failed(String),
}

/// What remains to do after a batch's updates were sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Landed {
    /// Nothing: the engine and the replica agree.
    Done,
    /// The engine may differ from the replica (an id was not written back): rebuild it from
    /// the replica.
    Resync,
    /// The replica refused some steps: rebuild the engine and tell the user part of the edit
    /// was taken back.
    Partial,
}

/// One app's side of each step of a live session: its engine session plus its mapping. Made by
/// the app for one call (it may borrow the engine session) and passed to the call.
pub trait LiveApp<K: LiveKind> {
    /// Map a batch of local edits (in the order reported) onto `replica`: the updates to send,
    /// in order. Called only while [`LiveEditing::editable`]. Refused, nothing is sent.
    fn local(&mut self, replica: &mut K::Replica, edits: &[K::Edit]) -> Result<Vec<Vec<u8>>, LocalError>;
    /// After [`LiveApp::local`]'s updates were sent: write back into the engine whatever the
    /// mapping minted (stable ids), with no undo step, and say what remains to do.
    fn landed(&mut self) -> Landed;
    /// Put the engine back to the state before `edits` (a refused batch), with no undo step.
    fn put_back(&mut self, edits: Vec<K::Edit>) -> Result<(), String>;
    /// What remote `changes` mean for this peer, inside the replica, before they reach the
    /// engine: the changes for [`LiveApp::apply`] and the updates this peer sends in answer
    /// (text it recovered or forwarded, made outside the replica's undo history).
    fn received(&mut self, replica: &mut K::Replica, changes: Vec<Change<K>>) -> (Vec<Change<K>>, Vec<Vec<u8>>);
    /// Bring the engine in line with what `changes` changed in `replica`, with no undo step and
    /// the selection kept by stable id.
    fn apply(&mut self, replica: &K::Replica, changes: &[Change<K>]) -> Result<(), String>;
    /// Rebuild the engine's whole shared part from `replica`, with no undo step.
    fn resync(&mut self, replica: &K::Replica) -> Result<(), String>;
    /// The replica was replaced by the room's checkpoint: start the mapping again for an empty
    /// shared part, minting in `space` (0 when none was granted). A [`LiveApp::resync`] follows.
    fn reset(&mut self, space: u32);
    /// The relay granted client `space`: mint new ids there from now on.
    fn set_space(&mut self, space: u32);
    /// A peer's caret from its [`CURSOR_KIND`] payload, read against the engine's document;
    /// `None` drops the peer's caret.
    fn caret(&self, payload: &Value) -> Option<K::Caret>;
    /// Keep each caret on the same content after an edit; drop those whose content is gone.
    fn follow(&self, carets: &mut HashMap<String, K::Caret>);
    /// The replica is about to drop local edits the room does not have (an ordered run taken
    /// back, or the replica given up: needs a resync, damaged, replaced, refused). `state` is the
    /// replica's full state with them (yrs update v1, `Replica::from_state`), taken before the
    /// drop; `edits` how many local updates the room lacks. Called before the reset or the break
    /// that drops them: save it as an "unmerged copy" of the file. Nothing by default.
    fn unmerged(&mut self, state: &[u8], edits: usize) {
        let _ = (state, edits);
    }
}

/// What the hub's join messages said, read as they pass.
#[derive(Default)]
struct Seen {
    joins: u64,
    space: Option<u32>,
    /// The join's ordered replay ends at this sequence, not yet seen.
    replay_until: Option<u64>,
    /// The join's catch-up comes as text ops, whose last piece has not arrived.
    text_ops_owed: bool,
    /// The relay could not catch this link up (`snapshot`: fell too far behind).
    replaced: bool,
    /// This connection's peer id, from its join.
    me: Option<String>,
    /// This replica's edit submits (their `local` counters) the relay has not ordered yet.
    pending: BTreeSet<u64>,
    /// The highest sequence the relay gave one of this replica's edits.
    confirmed: Option<u64>,
    /// Edits sent before a rejoin, unconfirmed, and not yet sent again (`Live` resends them,
    /// merged, while it handles the join).
    stale: bool,
    /// A join was just seen: the submits sent before the next poll are the resend.
    resending: bool,
    /// What the relay said about lock scopes since the app last took it, oldest first. Read
    /// here, in the order the messages arrive, so a join empties exactly what came before it
    /// (the old link's answers), however the poll batches them. A fenced grant and a lost grant
    /// are `Live`'s own word (it announces a grant when its fence starts) and join this list as
    /// `Live` reports them.
    lock_events: Vec<LockEvent>,
    /// Fenced grants a join dropped from [`Seen::lock_events`] before the app took them: their
    /// fences are given back, since the app, never told, would not.
    dropped_grants: Vec<(String, u64)>,
}

impl Seen {
    /// Whether the last join's catch-up is still arriving. Until it has, the replica may
    /// still be replaced by the room's snapshot, and an edit made meanwhile would break it.
    fn catching_up(&self) -> bool {
        self.replay_until.is_some() || self.text_ops_owed
    }

    /// Whether a save through `saved` holds every edit this replica made: each one the relay
    /// ordered is at or before it, and none is waiting to be ordered or sent again. `unconfirmed`
    /// is the replica's count of edit updates the relay has not ordered: tracked by update, so a
    /// submit refused for now and sent again under a new counter is counted once.
    fn covered(&self, saved: Option<u64>, unconfirmed: usize) -> bool {
        !self.stale && unconfirmed == 0 && self.confirmed.is_none_or(|c| saved.is_some_and(|s| c <= s))
    }

    /// An edit submit went out.
    fn sent(&mut self, msg: &ClientMsg) {
        if let ClientMsg::Submit { local, command, .. } = msg
            && craft_collab::is_crdt_command(command)
        {
            self.pending.insert(*local);
            // The rejoin's resend carries the edits sent before it.
            if self.resending {
                self.stale = false;
            }
        }
    }

    fn saw(&mut self, m: &ServerMsg) {
        // A submit refused for now is sent again under a new counter (`Live` keeps the update
        // and counts it once), so its old counter leaves `pending` here: kept, it would make a
        // later rejoin with nothing to resend read as owing a resend for good.
        if let ServerMsg::Error { local: Some(local), detail, code, .. } = m
            && (code.as_deref() == Some(craft_collab::RETRY) || !craft_collab::refusal_is_permanent(detail))
        {
            self.pending.remove(local);
        }
        if let ServerMsg::Op { seq, origin, local: Some(local), .. } = m
            && self.me.as_ref() == Some(origin)
            && self.pending.remove(local)
        {
            self.confirmed = self.confirmed.max(Some(*seq));
        }
        match m {
            // A fenced grant is announced by `Live` once its fence starts (a newer grant waits
            // for the old fence to settle), so the app never runs under a fence not yet started.
            ServerMsg::LockGranted { scope, seq, token: None, .. } => {
                self.lock_events.push(LockEvent::Granted { scope: scope.clone(), seq: *seq, token: None });
            }
            ServerMsg::LockDenied { scope, holder, .. } => self.lock_events.push(LockEvent::Denied { scope: scope.clone(), holder: holder.clone() }),
            ServerMsg::LockReleased { scope, .. } => self.lock_events.push(LockEvent::Released { scope: scope.clone() }),
            // This peer's own take is not somebody else's.
            ServerMsg::PresenceLock { scope, holder, .. } if self.me.as_ref() != Some(holder) => {
                self.lock_events.push(LockEvent::HeldByOther { scope: scope.clone(), holder: holder.clone() });
            }
            ServerMsg::Joined { peer, seq, space, replay_until, fallback, .. } => {
                // The relay released this peer's locks with the old link and told nobody here:
                // every lock answer heard before this join is stale. A lost grant is not an
                // answer of the link (the run it ended was taken back), so it stays. A fenced
                // grant never taken leaves a fence nobody would end: noted, to give it back.
                let dropped = &mut self.dropped_grants;
                self.lock_events.retain(|e| match e {
                    LockEvent::Lost { .. } => true,
                    LockEvent::Granted { scope, token: Some(token), .. } => {
                        dropped.push((scope.clone(), *token));
                        false
                    }
                    _ => false,
                });
                // What was sent before is never ordered under these counters now: `Live` sends
                // it again, merged, as it handles the join.
                if !self.pending.is_empty() {
                    self.pending.clear();
                    self.stale = true;
                    self.resending = true;
                }
                self.me = Some(peer.clone());
                self.joins += 1;
                self.space = Some(*space);
                // An older relay does not mark its replay; there nothing can be waited for.
                self.replay_until = replay_until.filter(|until| until > seq);
                self.text_ops_owed = *fallback;
            }
            ServerMsg::Op { seq, .. } if self.replay_until.is_some_and(|until| *seq >= until) => self.replay_until = None,
            ServerMsg::TextOps { more: false, .. } => self.text_ops_owed = false,
            ServerMsg::Snapshot { .. } => self.replaced = true,
            _ => {}
        }
    }
}

/// The host's hub, watched for the relay's join answer (the granted space, where the catch-up
/// ends) and for a relay that gave up catching it up, which [`Live`]
/// keeps to itself.
pub struct Tap<H> {
    hub: H,
    seen: Arc<Mutex<Seen>>,
}

impl<H: Hub> Hub for Tap<H> {
    fn send(&mut self, msg: ClientMsg) -> craft_collab::Result<()> {
        lock(&self.seen).sent(&msg);
        self.hub.send(msg)
    }
    fn poll(&mut self) -> Vec<ServerMsg> {
        let msgs = self.hub.poll();
        let mut seen = lock(&self.seen);
        // The last join's resend, if any, has gone out by now.
        seen.resending = false;
        for m in &msgs {
            seen.saw(m);
        }
        msgs
    }
    fn connected(&self) -> bool {
        self.hub.connected()
    }
    fn set_token(&mut self, token: String) {
        self.hub.set_token(token);
    }
    fn set_space(&mut self, space: u32) {
        self.hub.set_space(space);
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A local change waiting for the pump.
enum Local<E> {
    Edit(E),
    History { steps: usize, redo: bool },
}

/// Shared with the engine's observer and undo hook, which run inside the engine's command.
struct Shared<E> {
    queue: Vec<Local<E>>,
    /// Replica undo steps per shared edit, newest last.
    undo: Vec<usize>,
    redo: Vec<usize>,
}

impl<E> Default for Shared<E> {
    fn default() -> Self {
        Shared { queue: Vec::new(), undo: Vec::new(), redo: Vec::new() }
    }
}

/// Where the engine's observer and live undo hook put what happened inside a command, for the
/// next [`LiveEditing::flush`].
pub struct EditQueue<E>(Arc<Mutex<Shared<E>>>);

impl<E> Clone for EditQueue<E> {
    fn clone(&self) -> Self {
        EditQueue(self.0.clone())
    }
}

impl<E: Send + 'static> EditQueue<E> {
    /// One local edit the engine reported.
    pub fn push(&self, edit: E) {
        lock(&self.0).queue.push(Local::Edit(edit));
    }

    /// The engine's live undo hook (`redo` true for Redo): takes the step off its stack now (so
    /// the answer, whether there was one, is honest) and leaves the replica's undo to the next
    /// flush.
    pub fn history_hook(&self) -> Box<dyn FnMut(bool) -> bool + Send> {
        let shared = self.0.clone();
        Box::new(move |redo| {
            let mut s = lock(&shared);
            let step = if redo { s.redo.pop() } else { s.undo.pop() };
            match step {
                Some(steps) => {
                    s.queue.push(Local::History { steps, redo });
                    true
                }
                None => false,
            }
        })
    }
}

/// A join's catch-up that has not finished arriving.
struct CatchUp {
    since: Option<f64>,
    frames: u32,
    told: bool,
}

#[derive(Default)]
struct CursorOut {
    pending: Option<Value>,
    last: Option<Value>,
    sent_at: Option<f64>,
    frames: u32,
}

type Clock = Box<dyn FnMut() -> f64 + Send>;

#[cfg(not(target_arch = "wasm32"))]
fn default_clock() -> Option<Clock> {
    let start = std::time::Instant::now();
    Some(Box::new(move || start.elapsed().as_secs_f64() * 1000.0))
}

/// `std::time` has no clock on the web; the host gives one with [`LiveEditing::set_clock`].
#[cfg(target_arch = "wasm32")]
fn default_clock() -> Option<Clock> {
    None
}

/// One document edited live: the session's side of a [`Live`] link, for app kind `K`.
pub struct LiveEditing<H: Hub, K: LiveKind> {
    live: Live<Tap<H>, K::Replica>,
    seen: Arc<Mutex<Seen>>,
    shared: Arc<Mutex<Shared<K::Edit>>>,
    access: Access,
    status: LiveStatus,
    space: u32,
    joins: u64,
    need_join: bool,
    been_live: bool,
    broken: Option<String>,
    refused: Option<String>,
    saved: Option<(u64, String)>,
    service_uid: String,
    carets: HashMap<String, K::Caret>,
    notices: Vec<String>,
    /// The newest service notice taken from each peer: a rejoin's catch-up sends it again.
    told: HashMap<String, String>,
    /// A local edit stayed in the session without reaching the room.
    lost: bool,
    save_events: Vec<SaveEvent>,
    cursor: CursorOut,
    clock: Option<Clock>,
    catch_up: Option<CatchUp>,
    /// A join that lost the room's history ([`Work::HistoryLost`]), not yet taken by the saver.
    history_lost: bool,
    /// Since when a replica with nothing settled has waited for the room's snapshot.
    snapshot_wait: Option<CatchUp>,
    /// Since when the link has been live with no document service in the room.
    saver_wait: Option<CatchUp>,
    /// Live, yet no document service has been in the room for [`SAVER_WAIT_MS`].
    saver_missing: bool,
    /// The ordered run whose edit was refused because the link was down, told as lost once.
    lost_offline: Option<(String, u64)>,
    /// The app was handed the unconfirmed edits ([`LiveApp::unmerged`]) while this many were
    /// waiting. A later break with the same count waiting is taken to be the same edits and does
    /// not hand them over a second time; cleared once nothing is waiting.
    handed_unmerged: Option<usize>,
}

impl<H: Hub, K: LiveKind> LiveEditing<H, K> {
    /// Start a live session on `hub` in room `room`, the replica seeded from `seed` (the file,
    /// as every peer opening it seeds it), with the ticket's `access`. Nothing has been polled
    /// yet, so nothing is held and nothing changes.
    pub fn start(hub: H, room: &str, seed: &<K::Replica as Replica>::Seed, access: Access) -> Result<Self, LiveError> {
        let seen = Arc::new(Mutex::new(Seen::default()));
        let mut live = Live::with_replica(Tap { hub, seen: seen.clone() }, room);
        live.seed(seed)?;
        Ok(LiveEditing {
            live,
            seen,
            shared: Arc::new(Mutex::new(Shared::default())),
            access,
            status: if access == Access::Read { LiveStatus::ReadOnly } else { LiveStatus::Connecting },
            space: 0,
            joins: 0,
            need_join: true,
            been_live: false,
            broken: None,
            refused: None,
            saved: None,
            service_uid: SERVICE_UID.to_owned(),
            carets: HashMap::new(),
            notices: Vec::new(),
            told: HashMap::new(),
            lost: false,
            save_events: Vec::new(),
            cursor: CursorOut::default(),
            clock: default_clock(),
            catch_up: None,
            history_lost: false,
            snapshot_wait: None,
            saver_wait: None,
            saver_missing: false,
            lost_offline: None,
            handed_unmerged: None,
        })
    }

    /// Where the engine's observer and live undo hook report to.
    pub fn queue(&self) -> EditQueue<K::Edit> {
        EditQueue(self.shared.clone())
    }

    /// One frame: share the local edits, take in the other editors' edits, carets and saves,
    /// and say who else is here.
    pub fn pump(&mut self, app: &mut impl LiveApp<K>) -> Vec<Peer<K::Pos>> {
        let was_broken = self.broken.is_some();
        self.flush(app);
        let works = self.live.pump();
        self.give_back_dropped_grants();
        if let Some(now) = self.clock.as_mut().map(|c| c()) {
            self.live.refresh_lease(now);
        }
        self.note_join(app);
        for work in works {
            self.act(work, app);
        }
        self.take_live_notices();
        self.update_status();
        if !was_broken {
            self.hand_unmerged_on_break(app);
        }
        self.cursor.frames = self.cursor.frames.saturating_add(1);
        self.send_due_cursor();
        self.roster()
    }

    /// Share the edits the engine made since the last flush, and run the Undo and Redo it
    /// asked for. Refused edits are put back, with a notice.
    pub fn flush(&mut self, app: &mut impl LiveApp<K>) {
        let queue = std::mem::take(&mut lock(&self.shared).queue);
        let mut group = Vec::new();
        let mut stale = false;
        let mut dropped = false;
        for item in queue {
            match item {
                Local::Edit(edit) if !stale => group.push(edit),
                Local::Edit(_) => dropped = true,
                Local::History { steps, redo } => {
                    self.share(app, std::mem::take(&mut group));
                    self.history(app, steps, redo);
                    stale = true;
                }
            }
        }
        self.share(app, group);
        if dropped {
            // The engine holds those edits and the room does not: take them back.
            self.resync(app);
            self.notices.push(AFTER_UNDO.to_owned());
        }
    }

    /// Tell the room where this editor's caret is, in the app's stable ids. Held to at most
    /// five a second; the newest goes out when due.
    pub fn send_presence(&mut self, payload: Value) {
        if self.cursor.last.as_ref() == Some(&payload) {
            self.cursor.pending = None;
            return;
        }
        self.cursor.pending = Some(payload);
        self.send_due_cursor();
    }

    pub fn status(&self) -> &LiveStatus {
        &self.status
    }

    /// The highest relay sequence the replica has applied in order: an op that arrived ahead
    /// of a gap is not counted until the ops before it are in. An op can take a sequence and
    /// change nothing (a merged resend): the saver uses this to mark the file saved up to there
    /// when the document still equals it.
    pub fn newest_seq(&self) -> u64 {
        self.live.applied()
    }

    /// Ask the relay for `scope`; the answer comes back through [`take_lock_events`](Self::take_lock_events).
    ///
    /// From a fenced grant (`LockEvent::Granted` with a `token`) until [`unlock`](Self::unlock),
    /// every edit this window shares is an ordered edit under that grant: the relay orders it
    /// only while the grant is still this window's. If the relay refuses one (the lease lapsed
    /// while this window was paused or cut off, and may have gone to somebody else), the run is
    /// taken back: the replica returns to the room's state, the app is reset and resynced from
    /// it ([`LiveApp::reset`]), and `LockEvent::Lost` follows. The lease is refreshed at half its
    /// length while held, on the host's clock ([`set_clock`](Self::set_clock)).
    ///
    /// Refused while the link is down: nothing is sent and nothing changes ([`ORDERED_OFFLINE`]).
    /// Gate the command itself on [`ordered_editable`](Self::ordered_editable), which also needs
    /// the link to be live and caught up; a lock asked for while connected but still catching up
    /// is sent, and the run waits for the grant's sequence as usual.
    pub fn lock(&mut self, scope: &str) -> craft_collab::Result<()> {
        if !self.live.connected() {
            return Err(craft_collab::Error::Hub(ORDERED_OFFLINE.to_owned()));
        }
        self.live.lock(scope)
    }

    /// Give `scope` back. Do it only once [`local_settled`](Self::local_settled): a lock released
    /// ahead of the holder's own edit lets the next holder run on stale state. The grant's
    /// fencing token goes with it, so a late unlock never releases a newer grant; edits are free
    /// again from here.
    pub fn unlock(&mut self, scope: &str) -> craft_collab::Result<()> {
        self.live.unlock(scope)
    }

    /// What the relay said about locks since the last call, oldest first. A rejoin drops every
    /// answer that came before it, taken or not yet taken, even within one poll: the relay
    /// released this peer's locks with the old link, so a grant from it is never handed out (a
    /// fenced one's fence is given back here, since nobody else would). `LockEvent::Lost` is
    /// kept across a rejoin. A fenced grant (`token` set) is handed out when its fence starts:
    /// a re-grant of a scope whose old fence still has an edit unanswered comes after that
    /// fence's `Lost`, once its edit is answered.
    /// What came after the join is kept. Feed these to `Ordered::on_lock`, then call
    /// `Ordered::rejoined` if the link rejoined; the order does not matter for correctness.
    pub fn take_lock_events(&mut self) -> Vec<LockEvent> {
        std::mem::take(&mut lock(&self.seen).lock_events)
    }

    /// Every local edit has been ordered by the relay: none still in the engine's queue, none
    /// sent and unanswered, none refused for now or waiting to be sent again after a rejoin.
    /// Tracked by update, so a resend under a new counter does not leave the old one counted.
    pub fn local_settled(&self) -> bool {
        lock(&self.shared).queue.is_empty() && self.live.unconfirmed_edits() == 0 && !lock(&self.seen).stale
    }

    /// Whether the host may take free (unordered) edits now, with an edit ticket: while live, and
    /// also while [`LiveStatus::Reconnecting`] once the link had been live. Edits made while the
    /// link is down land in the replica and go out, merged, on the rejoin, where they merge with
    /// what the room did meanwhile. Not while the link is down and an ordered run is still open
    /// (its edits need the lock), and not while too much is waiting to be sent
    /// ([`BUFFER_FULL`]). Ordered edits have their own gate,
    /// [`ordered_editable`](Self::ordered_editable).
    pub fn editable(&self) -> bool {
        self.refusal().is_none()
    }

    /// Whether the host may start an ordered (structure-locked) edit now: live and connected,
    /// with room in the buffer. False while reconnecting: grey those commands out, and refuse
    /// one tried anyway with [`ORDERED_OFFLINE`] ([`lock`](Self::lock) refuses it too).
    pub fn ordered_editable(&self) -> bool {
        matches!(self.status, LiveStatus::Live { .. })
            && self.access == Access::Edit
            && self.live.connected()
            && self.live.ready_to_type()
            && !self.live.buffer_full()
    }

    /// Why a local edit is refused now, for the user; `None` when it is taken.
    fn refusal(&self) -> Option<String> {
        let line = match &self.status {
            LiveStatus::Live { .. } | LiveStatus::Reconnecting if self.access == Access::Edit => {
                let online = matches!(self.status, LiveStatus::Live { .. }) && self.live.connected();
                if !self.live.ready_to_type() || !self.been_live {
                    RECONNECTING
                } else if !online && self.live.open_fence().is_some() {
                    // Under the lock every edit is part of the ordered run, which needs the link.
                    ORDERED_OFFLINE
                } else if self.live.buffer_full() {
                    BUFFER_FULL
                } else {
                    return None;
                }
            }
            LiveStatus::Live { .. } | LiveStatus::Reconnecting | LiveStatus::ReadOnly => READ_ONLY,
            LiveStatus::Connecting => CONNECTING,
            LiveStatus::Refused(m) | LiveStatus::Broken(m) => return Some(m.clone()),
        };
        Some(line.to_owned())
    }

    /// Why no edit is taken now, for the user: connecting, reconnecting, read-only, too much
    /// waiting to be sent, an ordered run that needs the link, or the reason the link was
    /// refused or broke.
    pub fn not_editable(&self) -> String {
        if let Some(why) = self.refusal() {
            return why;
        }
        match &self.status {
            LiveStatus::Connecting => CONNECTING.to_owned(),
            LiveStatus::Live { .. } | LiveStatus::Reconnecting => RECONNECTING.to_owned(),
            LiveStatus::ReadOnly => READ_ONLY.to_owned(),
            LiveStatus::Refused(m) | LiveStatus::Broken(m) => m.clone(),
        }
    }

    /// Whether the newest save announced holds every local edit this replica made: each one
    /// the relay ordered is at or before the save's sequence, none is waiting to be ordered or
    /// sent again, and none was kept back from the room. False means the document has edits
    /// only this window holds, which reopening the file would lose.
    pub fn edits_saved(&self) -> bool {
        !self.lost && lock(&self.seen).covered(self.saved.as_ref().map(|(seq, _)| *seq), self.live.unconfirmed_edits())
    }

    /// Lines for the user since the last call, in plain English (untranslated).
    pub fn take_notices(&mut self) -> Vec<String> {
        std::mem::take(&mut self.notices)
    }

    /// Add a line for the user (an app's own refusal), taken with the link's.
    pub fn push_notice(&mut self, line: String) {
        self.notices.push(line);
    }

    /// Whether the link is live but no document service has been in the room for 15 s (it never
    /// joined, or it left): nothing saves this window's edits, so the window keeps them itself.
    /// Clears as soon as the service is in the room again.
    pub fn saver_missing(&self) -> bool {
        self.saver_missing
    }

    /// Whether a join since the last call found the room had lost its history (the relay
    /// restarted with nothing to replay). The saver, holding a settled document, checkpoints
    /// it at once, so the room and every editor that waits for it have a snapshot again.
    pub fn take_history_lost(&mut self) -> bool {
        std::mem::take(&mut self.history_lost)
    }

    /// Checkpoint outcomes since the last call, for the saver.
    pub fn take_save_events(&mut self) -> Vec<SaveEvent> {
        std::mem::take(&mut self.save_events)
    }

    /// Checkpoint the shared document (the saver only): returns the state snapshotted. See
    /// the [crate documentation](crate) and `Live::checkpoint`.
    pub fn checkpoint(&mut self) -> craft_collab::Result<Vec<u8>> {
        self.live.checkpoint()
    }

    /// Announce a saved file (the saver only): `seq` the checkpoint's sequence, `etag` the
    /// file's new ETag. Every editor's status shows it.
    pub fn announce_saved(&mut self, seq: u64, etag: &str) -> craft_collab::Result<()> {
        self.live.send_ephemeral(SAVED_KIND, json!({"seq": seq, "etag": etag}))?;
        let me = self.service_uid.clone();
        self.take_saved(&me, &json!({"seq": seq, "etag": etag}));
        Ok(())
    }

    /// The newest save announced: its sequence and the file's ETag.
    pub fn saved(&self) -> Option<(u64, &str)> {
        self.saved.as_ref().map(|(seq, etag)| (*seq, etag.as_str()))
    }

    /// Announce that the service leaves the room (the saver only): the newest save again, marked
    /// `closed`. The relay keeps it after the service has left, so an editor that rejoins the
    /// room later learns that nothing saves it any more.
    pub fn announce_closed(&mut self, seq: u64, etag: &str) -> craft_collab::Result<()> {
        self.live.send_ephemeral(SAVED_KIND, json!({"seq": seq, "etag": etag, "closed": true}))
    }

    /// End the link from outside (the host learned the session is over: another room, less
    /// access): [`LiveStatus::Broken`] with `reason`, at once and for good.
    pub fn end(&mut self, reason: &str) {
        self.break_with(reason);
        self.update_status();
    }

    /// The uid whose `saved` markers count, [`SERVICE_UID`] unless the relay names another.
    pub fn set_service_uid(&mut self, uid: impl Into<String>) {
        self.service_uid = uid.into();
    }

    /// The clock the caret limit runs on, in milliseconds. Native builds have one; a browser
    /// host passes the page's.
    pub fn set_clock(&mut self, clock: Box<dyn FnMut() -> f64 + Send>) {
        self.clock = Some(clock);
    }

    /// The link, for reading its state.
    pub fn live(&self) -> &Live<Tap<H>, K::Replica> {
        &self.live
    }

    /// The host's hub.
    pub fn hub(&self) -> &H {
        &self.live.hub().hub
    }

    /// The host's hub, to steer its transport (a fresh ticket, for one).
    pub fn hub_mut(&mut self) -> &mut H {
        &mut self.live.hub_mut().hub
    }

    /// An edit under an open fence was refused because the link is down ([`ORDERED_OFFLINE`]):
    /// the ordered run it belongs to failed. The app's `Ordered` is told the grant is lost (once
    /// per grant), so a run that was under way ends as `Polled::LockLost`, a failed ordered edit,
    /// and never reads as done. Nothing reached the replica, so nothing is taken back; the
    /// run's unlock ends the fence.
    fn lose_run_offline(&mut self, why: &str) {
        if why != ORDERED_OFFLINE {
            return;
        }
        let Some(fence) = self.live.open_fence() else { return };
        let grant = (fence.scope, fence.token);
        if self.lost_offline.as_ref() == Some(&grant) {
            return;
        }
        log::info!("live: an ordered edit was refused while the link is down; the run failed");
        lock(&self.seen).lock_events.push(LockEvent::Lost { scope: grant.0.clone(), token: grant.1 });
        self.lost_offline = Some(grant);
    }

    /// A join dropped fenced grants the app never took ([`Seen::dropped_grants`]): they went with
    /// the old link, and no `Ordered` will unlock them, so their fences end here. Otherwise every
    /// later edit would go out under a grant the relay no longer holds and be taken back.
    fn give_back_dropped_grants(&mut self) {
        let dropped = std::mem::take(&mut lock(&self.seen).dropped_grants);
        for (scope, token) in dropped {
            if self.live.open_fence().is_some_and(|f| f.scope == scope && f.token == token) {
                log::info!("live: a grant from the old link was never taken; giving it back");
                if let Err(e) = self.live.unlock(&scope) {
                    log::debug!("live: an unlock was not sent: {e}");
                }
            }
        }
    }

    /// The link broke in this pump for a reason of this side's own (the service ended or closed
    /// the session, the catch-up never came, an edit could not be sent) while local edits were
    /// still waiting for the room: hand them to the app ([`LiveApp::unmerged`]), as `Live` does
    /// for the drops it makes, unless it already handed over these same edits (in this pump or
    /// an earlier one).
    fn hand_unmerged_on_break(&mut self, app: &mut impl LiveApp<K>) {
        let edits = self.live.unconfirmed_edits();
        if edits == 0 {
            self.handed_unmerged = None;
        }
        if self.broken.is_none() || self.handed_unmerged == Some(edits) || edits == 0 {
            return;
        }
        if let Some(doc) = self.live.doc() {
            log::warn!("live: the link ended with {edits} local edits the room does not have; handing them to the app");
            app.unmerged(&doc.state(), edits);
            self.handed_unmerged = Some(edits);
        }
    }

    fn note_join(&mut self, app: &mut impl LiveApp<K>) {
        let (joins, space) = {
            let s = lock(&self.seen);
            (s.joins, s.space)
        };
        if joins == self.joins {
            return;
        }
        self.joins = joins;
        self.need_join = false;
        match space {
            Some(granted) if granted > 0 => {
                self.refused = None;
                // The replica keeps its first space across rejoins (`Live::space`): a new one
                // only when the room lost its history.
                let space = self.live.space().filter(|s| *s > 0).unwrap_or(granted);
                if space != self.space {
                    self.space = space;
                    app.set_space(space);
                    // The replica took a new client id, which starts its undo history again.
                    let mut s = lock(&self.shared);
                    s.undo.clear();
                    s.redo.clear();
                }
            }
            _ => self.refused = Some(NO_SPACE.to_owned()),
        }
    }

    fn act(&mut self, work: Work<Change<K>>, app: &mut impl LiveApp<K>) {
        match work {
            Work::Doc { changes } => self.take_remote(app, changes),
            Work::Reset => self.reset(app),
            Work::TextIncomplete { .. } => self.break_with(BROKEN),
            Work::Ephemeral { origin, kind, payload } => self.ephemeral(&origin, &kind, &payload, &*app),
            Work::Checkpointed { seq } => self.save_events.push(SaveEvent::Checkpointed { seq }),
            Work::CheckpointRaced => self.save_events.push(SaveEvent::Raced),
            Work::CheckpointRefused { reason } => self.save_events.push(SaveEvent::Refused { reason }),
            Work::SeedWanted { .. } => log::debug!("live: the shared document was seeded at the start"),
            Work::Command { command, .. } => log::warn!("live: ignoring the locked command {command} from another editor"),
            Work::Frame { .. } => {}
            // A lost grant is `Live`'s own word (a fenced edit refused, a refresh answered), not
            // a lock answer on the wire, so it joins the events here. So does a fenced grant:
            // `Live` announces it when its fence starts, after an older fence of the scope has
            // settled, and leaves out one that a rejoin in the same poll made stale.
            Work::Lock(event @ (LockEvent::Lost { .. } | LockEvent::Granted { token: Some(_), .. })) => lock(&self.seen).lock_events.push(event),
            // Other lock answers are read off the wire in `Seen`, in order with the joins.
            Work::Lock(_) => {}
            // Local edits only this replica holds are about to be dropped: the app saves them.
            Work::Unmerged { state, edits } => {
                self.handed_unmerged = Some(self.live.unconfirmed_edits());
                log::warn!("live: {edits} local edits the room does not have are about to be dropped; handing them to the app");
                app.unmerged(&state, edits);
            }
            // The relay lost the room's history. A replica that held a settled state goes on
            // (the saver checkpoints it at once, see `take_history_lost`); one that held nothing
            // waits for the snapshot (`Live::awaiting_snapshot`), see `update_status`.
            Work::HistoryLost => {
                log::warn!("live: the room lost its history");
                self.history_lost = true;
            }
            // The relay refused the ticket or the protocol and closed the link, and the hub has
            // stopped reconnecting until the host gives it a new ticket (`Hub::set_token`
            // through `live().hub_mut()`). Refused until a later join is granted a space.
            Work::ConnectionRefused { code, reason } => {
                log::warn!("live: the relay refused the connection ({code})");
                self.refused = Some(reason);
            }
        }
    }

    /// Remote edits `Live` already merged: the app's answer inside the replica, then the engine.
    fn take_remote(&mut self, app: &mut impl LiveApp<K>, changes: Vec<Change<K>>) {
        let fallback = changes.clone();
        // An answer, not a user command: never held to the bound on local edits.
        let changes = match edit_live(&mut self.live, Bound::Answer, |d| app.received(d, changes)) {
            Ok((changes, sent)) => {
                if !sent {
                    self.unsent();
                }
                changes
            }
            Err(_) => fallback,
        };
        self.apply(app, &changes);
    }

    fn apply(&mut self, app: &mut impl LiveApp<K>, changes: &[Change<K>]) {
        let Some(doc) = self.live.doc() else { return };
        if let Err(e) = app.apply(doc, changes) {
            log::warn!("live: a remote edit did not land: {e}");
            self.break_with(BROKEN);
        }
        self.follow_carets(&*app);
    }

    /// The whole shared part again from the replica.
    fn resync(&mut self, app: &mut impl LiveApp<K>) {
        let Some(doc) = self.live.doc() else { return };
        if let Err(e) = app.resync(doc) {
            log::warn!("live: a remote edit did not land: {e}");
            self.break_with(BROKEN);
        }
        self.follow_carets(&*app);
    }

    /// The replica was replaced by the room's checkpoint.
    fn reset(&mut self, app: &mut impl LiveApp<K>) {
        app.reset(self.space);
        let mut s = lock(&self.shared);
        s.undo.clear();
        s.redo.clear();
        drop(s);
        self.resync(app);
    }

    /// Share one batch of reported edits as one.
    fn share(&mut self, app: &mut impl LiveApp<K>, group: Vec<K::Edit>) {
        if group.is_empty() {
            return;
        }
        if !self.editable() {
            self.put_back(app, group);
            let why = self.not_editable();
            self.lose_run_offline(&why);
            self.notices.push(why);
            return;
        }
        let result = edit_live(&mut self.live, Bound::Local, |d| match app.local(d, &group) {
            Ok(updates) => (Ok(updates.len()), updates),
            Err(e) => (Err(e), Vec::new()),
        });
        match result {
            Ok((Ok(count), sent)) => {
                if !sent {
                    self.unsent();
                }
                if count > 0 {
                    let mut s = lock(&self.shared);
                    s.undo.push(count);
                    s.redo.clear();
                }
                match app.landed() {
                    Landed::Done => {}
                    Landed::Resync => self.resync(app),
                    Landed::Partial => {
                        self.resync(app);
                        self.notices.push(PARTIAL.to_owned());
                    }
                }
                self.follow_carets(&*app);
            }
            Ok((Err(LocalError::Refused(why)), _)) => {
                self.put_back(app, group);
                self.notices.push(why);
            }
            Ok((Err(LocalError::Failed(e)), _)) => {
                log::warn!("live: the shared document refused an edit: {e}");
                self.resync(app);
                self.notices.push(PARTIAL.to_owned());
            }
            // The link dropped (or the buffer filled) between `editable()` and the edit, so the
            // stricter offline bound applies now: a refusal like the one `editable()` makes.
            Err(Skipped::PastBound) => {
                self.put_back(app, group);
                let why = if self.editable() { BUFFER_FULL.to_owned() } else { self.not_editable() };
                self.lose_run_offline(&why);
                self.notices.push(why);
            }
            Err(Skipped::Broken) => {
                self.put_back(app, group);
                self.break_with(BROKEN);
            }
        }
    }

    /// An update this replica holds never reached the relay, so the room will not have it:
    /// carrying on would let this window drift from the others for good.
    fn unsent(&mut self) {
        self.lost = true;
        self.notices.push(UNSENT.to_owned());
        self.break_with(BROKEN);
    }

    fn put_back(&mut self, app: &mut impl LiveApp<K>, group: Vec<K::Edit>) {
        if let Err(e) = app.put_back(group) {
            log::warn!("live: a refused edit could not be put back: {e}");
            self.break_with(BROKEN);
        }
    }

    /// Undo or redo `steps` replica steps (one shared edit), then the engine from the replica.
    fn history(&mut self, app: &mut impl LiveApp<K>, steps: usize, redo: bool) {
        if !self.editable() {
            let mut s = lock(&self.shared);
            if redo {
                s.redo.push(steps)
            } else {
                s.undo.push(steps)
            }
            drop(s);
            let why = self.not_editable();
            self.lose_run_offline(&why);
            self.notices.push(why);
            return;
        }
        let mut done = 0;
        for _ in 0..steps {
            let step = if redo { self.live.redo() } else { self.live.undo() };
            match step {
                Ok(true) => done += 1,
                Ok(false) => break,
                Err(e @ (craft_collab::Error::Hub(_) | craft_collab::Error::Refused(_))) => {
                    // The step landed in the replica, but the room will never see it.
                    log::warn!("live: an undo step was not sent: {e}");
                    done += 1;
                    self.unsent();
                    break;
                }
                Err(e) => {
                    log::warn!("live: undo did not go through: {e}");
                    break;
                }
            }
        }
        if done > 0 {
            let mut s = lock(&self.shared);
            if redo {
                s.undo.push(done)
            } else {
                s.redo.push(done)
            }
            drop(s);
            self.resync(app);
        }
    }

    fn ephemeral(&mut self, origin: &str, kind: &str, payload: &Value, app: &impl LiveApp<K>) {
        match kind {
            SAVED_KIND => self.take_saved(origin, payload),
            NOTICE_KIND => {
                if let Some(text) = self.service_text(origin, payload, "text")
                    && self.told.get(origin) != Some(&text)
                {
                    self.told.insert(origin.to_owned(), text.clone());
                    self.notices.push(text);
                }
            }
            ENDED_KIND => {
                if let Some(reason) = self.service_text(origin, payload, "reason") {
                    log::warn!("live: the document service ended the session");
                    self.break_with(&reason);
                }
            }
            CURSOR_KIND => match app.caret(payload) {
                Some(caret) => {
                    self.carets.insert(origin.to_owned(), caret);
                }
                None => {
                    self.carets.remove(origin);
                }
            },
            _ => {}
        }
    }

    /// Field `field` of a service message: a non-empty string within [`SERVICE_TEXT_MAX_BYTES`],
    /// from the service uid only.
    fn service_text(&self, origin: &str, payload: &Value, field: &str) -> Option<String> {
        if uid_of(origin) != self.service_uid {
            log::debug!("live: ignoring a service message from a peer that is not the service");
            return None;
        }
        let text = payload.get(field).and_then(Value::as_str).map(str::trim)?;
        (!text.is_empty() && text.len() <= SERVICE_TEXT_MAX_BYTES).then(|| text.to_owned())
    }

    /// A `saved` marker counts only from the service uid, shaped `{seq, etag}` within the
    /// relay's size, and with `seq` not going back. One marked `closed` ends the link.
    fn take_saved(&mut self, origin: &str, payload: &Value) {
        if uid_of(origin) != self.service_uid {
            log::debug!("live: ignoring a saved marker from a peer that is not the service");
            return;
        }
        if payload.get("closed").and_then(Value::as_bool) == Some(true) {
            self.break_with(CLOSED);
        }
        let seq = payload.get("seq").and_then(Value::as_u64);
        let etag = payload.get("etag").and_then(Value::as_str);
        let small = serde_json::to_vec(payload).is_ok_and(|v| v.len() <= SAVED_MAX_BYTES);
        if let (Some(seq), Some(etag), true) = (seq, etag, small)
            && self.saved.as_ref().is_none_or(|(had, _)| seq >= *had)
        {
            self.saved = Some((seq, etag.to_owned()));
        }
    }

    /// Keep each peer's caret on the same content: followed through its edit, clamped to it,
    /// and dropped with it.
    fn follow_carets(&mut self, app: &impl LiveApp<K>) {
        if self.carets.is_empty() {
            return;
        }
        app.follow(&mut self.carets);
    }

    fn take_live_notices(&mut self) {
        for n in std::mem::take(&mut self.live.notices) {
            if n.starts_with("read-only access") {
                if self.access == Access::Edit {
                    self.broken = Some(READ_ONLY_BROKEN.to_owned());
                }
                continue;
            }
            self.notices.push(sentence(&n));
        }
    }

    fn update_status(&mut self) {
        let connected = self.live.connected();
        if !connected {
            self.need_join = true;
        }
        let (catching_up, replaced) = {
            let s = lock(&self.seen);
            (s.catching_up(), s.replaced)
        };
        if replaced {
            self.break_with(BEHIND);
        }
        let counting = catching_up && !self.need_join && self.broken.is_none() && self.refused.is_none();
        self.wait_for_catch_up(counting);
        let awaiting = self.live.awaiting_snapshot();
        self.wait_for_snapshot(awaiting && self.broken.is_none());
        if self.broken.is_none() && !self.need_join && self.space != 0 && !awaiting && !self.live.ready_to_type() {
            // Joined with a space, yet the replica takes no edits: Live gave up on it.
            self.broken = Some(BROKEN.to_owned());
        }
        self.status = if let Some(m) = &self.broken {
            LiveStatus::Broken(m.clone())
        } else if let Some(m) = &self.refused {
            LiveStatus::Refused(m.clone())
        } else if self.access == Access::Read {
            LiveStatus::ReadOnly
        } else if connected && !self.need_join && self.space != 0 && !catching_up && !awaiting {
            self.been_live = true;
            LiveStatus::Live { saved_at: self.saved.as_ref().map(|(_, e)| e.clone()) }
        } else if self.been_live {
            LiveStatus::Reconnecting
        } else {
            LiveStatus::Connecting
        };
        self.watch_saver();
    }

    /// Count how long the link has been live with no document service in the room.
    fn watch_saver(&mut self) {
        let service = &self.service_uid;
        if self.live.roster.iter().any(|p| uid_of(&p.peer) == service) {
            self.saver_wait = None;
            self.saver_missing = false;
            return;
        }
        if !matches!(self.status, LiveStatus::Live { .. }) {
            // Counted from Live; what it found stays until the service is seen again.
            self.saver_wait = None;
            return;
        }
        let now = self.clock.as_mut().map(|c| c());
        let wait = self.saver_wait.get_or_insert(CatchUp { since: now, frames: 0, told: false });
        wait.frames = wait.frames.saturating_add(1);
        let over = match (now, wait.since) {
            (Some(now), Some(t)) => now - t >= SAVER_WAIT_MS,
            _ => wait.frames >= SAVER_WAIT_FRAMES,
        };
        if over && !self.saver_missing {
            log::warn!("live: no document service in the room; this window's edits are not being saved");
            self.saver_missing = true;
        }
    }

    /// Count how long the join's catch-up has been arriving: say so once when it is late, and
    /// give up (reopen) when it is twice as late.
    fn wait_for_catch_up(&mut self, waiting: bool) {
        if !waiting {
            self.catch_up = None;
            return;
        }
        let now = self.clock.as_mut().map(|c| c());
        let since = self.catch_up.get_or_insert(CatchUp { since: now, frames: 0, told: false });
        since.frames = since.frames.saturating_add(1);
        // How many late periods have passed.
        let periods = match (now, since.since) {
            (Some(now), Some(t)) => (now - t) / CATCH_UP_LATE_MS,
            _ => f64::from(since.frames) / f64::from(CATCH_UP_LATE_FRAMES),
        };
        if periods >= 2.0 {
            self.catch_up = None;
            self.break_with(CATCH_UP_FAILED);
        } else if periods >= 1.0 && !std::mem::replace(&mut since.told, true) {
            self.notices.push(CATCHING_UP.to_owned());
        }
    }

    /// Count how long a replica that holds nothing has waited for the room's snapshot after the
    /// room lost its history, and give up (download a copy, reopen) after [`HISTORY_WAIT_MS`].
    fn wait_for_snapshot(&mut self, waiting: bool) {
        if !waiting {
            self.snapshot_wait = None;
            return;
        }
        let now = self.clock.as_mut().map(|c| c());
        let wait = self.snapshot_wait.get_or_insert(CatchUp { since: now, frames: 0, told: false });
        wait.frames = wait.frames.saturating_add(1);
        let over = match (now, wait.since) {
            (Some(now), Some(t)) => now - t >= HISTORY_WAIT_MS,
            _ => wait.frames >= HISTORY_WAIT_FRAMES,
        };
        if over {
            self.snapshot_wait = None;
            self.break_with(HISTORY_LOST);
        }
    }

    fn break_with(&mut self, msg: &str) {
        if self.broken.is_none() {
            self.broken = Some(msg.to_owned());
        }
    }

    fn send_due_cursor(&mut self) {
        let Some(payload) = self.cursor.pending.take() else { return };
        let now = self.clock.as_mut().map(|c| c());
        let due = match now {
            Some(now) => self.cursor.sent_at.is_none_or(|t| now - t >= CURSOR_INTERVAL_MS),
            None => self.cursor.last.is_none() || self.cursor.frames >= CURSOR_FRAMES,
        };
        if !due || !self.live.connected() {
            self.cursor.pending = Some(payload);
            return;
        }
        if let Err(e) = self.live.send_ephemeral(CURSOR_KIND, payload.clone()) {
            log::debug!("live: the caret did not go out: {e}");
            self.cursor.pending = Some(payload);
            return;
        }
        self.cursor.sent_at = now;
        self.cursor.frames = 0;
        self.cursor.last = Some(payload);
    }

    /// The other editors, without the document service, with their carets.
    fn roster(&mut self) -> Vec<Peer<K::Pos>> {
        let roster = &self.live.roster;
        self.carets.retain(|id, _| roster.iter().any(|p| &p.peer == id));
        roster
            .iter()
            .filter(|p| uid_of(&p.peer) != self.service_uid)
            .map(|p| Peer {
                id: p.peer.clone(),
                display: p.display.clone(),
                color_seed: fnv(uid_of(&p.peer)),
                caret: self.carets.get(&p.peer).map(K::pos),
                access: match p.access {
                    craft_collab::PeerAccess::Edit => Access::Edit,
                    craft_collab::PeerAccess::View => Access::Read,
                },
            })
            .collect()
    }
}

/// Make an edit on the replica with `f`, which returns its result and the updates it made,
/// and send those as one command (`Live::edit_all`, or `Live::answer_all` for an answer to
/// remote changes). `Err` when `f` never ran, and why; otherwise whether every update went out.
fn edit_live<H: Hub, R: Replica, T>(
    live: &mut Live<Tap<H>, R>,
    bound: Bound,
    f: impl FnOnce(&mut R) -> (T, Vec<Vec<u8>>),
) -> Result<(T, bool), Skipped> {
    let mut out = None;
    let run = |d: &mut R| {
        let (r, updates) = f(d);
        out = Some(r);
        Ok(updates)
    };
    let sent = match bound {
        Bound::Local => live.edit_all(run),
        Bound::Answer => live.answer_all(run),
    };
    let Some(r) = out else {
        return Err(match sent {
            Err(craft_collab::Error::Malformed { detail, .. }) if detail == craft_collab::PAST_BOUND => Skipped::PastBound,
            _ => Skipped::Broken,
        });
    };
    let sent = match sent {
        Ok(sent) => sent,
        Err(e) => {
            log::warn!("live: an update did not go out: {e}");
            false
        }
    };
    Ok((r, sent))
}

/// Whether [`edit_live`] holds an edit to the bound on local edits.
#[derive(Clone, Copy)]
enum Bound {
    /// A user command: refused past twice the bound ([`Live::edit_all`]).
    Local,
    /// The app's answer to remote changes: never refused for the bound ([`Live::answer_all`]).
    Answer,
}

/// Why [`edit_live`] did not run the edit.
#[derive(Debug, PartialEq, Eq)]
enum Skipped {
    /// Past twice the bound: nothing changed, the command can be tried again.
    PastBound,
    /// The replica is broken, damaged or not loaded.
    Broken,
}

/// A notice from the link as a sentence.
fn sentence(n: &str) -> String {
    let mut chars = n.chars();
    let mut s: String = chars.next().map(|c| c.to_uppercase().chain(chars).collect()).unwrap_or_default();
    if !s.ends_with(['.', '!', '?']) {
        s.push('.');
    }
    s
}

/// The uid part of a relay peer id (`<uid>~<hex>`).
pub fn uid_of(peer: &str) -> &str {
    peer.split('~').next().unwrap_or(peer)
}

/// FNV-1a, for a colour seed that is the same on every build and platform.
fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3))
}
