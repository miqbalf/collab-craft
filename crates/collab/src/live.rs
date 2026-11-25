//! A connected relay session with a shared yrs document, for native and browser hubs.
//!
//! The app maps its own content onto the blocks of a [`LiveDoc`] (a paragraph, a text frame).
//! This type owns the socket, the ordering session and the document replica, so WordCraft and
//! DeckCraft do not each carry a copy, and a wasm build uses the same code path as a desktop one.
//!
//! # The wire
//!
//! Every edit is one yrs update (v1), sent as a [`CRDT_TEXT`] submit whose params are
//! [`TextPayload`]: `{"scope": "doc", "ops": [<base64 update>]}`. The relay retains `ops` per
//! scope for joiners and never reads them, so its replay keeps working unchanged.
//!
//! # Checkpoints
//!
//! A saved file cannot be a joiner's starting point once the room has edited it: seeding its
//! text as client 0 gives different items from the ones the live replicas hold, and every later
//! edit anchored on those items would never land. So a checkpoint is a snapshot op instead,
//! `{"scope": "doc", "snap": true, "ops": [<base64 full state>]}`, made by [`Live::checkpoint`].
//! Once the relay orders it at `S` and nothing remote landed in between, the saver reports
//! `Saved{S - 1}`, so the relay keeps the snapshot plus everything after it.
//!
//! A peer joining for the first time whose first catch-up op is a snapshot starts from that
//! snapshot and ignores the file. Without a snapshot it seeds the file. It never mixes the two.
//!
//! # The join sequence an app has to respect
//!
//! 1. The relay grants an id space in its join payload. [`Live`] makes it the replica's client
//!    id. Nothing may be typed before that, and [`edit`](Live::edit) refuses until it arrives.
//! 2. The relay replays the updates this peer missed. Until the file is loaded they are held.
//! 3. The app seeds the document from the file through [`seed`](Live::seed). The held updates
//!    land on top, and [`seed`] answers with what they changed. When the catch-up began with a
//!    snapshot, `seed` restores that instead and the file's blocks are not used.
//!
//! The order of 2 and 3 does not matter, which is the point of holding. An app that seeds
//! on first paint and a relay that replays immediately both work.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::hub::Hub;
use crate::proto::{CRDT_TEXT, ClientMsg, FENCED, Fence, PeerId, RETRY, Scope, ServerMsg};
use crate::replica::Replica;
use crate::session::{Effect, Session};
use crate::ydoc::{BlockChange, LiveDoc, LiveError};

/// The one scope the document's updates travel under.
pub const DOC_SCOPE: &str = "doc";

/// What rides as `params` on a [`CRDT_TEXT`] command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextPayload {
    /// Always [`DOC_SCOPE`]. Kept on the wire because the relay retains updates per scope.
    pub scope: String,
    /// yrs updates (v1), base64.
    pub ops: Vec<String>,
    /// The first of `ops` is a checkpoint snapshot: a full document state.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub snap: bool,
}

impl TextPayload {
    /// The payload for one update.
    #[must_use]
    pub fn new(update: &[u8]) -> TextPayload {
        TextPayload { scope: DOC_SCOPE.to_owned(), ops: vec![update_to_wire(update)], snap: false }
    }

    /// The payload for a checkpoint snapshot of a full state ([`LiveDoc::state`]).
    #[must_use]
    pub fn snapshot(state: &[u8]) -> TextPayload {
        TextPayload { scope: DOC_SCOPE.to_owned(), ops: vec![update_to_wire(state)], snap: true }
    }
}

/// One yrs update as the wire carries it.
#[must_use]
pub fn update_to_wire(update: &[u8]) -> String {
    STANDARD.encode(update)
}

/// One yrs update back from the wire.
pub fn update_from_wire(op: &str) -> std::result::Result<Vec<u8>, LiveError> {
    STANDARD.decode(op).map_err(|_| LiveError::Decode)
}

/// Work the app must do after one [`Live::pump`].
///
/// What the relay said about a lock scope, in the order it said it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockEvent {
    /// This client holds `scope`. `seq` is the room's sequence at the grant, when the relay says;
    /// an edit under the lock is safe once the replica has applied up to it. `token` is the
    /// grant's fencing token when the relay fences: [`Live`] then sends every edit made while
    /// it holds the lock as an ordered edit under it, and gives it back with [`Live::unlock`].
    Granted { scope: Scope, seq: Option<u64>, token: Option<u64> },
    /// The request was refused; `holder` has the scope.
    Denied { scope: Scope, holder: PeerId },
    /// A scope this client held, or was refused, is free or no longer held.
    Released { scope: Scope },
    /// Somebody else took `scope`.
    HeldByOther { scope: Scope, holder: PeerId },
    /// This client no longer holds `scope` under the grant `token`: the relay refused an edit
    /// made under it (the lease lapsed and the scope may have been granted again), or a lease
    /// refresh found it gone. When an edit was refused, the edits under the lock that the relay
    /// did not order are taken out of the replica ([`Work::Reset`]) as soon as the relay has
    /// answered every edit made under it, usually in the same pump.
    Lost { scope: Scope, token: u64 },
}

/// Document updates are already merged here. [`Work::Doc`] says what changed, in the replica
/// schema's own terms (`C`, [`BlockChange`] for [`LiveDoc`]), to re-read through [`Live::doc`].
/// Other commands still go through the app's `apply_remote`.
#[derive(Debug)]
pub enum Work<C = BlockChange> {
    /// The relay answered a lock request, released one, or announced somebody else's.
    Lock(LockEvent),
    /// A lock-scoped command that is not a document update.
    Command { command: String, params: Value },
    /// Remote updates landed. Re-read what they changed.
    Doc { changes: Vec<C> },
    /// Load the document from the file, through [`Live::seed`].
    ///
    /// Raised when updates arrive before this replica has loaded it. They are held until it
    /// does, so the app can answer at its own pace.
    SeedWanted { scope: String },
    /// The document cannot be rebuilt from the file plus what the relay kept.
    ///
    /// The relay dropped updates, so the content has to come from somewhere else. The honest
    /// answers are to leave the collaboration or to reopen the file.
    TextIncomplete { scope: String },
    /// An opaque frame from another peer. The relay did not read it and neither did this crate.
    Frame { origin: String, payload: String },
    /// Latest-wins state about another peer: a camera, a selection, a caret.
    Ephemeral { origin: String, kind: String, payload: Value },
    /// The document was replaced by the room's checkpoint snapshot. Re-read every block.
    ///
    /// Raised when the app seeded the file before the catch-up showed that the room had
    /// checkpointed since that file was written.
    Reset,
    /// This replica's checkpoint was ordered at `seq`, and the relay acknowledged the save
    /// report through `seq - 1`: the room's join baseline is that snapshot now. Write the file
    /// now, from the same state the snapshot was taken of.
    Checkpointed { seq: u64 },
    /// The checkpoint was dropped for a reason that can pass, so retrying makes sense. Call
    /// [`Live::checkpoint`] again. Raised when:
    ///
    /// * a remote edit landed between this replica's snapshot and its ordering, so the snapshot
    ///   may miss it;
    /// * the link rejoined, the relay replaced the document, or the replica was reset onto the
    ///   room's own snapshot, so the submit may never be ordered or was taken of a state the
    ///   room does not share;
    /// * the relay refused the snapshot submit or the save report for a reason that may pass
    ///   (it could not order the edit, or no retained snapshot follows the reported sequence);
    /// * the save report never left this client;
    /// * no answer came within the checkpoint timeout (10 s unless
    ///   [`Live::set_checkpoint_timeout`] says otherwise) plus the snapshot's own transfer
    ///   time at [`crate::SLOW_LINK_BYTES_PER_SEC`]. A relay too old to acknowledge a save
    ///   never answers, so against one every checkpoint ends here.
    CheckpointRaced,
    /// The relay refused the checkpoint for a reason a retry does not change: this peer may
    /// only read, or the snapshot is larger than the relay carries. `reason` is the relay's
    /// own words, for the user. Do not retry until something changes (a smaller document, a
    /// ticket that may edit).
    ///
    /// Only a refusal the relay tied to this checkpoint (the snapshot submit's counter, or the
    /// save report's sequence) counts. Other relay errors leave a checkpoint in flight alone.
    CheckpointRefused { reason: String },
    /// The relay refused the connection itself and closed it: `code` is
    /// [`crate::REFUSED_AUTH`] (a bad or expired ticket), [`crate::REFUSED_PROTO`] or
    /// [`crate::REFUSED_FULL`]. The hub stops reconnecting; give it a new ticket with
    /// [`Hub::set_token`] (through [`Live::hub_mut`]) and it connects again.
    ConnectionRefused { code: String, reason: String },
    /// The replica holds local edits the room does not have, and they are about to be dropped:
    /// taken back after a lost ordered run ([`Work::Reset`] follows), or given up with the replica
    /// ([`Work::TextIncomplete`] follows, or the replica broke on a local edit). Raised before the
    /// work that drops them, at the front of the pump's work (or of the next pump's, when the
    /// replica broke inside [`Live::edit`]).
    ///
    /// `state` is this replica's full state (yrs update v1) with those edits in it, taken just
    /// before the drop: restore it ([`Replica::from_state`]) and save it as an "unmerged copy"
    /// so the user loses nothing. `edits` is how many local updates the room lacks, an upper
    /// bound (it counts free edits a take-back keeps and sends again). Raised once per drop; for
    /// a replica yrs found damaged, once, and that state may hold part of a broken update.
    Unmerged { state: Vec<u8>, edits: usize },
    /// The relay joined this link to a room that lost its history (`Joined.history_lost`): it
    /// restarted, or the room was dropped after its grace period, and nobody has checkpointed
    /// since. Raised once per such join.
    ///
    /// * A replica that held a settled document before this join (the document service's
    ///   saver, an editor that stayed open) keeps working. The saver should checkpoint at once
    ///   ([`Live::checkpoint`]): that snapshot is what every other replica restarts from.
    /// * A replica that held nothing from before (a fresh file seed) cannot trust the file: its
    ///   items are not the ones the running replicas hold. It refuses edits
    ///   ([`Live::awaiting_snapshot`]) until the room's next checkpoint snapshot arrives, then
    ///   restarts from it ([`Work::Reset`], or the snapshot is what [`Live::seed`] restores).
    HistoryLost,
}

/// Whether a relay error says something a retry cannot change.
///
/// The relay's errors carry no code, so this reads its wording: a read-only ticket, a message
/// past what the relay carries, and a checkpoint past what it retains for joiners.
#[must_use]
pub fn refusal_is_permanent(detail: &str) -> bool {
    // ReadOnly, NotTheSaver and TooLarge (both a submit and a checkpoint snapshot), and nothing
    // else: every other refusal of an edit is sent again.
    detail.starts_with("read-only access")
        || detail.starts_with("only the document service may checkpoint")
        || detail.contains("more than this relay will carry")
        || detail.contains("too large to retain")
        || detail.contains(INVALID_UPDATE)
}

/// The relay's wording (with `code: "invalid"`) for an update it will not order: it does not
/// decode, or its ids overflow. The same bytes would be refused again.
const INVALID_UPDATE: &str = "not a valid document update";

/// [`ServerMsg::Error`]'s `code` for it.
const INVALID: &str = "invalid";

/// The most local edit bytes a replica holds unconfirmed while the link is up. Past it, new local
/// edits are refused ([`Live::buffer_full`]) until the relay has ordered some; nothing held is
/// dropped. Also the most an ordered run's take-back log holds.
pub const MAX_UNCONFIRMED_BYTES: usize = 32 * 1024 * 1024;

/// The most local edit bytes a replica buffers while the link is down. Past it, new local edits
/// are refused until the link is back and the buffer has gone out; nothing buffered is dropped.
///
/// Half the relay's default retention per scope (`TEXT_BYTES`, 2 MiB, and every text edit is in
/// one scope): a reconnect bundle leaves at least half of what the relay keeps for joiners to
/// what the room edited since its last save, so one rejoin alone does not make their replay
/// incomplete. It uploads
/// in about 16 s on a slow link ([`crate::SLOW_LINK_BYTES_PER_SEC`]).
pub const OFFLINE_MAX_BYTES: usize = 1024 * 1024;

/// The detail of the `Error::Malformed` a local edit is refused with past twice the bound
/// ([`Live::edit`], [`Live::edit_all`]), so a caller can tell this refusal (nothing changed; try
/// again once the waiting edits are sent) from a broken or damaged replica.
pub const PAST_BOUND: &str = "too many edits are waiting for the other editors; nothing was changed";

/// The most local edits (separate updates) a replica holds unconfirmed, online or offline. About
/// half an hour of typing at one edit per keystroke; it bounds the per-edit bookkeeping and the
/// merge on the reconnect.
pub const MAX_UNCONFIRMED_EDITS: usize = 10_000;

/// How long a checkpoint may wait for its ordering and the save acknowledgement.
pub const CHECKPOINT_TIMEOUT: Duration = Duration::from_secs(10);

/// The first wait before an edit the relay refused for now is sent again. It doubles on each
/// refusal in a row, up to [`RETRY_MOST`], and starts over once an edit is confirmed.
const RETRY_FIRST: Duration = Duration::from_millis(250);

/// The longest wait before an edit refused for now is sent again.
const RETRY_MOST: Duration = Duration::from_secs(5);

/// A checkpoint snapshot this replica sent and the relay has not acknowledged yet.
struct PendingCheckpoint {
    /// The session's counter for the snapshot submit.
    local: u64,
    /// A remote update landed while it was in flight. Those are ordered before it and may be
    /// missing from it, so it must not be reported as saved.
    raced: bool,
    /// Once ordered: the snapshot's sequence, with the save report through the one before it
    /// sent and waiting for the relay's answer.
    ordered: Option<u64>,
    /// When it was sent, on [`now_ms`]. `None` where there is no clock.
    sent_at: Option<f64>,
    /// The time its own bytes take on a slow link ([`crate::transfer_time`]), on top of the
    /// checkpoint timeout: a large snapshot on a slow link is not an unanswered one.
    allowance: Duration,
}

impl PendingCheckpoint {
    /// The sequence its save report names, once sent.
    fn saved_through(&self) -> Option<u64> {
        self.ordered.map(|seq| seq.saturating_sub(1))
    }
}

/// An ordered run's fence: the lock the edits are made under, and what the replica needs to go
/// back to the room's state when the relay refuses one of them ([`FENCED`]).
///
/// From the grant until [`Live::unlock`], every local edit is made under the fence. The relay
/// orders a fenced edit only while this peer still holds the scope under the token, so a holder
/// that was paused or cut off past its lease can never append an edit after somebody else was
/// granted the lock. Edits under the fence go out one submit at a time: the next waits until the
/// relay has ordered the one before, so a refusal can never leave a later edit ordered behind a
/// refused one.
///
/// Once the grant is gone (an edit refused, a refresh answered, or a newer grant of the scope)
/// the fence is dead but stays until [`Live::unlock`]: edits made under it are never sent, and
/// whatever of the run the relay did not order is taken back. The replica is rebuilt from its
/// state at the grant plus everything the room has ordered since, which is the room's state, and
/// the app is told ([`Work::Reset`] and [`LockEvent::Lost`]).
struct Fenced {
    scope: Scope,
    token: u64,
    /// The replica's full state at the grant. `None` when it had none (not loaded, broken), or
    /// when [`Fenced::log`] outgrew [`MAX_UNCONFIRMED_BYTES`]: a take-back then breaks the replica.
    base: Option<Vec<u8>>,
    /// The updates the room ordered since [`Fenced::base`], in the order this replica took them:
    /// other peers' it applied, and its own fenced ones the relay ordered. Each says whether it
    /// was a checkpoint snapshot, which a take-back replays under the state item cap.
    log: Vec<(Vec<u8>, bool)>,
    log_bytes: usize,
    /// Edits made under the fence and not sent yet, oldest first.
    queue: Vec<Vec<u8>>,
    /// The one fenced submit sent and not answered yet.
    inflight: Option<InFlight>,
    /// Every counter a submit under this fence went out with: a refusal of any of them as
    /// [`FENCED`] means the grant is gone.
    sent: std::collections::BTreeSet<u64>,
    /// The relay refused [`Fenced::inflight`] for now: it goes again after the backoff.
    retry: bool,
    /// An edit made under the fence was refused, or never sent once the fence died: the replica
    /// holds what the room does not.
    owed_back: bool,
    /// The grant is gone. Nothing more is sent under the fence.
    dead: bool,
    /// The app gave the lock back. The unlock goes out once every edit under the fence is
    /// answered, and the fence ends then.
    unlocked: bool,
    /// Edits made after [`Live::unlock`] while the fence drains. Sent free once it ends cleanly;
    /// dropped with the run if it is taken back (they were made on state the room may not have).
    held_back: Vec<Vec<u8>>,
    /// [`LockEvent::Lost`] went out for this fence.
    told: bool,
    /// How long the relay's lease lasts unrefreshed.
    lease_ms: u64,
    /// When the lease was last granted or refreshed, on the clock [`Live::refresh_lease`] is given.
    refreshed_at: Option<f64>,
    /// A newer grant of this scope that starts its fence once this one ends.
    next: Option<Parked>,
}

/// A newer grant of the fenced scope, waiting for the old fence to settle. The app hears of it
/// ([`LockEvent::Granted`]) only once its fence starts, so no edit meant for it can land in the
/// old, dead fence and be taken back. Its lease is refreshed meanwhile.
struct Parked {
    token: u64,
    lease_ms: u64,
    seq: Option<u64>,
    refreshed_at: Option<f64>,
}

/// The fenced submit in flight.
struct InFlight {
    update: Vec<u8>,
    /// Every counter it was sent under, oldest first. A rejoin sends it again under a new one,
    /// and the relay's order for an old one can still arrive in the rejoin's catch-up (it was
    /// ordered just before the link dropped), which counts as its order too.
    locals: Vec<u64>,
}

impl Fenced {
    fn new(scope: &str, token: u64, lease_ms: u64, base: Option<Vec<u8>>) -> Fenced {
        Fenced {
            scope: scope.to_owned(),
            token,
            base,
            log: Vec::new(),
            log_bytes: 0,
            queue: Vec::new(),
            inflight: None,
            sent: std::collections::BTreeSet::new(),
            retry: false,
            owed_back: false,
            dead: false,
            unlocked: false,
            held_back: Vec::new(),
            told: false,
            lease_ms,
            refreshed_at: None,
            next: None,
        }
    }

    fn fence(&self) -> Fence {
        Fence { scope: self.scope.clone(), token: self.token }
    }

    /// Edits made under the fence (or held back behind it) the room has not ordered yet.
    fn outstanding(&self) -> usize {
        self.queue.len() + usize::from(self.inflight.is_some()) + self.held_back.len()
    }

    /// Note an update the room ordered since the grant (`snapshot`: a checkpoint snapshot).
    fn record(&mut self, update: &[u8], snapshot: bool) {
        if self.base.is_none() {
            return;
        }
        self.log_bytes += update.len();
        if self.log_bytes > MAX_UNCONFIRMED_BYTES {
            // Too much to replay: the run can no longer be taken back, only given up on.
            log::warn!("collab: an ordered run outgrew what it can take back; a take-back will break the replica");
            self.base = None;
            self.log.clear();
            self.log_bytes = 0;
            return;
        }
        self.log.push((update.to_vec(), snapshot));
    }
}

/// The lease an older relay grants without saying how long, in milliseconds.
const DEFAULT_LEASE_MS: u64 = 60_000;

/// A submit's `params` in wire bytes, as the relay measures them.
fn measure(params: &serde_json::Value) -> u64 {
    serde_json::to_vec(params).map_or(u64::MAX, |v| v.len() as u64)
}

/// Milliseconds on a clock that only moves forward.
#[cfg(not(target_arch = "wasm32"))]
fn now_ms() -> Option<f64> {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    Some(START.get_or_init(std::time::Instant::now).elapsed().as_secs_f64() * 1000.0)
}

/// Milliseconds on the page's clock.
#[cfg(all(target_arch = "wasm32", feature = "web-hub"))]
fn now_ms() -> Option<f64> {
    Some(js_sys::Date::now())
}

/// A wasm build without the page has no clock, so a checkpoint there never times out.
#[cfg(all(target_arch = "wasm32", not(feature = "web-hub")))]
fn now_ms() -> Option<f64> {
    None
}

/// Where this link's first join said its ordered catch-up sits.
#[derive(Clone, Copy)]
struct Catchup {
    /// The sequence the join started from. The catch-up's first op is `joined + 1`.
    joined: u64,
    /// The last sequence of the ordered replay, when the relay says (`Joined.replay_until`).
    until: Option<u64>,
}

impl Catchup {
    /// Whether the join replays anything a restart could start from.
    ///
    /// A baseline of 0 never had a save, and a save is only accepted just before a snapshot,
    /// so there is no snapshot to start from: the file is the room's own seed. A replay that
    /// ends where it starts is empty.
    fn replays_anything(self) -> bool {
        self.joined > 0 && self.until.is_none_or(|until| until > self.joined)
    }

    /// Whether the op ordered at `seq` belongs to the replay. An older relay does not mark
    /// the replay's end, so there the first op is taken to.
    fn covers(self, seq: u64) -> bool {
        self.until.is_none_or(|until| seq <= until)
    }

    /// Whether the op ordered at `seq` is the first op of the catch-up replay.
    fn starts_at(self, seq: u64) -> bool {
        self.joined > 0 && seq == self.joined + 1 && self.covers(seq)
    }
}

/// One live shared-editing link, over any [`Hub`], with a replica of any [`Replica`] schema
/// ([`LiveDoc`] unless named).
pub struct Live<H, R: Replica = LiveDoc> {
    hub: H,
    wire: Session,
    /// The replica, once the app has seeded it from the file.
    doc: Option<R>,
    /// Updates that arrived before the file was loaded, in arrival order. Each says whether it
    /// is a checkpoint snapshot (a second one before the load lands as an ordinary update, but
    /// is still checked under the state item cap).
    held: Vec<(Vec<u8>, bool)>,
    /// The id space the relay granted, once it has.
    space: Option<u32>,
    /// A snapshot to start from, waiting for [`Live::seed`].
    snapshot: Option<Vec<u8>>,
    /// The first join's ordered catch-up, until its first op has been handled. `None` when
    /// the join replays nothing.
    catchup: Option<Catchup>,
    /// The first join's catch-up comes as [`ServerMsg::TextOps`], which has not arrived yet.
    /// Kept apart from [`Live::catchup`] because a live op can reach the socket first, and
    /// that op must not use up the restart the text ops are owed.
    text_catchup: bool,
    /// The relay marks its replay (`Joined.replay_until`), so [`Live::text_catchup`] is known
    /// to be owed rather than assumed.
    marked: bool,
    /// Updates applied to a file-seeded replica while [`Live::text_catchup`] was pending.
    /// A restart onto the room's snapshot replaces that replica, so they are applied again.
    since_join: Vec<(Vec<u8>, bool)>,
    /// Some catch-up has been handled, or the link has gone live. A later join then shares
    /// the room's history and never restarts.
    settled: bool,
    /// Whether this link has joined before.
    joined_once: bool,
    /// The replica was seeded from the file, not restored from a snapshot.
    from_file: bool,
    /// A local edit has been sent from this replica.
    edited: bool,
    /// This replica can no longer be trusted: a snapshot did not restore, or yrs panicked. It
    /// never seeds the file afterwards and takes no more edits.
    broken: bool,
    /// A checkpoint in flight.
    checkpoint: Option<PendingCheckpoint>,
    /// How long [`Live::checkpoint`] waits for an answer.
    checkpoint_timeout: Duration,
    /// The largest submit, as the relay measures its params, the relay takes and retains
    /// (`Joined.text_cap`).
    text_cap: Option<u64>,
    /// A piece of the `text_ops` catch-up arrived and more follow (`TextOps.more`). The
    /// catch-up is not over, so a rejoin re-arms it and no checkpoint goes out.
    text_more: bool,
    /// This catch-up already told the app its text is incomplete. Told once, not per piece.
    told_incomplete: bool,
    /// Local updates sent and not yet confirmed, by submit counter. Sent again on a rejoin, and
    /// what tells a refused edit (broken replica) from any other refused submit.
    unconfirmed: std::collections::BTreeMap<u64, Vec<u8>>,
    /// The bytes in [`Live::unconfirmed`], bounded by [`MAX_UNCONFIRMED_BYTES`].
    unconfirmed_bytes: usize,
    /// Edits that were unconfirmed when the replica broke: the room will never order them.
    abandoned: usize,
    /// Submits in [`Live::unconfirmed`] the relay refused for now (a transient failure), to be
    /// sent again at [`Live::retry_at`].
    retrying: std::collections::BTreeSet<u64>,
    /// When to send [`Live::retrying`] again, on [`now_ms`]. `None` with no clock: next pump.
    retry_at: Option<f64>,
    /// The wait before the next resend, doubling from [`RETRY_FIRST`] to [`RETRY_MOST`].
    retry_wait: Duration,
    /// This replica joined a room that lost its history holding nothing from before, so the
    /// file it seeded is not the room's: it takes no edit until the room's snapshot restarts it.
    awaiting_snapshot: bool,
    /// [`Work::HistoryLost`] was raised for the current join.
    told_lost: bool,
    /// The ordered run's fence, from a fenced grant until [`Live::unlock`].
    fence: Option<Fenced>,
    /// The newest fencing token granted per scope, for [`Live::unlock`].
    grants: std::collections::HashMap<Scope, u64>,
    /// Unmerged local states taken before a drop, for [`Work::Unmerged`] at the next pump's front.
    unmerged: Vec<(Vec<u8>, usize)>,
    /// The join being handled lost the room's history: take the space it grants, even though
    /// this replica already has one.
    adopt_space: bool,
    /// The replica was found damaged and its unmerged state handed over once already.
    told_damaged: bool,
    /// Lines for the user, drained by the app each frame.
    pub notices: Vec<String>,
    /// Who else is in the document: the relay's peer id (a badge's colour is derived from it),
    /// the display name, and what the member may do ([`PeerAccess`](crate::PeerAccess)).
    pub roster: Vec<crate::Presence>,
    was_connected: bool,
}

impl<H: Hub> Live<H> {
    /// Takes an already-connected hub. The relay admits a peer from the token in the
    /// handshake; nothing is sent here on purpose. The replica is a [`LiveDoc`]; another schema
    /// starts with [`Live::with_replica`].
    pub fn new(hub: H, doc: impl Into<String>) -> Live<H> {
        Live::with_replica(hub, doc)
    }
}

impl<H: Hub, R: Replica> Live<H, R> {
    /// [`Live::new`] for a replica of schema `R`.
    pub fn with_replica(hub: H, doc: impl Into<String>) -> Live<H, R> {
        Live {
            hub,
            wire: Session::new(doc.into()),
            doc: None,
            held: Vec::new(),
            // No id space until the relay grants one. A default would be a client id another
            // peer is also minting with, and the two would corrupt each other's items.
            space: None,
            snapshot: None,
            catchup: None,
            text_catchup: false,
            marked: false,
            since_join: Vec::new(),
            settled: false,
            joined_once: false,
            from_file: false,
            edited: false,
            broken: false,
            checkpoint: None,
            checkpoint_timeout: CHECKPOINT_TIMEOUT,
            text_cap: None,
            text_more: false,
            told_incomplete: false,
            unconfirmed: std::collections::BTreeMap::new(),
            unconfirmed_bytes: 0,
            abandoned: 0,
            retrying: std::collections::BTreeSet::new(),
            retry_at: None,
            retry_wait: RETRY_FIRST,
            awaiting_snapshot: false,
            told_lost: false,
            fence: None,
            grants: std::collections::HashMap::new(),
            unmerged: Vec::new(),
            adopt_space: false,
            told_damaged: false,
            notices: Vec::new(),
            roster: Vec::new(),
            was_connected: true,
        }
    }

    /// The hub this link runs on.
    pub fn hub(&self) -> &H {
        &self.hub
    }

    /// The hub this link runs on, for a host that steers its transport (a fresh ticket).
    pub fn hub_mut(&mut self) -> &mut H {
        &mut self.hub
    }

    /// The replica, once seeded.
    pub fn doc(&self) -> Option<&R> {
        self.doc.as_ref()
    }

    /// Load the document and get what the held updates changed on top of it.
    ///
    /// When a checkpoint snapshot has arrived, the document is restored from that and `blocks`
    /// are not used: the file is behind the snapshot and its items are not the room's. If that
    /// restore fails the replica is broken for good and the file is never seeded instead,
    /// because the room has moved past it. A document already seeded is left alone and nothing
    /// is reported.
    pub fn seed(&mut self, blocks: &R::Seed) -> std::result::Result<Vec<R::Change>, LiveError> {
        if self.broken {
            return Err(LiveError::Decode);
        }
        if self.doc.is_some() {
            return Ok(Vec::new());
        }
        let mut doc = match self.snapshot.take() {
            Some(snapshot) => match R::from_state(&snapshot) {
                Ok(doc) => {
                    self.awaiting_snapshot = false;
                    doc
                }
                Err(e) => {
                    self.break_replica(&format!("the room's checkpoint did not restore: {e}"));
                    return Err(e);
                }
            },
            None => {
                let doc = R::seeded(blocks)?;
                self.from_file = true;
                doc
            }
        };
        if let Some(space) = self.space
            && let Err(e) = doc.set_client(space)
        {
            self.break_replica(&format!("the granted client id did not take: {e}"));
            return Err(e);
        }
        let mut changes = Vec::new();
        let held = std::mem::take(&mut self.held);
        if self.from_file && self.text_catchup && self.marked {
            // The text ops may still restart this file-seeded replica onto the room's
            // snapshot, and these would be lost with it.
            self.since_join.extend(held.iter().cloned());
        }
        for (update, snapshot) in held {
            let applied = if snapshot { doc.apply_state(&update) } else { doc.apply(&update) };
            match applied {
                Ok(found) => merge_changes(&mut changes, found),
                Err(e) => {
                    log::warn!("collab: a held update did not apply: {e}");
                    self.notices.push("could not apply a change from another editor".to_owned());
                }
            }
        }
        self.doc = Some(doc);
        Ok(changes)
    }

    /// How many local edit updates the relay has not ordered yet, counting one the relay refused
    /// for now and one waiting to be sent again after a rejoin. Tracked by update, not by wire
    /// counter: a resend takes a new counter, and the old one is gone. Edits dropped when the
    /// replica broke still count: the room never got them.
    pub fn unconfirmed_edits(&self) -> usize {
        self.unconfirmed.len() + self.abandoned + self.fence.as_ref().map_or(0, Fenced::outstanding)
    }

    /// The fence edits go out under now: the scope and the grant's token, while this replica
    /// holds a fenced grant it has not given back.
    pub fn fence(&self) -> Option<Fence> {
        self.fence.as_ref().filter(|f| !f.dead && !f.unlocked).map(Fenced::fence)
    }

    /// The fence of the ordered run this replica has not given back yet, in any state: live,
    /// lost (dead until [`Live::unlock`]) or draining towards its unlock. `None` once unlocked,
    /// or with no fenced grant. While it is `Some`, a local edit is part of the ordered run, not
    /// a free edit.
    pub fn open_fence(&self) -> Option<Fence> {
        self.fence.as_ref().filter(|f| !f.unlocked).map(Fenced::fence)
    }

    /// The client id space this replica mints in: the first the relay granted, kept across
    /// rejoins so the replica's own items, its undo history and an app's minted ids carry on
    /// (a new connection's space is taken only when the room lost its history). An app that
    /// mints its own ids takes the space from here, not from each join.
    pub fn space(&self) -> Option<u32> {
        self.space
    }

    /// Whether a new local command must be refused because too much is waiting for the relay:
    /// [`MAX_UNCONFIRMED_EDITS`] edits or more, or [`OFFLINE_MAX_BYTES`] or more while the link is
    /// down ([`MAX_UNCONFIRMED_BYTES`] while it is up). Nothing waiting is dropped; it clears as
    /// the relay orders what waits. `craft-live` refuses commands on it; an app on [`Live`]
    /// directly checks it before each command.
    pub fn buffer_full(&self) -> bool {
        self.past_bound(1)
    }

    /// Whether what waits for the relay is at `times` the bound or more. Edits made under a
    /// fence or held back behind one count, as do free ones.
    fn past_bound(&self, times: usize) -> bool {
        let bytes = if self.hub.connected() { MAX_UNCONFIRMED_BYTES } else { OFFLINE_MAX_BYTES };
        let (fenced, fenced_bytes) = self.fence.as_ref().map_or((0, 0), |f| {
            let parts = f.queue.iter().chain(&f.held_back).chain(f.inflight.as_ref().map(|i| &i.update));
            (f.outstanding(), parts.map(Vec::len).sum::<usize>())
        });
        self.unconfirmed.len() + fenced >= MAX_UNCONFIRMED_EDITS.saturating_mul(times)
            || self.unconfirmed_bytes + fenced_bytes >= bytes.saturating_mul(times)
    }

    /// The hard stop behind [`Live::buffer_full`], for an app that does not check it: past twice
    /// the bound, a local edit is refused before it touches the replica.
    fn backstop(&self) -> Result<()> {
        if self.past_bound(2) {
            return Err(Error::malformed("client", PAST_BOUND));
        }
        Ok(())
    }

    /// The highest relay sequence applied to the replica in order. An op that arrived ahead of
    /// a gap is held, not counted, until the ops before it have been applied.
    pub fn applied(&self) -> u64 {
        self.wire.applied()
    }

    /// Whether the relay has granted an id space and the document is loaded and sound, and the
    /// replica is not [waiting for a snapshot](Live::awaiting_snapshot). Editing is refused
    /// until all of that holds.
    pub fn ready_to_type(&self) -> bool {
        self.space.is_some() && self.doc.as_ref().is_some_and(|d| !d.damaged()) && !self.broken && !self.awaiting_snapshot
    }

    /// Whether this replica waits for the room's checkpoint snapshot before it may edit.
    ///
    /// Set when it joined a room that lost its history ([`Work::HistoryLost`]) holding nothing
    /// from before: the file it seeded has other item identities than the replicas still
    /// running, so an edit from it would never land right. Cleared when the room's next
    /// snapshot restarts it ([`Work::Reset`], or [`Live::seed`] restoring it). Meanwhile
    /// [`edit`](Live::edit), undo, redo and [`checkpoint`](Live::checkpoint) are refused, and
    /// the room's ordinary updates are not applied (the snapshot carries them).
    pub fn awaiting_snapshot(&self) -> bool {
        self.awaiting_snapshot
    }

    /// Whether the join's catch-up may still restart this replica onto the room's snapshot.
    /// A checkpoint is refused meanwhile: it would snapshot a replica about to be replaced.
    ///
    /// An older relay does not say whether text ops follow its join, so there only the ordered
    /// catch-up counts; a reset by its text ops still drops any checkpoint in flight.
    fn catching_up(&self) -> bool {
        self.catchup.is_some() || (self.text_catchup && self.marked)
    }

    /// Whether the transport believes it is connected.
    pub fn connected(&self) -> bool {
        self.hub.connected()
    }

    /// Make one local edit and send the update it produced. Lock-free.
    ///
    /// For example `live.edit(|d| d.insert_text(id, at, "x", &attrs))`.
    ///
    /// An edit whose update is past the relay's advertised cap (`Joined.text_cap`) is refused
    /// with [`Error::Refused`], permanently: the replica is broken and the file has to be
    /// reopened. Split very large pastes so each stays under it.
    ///
    /// Works while the link is down: the edit lands in the replica and goes out, merged with the
    /// others made meanwhile, when the link rejoins. Check [`buffer_full`](Live::buffer_full)
    /// before each user command and refuse the command while it holds: the bound is per command,
    /// so a command made of several updates (send those with [`Live::edit_all`]) is never cut in
    /// half. Past twice the bound this refuses by itself, before the replica changes.
    pub fn edit<F>(&mut self, edit: F) -> Result<()>
    where
        F: FnOnce(&mut R) -> std::result::Result<Vec<u8>, LiveError>,
    {
        self.backstop()?;
        let doc = self.sound_doc()?;
        let update = edit(doc).map_err(refusal)?;
        self.edited = true;
        self.send_update(&update)
    }

    /// [`Live::edit`] for one command that makes several updates: the bounds are checked once,
    /// before `edit` runs, and every update it returns is sent in order, so a command is never
    /// cut in half. `Ok(false)` when some update did not go out (it is in the replica, and the
    /// room will not have it); `Err` when the edit was refused before it ran, or failed.
    pub fn edit_all<F>(&mut self, edit: F) -> Result<bool>
    where
        F: FnOnce(&mut R) -> std::result::Result<Vec<Vec<u8>>, LiveError>,
    {
        self.backstop()?;
        self.edit_unbounded(edit)
    }

    /// [`Live::edit_all`] without the bound, for what the app writes in answer to remote changes
    /// (text it recovered or forwarded). That is not a user command, and skipping it would lose
    /// the answer, so the bound on local edits does not apply. Still refused when the replica is
    /// broken, damaged or not loaded.
    pub fn answer_all<F>(&mut self, edit: F) -> Result<bool>
    where
        F: FnOnce(&mut R) -> std::result::Result<Vec<Vec<u8>>, LiveError>,
    {
        self.edit_unbounded(edit)
    }

    fn edit_unbounded<F>(&mut self, edit: F) -> Result<bool>
    where
        F: FnOnce(&mut R) -> std::result::Result<Vec<Vec<u8>>, LiveError>,
    {
        let doc = self.sound_doc()?;
        let updates = edit(doc).map_err(refusal)?;
        let mut sent = true;
        for update in updates {
            if self.broken {
                // An update past the relay's cap broke the replica: the rest never goes out.
                sent = false;
                break;
            }
            self.edited = true;
            if let Err(e) = self.send_update(&update) {
                log::warn!("collab: an update of a local edit did not go out: {e}");
                sent = false;
            }
        }
        Ok(sent)
    }

    /// Send a checkpoint snapshot of the whole document.
    ///
    /// For the service saver only: the replica that saves the file, connected with the
    /// relay's service identity (`COLLABCRAFT_SERVICE_UID`). The relay refuses a snapshot from
    /// any other peer, permanently ([`Work::CheckpointRefused`]). When the relay orders it, [`pump`](Live::pump)
    /// reports [`Work::Checkpointed`] and tells the relay the save covers everything before it.
    /// [`Work::CheckpointRaced`] means try again; [`Work::CheckpointRefused`] means a retry
    /// will not help. One at a time.
    ///
    /// The order a saver keeps: call this, wait for [`Work::Checkpointed`], and only then
    /// write the file, from the state the snapshot was taken of. A file written before the
    /// checkpoint is accepted can get ahead of the room's join baseline, and a joiner that
    /// loads it would apply the same edits twice. [`Work::Checkpointed`] is raised only once
    /// the relay acknowledged the save report (`saved_ack`), so it means exactly that.
    ///
    /// Refused until [`ready_to_type`](Live::ready_to_type) holds and while the join's
    /// catch-up can still replace this replica with the room's snapshot. A snapshot larger
    /// than the relay retains (`Joined.text_cap`) is refused here with [`Error::Refused`],
    /// which, like [`Work::CheckpointRefused`], a retry does not change.
    ///
    /// Returns the snapshot: the full document state (yrs update v1) that was checkpointed.
    /// The saver writes the file from exactly these bytes, by restoring a document from them
    /// ([`LiveDoc::from_state`]) once [`Work::Checkpointed`] arrives, never from the live
    /// replica, which may have taken edits since.
    pub fn checkpoint(&mut self) -> Result<Vec<u8>> {
        if self.checkpoint.is_some() {
            return Err(Error::malformed("client", "a checkpoint is already in flight"));
        }
        if !self.ready_to_type() {
            return Err(Error::malformed("client", "the shared document is not ready to checkpoint"));
        }
        if self.catching_up() {
            return Err(Error::malformed("client", "the join's catch-up has not arrived yet"));
        }
        let doc = self.sound_doc()?;
        if doc.needs_resync() {
            // It dropped updates the room holds, so its state is not the room's: written as the
            // checkpoint it would take those edits out of the file and out of the replay.
            return Err(Error::malformed("client", "this replica is behind the room; reopen the file before saving"));
        }
        let state = doc.state();
        let params = serde_json::to_value(TextPayload::snapshot(&state)).map_err(|e| Error::malformed("client", e.to_string()))?;
        let bytes = measure(&params);
        if self.text_cap.is_some_and(|cap| bytes > cap) {
            return Err(Error::Refused(format!("that checkpoint is {bytes} bytes, too large to retain for joining editors")));
        }
        let allowance = crate::transfer_time(usize::try_from(bytes).unwrap_or(usize::MAX));
        let msg = self.wire.submit_crdt(CRDT_TEXT, params);
        let ClientMsg::Submit { local, .. } = &msg else {
            return Err(Error::malformed("client", "a checkpoint must be a submit"));
        };
        self.checkpoint = Some(PendingCheckpoint { local: *local, raced: false, ordered: None, sent_at: now_ms(), allowance });
        self.hub.send(msg)?;
        Ok(state)
    }

    /// How long a checkpoint waits for its ordering and the relay's acknowledgement before it
    /// is reported as [`Work::CheckpointRaced`], on top of the snapshot's own transfer time at
    /// [`crate::SLOW_LINK_BYTES_PER_SEC`]. [`CHECKPOINT_TIMEOUT`] by default.
    ///
    /// Measured from the submit, and checked at the end of each [`pump`](Live::pump), after
    /// that pump's messages: an answer that arrived in time is never beaten by the timer, and
    /// a checkpoint is reported as timed out on the first pump at or past the deadline.
    pub fn set_checkpoint_timeout(&mut self, timeout: Duration) {
        self.checkpoint_timeout = timeout;
    }

    /// Undo this replica's last local edit and send it. `false` when there is nothing to undo.
    pub fn undo(&mut self) -> Result<bool> {
        self.backstop()?;
        match self.sound_doc()?.undo() {
            Some(update) => {
                self.edited = true;
                self.send_update(&update).map(|()| true)
            }
            None => Ok(false),
        }
    }

    /// Whether undoing the newest `steps` local edits would take away another replica's text
    /// ([`Replica::undo_takes_others_text`]). `false` before the replica is seeded.
    #[must_use]
    pub fn undo_takes_others_text(&self, steps: usize) -> bool {
        self.doc.as_ref().is_some_and(|d| d.undo_takes_others_text(steps))
    }

    /// Whether redoing the newest `steps` undone edits would take away another replica's text
    /// ([`Replica::redo_takes_others_text`]). `false` before the replica is seeded.
    #[must_use]
    pub fn redo_takes_others_text(&self, steps: usize) -> bool {
        self.doc.as_ref().is_some_and(|d| d.redo_takes_others_text(steps))
    }

    /// Redo the last undone local edit and send it. `false` when there is nothing to redo.
    pub fn redo(&mut self) -> Result<bool> {
        self.backstop()?;
        match self.sound_doc()?.redo() {
            Some(update) => {
                self.edited = true;
                self.send_update(&update).map(|()| true)
            }
            None => Ok(false),
        }
    }

    /// Send a lock-scoped command. Document edits go through [`edit`](Live::edit).
    pub fn submit(&mut self, command: impl Into<String>, params: Value, scope: Option<String>) -> Result<()> {
        let msg = self.wire.submit(command, params, scope);
        self.hub.send(msg)
    }

    /// Ask the relay for `scope`. The answer arrives from [`pump`](Live::pump) as
    /// [`Work::Lock`].
    pub fn lock(&mut self, scope: &str) -> Result<()> {
        let msg = self.wire.request_lock(scope);
        self.hub.send(msg)
    }

    /// Give `scope` back. Nothing waits on it: a release cannot be refused.
    ///
    /// Carries the grant's fencing token when the relay gave one, so the relay releases only
    /// that grant: a late unlock never releases a newer one. Ends the fence: later edits are free
    /// again. While edits made under the lock are still unanswered the unlock waits for them
    /// (it would otherwise reach the relay first and get the holder's own last edit refused),
    /// and edits made meanwhile wait behind them. A dead fence (the grant was lost) ends here,
    /// once what the relay did not order is taken back.
    pub fn unlock(&mut self, scope: &str) -> Result<()> {
        let token = self.grants.get(scope).copied();
        match self.fence.as_mut() {
            Some(f) if f.scope == scope => {
                f.unlocked = true;
                let drained = f.inflight.is_none() && f.queue.is_empty() && !f.owed_back;
                if drained {
                    // Nothing waits: end it now (a dead one owes no unlock, but sending it is harmless).
                    let mut work = Vec::new();
                    self.settle_fence(&mut work);
                    debug_assert!(work.is_empty(), "a drained fence needs no take-back");
                }
                Ok(())
            }
            _ => {
                self.grants.remove(scope);
                let msg = self.wire.release_grant(scope, token);
                self.hub.send(msg)
            }
        }
    }

    /// Extend the lease of the fenced grant, at half the lease, while it is held. `now_ms` is the
    /// host's clock in milliseconds. A long ordered run (a macro) keeps its lock this way; the
    /// relay also extends it on each fenced edit. When the lease is gone the relay answers and
    /// [`pump`](Live::pump) reports [`LockEvent::Lost`].
    pub fn refresh_lease(&mut self, now_ms: f64) {
        let Some(f) = self.fence.as_mut() else { return };
        let scope = f.scope.clone();
        // The live grant, or a newer one parked behind a dead fence.
        let (token, lease_ms, refreshed_at) = match f.next.as_mut() {
            Some(next) => (next.token, next.lease_ms, &mut next.refreshed_at),
            None if !f.dead && !f.unlocked => (f.token, f.lease_ms, &mut f.refreshed_at),
            None => return,
        };
        let at = *refreshed_at.get_or_insert(now_ms);
        if now_ms - at < lease_ms as f64 / 2.0 {
            return;
        }
        *refreshed_at = Some(now_ms);
        let msg = self.wire.refresh_lock(scope, token);
        if let Err(e) = self.hub.send(msg) {
            log::debug!("collab: a lease refresh was not sent: {e}");
        }
    }

    /// Pass an opaque frame to the room, unread by the relay.
    ///
    /// For a caller holding its own ordering, signing and encryption. Nothing here inspects
    /// the payload, and the relay keeps no copy.
    pub fn send_frame(&mut self, payload: impl Into<String>) -> Result<()> {
        let msg = self.wire.frame(payload);
        self.hub.send(msg)
    }

    /// Tell the room where this peer is: a camera, a selection, a caret.
    ///
    /// Latest-wins and never journaled. The relay keeps only the newest value per kind and
    /// hands it to a joining peer, so somebody arriving late sees where everyone is without
    /// waiting for them to move.
    pub fn send_ephemeral(&mut self, kind: impl Into<String>, payload: Value) -> Result<()> {
        let msg = self.wire.ephemeral(kind, payload);
        self.hub.send(msg)
    }

    /// One frame of inbound work. Outbound is the app's: it calls edit/submit
    /// first, then this.
    pub fn pump(&mut self) -> Vec<Work<R::Change>> {
        let now = self.hub.connected();
        if now != self.was_connected {
            self.notices.push(if now {
                "reconnected to the other editors".to_owned()
            } else {
                "not connected to the other editors; your edits are waiting".to_owned()
            });
            self.was_connected = now;
        }

        let mut work = Vec::new();
        for msg in self.hub.poll() {
            if let (Some(code), ServerMsg::Error { detail, .. }) = (msg.refuses_connection(), &msg) {
                log::warn!("collab: the relay refused the connection ({code}): {detail}");
                work.push(Work::ConnectionRefused { code: code.to_owned(), reason: detail.clone() });
            }
            if let ServerMsg::Joined { seq, replay_until, fallback, text_cap, history_lost, .. } = &msg {
                self.text_cap = *text_cap;
                self.joined(*seq, *replay_until, *fallback, *history_lost, &mut work);
                let named = msg.doc().to_owned();
                if named != self.wire.doc() {
                    log::info!("collab: the relay named the room {named}");
                    self.wire = Session::new(named);
                }
            }
            let effects = self.wire.handle(msg);
            for effect in effects {
                self.act(effect, &mut work);
            }
        }
        self.time_out_checkpoint(&mut work);
        self.send_retries();
        self.settle_fence(&mut work);
        // Unmerged states go first: each was taken before the work that drops its edits.
        let unmerged = std::mem::take(&mut self.unmerged).into_iter().map(|(state, edits)| Work::Unmerged { state, edits });
        work.splice(0..0, unmerged);
        work
    }

    /// Keep the replica's state for [`Work::Unmerged`] when it holds local edits the room does
    /// not (`extra` more than the ones tracked), before they are dropped. Taken once per drop: a
    /// state already waiting for the next pump covers it.
    fn keep_unmerged(&mut self, extra: usize) {
        let edits = self.unconfirmed.len() + self.fence.as_ref().map_or(0, Fenced::outstanding) + extra;
        if edits == 0 || !self.unmerged.is_empty() {
            return;
        }
        if let Some(doc) = self.doc.as_ref() {
            log::info!("collab: keeping this replica's {edits} unmerged edits for the app before they are dropped");
            self.unmerged.push((doc.state(), edits));
        }
    }

    /// The document, when it may take a local edit.
    fn sound_doc(&mut self) -> Result<&mut R> {
        if self.broken {
            return Err(Error::malformed("client", "the shared document is broken; reopen the file"));
        }
        if self.awaiting_snapshot {
            return Err(Error::malformed("client", "the live session lost its history; editing waits for the document service's snapshot"));
        }
        let doc = self.doc.as_mut().ok_or_else(|| Error::malformed("client", "the document was never seeded"))?;
        if doc.damaged() {
            return Err(Error::malformed("client", LiveError::Damaged.to_string()));
        }
        Ok(doc)
    }

    /// Give up on this replica for good, and say so.
    fn break_replica(&mut self, why: &str) {
        log::warn!("collab: {why}");
        self.keep_unmerged(0);
        self.broken = true;
        self.snapshot = None;
        self.abandoned += self.unconfirmed.len();
        self.unconfirmed.clear();
        self.unconfirmed_bytes = 0;
        self.retrying.clear();
        self.held.clear();
        if let Some(f) = self.fence.take() {
            self.abandoned += f.outstanding();
        }
        self.notices.push("the shared document could not be restored; reopen the file".to_owned());
    }

    /// A join, first or not. Only the first one's catch-up can restart the replica; a later one
    /// re-arms it only if no catch-up was handled and nothing went live since. A checkpoint in
    /// flight is treated as lost, because the submit may not survive the reconnect.
    ///
    /// On a join that lost the room's history, a replica that held nothing from before waits
    /// for a snapshot, and [`Work::HistoryLost`] is raised either way.
    fn joined(&mut self, seq: u64, replay_until: Option<u64>, fallback: bool, history_lost: bool, work: &mut Vec<Work<R::Change>>) {
        if history_lost {
            // What it held before is only the room's if it settled on the room's history,
            // which a replica still waiting for a snapshot never did.
            let held_before = self.joined_once && self.settled && !self.awaiting_snapshot && self.doc.is_some();
            if !held_before && !self.broken && !self.awaiting_snapshot {
                log::warn!("collab: the relay lost this room's history; waiting for the room's snapshot");
                self.awaiting_snapshot = true;
                self.notices.push("the live session restarted; waiting for the document service before editing".to_owned());
            }
            if !std::mem::replace(&mut self.told_lost, true) {
                work.push(Work::HistoryLost);
            }
        } else {
            self.told_lost = false;
        }
        // Only a room that lost its history may hand this replica a new client id space: then
        // the counter behind the spaces may have started over, and the old one could be granted
        // to somebody else. Otherwise the replica keeps minting in its own (see `Live::space`).
        self.adopt_space = history_lost;
        if !self.joined_once || !self.settled {
            let catchup = Catchup { joined: seq, until: replay_until };
            self.catchup = catchup.replays_anything().then_some(catchup);
            // A relay that marks the replay also says whether text ops follow. An older one
            // does not, and its text ops have always been the catch-up.
            self.text_catchup = fallback || replay_until.is_none();
            if self.catchup.is_none() && !self.text_catchup && !self.awaiting_snapshot && self.doc.is_some() {
                // A marked join with nothing to replay: there is no snapshot to restart from, so
                // the loaded replica is the room's. Settled now, a later join (a reconnect after
                // edits made offline) never restarts it onto a snapshot and drops those edits.
                self.settled = true;
            }
        } else {
            self.catchup = None;
            self.text_catchup = false;
        }
        self.marked = replay_until.is_some();
        self.since_join.clear();
        // Pieces of an earlier catch-up that never finished are abandoned: the relay sends the
        // whole catch-up again from its first piece.
        self.text_more = false;
        self.told_incomplete = false;
        if self.joined_once {
            self.drop_old_grants(work);
            self.resend_unconfirmed();
            // The fenced edit in flight goes again under its own fence. If the relay ordered it
            // just before the drop, the catch-up above carries that order and it counts; if
            // not, the relay answers this one.
            self.resend_fenced();
        }
        self.retrying.clear();
        self.retry_at = None;
        self.joined_once = true;
        self.lose_checkpoint(work, "the link rejoined");
    }

    /// A rejoin: grants the old link was given and the app has not heard of yet are gone with
    /// that link (the relay released them on the disconnect), and nobody would ever unlock their
    /// fences. A grant announced earlier in this same pump is taken out of `work` again (a join
    /// drops every lock answer before it), and its fence, which nothing was made under yet, is
    /// given back. A newer grant parked behind the fence is dropped the same way. The app asks
    /// again on the rejoin.
    fn drop_old_grants(&mut self, work: &mut Vec<Work<R::Change>>) {
        let mut untold = Vec::new();
        work.retain(|w| match w {
            Work::Lock(LockEvent::Granted { scope, token: Some(token), .. }) => {
                untold.push((scope.clone(), *token));
                false
            }
            _ => true,
        });
        if let Some(f) = self.fence.as_mut()
            && let Some(next) = f.next.take()
        {
            log::info!("collab: a grant parked behind an ordered run went with the old link");
            if self.grants.get(&f.scope) == Some(&next.token) {
                self.grants.remove(&f.scope);
            }
            let msg = self.wire.release_grant(&f.scope, Some(next.token));
            if let Err(e) = self.hub.send(msg) {
                log::debug!("collab: an unlock was not sent: {e}");
            }
        }
        for (scope, token) in untold {
            let empty = self.fence.as_ref().is_some_and(|f| f.scope == scope && f.token == token && f.outstanding() == 0 && !f.owed_back);
            if empty && let Some(f) = self.fence.take() {
                log::info!("collab: a grant the app never heard of went with the old link; giving it back");
                self.end_fence(&f);
            }
        }
    }

    /// Drop a checkpoint whose submit may never be ordered, reported as raced so it is retried.
    fn lose_checkpoint(&mut self, work: &mut Vec<Work<R::Change>>, why: &str) {
        if self.checkpoint.take().is_some() {
            log::info!("collab: a checkpoint in flight was dropped: {why}");
            work.push(Work::CheckpointRaced);
        }
    }

    /// Drop the checkpoint in flight on a refusal the relay tied to it. Permanent refusals are
    /// told apart from the rest, so an app that retries on [`Work::CheckpointRaced`] does not
    /// loop on them.
    fn refuse_checkpoint(&mut self, work: &mut Vec<Work<R::Change>>, detail: &str) {
        if refusal_is_permanent(detail) {
            if self.checkpoint.take().is_some() {
                log::info!("collab: the relay refused a checkpoint: {detail}");
                work.push(Work::CheckpointRefused { reason: detail.to_owned() });
            }
        } else {
            self.lose_checkpoint(work, "the relay refused it for now");
        }
    }

    /// Give up on a checkpoint that got no answer in time.
    fn time_out_checkpoint(&mut self, work: &mut Vec<Work<R::Change>>) {
        let Some((sent_at, allowance)) = self.checkpoint.as_ref().and_then(|p| p.sent_at.map(|at| (at, p.allowance))) else { return };
        let Some(now) = now_ms() else { return };
        if now - sent_at >= (self.checkpoint_timeout + allowance).as_secs_f64() * 1000.0 {
            self.lose_checkpoint(work, "no answer came in time");
        }
    }

    /// The submit's size as the relay measures it, when that is past the cap it advertised.
    ///
    /// The relay measures a submit by its serialized params, the same bytes counted here, and
    /// takes any up to the cap; the ops it retains are a part of them. The transport carries
    /// the cap plus a margin for the message around the params, so a submit within the cap is
    /// never dropped at the socket.
    fn over_cap(&self, params: &serde_json::Value) -> Option<u64> {
        let bytes = measure(params);
        self.text_cap.is_some_and(|cap| bytes > cap).then_some(bytes)
    }

    /// Send one local update.
    ///
    /// An update past the relay's cap would be dropped with the socket, and the reconnect
    /// would send it again, so it is refused here for good with [`Error::Refused`]. It has
    /// already landed in this replica, which the room will never see, so the replica is
    /// broken too: carrying on would let the two drift apart for the rest of the session.
    fn send_update(&mut self, update: &[u8]) -> Result<()> {
        match self.fence.as_mut() {
            None => self.send_with(update, None).map(|_| ()),
            Some(f) if f.unlocked => {
                // The lock is given back and edits under it are still unanswered: this one
                // waits, unsent, for the fence to end (see `Fenced::held_back`).
                f.held_back.push(update.to_vec());
                Ok(())
            }
            Some(f) => {
                // Under the lock: queued, and sent once the edit before it is ordered. Under a
                // dead fence it is never sent, and is taken back with the run.
                f.queue.push(update.to_vec());
                if f.dead {
                    f.owed_back = true;
                }
                self.send_fenced()
            }
        }
    }

    /// Send the next queued edit under the fence, merged with the ones after it where they fit,
    /// when none is in flight and the grant is still current.
    fn send_fenced(&mut self) -> Result<()> {
        let Some(f) = self.fence.as_mut() else { return Ok(()) };
        if f.dead || f.inflight.is_some() || f.queue.is_empty() || self.broken {
            return Ok(());
        }
        let queue = std::mem::take(&mut f.queue);
        let fence = f.fence();
        let mut chunks = self.chunked(queue).into_iter();
        let Some(first) = chunks.next() else { return Ok(()) };
        if let Some(f) = self.fence.as_mut() {
            f.queue = chunks.collect();
        }
        let local = self.send_with(&first, Some(fence));
        // Kept unless it never left for good (too large: the replica broke, and the fence went
        // with it). One the hub could not send goes again after the backoff, or on the rejoin.
        if let Some(f) = self.fence.as_mut() {
            let locals: Vec<u64> = local.as_ref().ok().copied().flatten().into_iter().collect();
            f.sent.extend(locals.iter().copied());
            f.inflight = Some(InFlight { update: first, locals });
            if local.is_err() {
                f.retry = true;
                if self.retry_at.is_none() {
                    self.retry_at = now_ms().map(|now| now + self.retry_wait.as_secs_f64() * 1000.0);
                    self.retry_wait = (self.retry_wait * 2).min(RETRY_MOST);
                }
            }
        }
        local.map(|_| ())
    }

    /// Send the fenced edit in flight again, under the fence it was made under, with a new
    /// counter: after a rejoin, or once a refusal for now has waited out its backoff.
    fn resend_fenced(&mut self) {
        let Some(f) = self.fence.as_mut() else { return };
        let Some(update) = f.inflight.as_ref().map(|i| i.update.clone()) else { return };
        f.retry = false;
        let fence = f.fence();
        match self.send_with(&update, Some(fence)) {
            Ok(Some(local)) => {
                if let Some(f) = self.fence.as_mut() {
                    f.sent.insert(local);
                    if let Some(i) = f.inflight.as_mut() {
                        i.locals.push(local);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                log::warn!("collab: an edit made under the lock could not be sent again: {e}");
                if let Some(f) = self.fence.as_mut().filter(|f| f.inflight.is_some()) {
                    f.retry = true;
                    if self.retry_at.is_none() {
                        self.retry_at = now_ms().map(|now| now + self.retry_wait.as_secs_f64() * 1000.0);
                        self.retry_wait = (self.retry_wait * 2).min(RETRY_MOST);
                    }
                }
            }
        }
    }

    /// Send one local update, fenced or free. A free one is tracked in [`Live::unconfirmed`]
    /// until the relay orders it; a fenced one is [`Fenced::inflight`]'s. Returns its counter.
    fn send_with(&mut self, update: &[u8], fence: Option<Fence>) -> Result<Option<u64>> {
        let params = serde_json::to_value(TextPayload::new(update)).map_err(|e| Error::malformed("client", e.to_string()))?;
        if let Some(bytes) = self.over_cap(&params) {
            // The edit itself is in the replica and tracked nowhere: count it.
            self.keep_unmerged(1);
            self.break_replica(&format!("a local edit of {bytes} bytes is past what the relay carries"));
            self.notices.push("that change is too large to share with the other editors; reopen the file".to_owned());
            return Err(Error::Refused(format!("that change is {bytes} bytes, more than this relay will carry")));
        }
        let fenced = fence.is_some();
        let msg: ClientMsg = self.wire.submit_fenced(CRDT_TEXT, params, fence);
        let local = match &msg {
            ClientMsg::Submit { local, .. } => Some(*local),
            _ => None,
        };
        if let (Some(local), false) = (local, fenced) {
            self.unconfirmed.insert(local, update.to_vec());
            self.unconfirmed_bytes += update.len();
        }
        // Past the buffer's bounds, the next local edit is refused (`Live::buffer_full`); what
        // is held here is never dropped for it.
        self.hub.send(msg)?;
        Ok(local)
    }

    /// Send again every local update the relay has not confirmed, after a rejoin.
    ///
    /// A link that drops can take submits with it: sent into a socket that was already dying,
    /// or refused at a hub that was reconnecting. They are in this replica and maybe never in
    /// the room. Sending them again is safe either way: an update the room already has merges
    /// as a no-op.
    ///
    /// They go as one update, merged. A hub drops what is sent while its link is down
    /// ([`crate::native::NativeHub`]), so this is the one path an edit made in an outage takes.
    fn resend_unconfirmed(&mut self) {
        let updates = std::mem::take(&mut self.unconfirmed);
        self.unconfirmed_bytes = 0;
        if self.broken || self.doc.is_none() || updates.is_empty() {
            return;
        }
        log::info!("collab: sending {} unconfirmed edits again after the rejoin", updates.len());
        self.resend(updates.into_iter().collect(), "unconfirmed edits could not be sent again");
    }

    /// Send free `updates` (by their old counters) again, merged where they fit. Edits made under
    /// a fence are never in here: [`Live::resend_fenced`] sends those, under their own fence.
    fn resend(&mut self, updates: Vec<(u64, Vec<u8>)>, why: &str) {
        for chunk in self.chunked(updates.into_iter().map(|(_, update)| update)) {
            if self.broken {
                return;
            }
            if let Err(e) = self.send_with(&chunk, None) {
                log::warn!("collab: {why}: {e}");
            }
        }
    }

    /// Send again the edits the relay refused for now, once their backoff is over and the link
    /// is up. While it is down they wait for the rejoin, which sends every unconfirmed edit.
    fn send_retries(&mut self) {
        let fenced = self.fence.as_ref().is_some_and(|f| f.retry);
        if (self.retrying.is_empty() && !fenced) || self.broken || !self.hub.connected() {
            return;
        }
        if let (Some(at), Some(now)) = (self.retry_at, now_ms())
            && now < at
        {
            return;
        }
        self.retry_at = None;
        let mut updates = Vec::new();
        for local in std::mem::take(&mut self.retrying) {
            if let Some(update) = self.unconfirmed.remove(&local) {
                self.unconfirmed_bytes -= update.len();
                updates.push((local, update));
            }
        }
        log::info!("collab: sending {} edits the relay refused for now again", updates.len());
        self.resend(updates, "an edit refused for now could not be sent again");
        if fenced {
            self.resend_fenced();
        }
    }

    /// Merge `updates` in order into updates that surely fit the relay's caps: the text cap
    /// and the op block cap ([`MAX_OP_BLOCKS`](crate::MAX_OP_BLOCKS), counted as
    /// [`update_blocks`](crate::update_blocks) counts it), past which the relay refuses an op for
    /// good. Bytes alone do not bound the blocks: at the default text cap (just under 16 MiB) a
    /// chunk of letters each typed apart from the last (about 152 k blocks per MiB) would pass
    /// 2^20 blocks (re-review R3-M6). An update that does not fit with anything goes as it was:
    /// it was sent once, so it fits alone.
    ///
    /// Merging only joins the updates' own items: no clock is minted, so an update the room
    /// already holds merges and lands as a no-op. Each group is merged once, so a reconnect
    /// bundle of thousands of small edits costs about its own size, not a pass per edit. The
    /// grouping is greedy and judged on the parts' summed size and blocks, not on the merged
    /// update, so it is not the fewest chunks possible: near the caps a chunk can be smaller
    /// than it needs to be, never larger.
    fn chunked(&self, updates: impl IntoIterator<Item = Vec<u8>>) -> Vec<Vec<u8>> {
        // The params a submit of `bytes` of update would serialize to, about: base64, the
        // quotes, and the scope around it. Merged updates are no larger than the parts summed,
        // so a group within 90% of the cap by this estimate surely fits.
        let estimate = |bytes: usize| bytes.div_ceil(3) * 4 + 32;
        let budget = self.text_cap.map(|cap| usize::try_from(cap).unwrap_or(usize::MAX) / 10 * 9);
        let fits = |summed: usize| budget.is_none_or(|budget| estimate(summed) <= budget);
        // A merge writes no more real blocks, client sections or delete entries than its parts
        // (it drops or trims overlap, never splits a block), and at most one skip per real
        // block: so twice the parts' summed count bounds the merged count, and a group whose
        // summed count is under half the cap surely fits. An update the walk cannot read counts
        // as past the cap, so it goes alone.
        let blocks_of = |update: &[u8]| crate::update_blocks(update).unwrap_or(usize::MAX / 4);
        let blocks_fit = |summed: usize| summed.saturating_mul(2) <= crate::MAX_OP_BLOCKS;
        // Each group, with the summed size and block count of the updates in it.
        let mut groups: Vec<(Vec<Vec<u8>>, usize, usize)> = Vec::new();
        for update in updates {
            let blocks = blocks_of(&update);
            match groups.last_mut() {
                Some((group, summed, summed_blocks)) if fits(*summed + update.len()) && blocks_fit(summed_blocks.saturating_add(blocks)) => {
                    *summed += update.len();
                    *summed_blocks = summed_blocks.saturating_add(blocks);
                    group.push(update);
                }
                _ => {
                    let size = update.len();
                    groups.push((vec![update], size, blocks));
                }
            }
        }
        let mut chunks = Vec::new();
        for (group, ..) in groups {
            if group.len() == 1 {
                chunks.extend(group);
                continue;
            }
            match yrs::merge_updates_v1(&group) {
                Ok(merged) => chunks.push(merged),
                Err(e) => {
                    log::warn!("collab: {} edits did not merge ({e}); they go one by one", group.len());
                    chunks.extend(group);
                }
            }
        }
        chunks
    }

    /// Merge a batch of wire updates, or hold them until the document is loaded.
    ///
    /// `snap` says the first update is a checkpoint snapshot. `restart` says it is the one this
    /// replica must start from (the first op of its first catch-up).
    fn merge_updates(&mut self, scope: String, ops: &[String], snap: bool, restart: bool, work: &mut Vec<Work<R::Change>>) {
        if scope != DOC_SCOPE {
            log::warn!("collab: ignoring updates for scope {scope:?}, not {DOC_SCOPE:?}");
            return;
        }
        if self.broken {
            return;
        }
        let mut updates = Vec::new();
        for (i, op) in ops.iter().enumerate() {
            match update_from_wire(op) {
                Ok(u) => updates.push(u),
                Err(e) if snap && i == 0 => {
                    self.break_replica(&format!("a checkpoint snapshot did not decode: {e}"));
                    work.push(Work::TextIncomplete { scope });
                    return;
                }
                Err(e) => {
                    log::warn!("collab: an update did not decode: {e}");
                    self.notices.push("could not apply a change from another editor".to_owned());
                }
            }
        }
        // While nothing is loaded, any snapshot is a starting point: the file must never be
        // seeded once the room is known to have checkpointed past it. So is the first one a
        // replica waiting after a lost history sees.
        let restart = restart || (snap && self.awaiting_snapshot);
        // Whether `updates` still starts with the snapshot (checked under the state cap).
        let mut snapshot_first = snap && !updates.is_empty();
        if snap && !updates.is_empty() && (restart || self.doc.is_none()) {
            let snapshot = updates.remove(0);
            match self.restart_from(snapshot, &scope, work) {
                Restart::Done => snapshot_first = false,
                Restart::Refused => return,
                Restart::Ordinary(snapshot) => updates.insert(0, snapshot),
            }
        }
        // Each update with whether it is a checkpoint snapshot (checked under the state cap),
        // a flag kept wherever it is held or replayed.
        let mut updates: Vec<(Vec<u8>, bool)> = updates.into_iter().enumerate().map(|(i, update)| (update, snapshot_first && i == 0)).collect();
        if self.awaiting_snapshot && self.snapshot.is_none() {
            // Ordered before the snapshot this replica waits for, which carries them. Laid
            // over the file they would only show text the room does not have.
            log::debug!("collab: waiting for the room's snapshot; {} updates left to it", updates.len());
            return;
        }
        if restart {
            // Live ops that overtook the text ops landed on the replica the restart may just
            // have replaced. Applying them again is harmless when it did not.
            updates.append(&mut self.since_join);
        } else if self.text_catchup && !self.text_more && self.marked && self.from_file && self.doc.is_some() {
            // Only for a relay that marks its fallback: there the text ops are the very next
            // message, so this holds the few live ops that overtook them and nothing more.
            self.since_join.extend(updates.iter().cloned());
        }
        let Some(doc) = self.doc.as_mut() else {
            self.held.extend(updates);
            work.push(Work::SeedWanted { scope });
            return;
        };
        if let Some(pending) = self.checkpoint.as_mut()
            && !updates.is_empty()
        {
            pending.raced = true;
        }
        let mut changes = Vec::new();
        let mut damaged = false;
        for (update, snapshot) in updates {
            if let Some(f) = self.fence.as_mut() {
                f.record(&update, snapshot);
            }
            // A snapshot holds the whole document: the state item cap.
            let applied = if snapshot { doc.apply_state(&update) } else { doc.apply(&update) };
            match applied {
                Ok(found) => merge_changes(&mut changes, found),
                Err(e) => {
                    log::warn!("collab: an update did not apply: {e}");
                    self.notices.push("could not apply a change from another editor".to_owned());
                    damaged |= doc.damaged();
                }
            }
        }
        if !changes.is_empty() {
            work.push(Work::Doc { changes });
        }
        let resync = doc.needs_resync();
        let newly_damaged = damaged && !std::mem::replace(&mut self.told_damaged, true);
        if (resync && !self.told_incomplete) || newly_damaged {
            // The app gives this replica up next (reopen): keep what only it holds.
            self.keep_unmerged(0);
        }
        if resync && !std::mem::replace(&mut self.told_incomplete, true) {
            // Updates this replica was holding for missing dependencies were dropped to stay
            // within bounds, so it is behind with no way to catch up from the ops alone.
            log::warn!("collab: too many updates waited for dependencies that did not come in {scope}");
            self.notices.push(format!("{scope} has changes this session cannot rebuild; reopen the file"));
            work.push(Work::TextIncomplete { scope: scope.clone() });
        }
        if damaged {
            work.push(Work::TextIncomplete { scope });
        }
    }

    /// Start from a snapshot.
    fn restart_from(&mut self, snapshot: Vec<u8>, scope: &str, work: &mut Vec<Work<R::Change>>) -> Restart {
        if self.doc.is_none() {
            // The common case: the file is not loaded yet, so it never will be. A second
            // snapshot before then is the same history, so it lands as an ordinary update.
            if self.snapshot.is_some() {
                return Restart::Ordinary(snapshot);
            }
            self.snapshot = Some(snapshot);
            return Restart::Done;
        }
        if !self.from_file && !self.awaiting_snapshot {
            return Restart::Ordinary(snapshot);
        }
        if self.edited {
            // Typed against the file before the catch-up arrived. Those edits name items the
            // room does not have, so there is no honest merge.
            self.lose_checkpoint(work, "the replica broke");
            self.break_replica("edits were made against a file the room had already checkpointed past");
            work.push(Work::TextIncomplete { scope: scope.to_owned() });
            return Restart::Refused;
        }
        let restored = R::from_state(&snapshot).and_then(|mut doc| match self.space {
            Some(space) => doc.set_client(space).map(|()| doc),
            None => Ok(doc),
        });
        // Whatever happens next, a snapshot taken of the replica before this one is not the
        // room's and must never be reported as saved.
        self.lose_checkpoint(work, "the replica was reset onto the room's snapshot");
        match restored {
            Ok(doc) => {
                self.doc = Some(doc);
                self.from_file = false;
                // A run's base was taken of the replica just replaced.
                if let Some(f) = self.fence.as_mut() {
                    f.base = None;
                }
                if std::mem::replace(&mut self.awaiting_snapshot, false) {
                    log::info!("collab: restarted from the room's snapshot after its history was lost");
                    self.settled = true;
                }
                work.push(Work::Reset);
                Restart::Done
            }
            Err(e) => {
                self.doc = None;
                self.break_replica(&format!("the room's checkpoint did not restore: {e}"));
                work.push(Work::TextIncomplete { scope: scope.to_owned() });
                Restart::Refused
            }
        }
    }

    /// Whether the ordered op at `seq` is the first of the first catch-up. Consumed by the first
    /// ordered op of any kind inside the replay, so a later checkpoint from a peer is never
    /// mistaken for one. An op past the replay is live and leaves the catch-up alone.
    fn first_of_catchup(&mut self, seq: u64) -> bool {
        match self.catchup {
            Some(catchup) if catchup.covers(seq) => {
                self.catchup = None;
                self.text_catchup = false;
                self.settled = true;
                catchup.starts_at(seq)
            }
            Some(_) => false,
            None => {
                // A live op before a marked fallback's text ops does not end the catch-up:
                // only its last piece does. Nor does one before the snapshot a replica waits
                // for after a lost history: until that, it holds nothing of the room's.
                if !(self.text_catchup && self.marked) && !self.awaiting_snapshot {
                    self.settled = true;
                }
                false
            }
        }
    }

    /// One of this replica's submits was ordered. Finishes a checkpoint when it was that one.
    fn confirmed(&mut self, local: u64, seq: u64, work: &mut Vec<Work<R::Change>>) {
        if self.checkpoint.as_ref().is_none_or(|p| p.local != local) {
            return;
        }
        let (raced, sent_at, allowance) = self.checkpoint.take().map_or((false, None, Duration::ZERO), |p| (p.raced, p.sent_at, p.allowance));
        if raced {
            log::info!("collab: a remote edit landed before checkpoint {seq}; it was not reported as saved");
            work.push(Work::CheckpointRaced);
            return;
        }
        let saved = self.wire.saved_through(seq.saturating_sub(1));
        match self.hub.send(saved) {
            // Not done yet: only the relay's acknowledgement moves the baseline.
            // The timeout keeps running from the submit.
            Ok(()) => self.checkpoint = Some(PendingCheckpoint { local, raced: false, ordered: Some(seq), sent_at, allowance }),
            Err(e) => {
                log::warn!("collab: checkpoint {seq} was ordered but the save report never left: {e}");
                work.push(Work::CheckpointRaced);
            }
        }
    }

    /// The relay answered a save report. Only the answer for the one in flight counts.
    fn save_answered(&mut self, seq: u64, refusal: Option<&str>, work: &mut Vec<Work<R::Change>>) {
        if self.checkpoint.as_ref().and_then(PendingCheckpoint::saved_through) != Some(seq) {
            return;
        }
        match refusal {
            Some(reason) => self.refuse_checkpoint(work, reason),
            None => {
                if let Some(ordered) = self.checkpoint.take().and_then(|p| p.ordered) {
                    work.push(Work::Checkpointed { seq: ordered });
                }
            }
        }
    }

    /// A grant with a fencing token: edits from now on go out under it. A newer grant of the
    /// scope already fenced (the app asked again) makes the old fence dead: its edits stay
    /// under the old token, so the relay refuses them, and the new fence starts once the old
    /// one has settled. An edit made under a grant is never sent under another.
    ///
    /// Announces the grant ([`LockEvent::Granted`]) when its fence starts: at once, or for a
    /// parked one once the old fence has settled.
    fn fenced_grant(&mut self, scope: &str, seq: Option<u64>, token: u64, lease_ms: Option<u64>, work: &mut Vec<Work<R::Change>>) {
        self.grants.insert(scope.to_owned(), token);
        let lease_ms = lease_ms.unwrap_or(DEFAULT_LEASE_MS);
        let announce = match self.fence.as_mut() {
            Some(f) if f.scope == scope && f.token == token => {
                // The same grant again (a refresh of the same holder): the lease is fresh.
                f.refreshed_at = None;
                f.lease_ms = lease_ms;
                true
            }
            Some(f) if f.scope == scope => {
                f.next = Some(Parked { token, lease_ms, seq, refreshed_at: None });
                let old = f.token;
                self.kill_fence(old, work);
                false
            }
            Some(f) => {
                log::warn!("collab: {scope} was granted while a run under {} is still fenced; its edits go out under that one", f.scope);
                true
            }
            None => {
                self.start_fence(scope, token, lease_ms);
                true
            }
        };
        if announce {
            work.push(Work::Lock(LockEvent::Granted { scope: scope.to_owned(), seq, token: Some(token) }));
        }
        self.settle_fence(work);
    }

    fn start_fence(&mut self, scope: &str, token: u64, lease_ms: u64) {
        let base = self.doc.as_ref().filter(|_| !self.broken && !self.awaiting_snapshot).map(Replica::state);
        self.fence = Some(Fenced::new(scope, token, lease_ms, base));
    }

    /// The grant `token` of `scope` is gone (an edit under it was refused, or a refresh found
    /// the lease gone). The app is told once.
    fn lose_fence(&mut self, scope: &str, token: u64, work: &mut Vec<Work<R::Change>>) {
        if self.grants.get(scope) == Some(&token) {
            self.grants.remove(scope);
        }
        match self.fence.as_ref() {
            Some(f) if f.scope == scope && f.token == token => self.kill_fence(token, work),
            Some(_) => {}
            None => work.push(Work::Lock(LockEvent::Lost { scope: scope.to_owned(), token })),
        }
        self.settle_fence(work);
    }

    /// Mark the fence under `token` dead: nothing more goes out under it, and whatever of the
    /// run the relay did not order is taken back when it settles. It stays until the unlock.
    fn kill_fence(&mut self, token: u64, work: &mut Vec<Work<R::Change>>) {
        let Some(f) = self.fence.as_mut().filter(|f| f.token == token) else { return };
        f.dead = true;
        if !f.queue.is_empty() {
            f.owed_back = true;
        }
        if f.retry {
            // Refused for now and not sent again: never ordered, and never will be now.
            f.retry = false;
            f.inflight = None;
            f.owed_back = true;
        }
        if !std::mem::replace(&mut f.told, true) {
            work.push(Work::Lock(LockEvent::Lost { scope: f.scope.clone(), token }));
        }
    }

    /// Move the fence on once its submit in flight is answered: send the next queued edit, take
    /// back what a dead fence's relay did not order, and end the fence on the unlock (or when a
    /// newer grant waits).
    fn settle_fence(&mut self, work: &mut Vec<Work<R::Change>>) {
        loop {
            let Some(f) = self.fence.as_mut() else { return };
            if f.inflight.is_some() {
                return;
            }
            if !f.dead {
                if !f.queue.is_empty() {
                    if let Err(e) = self.send_fenced() {
                        log::warn!("collab: an edit made under the lock could not be sent: {e}");
                    }
                    return;
                }
                if !f.unlocked {
                    return;
                }
                // Every edit under the lock is ordered: give it back, then what waited goes free.
                let Some(f) = self.fence.take() else { return };
                self.end_fence(&f);
                for chunk in self.chunked(f.held_back) {
                    if self.broken {
                        return;
                    }
                    if let Err(e) = self.send_with(&chunk, None) {
                        log::warn!("collab: an edit held back for the end of an ordered run could not be sent: {e}");
                    }
                }
                return;
            }
            if f.owed_back || !f.queue.is_empty() || (f.unlocked && !f.held_back.is_empty()) {
                // What is taken back is in the replica now and nowhere else. A refused edit is
                // no longer in flight or queued, so `owed_back` counts it.
                let owed = usize::from(f.owed_back);
                self.keep_unmerged(owed);
                let Some(f) = self.fence.as_mut() else { return };
                f.owed_back = false;
                f.queue.clear();
                f.held_back.clear();
                self.take_back(work);
                if self.broken {
                    return;
                }
            }
            let Some(f) = self.fence.as_ref() else { return };
            if !f.unlocked && f.next.is_none() {
                return;
            }
            let Some(f) = self.fence.take() else { return };
            if f.unlocked {
                self.end_fence(&f);
            }
            if let Some(next) = f.next {
                self.start_fence(&f.scope, next.token, next.lease_ms);
                if let Some(started) = self.fence.as_mut() {
                    started.refreshed_at = next.refreshed_at;
                }
                work.push(Work::Lock(LockEvent::Granted { scope: f.scope.clone(), seq: next.seq, token: Some(next.token) }));
                if f.unlocked {
                    return;
                }
                // Asked again without an unlock: the old fence's unlock is moot.
                continue;
            }
            return;
        }
    }

    /// The fence `f` ended: its unlock goes out under its own token, and the token is no longer
    /// a grant this client holds unless a newer one replaced it.
    fn end_fence(&mut self, f: &Fenced) {
        if self.grants.get(&f.scope) == Some(&f.token) {
            self.grants.remove(&f.scope);
        }
        let msg = self.wire.release_grant(&f.scope, Some(f.token));
        if let Err(e) = self.hub.send(msg) {
            log::debug!("collab: an unlock was not sent: {e}");
        }
    }

    /// The relay refused an edit made under the lock, or the lock died with edits unsent.
    /// Rebuild the replica as the room has it: the state at the grant, plus every update the
    /// room ordered since (other peers', and this replica's own fenced ones the relay ordered).
    /// The edits it did not order, and those made after them, are gone from the replica; the
    /// app re-reads it ([`Work::Reset`]). The base and log stay valid for a later take-back.
    fn take_back(&mut self, work: &mut Vec<Work<R::Change>>) {
        let Some(f) = self.fence.as_ref() else { return };
        let scope = f.scope.clone();
        let Some(base) = f.base.as_ref() else {
            self.lose_checkpoint(work, "the replica broke");
            self.break_replica("an edit made under a lost lock was refused and could not be taken back");
            work.push(Work::TextIncomplete { scope: DOC_SCOPE.to_owned() });
            return;
        };
        let rebuilt = R::from_state(base).and_then(|mut doc| {
            if let Some(space) = self.space {
                doc.set_client(space)?;
            }
            for (update, snapshot) in &f.log {
                // A snapshot holds the whole document: the state item cap, as when it landed.
                if *snapshot {
                    doc.apply_state(update)?;
                } else {
                    doc.apply(update)?;
                }
            }
            Ok(doc)
        });
        match rebuilt {
            Ok(doc) => {
                self.doc = Some(doc);
                // A snapshot taken of the replica before this is not the room's.
                self.lose_checkpoint(work, "an ordered run was taken back");
                self.notices.push(format!("another editor took {scope} before your change was shared; it was taken back"));
                work.push(Work::Reset);
            }
            Err(e) => {
                self.lose_checkpoint(work, "the replica broke");
                self.break_replica(&format!("an ordered run could not be taken back: {e}"));
                work.push(Work::TextIncomplete { scope: DOC_SCOPE.to_owned() });
            }
        }
    }

    fn act(&mut self, effect: Effect, work: &mut Vec<Work<R::Change>>) {
        match effect {
            Effect::Space { space } => {
                if self.space.is_some_and(|own| own != space) && !std::mem::take(&mut self.adopt_space) {
                    // A rejoin's new space. This replica keeps its own: the rejoin named it
                    // (`Hub::set_space`), so the relay raised its counter past it, and the relay
                    // also floors the counter at the highest space it granted. Not covered: a
                    // relay restart together with lost Valkey grants (see `Hub::set_space`).
                    // A new client id would rebuild the replica and start its
                    // undo history again, so a command made before (or while the link was down)
                    // could no longer be undone as one step.
                    log::debug!("collab: keeping id space {own:?} across the rejoin", own = self.space);
                    return;
                }
                self.adopt_space = false;
                self.space = Some(space);
                // Every rejoin names it from now on, so the relay raises its counter past it.
                self.hub.set_space(space);
                if let Some(doc) = self.doc.as_mut()
                    && let Err(e) = doc.set_client(space)
                {
                    self.break_replica(&format!("the granted client id did not take: {e}"));
                    work.push(Work::TextIncomplete { scope: DOC_SCOPE.to_owned() });
                }
                log::debug!("collab: the relay granted id space {space}");
            }
            Effect::Apply { command, params, seq } if command == CRDT_TEXT => {
                let restart = self.first_of_catchup(seq);
                match serde_json::from_value::<TextPayload>(params) {
                    Ok(payload) => self.merge_updates(payload.scope, &payload.ops, payload.snap, payload.snap && restart, work),
                    Err(e) => {
                        log::warn!("collab: a crdt.text payload did not decode: {e}");
                        self.notices.push("could not apply a change from another editor".to_owned());
                    }
                }
            }
            Effect::Apply { command, params, seq } => {
                self.first_of_catchup(seq);
                work.push(Work::Command { command, params });
            }
            Effect::TextOps { scope, ops, complete, snap, more } => {
                // The fallback catch-up: it replaces the ordered replay, so it is the catch-up.
                // It may come in pieces; only the first can start from a snapshot, and only
                // the last ends it.
                let first = !self.text_more;
                let restart = self.text_catchup && first;
                if !self.marked {
                    // An older relay sends text ops instead of a replay, and does not say so.
                    // They are the catch-up, so the ordered one it seemed to promise is over:
                    // left armed, it would block checkpoints and take a peer's next snapshot
                    // for a restart.
                    self.catchup = None;
                }
                if !complete {
                    if !std::mem::replace(&mut self.told_incomplete, true) {
                        // The app gives this replica up next (reopen): keep what only it holds.
                        self.keep_unmerged(0);
                        log::warn!("collab: the relay could not keep every change to {scope}");
                        self.notices.push(format!("{scope} has changes this session cannot rebuild; reopen the file"));
                        work.push(Work::TextIncomplete { scope });
                    }
                } else {
                    let snap = snap && first;
                    match serde_json::from_value::<Vec<String>>(ops) {
                        Ok(ops) => self.merge_updates(scope, &ops, snap, snap && restart, work),
                        Err(e) => {
                            log::warn!("collab: replayed operations for {scope} did not decode: {e}");
                            if !std::mem::replace(&mut self.told_incomplete, true) {
                                self.keep_unmerged(0);
                                self.notices.push(format!("{scope} could not be caught up; reopen the file"));
                                work.push(Work::TextIncomplete { scope });
                            }
                        }
                    }
                }
                if more {
                    self.text_more = true;
                } else {
                    self.text_more = false;
                    self.text_catchup = false;
                    self.settled |= !self.awaiting_snapshot;
                    self.since_join.clear();
                }
            }
            Effect::Frame { origin, payload } => work.push(Work::Frame { origin, payload }),
            Effect::Ephemeral { origin, kind, payload } => work.push(Work::Ephemeral { origin, kind, payload }),
            Effect::Confirmed { local, seq } => {
                if let Some(update) = self.unconfirmed.remove(&local) {
                    self.unconfirmed_bytes -= update.len();
                    self.retry_wait = RETRY_FIRST;
                }
                if let Some(f) = self.fence.as_mut()
                    && f.inflight.as_ref().is_some_and(|i| i.locals.contains(&local))
                    && let Some(i) = f.inflight.take()
                {
                    // Ordered under the lock, by any counter it went out with (an old one's order
                    // can arrive in a rejoin's catch-up): the room's now, so a take-back keeps it.
                    f.record(&i.update, false);
                    f.retry = false;
                    self.retry_wait = RETRY_FIRST;
                }
                self.first_of_catchup(seq);
                self.confirmed(local, seq, work);
                self.settle_fence(work);
            }
            Effect::Replace { document, seq } => {
                self.keep_unmerged(0);
                self.lose_checkpoint(work, "the relay replaced the document");
                // Edits made against what is being replaced cannot be reconciled with it, and
                // the room never ordered them: they stay counted, so they never read as saved.
                self.abandoned += self.unconfirmed.len();
                self.unconfirmed.clear();
                self.unconfirmed_bytes = 0;
                self.retrying.clear();
                if let Some(f) = self.fence.take() {
                    self.abandoned += f.outstanding();
                    if !f.told {
                        work.push(Work::Lock(LockEvent::Lost { scope: f.scope.clone(), token: f.token }));
                    }
                    if let Some(next) = f.next {
                        // Held: the app hears of it, so it can give it back.
                        work.push(Work::Lock(LockEvent::Granted { scope: f.scope, seq: next.seq, token: Some(next.token) }));
                    }
                }
                if document.is_null() {
                    // The relay keeps no document, so this is not a recovery. Saying nothing
                    // and carrying on would leave the peer believing it is current when it has
                    // permanently missed edits.
                    log::warn!("collab: fell too far behind at seq {seq} and the relay holds no document");
                    self.notices.push("this window fell too far behind the other editors; reopen the file to catch up".to_owned());
                } else {
                    log::warn!("collab: a snapshot arrived at seq {seq} and this app cannot install one yet");
                    self.notices.push("the other editors sent a document this window cannot install; reopen the file".to_owned());
                }
            }
            Effect::LockHeld { scope, seq, token, lease_ms } => match token {
                Some(token) => self.fenced_grant(&scope, seq, token, lease_ms, work),
                None => work.push(Work::Lock(LockEvent::Granted { scope, seq, token })),
            },
            Effect::LockLost { scope, token } => self.lose_fence(&scope, token, work),
            Effect::LockDropped { scope } => work.push(Work::Lock(LockEvent::Released { scope })),
            Effect::RemoteLocks { .. } => {}
            Effect::LockBusy { scope, holder } => work.push(Work::Lock(LockEvent::HeldByOther { scope, holder })),
            Effect::LockRefused { scope, holder } => work.push(Work::Lock(LockEvent::Denied { scope, holder })),
            Effect::Roster { roster } => {
                let me = self.wire.me().cloned();
                self.roster = roster.into_iter().filter(|p| Some(&p.peer) != me.as_ref()).collect();
            }
            Effect::Notice { detail } => {
                log::info!("collab: {detail}");
                self.notices.push(detail);
            }
            Effect::SubmitRefused { local, code, .. }
                if code.as_deref() == Some(FENCED) && self.fence.as_ref().is_some_and(|f| f.sent.contains(&local)) =>
            {
                // An edit made under the lock, refused because the lock is no longer this
                // grant's. Never ordered and never will be: out of the replica it goes, with
                // every other edit under the lock the relay did not order.
                let Some(f) = self.fence.as_mut() else { return };
                if f.inflight.as_ref().and_then(|i| i.locals.last()) == Some(&local) {
                    f.inflight = None;
                    f.retry = false;
                    f.owed_back = true;
                }
                let (scope, token) = (f.scope.clone(), f.token);
                log::info!("collab: the relay refused an edit made under {scope} (grant {token}); taking the run back");
                self.lose_fence(&scope, token, work);
            }
            Effect::SubmitRefused { local, code, detail }
                if self.fence.as_ref().and_then(|f| f.inflight.as_ref()).and_then(|i| i.locals.last()) == Some(&local) && !self.broken =>
            {
                let retry = code.as_deref() == Some(RETRY) || !(refusal_is_permanent(&detail) || matches!(code.as_deref(), Some(INVALID)));
                let Some(f) = self.fence.as_mut() else { return };
                if retry && !f.dead {
                    log::info!("collab: the relay refused an edit made under the lock for now; sending it again in {:?}", self.retry_wait);
                    f.retry = true;
                    if self.retry_at.is_none() {
                        self.retry_at = now_ms().map(|now| now + self.retry_wait.as_secs_f64() * 1000.0);
                        self.retry_wait = (self.retry_wait * 2).min(RETRY_MOST);
                    }
                } else if retry {
                    f.inflight = None;
                    f.owed_back = true;
                    self.settle_fence(work);
                } else {
                    self.lose_checkpoint(work, "an edit was refused");
                    self.break_replica(&format!("the relay refused an edit: {detail}"));
                    work.push(Work::TextIncomplete { scope: DOC_SCOPE.to_owned() });
                }
            }
            Effect::SubmitRefused { local, detail, code } => {
                // The relay refused one of this client's submits by its counter. It is the
                // checkpoint's own only while that has not been ordered.
                let retry = code.as_deref() == Some(RETRY) || !(refusal_is_permanent(&detail) || matches!(code.as_deref(), Some(INVALID | FENCED)));
                if self.checkpoint.as_ref().is_some_and(|p| p.local == local && p.ordered.is_none()) {
                    self.refuse_checkpoint(work, &detail);
                } else if self.unconfirmed.contains_key(&local) && !self.broken && retry {
                    // Refused for now (the relay's bus did not answer, or the link was down too
                    // long). Nothing was ordered, so it stays unconfirmed and goes again after a
                    // backoff; only a refusal a retry cannot change breaks the replica.
                    log::info!("collab: the relay refused an edit for now; sending it again in {:?}", self.retry_wait);
                    self.retrying.insert(local);
                    if self.retry_at.is_none() {
                        self.retry_at = now_ms().map(|now| now + self.retry_wait.as_secs_f64() * 1000.0);
                        self.retry_wait = (self.retry_wait * 2).min(RETRY_MOST);
                    }
                } else if self.unconfirmed.contains_key(&local) && !self.broken {
                    // An edit. It already landed in this replica and the room will never order
                    // it, so the two have parted for good.
                    self.lose_checkpoint(work, "an edit was refused");
                    self.break_replica(&format!("the relay refused an edit: {detail}"));
                    work.push(Work::TextIncomplete { scope: DOC_SCOPE.to_owned() });
                }
            }
            Effect::SavedAck { seq } => self.save_answered(seq, None, work),
            Effect::SavedRefused { seq, reason } => self.save_answered(seq, Some(&reason), work),
            Effect::Send { msg } => {
                if let Err(e) = self.hub.send(msg) {
                    log::warn!("collab: a resync request never left: {e}");
                }
            }
        }
    }
}

/// What [`Live::restart_from`] did with a snapshot.
enum Restart {
    /// It is the starting point now, or held as the one [`Live::seed`] will use.
    Done,
    /// The replica could not take it and is broken; drop the rest of the batch.
    Refused,
    /// This replica already shares the room's history, so it is an ordinary update.
    Ordinary(Vec<u8>),
}

/// Add changes not already listed.
fn merge_changes<C: PartialEq>(into: &mut Vec<C>, found: Vec<C>) {
    for c in found {
        if !into.contains(&c) {
            into.push(c);
        }
    }
}

/// A refused edit, as a transport-level error with the reason kept.
fn refusal(e: LiveError) -> Error {
    Error::malformed("client", e.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::cell::RefCell;
    use std::rc::Rc;

    use serde_json::Map;

    use super::*;
    use crate::SeedBlock;

    #[derive(Clone, Default)]
    struct Hub0(Rc<RefCell<(Vec<ClientMsg>, Vec<ServerMsg>)>>);

    impl Hub for Hub0 {
        fn send(&mut self, msg: ClientMsg) -> Result<()> {
            self.0.borrow_mut().0.push(msg);
            Ok(())
        }
        fn poll(&mut self) -> Vec<ServerMsg> {
            std::mem::take(&mut self.0.borrow_mut().1)
        }
        fn connected(&self) -> bool {
            true
        }
        fn set_space(&mut self, _: u32) {}
    }

    fn joined_msg() -> ServerMsg {
        ServerMsg::Joined {
            doc: "d".to_owned(),
            peer: "me".to_owned(),
            seq: 0,
            space: 1,
            roster: Vec::new(),
            locks: Vec::new(),
            replay_until: None,
            fallback: false,
            text_cap: None,
            heartbeat: false,
            history_lost: false,
        }
    }

    #[test]
    fn lock_events_reach_the_app_in_order() {
        let hub = Hub0::default();
        let mut live = Live::new(hub.clone(), "d");
        hub.0.borrow_mut().1.push(joined_msg());
        live.pump();
        let d = || "d".to_owned();
        let b2 = || "B2".to_owned();
        hub.0.borrow_mut().1.extend([
            ServerMsg::LockGranted { doc: d(), scope: b2(), seq: Some(7), token: None, lease_ms: None },
            ServerMsg::LockDenied { doc: d(), scope: "C3".to_owned(), holder: "bob".to_owned() },
            ServerMsg::LockReleased { doc: d(), scope: b2() },
            ServerMsg::PresenceLock { doc: d(), scope: "D4".to_owned(), holder: "carol".to_owned() },
        ]);
        let events: Vec<LockEvent> = live
            .pump()
            .into_iter()
            .filter_map(|w| match w {
                Work::Lock(e) => Some(e),
                _ => None,
            })
            .collect();
        assert_eq!(
            events,
            vec![
                LockEvent::Granted { scope: b2(), seq: Some(7), token: None },
                LockEvent::Denied { scope: "C3".to_owned(), holder: "bob".to_owned() },
                LockEvent::Released { scope: b2() },
                LockEvent::HeldByOther { scope: "D4".to_owned(), holder: "carol".to_owned() },
            ]
        );
    }

    #[test]
    fn lock_and_unlock_send_the_scope() {
        let hub = Hub0::default();
        let mut live = Live::new(hub.clone(), "d");
        live.lock("grid/struct").unwrap();
        live.unlock("grid/struct").unwrap();
        let sent = hub.0.borrow().0.clone();
        assert_eq!(
            sent,
            vec![
                ClientMsg::Lock { doc: "d".to_owned(), scope: "grid/struct".to_owned() },
                ClientMsg::Unlock { doc: "d".to_owned(), scope: "grid/struct".to_owned(), token: None },
            ]
        );
    }

    #[test]
    fn denied_lock_adds_no_notice() {
        let hub = Hub0::default();
        let mut live = Live::new(hub.clone(), "d");
        hub.0.borrow_mut().1.push(joined_msg());
        live.pump();
        let before = live.notices.clone();
        hub.0.borrow_mut().1.push(ServerMsg::LockDenied { doc: "d".to_owned(), scope: "B2".to_owned(), holder: "bob".to_owned() });
        live.pump();
        assert_eq!(live.notices, before);
    }

    fn block() -> SeedBlock {
        SeedBlock { id: 1, text: "hello".to_owned(), attrs: Map::new(), runs: Vec::new() }
    }

    /// I3: a damaged replica takes no edit, undo, redo or checkpoint.
    #[test]
    fn a_damaged_replica_refuses_local_work() {
        let hub = Hub0::default();
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.0.borrow_mut().1.push(ServerMsg::Joined {
            doc: "d".to_owned(),
            peer: "me".to_owned(),
            seq: 0,
            space: 1,
            roster: Vec::new(),
            locks: Vec::new(),
            replay_until: None,
            fallback: false,
            text_cap: None,
            heartbeat: false,
            history_lost: false,
        });
        live.pump();
        live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).unwrap();
        live.doc.as_mut().unwrap().force_damaged();
        assert!(!live.ready_to_type());
        assert!(live.edit(|d| d.insert_text(1, 0, "y", &Map::new())).is_err());
        assert!(live.undo().is_err());
        assert!(live.redo().is_err());
        assert!(live.checkpoint().is_err());
        assert_eq!(hub.0.borrow().0.len(), 1, "only the edit before the damage went out");
    }

    fn lock_events<R: Replica>(work: Vec<Work<R::Change>>) -> Vec<LockEvent> {
        work.into_iter()
            .filter_map(|w| match w {
                Work::Lock(e) => Some(e),
                _ => None,
            })
            .collect()
    }

    /// The re-review's N1 and N2: a newer grant of a scope whose old fence still has an edit in
    /// flight is parked. The app hears of it only once the old fence has settled (so nothing
    /// meant for it lands in the dead fence and is taken back), and its lease is refreshed
    /// meanwhile.
    #[test]
    fn a_newer_grant_waits_for_the_old_fence_and_keeps_its_lease() {
        let hub = Hub0::default();
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.0.borrow_mut().1.push(joined_msg());
        live.pump();
        let d = || "d".to_owned();
        let scope = || "grid/struct".to_owned();
        let grant = |token: u64| ServerMsg::LockGranted { doc: d(), scope: scope(), seq: Some(0), token: Some(token), lease_ms: Some(1000) };
        hub.0.borrow_mut().1.push(grant(1));
        assert_eq!(lock_events::<LiveDoc>(live.pump()), vec![LockEvent::Granted { scope: scope(), seq: Some(0), token: Some(1) }]);
        live.edit(|doc| doc.insert_text(1, 0, "x", &Map::new())).unwrap();
        let local = hub
            .0
            .borrow()
            .0
            .iter()
            .rev()
            .find_map(|m| match m {
                ClientMsg::Submit { local, fence: Some(_), .. } => Some(*local),
                _ => None,
            })
            .expect("a fenced submit");
        // Asked again: token 2, while the edit under token 1 is unanswered.
        hub.0.borrow_mut().1.push(grant(2));
        let events = lock_events::<LiveDoc>(live.pump());
        assert_eq!(events, vec![LockEvent::Lost { scope: scope(), token: 1 }], "the parked grant was announced early");
        assert_eq!(live.fence(), None);
        // Its lease is refreshed while it waits.
        hub.0.borrow_mut().0.clear();
        live.refresh_lease(0.0);
        live.refresh_lease(600.0);
        assert!(
            hub.0.borrow().0.iter().any(|m| matches!(m, ClientMsg::Refresh { token: 2, .. })),
            "the parked grant's lease was not refreshed: {:?}",
            hub.0.borrow().0
        );
        // The old edit is refused: taken back, and the new grant starts and is announced.
        hub.0.borrow_mut().1.push(ServerMsg::Error { doc: d(), detail: "fenced".to_owned(), local: Some(local), code: Some(FENCED.to_owned()) });
        let work = live.pump();
        assert!(work.iter().any(|w| matches!(w, Work::Reset)));
        assert_eq!(lock_events::<LiveDoc>(work), vec![LockEvent::Granted { scope: scope(), seq: Some(0), token: Some(2) }]);
        assert_eq!(live.fence().map(|f| f.token), Some(2));
        assert_eq!(live.doc().unwrap().block(1).unwrap().text, "hello");
    }

    /// What a [`Flaky`] hub sent and has waiting, and whether its link is up.
    #[derive(Default)]
    struct Link {
        sent: Vec<ClientMsg>,
        inbox: Vec<ServerMsg>,
        up: bool,
        /// The space every rejoin names (`Hub::set_space`).
        space: u32,
    }

    /// A hub whose link can drop: while down it drops edits, as the real hubs do.
    #[derive(Clone, Default)]
    struct Flaky(Rc<RefCell<Link>>);

    impl Hub for Flaky {
        fn send(&mut self, msg: ClientMsg) -> Result<()> {
            let mut s = self.0.borrow_mut();
            if s.up {
                s.sent.push(msg);
            }
            Ok(())
        }
        fn poll(&mut self) -> Vec<ServerMsg> {
            std::mem::take(&mut self.0.borrow_mut().inbox)
        }
        fn connected(&self) -> bool {
            self.0.borrow().up
        }
        fn set_space(&mut self, space: u32) {
            self.0.borrow_mut().space = space;
        }
    }

    impl Flaky {
        fn up(&self, up: bool) {
            self.0.borrow_mut().up = up;
        }
        fn deliver(&self, msg: ServerMsg) {
            self.0.borrow_mut().inbox.push(msg);
        }
        fn submits(&self) -> Vec<(u64, Value)> {
            self.0
                .borrow()
                .sent
                .iter()
                .filter_map(|m| match m {
                    ClientMsg::Submit { local, params, .. } => Some((*local, params.clone())),
                    _ => None,
                })
                .collect()
        }
    }

    /// A join as the relay sends it: baseline `seq`, its replay ending at `until`.
    fn join_at(space: u32, seq: u64, until: u64, history_lost: bool) -> ServerMsg {
        ServerMsg::Joined {
            doc: "d".to_owned(),
            peer: format!("p{space}"),
            seq,
            space,
            roster: Vec::new(),
            locks: Vec::new(),
            replay_until: Some(until),
            fallback: false,
            text_cap: None,
            heartbeat: false,
            history_lost,
        }
    }

    /// An ordered `crdt.text` op from another peer carrying `update`.
    fn op(seq: u64, update: &[u8], snap: bool) -> ServerMsg {
        let payload = if snap { TextPayload::snapshot(update) } else { TextPayload::new(update) };
        ServerMsg::Op {
            doc: "d".to_owned(),
            seq,
            origin: "other".to_owned(),
            local: None,
            command: CRDT_TEXT.to_owned(),
            params: serde_json::to_value(payload).unwrap(),
            scope: None,
        }
    }

    /// The text of block 1.
    fn text(live: &Live<Flaky>) -> String {
        live.doc().and_then(|d| d.block(1)).map(|b| b.text).unwrap_or_default()
    }

    /// C9: a replica that falls behind (`needs_resync`) with a local edit the room never ordered
    /// hands that edit's state over (`Work::Unmerged`) before it is given up
    /// (`Work::TextIncomplete`), so the app can save an unmerged copy.
    #[test]
    fn a_replica_given_up_hands_over_its_unmerged_edits_first() {
        let (ha, hb) = (Hub0::default(), Hub0::default());
        let mut a = Live::new(ha.clone(), "d");
        let mut b = Live::new(hb.clone(), "d");
        a.seed(&[block()]).unwrap();
        b.seed(&[block()]).unwrap();
        ha.0.borrow_mut().1.push(joined(1));
        hb.0.borrow_mut().1.push(joined(2));
        a.pump();
        b.pump();
        // A's own edit, never ordered (the relay never answers it).
        a.edit(|d| d.insert_text(1, 0, "mine ", &Map::new())).unwrap();
        // B's edit reaches A while A is behind.
        b.edit(|d| d.insert_text(1, 5, "!", &Map::new())).unwrap();
        let Some(ClientMsg::Submit { command, params, local, .. }) = hb.0.borrow_mut().0.pop() else { panic!("B sent nothing") };
        a.doc.as_mut().unwrap().force_resync(true);
        ha.0.borrow_mut().1.push(ServerMsg::Op {
            doc: "d".to_owned(),
            seq: 1,
            origin: "p2".to_owned(),
            local: Some(local),
            command,
            params,
            scope: None,
        });
        let work = a.pump();
        let unmerged = work.iter().position(|w| matches!(w, Work::Unmerged { .. }));
        let incomplete = work.iter().position(|w| matches!(w, Work::TextIncomplete { .. }));
        assert!(unmerged.is_some() && incomplete.is_some(), "{work:?}");
        assert!(unmerged < incomplete, "the unmerged state came after the drop: {work:?}");
        let Some(Work::Unmerged { state, edits }) = work.into_iter().find(|w| matches!(w, Work::Unmerged { .. })) else {
            panic!("no unmerged state")
        };
        assert!(edits >= 1);
        let copy = LiveDoc::from_state(&state).unwrap();
        assert!(copy.block(1).unwrap().text.contains("mine"), "the copy lacks the unmerged edit");
    }

    /// C9: edits made while the link is down go out on the rejoin as one merged submit, and a
    /// replica past the offline bound says so (`buffer_full`) without dropping anything.
    #[test]
    fn offline_edits_go_out_merged_and_the_bound_drops_nothing() {
        let hub = Flaky::default();
        hub.up(true);
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.0.borrow_mut().inbox.push(joined(1));
        live.pump();
        hub.0.borrow_mut().up = false;
        let big = "x".repeat(OFFLINE_MAX_BYTES / 2 + 1);
        let mut made = 0;
        while !live.buffer_full() {
            live.edit(|d| d.insert_text(1, 0, &big, &Map::new())).unwrap();
            made += 1;
            assert!(made < 8, "the bound never held");
        }
        assert_eq!(live.unconfirmed_edits(), made, "an edit was dropped at the bound");
        assert!(hub.0.borrow().sent.is_empty());
        // Back: the rejoin sends everything, merged, and it keeps its own space.
        hub.0.borrow_mut().up = true;
        hub.0.borrow_mut().inbox.push(joined(7));
        live.pump();
        assert_eq!(live.space(), Some(1), "the rejoin's space replaced the replica's own");
        assert_eq!(hub.0.borrow().space, 1, "rejoins do not name the space the replica keeps");
        let submits = hub.0.borrow().sent.iter().filter(|m| matches!(m, ClientMsg::Submit { .. })).count();
        assert_eq!(submits, 1, "the offline edits did not go out as one bundle");
        assert!(!live.buffer_full(), "online, the larger bound applies");
    }

    /// C9 fix round 1, I1: the relay's catch-up says the text is incomplete while this replica
    /// holds an edit the room never ordered. The app gets that state before it gives up.
    #[test]
    fn incomplete_text_ops_hand_over_the_unmerged_edits_first() {
        let hub = Flaky::default();
        hub.up(true);
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.deliver(joined(1));
        live.pump();
        live.edit(|d| d.insert_text(1, 0, "mine ", &Map::new())).unwrap();
        hub.deliver(ServerMsg::TextOps {
            doc: "d".to_owned(),
            scope: DOC_SCOPE.to_owned(),
            ops: Value::Null,
            complete: false,
            snap: false,
            more: false,
        });
        let work = live.pump();
        let unmerged = work.iter().position(|w| matches!(w, Work::Unmerged { .. }));
        let incomplete = work.iter().position(|w| matches!(w, Work::TextIncomplete { .. }));
        assert!(unmerged.is_some() && unmerged < incomplete, "{work:?}");
        let Some(Work::Unmerged { state, .. }) = work.into_iter().find(|w| matches!(w, Work::Unmerged { .. })) else { panic!("no state") };
        assert!(LiveDoc::from_state(&state).unwrap().block(1).unwrap().text.contains("mine"));
    }

    /// C9 fix round 1, I1: the fallback catch-up's ops do not decode. The replica is given up
    /// next, so its edits are handed over first, on this path too.
    #[test]
    fn undecodable_text_ops_hand_over_the_unmerged_edits_first() {
        let hub = Flaky::default();
        hub.up(true);
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.deliver(joined(1));
        live.pump();
        live.edit(|d| d.insert_text(1, 0, "mine ", &Map::new())).unwrap();
        hub.deliver(ServerMsg::TextOps {
            doc: "d".to_owned(),
            scope: DOC_SCOPE.to_owned(),
            ops: Value::from(5),
            complete: true,
            snap: false,
            more: false,
        });
        let work = live.pump();
        let unmerged = work.iter().position(|w| matches!(w, Work::Unmerged { .. }));
        let incomplete = work.iter().position(|w| matches!(w, Work::TextIncomplete { .. }));
        assert!(incomplete.is_some() && unmerged.is_some() && unmerged < incomplete, "{work:?}");
    }

    /// C9 fix round 1: a damaged replica hands its state over once, not on every batch that
    /// finds it damaged.
    #[test]
    fn a_damaged_replica_hands_over_its_edits_once() {
        let hub = Flaky::default();
        hub.up(true);
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.deliver(joined(1));
        live.pump();
        live.edit(|d| d.insert_text(1, 0, "mine ", &Map::new())).unwrap();
        live.doc.as_mut().unwrap().force_damaged();
        let mut handed = 0;
        for seq in 1..=3 {
            hub.deliver(op(seq, &[0xff, 0xff, 0xff], false));
            handed += live.pump().iter().filter(|w| matches!(w, Work::Unmerged { .. })).count();
        }
        assert_eq!(handed, 1);
    }

    /// C9 fix round 1: edits made under a fence count in the bound, as free ones do.
    #[test]
    fn fenced_edits_count_in_the_buffer_bound() {
        let hub = Flaky::default();
        hub.up(true);
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.deliver(joined(1));
        live.pump();
        hub.deliver(ServerMsg::LockGranted {
            doc: "d".to_owned(),
            scope: "grid/struct".to_owned(),
            seq: Some(0),
            token: Some(1),
            lease_ms: Some(1000),
        });
        live.pump();
        assert!(live.fence().is_some());
        for _ in 0..MAX_UNCONFIRMED_EDITS {
            live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).unwrap();
        }
        assert!(live.buffer_full(), "the fenced edits were not counted");
    }

    /// C9 fix round 1, I2: a first join to a room never saved, with nothing to replay, settles the
    /// replica. Edits made offline then survive a rejoin whose replay starts with the snapshot the
    /// room checkpointed meanwhile: it lands as an ordinary update, not a restart.
    #[test]
    fn offline_edits_survive_a_rejoin_onto_the_rooms_new_snapshot() {
        let hub = Flaky::default();
        hub.up(true);
        let mut a = Live::new(hub.clone(), "d");
        a.seed(&[block()]).unwrap();
        hub.deliver(join_at(1, 0, 0, false));
        a.pump();
        // The link drops before any op arrived; A types.
        hub.up(false);
        a.pump();
        a.edit(|d| d.insert_text(1, 0, "offline ", &Map::new())).unwrap();
        // Meanwhile B edited the same file and the service checkpointed it at 2 (saved 1).
        let mut b = LiveDoc::seeded(&[block()]).unwrap();
        b.set_client(2).unwrap();
        b.insert_text(1, 5, " there", &Map::new()).unwrap();
        hub.up(true);
        hub.deliver(join_at(3, 1, 2, false));
        hub.deliver(op(2, &b.state(), true));
        let work = a.pump();
        assert!(!work.iter().any(|w| matches!(w, Work::TextIncomplete { .. } | Work::Reset)), "{work:?}");
        assert!(a.ready_to_type(), "the replica broke on the snapshot");
        assert!(text(&a).contains("offline") && text(&a).contains("there"), "{:?}", text(&a));
        // The resend carries A's edit: applied to B, both agree.
        let resent: Vec<Vec<u8>> = hub
            .submits()
            .into_iter()
            .filter_map(|(_, p)| serde_json::from_value::<TextPayload>(p).ok())
            .flat_map(|p| p.ops)
            .map(|o| update_from_wire(&o).unwrap())
            .collect();
        assert!(!resent.is_empty(), "the offline edit was not sent again");
        for u in &resent {
            b.apply(u).unwrap();
        }
        assert_eq!(b.block(1).unwrap().text, text(&a));
    }

    /// C9 fix round 1, I2: a first join to a room with history (a save, a replay to come) does
    /// not settle early: when the link drops before the replay arrived, the rejoin's replay
    /// snapshot is still the replica's starting point.
    #[test]
    fn a_first_join_with_a_replay_to_come_still_restarts_from_its_snapshot() {
        let hub = Flaky::default();
        hub.up(true);
        let mut a = Live::new(hub.clone(), "d");
        a.seed(&[block()]).unwrap();
        let mut room = LiveDoc::seeded(&[SeedBlock { id: 1, text: "elsewhere".to_owned(), attrs: Map::new(), runs: Vec::new() }]).unwrap();
        room.set_client(2).unwrap();
        hub.deliver(join_at(3, 1, 2, false));
        a.pump();
        hub.up(false);
        a.pump();
        hub.up(true);
        hub.deliver(join_at(4, 1, 2, false));
        hub.deliver(op(2, &room.state(), true));
        let work = a.pump();
        assert!(work.iter().any(|w| matches!(w, Work::Reset)), "the replica did not restart from the room's snapshot: {work:?}");
        assert_eq!(text(&a), "elsewhere");
    }

    /// C9 fix round 1: the count bound holds without dropping anything, and an app that ignores
    /// it meets the backstop at twice the bound, before its edit touches the replica.
    #[test]
    fn the_edit_count_bound_refuses_and_the_backstop_holds() {
        let hub = Flaky::default();
        hub.up(true);
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.deliver(joined(1));
        live.pump();
        hub.up(false);
        while !live.buffer_full() {
            live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).unwrap();
        }
        assert_eq!(live.unconfirmed_edits(), MAX_UNCONFIRMED_EDITS, "the count bound did not hold at its value");
        let mut made = MAX_UNCONFIRMED_EDITS;
        while live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).is_ok() {
            made += 1;
            assert!(made <= 3 * MAX_UNCONFIRMED_EDITS, "no backstop");
        }
        assert_eq!(made, 2 * MAX_UNCONFIRMED_EDITS);
        let before = text(&live);
        assert!(live.edit(|d| d.insert_text(1, 0, "refused", &Map::new())).is_err());
        assert!(live.undo().is_err());
        assert_eq!(text(&live), before, "a refused edit changed the replica");
        assert_eq!(live.unconfirmed_edits(), made, "an edit was dropped at the backstop");
    }

    /// C9 fix round 1, C1: a rejoin into a room that lost its history adopts the new space (the
    /// counter behind the old one may have started over) and names it on later rejoins.
    #[test]
    fn a_history_lost_rejoin_adopts_the_new_space() {
        let hub = Flaky::default();
        hub.up(true);
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.deliver(joined(1));
        live.pump();
        live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).unwrap();
        hub.deliver(join_at(9, 0, 0, true));
        live.pump();
        assert_eq!(live.space(), Some(9), "kept a space the lost counter may grant again");
        assert_eq!(hub.0.borrow().space, 9);
        hub.deliver(join_at(12, 0, 0, false));
        live.pump();
        assert_eq!(live.space(), Some(9), "an ordinary rejoin replaced the space");
    }

    /// A replica that dropped held updates (`needs_resync`) never becomes the room's checkpoint.
    #[test]
    fn a_replica_behind_the_room_does_not_checkpoint() {
        let hub = Hub0::default();
        let mut live = Live::new(hub.clone(), "d");
        live.seed(&[block()]).unwrap();
        hub.0.borrow_mut().1.push(joined_msg());
        live.pump();
        live.edit(|d| d.insert_text(1, 0, "x", &Map::new())).unwrap();
        live.doc.as_mut().unwrap().force_resync(true);
        let sent = hub.0.borrow().0.len();
        assert!(live.checkpoint().is_err());
        assert_eq!(hub.0.borrow().0.len(), sent, "nothing went out");
        live.doc.as_mut().unwrap().force_resync(false);
        assert!(live.checkpoint().is_ok(), "the refusal was about being behind");
    }

    thread_local! {
        /// How many times this thread's `Note`s took an update as a snapshot (`apply_state`).
        static STATE_APPLIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// A schema that is not `LiveDoc`: one text, written on `YReplica`. What `Live` needs from
    /// a replica is the trait, nothing of the block layout.
    struct Note {
        y: crate::YReplica,
        text: yrs::TextRef,
    }

    impl Note {
        fn assemble(doc: yrs::Doc, client: Option<u32>) -> Note {
            let text = doc.get_or_insert_text("note");
            let t = text.clone();
            Note { y: crate::YReplica::assemble(doc, client, move |doc, undo| undo.expand_scope(doc, &t)), text }
        }
        fn read(&self) -> String {
            use yrs::{GetString, Transact};
            self.text.get_string(&self.y.doc().transact())
        }
        fn append(&mut self, s: &str) -> std::result::Result<Vec<u8>, LiveError> {
            use yrs::Text;
            let text = self.text.clone();
            let at = u32::try_from(self.read().len()).map_err(|_| LiveError::OutOfRange)?;
            self.y.local(|txn| {
                text.insert(txn, at, s);
                Ok(())
            })
        }
    }

    impl Replica for Note {
        type Change = ();
        type Seed = str;
        fn seeded(seed: &str) -> std::result::Result<Note, LiveError> {
            use yrs::{Text, Transact};
            let doc = crate::YReplica::seed_doc();
            let text = doc.get_or_insert_text("note");
            text.insert(&mut doc.transact_mut_with(crate::replica::REMOTE), 0, seed);
            Ok(Note::assemble(doc, None))
        }
        fn from_state(state: &[u8]) -> std::result::Result<Note, LiveError> {
            let doc = crate::YReplica::seed_doc();
            crate::replica::integrate(&doc, state)?;
            Ok(Note::assemble(doc, None))
        }
        fn set_client(&mut self, space: u32) -> std::result::Result<(), LiveError> {
            let doc = self.y.rebuilt(space)?;
            *self = Note::assemble(doc, Some(space));
            Ok(())
        }
        fn apply(&mut self, update: &[u8]) -> std::result::Result<Vec<()>, LiveError> {
            let before = self.read();
            self.y.apply(update)?;
            Ok(if self.read() == before { Vec::new() } else { vec![()] })
        }
        fn apply_state(&mut self, update: &[u8]) -> std::result::Result<Vec<()>, LiveError> {
            STATE_APPLIES.with(|n| n.set(n.get() + 1));
            let before = self.read();
            self.y.apply_state(update)?;
            Ok(if self.read() == before { Vec::new() } else { vec![()] })
        }
        fn state(&self) -> Vec<u8> {
            self.y.state()
        }
        fn undo(&mut self) -> Option<Vec<u8>> {
            self.y.undo()
        }
        fn redo(&mut self) -> Option<Vec<u8>> {
            self.y.redo()
        }
        fn damaged(&self) -> bool {
            self.y.damaged()
        }
    }

    fn joined(space: u32) -> ServerMsg {
        ServerMsg::Joined {
            doc: "d".to_owned(),
            peer: format!("p{space}"),
            seq: 0,
            space,
            roster: Vec::new(),
            locks: Vec::new(),
            replay_until: None,
            fallback: false,
            text_cap: None,
            heartbeat: false,
            history_lost: false,
        }
    }

    /// Two links on another schema: an edit made on one, ordered by the relay, lands on the
    /// other as that schema's own change.
    #[test]
    fn another_schema_rides_the_same_link() {
        let (ha, hb) = (Hub0::default(), Hub0::default());
        let mut a: Live<Hub0, Note> = Live::with_replica(ha.clone(), "d");
        let mut b: Live<Hub0, Note> = Live::with_replica(hb.clone(), "d");
        a.seed("hi").unwrap();
        b.seed("hi").unwrap();
        ha.0.borrow_mut().1.push(joined(1));
        hb.0.borrow_mut().1.push(joined(2));
        a.pump();
        b.pump();
        assert!(a.ready_to_type());
        a.edit(|n| n.append(" there")).unwrap();
        let sent = std::mem::take(&mut ha.0.borrow_mut().0);
        let Some(ClientMsg::Submit { command, params, local, .. }) = sent.into_iter().next() else { panic!("nothing was sent") };
        let op = ServerMsg::Op { doc: "d".to_owned(), seq: 1, origin: "p1".to_owned(), local: Some(local), command, params, scope: None };
        hb.0.borrow_mut().1.push(op);
        let work = b.pump();
        assert!(work.iter().any(|w| matches!(w, Work::Doc { changes } if !changes.is_empty())), "{work:?}");
        assert_eq!(b.doc().map(Note::read), a.doc().map(Note::read));
        assert_ne!(b.doc().map(Note::read).as_deref(), Some("hi"), "the edit did not land");
    }

    fn snapshot_op(seq: u64, state: &[u8]) -> ServerMsg {
        let params = serde_json::to_value(TextPayload::snapshot(state)).unwrap();
        ServerMsg::Op { doc: "d".to_owned(), seq, origin: "service".to_owned(), local: None, command: CRDT_TEXT.to_owned(), params, scope: None }
    }

    /// "hi", and a peer's later state of it with " there" appended.
    fn two_snapshots() -> (Vec<u8>, Vec<u8>) {
        let first = Note::seeded("hi").unwrap().state();
        let mut peer = Note::from_state(&first).unwrap();
        peer.set_client(7).unwrap();
        peer.append(" there").unwrap();
        (first, peer.state())
    }

    /// R3-M5: a second snapshot that arrives before the file is loaded is held as an ordinary
    /// update, and still lands under the state item cap when the document is seeded.
    #[test]
    fn a_held_snapshot_lands_as_a_snapshot() {
        let hub = Hub0::default();
        let mut live: Live<Hub0, Note> = Live::with_replica(hub.clone(), "d");
        let (first, second) = two_snapshots();
        hub.0.borrow_mut().1.extend([joined(1), snapshot_op(1, &first), snapshot_op(2, &second)]);
        live.pump();
        assert!(live.doc().is_none());
        STATE_APPLIES.with(|n| n.set(0));
        live.seed("the file").unwrap();
        assert_eq!(STATE_APPLIES.with(std::cell::Cell::get), 1, "the held snapshot was applied under the op cap");
        assert_eq!(live.doc().map(Note::read).as_deref(), Some("hi there"));
    }

    /// R3-M5: a take-back replays a snapshot the room ordered under the fence as a snapshot
    /// (the state item cap), as it was applied when it landed.
    #[test]
    fn a_take_back_replays_a_snapshot_as_a_snapshot() {
        let hub = Hub0::default();
        let mut live: Live<Hub0, Note> = Live::with_replica(hub.clone(), "d");
        let (first, second) = two_snapshots();
        hub.0.borrow_mut().1.extend([joined(1), snapshot_op(1, &first)]);
        live.pump();
        live.seed("the file").unwrap();
        assert_eq!(live.doc().map(Note::read).as_deref(), Some("hi"));
        let scope = || "grid/struct".to_owned();
        hub.0.borrow_mut().1.push(ServerMsg::LockGranted { doc: "d".to_owned(), scope: scope(), seq: Some(1), token: Some(1), lease_ms: Some(1000) });
        live.pump();
        live.edit(|n| n.append("!")).unwrap();
        let local = hub
            .0
            .borrow()
            .0
            .iter()
            .rev()
            .find_map(|m| match m {
                ClientMsg::Submit { local, fence: Some(_), .. } => Some(*local),
                _ => None,
            })
            .expect("a fenced submit");
        STATE_APPLIES.with(|n| n.set(0));
        hub.0.borrow_mut().1.push(snapshot_op(2, &second));
        live.pump();
        assert_eq!(STATE_APPLIES.with(std::cell::Cell::get), 1, "the snapshot landed under the op cap");
        hub.0.borrow_mut().1.push(ServerMsg::Error {
            doc: "d".to_owned(),
            detail: "fenced".to_owned(),
            local: Some(local),
            code: Some(FENCED.to_owned()),
        });
        let work = live.pump();
        assert!(work.iter().any(|w| matches!(w, Work::Reset)), "{work:?}");
        assert_eq!(STATE_APPLIES.with(std::cell::Cell::get), 2, "the take-back replayed the snapshot under the op cap");
        assert_eq!(live.doc().map(Note::read).as_deref(), Some("hi there"));
    }

    /// `n` letters each typed at the front of the root text `t` by `client`, as one update:
    /// none merge, so each letter is its own block (the densest honest update).
    fn letters_at_the_front(client: u8, n: u32) -> Vec<u8> {
        fn varint(out: &mut Vec<u8>, mut v: u32) {
            while v >= 0x80 {
                out.push((v as u8 & 0x7f) | 0x80);
                v >>= 7;
            }
            out.push(v as u8);
        }
        let mut u = vec![1];
        varint(&mut u, n);
        u.extend([client, 0]);
        // The first letter names its parent, the root `t`; each later one is put before the last.
        u.extend([0x04, 1, 1, b't', 1, b'x']);
        for clock in 1..n {
            u.extend([0x44, client]);
            varint(&mut u, clock - 1);
            u.extend([1, b'x']);
        }
        u.push(0);
        u
    }

    /// R3-M6 (ruled): a resend is chunked under the op block cap as well as the text cap. Three
    /// dense updates of 400 k letters each fit the default text cap together (about 8 MiB), but
    /// merged they would pass 2^20 blocks, which the relay refuses for good. Every chunk stays
    /// under the cap, and no letter is lost.
    #[test]
    fn a_dense_resend_is_chunked_under_the_block_cap() {
        const LETTERS: u32 = 400_000;
        let mut live = Live::new(Hub0::default(), "d");
        live.text_cap = Some(16 * 1024 * 1024 - 64 * 1024);
        let updates: Vec<Vec<u8>> = (1..=3).map(|client| letters_at_the_front(client, LETTERS)).collect();
        for u in &updates {
            assert_eq!(crate::update_blocks(u), Some(LETTERS as usize + 1), "the fixture is not as dense as meant");
        }
        let total: usize = updates.iter().map(Vec::len).sum();
        assert!(total.div_ceil(3) * 4 < 16 * 1024 * 1024 / 10 * 9, "the text cap alone would split it");
        let chunks = live.chunked(updates);
        let counts: Vec<usize> = chunks.iter().map(|c| crate::update_blocks(c).unwrap()).collect();
        assert!(counts.iter().all(|&n| n <= crate::MAX_OP_BLOCKS), "a chunk the relay refuses for good: {counts:?}");
        assert!(chunks.len() >= 2, "{counts:?}");
        assert_eq!(counts.iter().sum::<usize>(), 3 * (LETTERS as usize + 1), "letters were lost or added: {counts:?}");
    }

    /// C7b review M-1: a live checkpoint that overtakes a marked fallback's text ops is kept
    /// in `since_join` with its snapshot flag, so when the text ops restart the file-seeded
    /// replica it is applied again under the state item cap, not the op cap.
    #[test]
    fn a_checkpoint_kept_since_join_is_replayed_as_a_snapshot() {
        let hub = Hub0::default();
        let mut live: Live<Hub0, Note> = Live::with_replica(hub.clone(), "d");
        live.seed("hi").unwrap();
        let (first, second) = two_snapshots();
        let fallback = ServerMsg::Joined {
            doc: "d".to_owned(),
            peer: "p1".to_owned(),
            seq: 0,
            space: 1,
            roster: Vec::new(),
            locks: Vec::new(),
            replay_until: Some(0),
            fallback: true,
            text_cap: None,
            heartbeat: false,
            history_lost: false,
        };
        STATE_APPLIES.with(|n| n.set(0));
        hub.0.borrow_mut().1.extend([fallback, snapshot_op(1, &second)]);
        live.pump();
        assert_eq!(STATE_APPLIES.with(std::cell::Cell::get), 1, "the live checkpoint landed under the op cap");
        let text_ops = ServerMsg::TextOps {
            doc: "d".to_owned(),
            scope: DOC_SCOPE.to_owned(),
            ops: serde_json::json!([update_to_wire(&first)]),
            complete: true,
            snap: true,
            more: false,
        };
        hub.0.borrow_mut().1.push(text_ops);
        let work = live.pump();
        assert!(work.iter().any(|w| matches!(w, Work::Reset)), "the text ops did not restart the replica: {work:?}");
        assert_eq!(STATE_APPLIES.with(std::cell::Cell::get), 2, "the checkpoint kept since the join was replayed under the op cap");
        assert_eq!(live.doc().map(Note::read).as_deref(), Some("hi there"));
    }
}
