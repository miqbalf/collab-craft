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
use crate::proto::{CRDT_TEXT, ClientMsg, RETRY, ServerMsg};
use crate::session::{Effect, Session};
use crate::ydoc::{BlockChange, LiveDoc, LiveError, SeedBlock};

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
/// Document updates are already merged here. [`Work::Doc`] says which blocks to re-read
/// through [`Live::doc`]. Other commands still go through the app's `apply_remote`.
#[derive(Debug)]
pub enum Work {
    /// A lock-scoped command that is not a document update.
    Command { command: String, params: Value },
    /// Remote updates landed. Re-read these blocks.
    Doc { changes: Vec<BlockChange> },
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
}

/// The most local edit bytes a replica holds unconfirmed before it gives up on the link.
const MAX_UNCONFIRMED_BYTES: usize = 32 * 1024 * 1024;

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

/// One live shared-editing link, over any [`Hub`].
pub struct Live<H> {
    hub: H,
    wire: Session,
    /// The replica, once the app has seeded it from the file.
    doc: Option<LiveDoc>,
    /// Updates that arrived before the file was loaded, in arrival order.
    held: Vec<Vec<u8>>,
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
    since_join: Vec<Vec<u8>>,
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
    /// Lines for the user, drained by the app each frame.
    pub notices: Vec<String>,
    /// Who else is in the document. The identifier comes with the name because a badge's
    /// colour is derived from it.
    pub roster: Vec<(String, String)>,
    was_connected: bool,
}

impl<H: Hub> Live<H> {
    /// Takes an already-connected hub. The relay admits a peer from the token in the
    /// handshake; nothing is sent here on purpose.
    pub fn new(hub: H, doc: impl Into<String>) -> Live<H> {
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
            retrying: std::collections::BTreeSet::new(),
            retry_at: None,
            retry_wait: RETRY_FIRST,
            awaiting_snapshot: false,
            told_lost: false,
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
    pub fn doc(&self) -> Option<&LiveDoc> {
        self.doc.as_ref()
    }

    /// Load the document and get what the held updates changed on top of it.
    ///
    /// When a checkpoint snapshot has arrived, the document is restored from that and `blocks`
    /// are not used: the file is behind the snapshot and its items are not the room's. If that
    /// restore fails the replica is broken for good and the file is never seeded instead,
    /// because the room has moved past it. A document already seeded is left alone and nothing
    /// is reported.
    pub fn seed(&mut self, blocks: &[SeedBlock]) -> std::result::Result<Vec<BlockChange>, LiveError> {
        if self.broken {
            return Err(LiveError::Decode);
        }
        if self.doc.is_some() {
            return Ok(Vec::new());
        }
        let mut doc = match self.snapshot.take() {
            Some(snapshot) => match LiveDoc::from_state(&snapshot) {
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
                let doc = LiveDoc::seeded(blocks)?;
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
        for update in held {
            match doc.apply(&update) {
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
    pub fn edit<F>(&mut self, edit: F) -> Result<()>
    where
        F: FnOnce(&mut LiveDoc) -> std::result::Result<Vec<u8>, LiveError>,
    {
        let doc = self.sound_doc()?;
        let update = edit(doc).map_err(refusal)?;
        self.edited = true;
        self.send_update(&update)
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
        let state = self.sound_doc()?.state();
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
        match self.sound_doc()?.undo() {
            Some(update) => {
                self.edited = true;
                self.send_update(&update).map(|()| true)
            }
            None => Ok(false),
        }
    }

    /// Redo the last undone local edit and send it. `false` when there is nothing to redo.
    pub fn redo(&mut self) -> Result<bool> {
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
    pub fn pump(&mut self) -> Vec<Work> {
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
        work
    }

    /// The document, when it may take a local edit.
    fn sound_doc(&mut self) -> Result<&mut LiveDoc> {
        if self.broken {
            return Err(Error::malformed("client", "the shared document is broken; reopen the file"));
        }
        if self.awaiting_snapshot {
            return Err(Error::malformed(
                "client",
                "the live session lost its history; editing waits for the document service's snapshot",
            ));
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
        self.broken = true;
        self.snapshot = None;
        self.unconfirmed.clear();
        self.unconfirmed_bytes = 0;
        self.retrying.clear();
        self.held.clear();
        self.notices.push("the shared document could not be restored; reopen the file".to_owned());
    }

    /// A join, first or not. Only the first one's catch-up can restart the replica; a later one
    /// re-arms it only if no catch-up was handled and nothing went live since. A checkpoint in
    /// flight is treated as lost, because the submit may not survive the reconnect.
    ///
    /// On a join that lost the room's history, a replica that held nothing from before waits
    /// for a snapshot, and [`Work::HistoryLost`] is raised either way.
    fn joined(&mut self, seq: u64, replay_until: Option<u64>, fallback: bool, history_lost: bool, work: &mut Vec<Work>) {
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
        if !self.joined_once || !self.settled {
            let catchup = Catchup { joined: seq, until: replay_until };
            self.catchup = catchup.replays_anything().then_some(catchup);
            // A relay that marks the replay also says whether text ops follow. An older one
            // does not, and its text ops have always been the catch-up.
            self.text_catchup = fallback || replay_until.is_none();
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
            self.resend_unconfirmed();
        }
        self.retrying.clear();
        self.retry_at = None;
        self.joined_once = true;
        self.lose_checkpoint(work, "the link rejoined");
    }

    /// Drop a checkpoint whose submit may never be ordered, reported as raced so it is retried.
    fn lose_checkpoint(&mut self, work: &mut Vec<Work>, why: &str) {
        if self.checkpoint.take().is_some() {
            log::info!("collab: a checkpoint in flight was dropped: {why}");
            work.push(Work::CheckpointRaced);
        }
    }

    /// Drop the checkpoint in flight on a refusal the relay tied to it. Permanent refusals are
    /// told apart from the rest, so an app that retries on [`Work::CheckpointRaced`] does not
    /// loop on them.
    fn refuse_checkpoint(&mut self, work: &mut Vec<Work>, detail: &str) {
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
    fn time_out_checkpoint(&mut self, work: &mut Vec<Work>) {
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
        let params = serde_json::to_value(TextPayload::new(update)).map_err(|e| Error::malformed("client", e.to_string()))?;
        if let Some(bytes) = self.over_cap(&params) {
            self.break_replica(&format!("a local edit of {bytes} bytes is past what the relay carries"));
            self.notices.push("that change is too large to share with the other editors; reopen the file".to_owned());
            return Err(Error::Refused(format!("that change is {bytes} bytes, more than this relay will carry")));
        }
        let msg: ClientMsg = self.wire.submit_crdt(CRDT_TEXT, params);
        if let ClientMsg::Submit { local, .. } = &msg {
            self.unconfirmed.insert(*local, update.to_vec());
            self.unconfirmed_bytes += update.len();
        }
        self.hub.send(msg)?;
        if self.unconfirmed_bytes > MAX_UNCONFIRMED_BYTES {
            // The relay has not confirmed this much for so long that the link is as good as
            // gone; holding more would grow without bound.
            self.break_replica("too many edits are waiting for the relay");
        }
        Ok(())
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
        for chunk in self.chunked(updates.into_values()) {
            if self.broken {
                break;
            }
            if let Err(e) = self.send_update(&chunk) {
                log::warn!("collab: unconfirmed edits could not be sent again: {e}");
            }
        }
    }

    /// Send again the edits the relay refused for now, once their backoff is over and the link
    /// is up. While it is down they wait for the rejoin, which sends every unconfirmed edit.
    fn send_retries(&mut self) {
        if self.retrying.is_empty() || self.broken || !self.hub.connected() {
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
                updates.push(update);
            }
        }
        log::info!("collab: sending {} edits the relay refused for now again", updates.len());
        for chunk in self.chunked(updates) {
            if self.broken {
                break;
            }
            if let Err(e) = self.send_update(&chunk) {
                log::warn!("collab: an edit refused for now could not be sent again: {e}");
            }
        }
    }

    /// Merge `updates` in order into as few updates as fit the relay's cap, each measured as
    /// [`Live::over_cap`] measures a submit. An update that does not fit merged with anything
    /// goes as it was: it was sent once, so it fits alone.
    fn chunked(&self, updates: impl IntoIterator<Item = Vec<u8>>) -> Vec<Vec<u8>> {
        // The params a submit of `bytes` of update would serialize to, about: base64, the
        // quotes, and the scope around it. Merged updates are no larger than the parts summed.
        let estimate = |bytes: usize| bytes.div_ceil(3) * 4 + 32;
        let fits = |update: &[u8]| serde_json::to_value(TextPayload::new(update)).map(|params| self.over_cap(&params).is_none()).unwrap_or(false);
        let cap = self.text_cap.map(|cap| usize::try_from(cap).unwrap_or(usize::MAX));
        // Each chunk, with the summed size of the updates in it.
        let mut chunks: Vec<(Vec<u8>, usize)> = Vec::new();
        for update in updates {
            let grown = chunks.last().and_then(|(last, summed)| {
                let summed = summed + update.len();
                // Only near the cap is the exact measure worth its serialization.
                let (sure, maybe) = match cap {
                    None => (true, true),
                    Some(cap) => (estimate(summed) <= cap / 10 * 9, estimate(summed) <= cap / 10 * 11),
                };
                if !maybe {
                    return None;
                }
                yrs::merge_updates_v1([last.as_slice(), update.as_slice()]).ok().filter(|merged| sure || fits(merged)).map(|merged| (merged, summed))
            });
            match (grown, chunks.last_mut()) {
                (Some(merged), Some(last)) => *last = merged,
                _ => {
                    let size = update.len();
                    chunks.push((update, size));
                }
            }
        }
        chunks.into_iter().map(|(chunk, _)| chunk).collect()
    }

    /// Merge a batch of wire updates, or hold them until the document is loaded.
    ///
    /// `snap` says the first update is a checkpoint snapshot. `restart` says it is the one this
    /// replica must start from (the first op of its first catch-up).
    fn merge_updates(&mut self, scope: String, ops: &[String], snap: bool, restart: bool, work: &mut Vec<Work>) {
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
        if snap && !updates.is_empty() && (restart || self.doc.is_none()) {
            let snapshot = updates.remove(0);
            match self.restart_from(snapshot, &scope, work) {
                Restart::Done => {}
                Restart::Refused => return,
                Restart::Ordinary(snapshot) => updates.insert(0, snapshot),
            }
        }
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
        for update in updates {
            match doc.apply(&update) {
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
        if damaged {
            work.push(Work::TextIncomplete { scope });
        }
    }

    /// Start from a snapshot.
    fn restart_from(&mut self, snapshot: Vec<u8>, scope: &str, work: &mut Vec<Work>) -> Restart {
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
        let restored = LiveDoc::from_state(&snapshot).and_then(|mut doc| match self.space {
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
    fn confirmed(&mut self, local: u64, seq: u64, work: &mut Vec<Work>) {
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
    fn save_answered(&mut self, seq: u64, refusal: Option<&str>, work: &mut Vec<Work>) {
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

    fn act(&mut self, effect: Effect, work: &mut Vec<Work>) {
        match effect {
            Effect::Space { space } => {
                self.space = Some(space);
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
                self.first_of_catchup(seq);
                self.confirmed(local, seq, work);
            }
            Effect::Replace { document, seq } => {
                self.lose_checkpoint(work, "the relay replaced the document");
                // Edits made against what is being replaced cannot be reconciled with it.
                self.unconfirmed.clear();
                self.unconfirmed_bytes = 0;
                self.retrying.clear();
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
            Effect::LockHeld { .. } | Effect::LockDropped { .. } | Effect::RemoteLocks { .. } => {}
            Effect::LockRefused { scope, holder } => {
                self.notices.push(format!("{holder} is editing {scope}"));
            }
            Effect::Roster { roster } => {
                let me = self.wire.me().cloned();
                self.roster = roster.into_iter().filter(|p| Some(&p.peer) != me.as_ref()).map(|p| (p.peer.to_string(), p.display)).collect();
            }
            Effect::Notice { detail } => {
                log::info!("collab: {detail}");
                self.notices.push(detail);
            }
            Effect::SubmitRefused { local, detail, code } => {
                // The relay refused one of this client's submits by its counter. It is the
                // checkpoint's own only while that has not been ordered.
                let retry = code.as_deref() == Some(RETRY) || !refusal_is_permanent(&detail);
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
fn merge_changes(into: &mut Vec<BlockChange>, found: Vec<BlockChange>) {
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
}
