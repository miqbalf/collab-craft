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

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::hub::Hub;
use crate::proto::{CRDT_TEXT, ClientMsg, ServerMsg};
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
    /// This replica's checkpoint was ordered at `seq` and reported as saved through `seq - 1`.
    Checkpointed { seq: u64 },
    /// A remote edit landed between this replica's snapshot and its ordering, so the snapshot
    /// may miss it and was not reported as saved. Call [`Live::checkpoint`] again.
    CheckpointRaced,
}

/// A checkpoint snapshot this replica sent and the relay has not ordered yet.
struct PendingCheckpoint {
    /// The session's counter for the snapshot submit.
    local: u64,
    /// A remote update landed while it was in flight. Those are ordered before it and may be
    /// missing from it, so it must not be reported as saved.
    raced: bool,
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
    /// Whether the op ordered at `seq` is the first op of the catch-up replay.
    fn starts_at(self, seq: u64) -> bool {
        let first = self.joined + 1;
        seq == first
            && match self.until {
                Some(until) => seq <= until,
                // An older relay does not mark the replay. Its baseline is the last save, and a
                // save is only accepted just before a snapshot, so a baseline of 0 (never saved)
                // has no snapshot to start from: the file is the room's own seed.
                None => self.joined > 0,
            }
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
    /// The first join's catch-up, until its first ordered op has been handled.
    catchup: Option<Catchup>,
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
            joined_once: false,
            from_file: false,
            edited: false,
            broken: false,
            checkpoint: None,
            notices: Vec::new(),
            roster: Vec::new(),
            was_connected: true,
        }
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
                Ok(doc) => doc,
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
        for update in std::mem::take(&mut self.held) {
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

    /// Whether the relay has granted an id space and the document is loaded and sound.
    /// Editing is refused until all three hold.
    pub fn ready_to_type(&self) -> bool {
        self.space.is_some() && self.doc.as_ref().is_some_and(|d| !d.damaged()) && !self.broken
    }

    /// Whether the transport believes it is connected.
    pub fn connected(&self) -> bool {
        self.hub.connected()
    }

    /// Make one local edit and send the update it produced. Lock-free.
    ///
    /// For example `live.edit(|d| d.insert_text(id, at, "x", &attrs))`.
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
    /// For the replica that saves the file. When the relay orders it, [`pump`](Live::pump)
    /// reports [`Work::Checkpointed`] and tells the relay the save covers everything before it,
    /// or [`Work::CheckpointRaced`] when a remote edit slipped in first or the submit was lost.
    /// One at a time.
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.checkpoint.is_some() {
            return Err(Error::malformed("client", "a checkpoint is already in flight"));
        }
        let state = self.sound_doc()?.state();
        let params = serde_json::to_value(TextPayload::snapshot(&state)).map_err(|e| Error::malformed("client", e.to_string()))?;
        let msg = self.wire.submit_crdt(CRDT_TEXT, params);
        let ClientMsg::Submit { local, .. } = &msg else {
            return Err(Error::malformed("client", "a checkpoint must be a submit"));
        };
        self.checkpoint = Some(PendingCheckpoint { local: *local, raced: false });
        self.hub.send(msg)
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
            if let ServerMsg::Joined { seq, replay_until, .. } = &msg {
                self.joined(*seq, *replay_until, &mut work);
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
        work
    }

    /// The document, when it may take a local edit.
    fn sound_doc(&mut self) -> Result<&mut LiveDoc> {
        if self.broken {
            return Err(Error::malformed("client", "the shared document is broken; reopen the file"));
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
        self.held.clear();
        self.notices.push("the shared document could not be restored; reopen the file".to_owned());
    }

    /// A join, first or not. Only the first one's catch-up can restart the replica; a later one
    /// re-arms it only if the first catch-up never arrived. A checkpoint in flight is treated
    /// as lost, because the submit may not survive the reconnect.
    fn joined(&mut self, seq: u64, replay_until: Option<u64>, work: &mut Vec<Work>) {
        if !self.joined_once || self.catchup.is_some() {
            self.catchup = Some(Catchup { joined: seq, until: replay_until });
        }
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

    fn send_update(&mut self, update: &[u8]) -> Result<()> {
        let params = serde_json::to_value(TextPayload::new(update)).map_err(|e| Error::malformed("client", e.to_string()))?;
        let msg: ClientMsg = self.wire.submit_crdt(CRDT_TEXT, params);
        self.hub.send(msg)
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
        // seeded once the room is known to have checkpointed past it.
        if snap && !updates.is_empty() && (restart || self.doc.is_none()) {
            let snapshot = updates.remove(0);
            match self.restart_from(snapshot, &scope, work) {
                Restart::Done => {}
                Restart::Refused => return,
                Restart::Ordinary(snapshot) => updates.insert(0, snapshot),
            }
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
        if !self.from_file {
            return Restart::Ordinary(snapshot);
        }
        if self.edited {
            // Typed against the file before the catch-up arrived. Those edits name items the
            // room does not have, so there is no honest merge.
            self.break_replica("edits were made against a file the room had already checkpointed past");
            work.push(Work::TextIncomplete { scope: scope.to_owned() });
            return Restart::Refused;
        }
        let restored = LiveDoc::from_state(&snapshot).and_then(|mut doc| match self.space {
            Some(space) => doc.set_client(space).map(|()| doc),
            None => Ok(doc),
        });
        match restored {
            Ok(doc) => {
                self.doc = Some(doc);
                self.from_file = false;
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
    /// ordered op of any kind, so a later checkpoint from a peer is never mistaken for one.
    fn first_of_catchup(&mut self, seq: u64) -> bool {
        self.catchup.take().is_some_and(|c| c.starts_at(seq))
    }

    /// One of this replica's submits was ordered. Finishes a checkpoint when it was that one.
    fn confirmed(&mut self, local: u64, seq: u64, work: &mut Vec<Work>) {
        if self.checkpoint.as_ref().is_none_or(|p| p.local != local) {
            return;
        }
        let raced = self.checkpoint.take().is_some_and(|p| p.raced);
        if raced {
            log::info!("collab: a remote edit landed before checkpoint {seq}; it was not reported as saved");
            work.push(Work::CheckpointRaced);
            return;
        }
        let saved = self.wire.saved_through(seq.saturating_sub(1));
        match self.hub.send(saved) {
            Ok(()) => work.push(Work::Checkpointed { seq }),
            Err(e) => {
                log::warn!("collab: checkpoint {seq} was ordered but the save report never left: {e}");
                work.push(Work::CheckpointRaced);
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
            Effect::Apply { command, params, .. } => {
                self.catchup = None;
                work.push(Work::Command { command, params });
            }
            Effect::TextOps { scope, ops, complete, snap } => {
                // The fallback catch-up: it replaces the ordered replay, so it is the catch-up.
                let restart = self.catchup.take().is_some();
                if !complete {
                    log::warn!("collab: the relay could not keep every change to {scope}");
                    self.notices.push(format!("{scope} has changes this session cannot rebuild; reopen the file"));
                    work.push(Work::TextIncomplete { scope });
                    return;
                }
                match serde_json::from_value::<Vec<String>>(ops) {
                    Ok(ops) => self.merge_updates(scope, &ops, snap, snap && restart, work),
                    Err(e) => {
                        log::warn!("collab: replayed operations for {scope} did not decode: {e}");
                        self.notices.push(format!("{scope} could not be caught up; reopen the file"));
                        work.push(Work::TextIncomplete { scope });
                    }
                }
            }
            Effect::Frame { origin, payload } => work.push(Work::Frame { origin, payload }),
            Effect::Ephemeral { origin, kind, payload } => work.push(Work::Ephemeral { origin, kind, payload }),
            Effect::Confirmed { local, seq } => {
                self.catchup = None;
                self.confirmed(local, seq, work);
            }
            Effect::Replace { document, seq } => {
                self.lose_checkpoint(work, "the relay replaced the document");
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
                // A relay error. It carries no counter, so a checkpoint in flight may be the
                // submit it refused; dropping it as raced only costs a retry.
                self.lose_checkpoint(work, "the relay reported an error");
                log::info!("collab: {detail}");
                self.notices.push(detail);
            }
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
