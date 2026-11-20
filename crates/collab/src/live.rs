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
    /// A snapshot the catch-up began with, waiting for [`Live::seed`].
    snapshot: Option<Vec<u8>>,
    /// The sequence of this link's first join, until the first document op after it arrives.
    /// Only that op can be a catch-up snapshot to start from.
    first_join: Option<u64>,
    /// Whether this link has joined before. A reconnect shares history and never restarts.
    joined_once: bool,
    /// The replica was seeded from the file, not restored from a snapshot.
    from_file: bool,
    /// A local edit has been sent from this replica.
    edited: bool,
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
            first_join: None,
            joined_once: false,
            from_file: false,
            edited: false,
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

    /// Load the document from the file and get what the held updates changed on top of it.
    ///
    /// When the catch-up began with a checkpoint snapshot, the document is restored from that
    /// and `blocks` are not used: the file is behind the snapshot and its items are not the
    /// room's. A document already seeded is left alone and nothing is reported.
    pub fn seed(&mut self, blocks: &[SeedBlock]) -> Vec<BlockChange> {
        if self.doc.is_some() {
            return Vec::new();
        }
        let mut doc = match self.snapshot.take() {
            Some(snapshot) => match LiveDoc::from_state(&snapshot) {
                Ok(doc) => doc,
                Err(e) => {
                    log::warn!("collab: the room's checkpoint did not restore: {e}");
                    self.notices.push("the shared document could not be restored; reopen the file".to_owned());
                    return Vec::new();
                }
            },
            None => {
                self.from_file = true;
                LiveDoc::seeded(blocks)
            }
        };
        if let Some(space) = self.space {
            doc.set_client(space);
        }
        let mut changes = Vec::new();
        for update in std::mem::take(&mut self.held) {
            match doc.apply(&update) {
                Ok(found) => changes.extend(found.into_iter().filter(|c| !changes.contains(c)).collect::<Vec<_>>()),
                Err(e) => {
                    log::warn!("collab: a held update did not apply: {e}");
                    self.notices.push("could not apply a change from another editor".to_owned());
                }
            }
        }
        self.doc = Some(doc);
        changes
    }

    /// Whether the relay has granted an id space and the file is loaded. Editing is refused
    /// until both hold.
    pub fn ready_to_type(&self) -> bool {
        self.space.is_some() && self.doc.is_some()
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
        let doc = self.doc.as_mut().ok_or_else(|| Error::malformed("client", "the document was never seeded"))?;
        let update = edit(doc).map_err(refusal)?;
        self.edited = true;
        self.send_update(&update)
    }

    /// Send a checkpoint snapshot of the whole document.
    ///
    /// For the replica that saves the file. When the relay orders it, [`pump`](Live::pump)
    /// reports [`Work::Checkpointed`] and tells the relay the save covers everything before it,
    /// or [`Work::CheckpointRaced`] when a remote edit slipped in first. One at a time.
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.checkpoint.is_some() {
            return Err(Error::malformed("client", "a checkpoint is already in flight"));
        }
        let doc = self.doc.as_ref().ok_or_else(|| Error::malformed("client", "the document was never seeded"))?;
        let params = serde_json::to_value(TextPayload::snapshot(&doc.state())).map_err(|e| Error::malformed("client", e.to_string()))?;
        let msg = self.wire.submit_crdt(CRDT_TEXT, params);
        let ClientMsg::Submit { local, .. } = &msg else {
            return Err(Error::malformed("client", "a checkpoint must be a submit"));
        };
        self.checkpoint = Some(PendingCheckpoint { local: *local, raced: false });
        self.hub.send(msg)
    }

    /// Undo this replica's last local edit and send it. `false` when there is nothing to undo.
    pub fn undo(&mut self) -> Result<bool> {
        match self.doc.as_mut().and_then(LiveDoc::undo) {
            Some(update) => {
                self.edited = true;
                self.send_update(&update).map(|()| true)
            }
            None => Ok(false),
        }
    }

    /// Redo the last undone local edit and send it. `false` when there is nothing to redo.
    pub fn redo(&mut self) -> Result<bool> {
        match self.doc.as_mut().and_then(LiveDoc::redo) {
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
            if let ServerMsg::Joined { seq, .. } = &msg {
                if !self.joined_once {
                    self.joined_once = true;
                    self.first_join = Some(*seq);
                }
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

    fn send_update(&mut self, update: &[u8]) -> Result<()> {
        let params = serde_json::to_value(TextPayload::new(update)).map_err(|e| Error::malformed("client", e.to_string()))?;
        let msg: ClientMsg = self.wire.submit_crdt(CRDT_TEXT, params);
        self.hub.send(msg)
    }

    /// Merge a batch of wire updates, or hold them until the file is loaded.
    ///
    /// `restart` says the first update is a catch-up snapshot this replica must start from.
    fn merge_updates(&mut self, scope: String, ops: &[String], restart: bool, work: &mut Vec<Work>) {
        if scope != DOC_SCOPE {
            log::warn!("collab: ignoring updates for scope {scope:?}, not {DOC_SCOPE:?}");
            return;
        }
        let mut updates = Vec::new();
        for op in ops {
            match update_from_wire(op) {
                Ok(u) => updates.push(u),
                Err(e) => {
                    log::warn!("collab: an update did not decode: {e}");
                    self.notices.push("could not apply a change from another editor".to_owned());
                    if restart && updates.is_empty() {
                        // The snapshot itself is unreadable, and the file is not a substitute.
                        work.push(Work::TextIncomplete { scope });
                        return;
                    }
                }
            }
        }
        if restart && !updates.is_empty() {
            let snapshot = updates.remove(0);
            if self.restart_from(snapshot, &scope, work) {
                return;
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
        for update in updates {
            match doc.apply(&update) {
                Ok(found) => {
                    for c in found {
                        if !changes.contains(&c) {
                            changes.push(c);
                        }
                    }
                }
                Err(e) => {
                    log::warn!("collab: an update did not apply: {e}");
                    self.notices.push("could not apply a change from another editor".to_owned());
                    if doc.damaged() {
                        work.push(Work::TextIncomplete { scope: scope.clone() });
                    }
                }
            }
        }
        if !changes.is_empty() {
            work.push(Work::Doc { changes });
        }
    }

    /// Start from a catch-up snapshot. `false` means the snapshot is an ordinary update here
    /// (this replica already shares the room's history) and the caller applies it as one.
    fn restart_from(&mut self, snapshot: Vec<u8>, scope: &str, work: &mut Vec<Work>) -> bool {
        if self.doc.is_none() {
            // The common case: the file is not loaded yet, so it never will be.
            self.snapshot = Some(snapshot);
            work.push(Work::SeedWanted { scope: scope.to_owned() });
            return true;
        }
        if !self.from_file {
            return false;
        }
        if self.edited {
            // Typed against the file before the catch-up arrived. Those edits name items the
            // room does not have, so there is no honest merge.
            log::warn!("collab: edits were made against a file the room had already checkpointed past");
            self.notices.push("this window edited an outdated copy; reopen the file".to_owned());
            work.push(Work::TextIncomplete { scope: scope.to_owned() });
            return true;
        }
        match LiveDoc::from_state(&snapshot) {
            Ok(mut doc) => {
                if let Some(space) = self.space {
                    doc.set_client(space);
                }
                self.doc = Some(doc);
                self.from_file = false;
                work.push(Work::Reset);
            }
            Err(e) => {
                log::warn!("collab: the room's checkpoint did not restore: {e}");
                self.notices.push("the shared document could not be restored; reopen the file".to_owned());
                work.push(Work::TextIncomplete { scope: scope.to_owned() });
            }
        }
        true
    }

    /// Whether the document op being handled is the first one after this link's first join,
    /// which is the only place a catch-up snapshot can sit. Consumed by the first document op.
    fn first_after_join(&mut self) -> bool {
        let first = self.first_join.take();
        first.is_some_and(|joined| self.wire.applied() == joined + 1)
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
        let saved = ClientMsg::Saved { doc: self.wire.doc().to_owned(), seq: seq.saturating_sub(1) };
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
                if let Some(doc) = self.doc.as_mut() {
                    doc.set_client(space);
                }
                log::debug!("collab: the relay granted id space {space}");
            }
            Effect::Apply { command, params } if command == CRDT_TEXT => match serde_json::from_value::<TextPayload>(params) {
                Ok(payload) => {
                    let first = self.first_after_join();
                    self.merge_updates(payload.scope, &payload.ops, payload.snap && first, work);
                }
                Err(e) => {
                    log::warn!("collab: a crdt.text payload did not decode: {e}");
                    self.notices.push("could not apply a change from another editor".to_owned());
                }
            },
            Effect::Apply { command, params } => work.push(Work::Command { command, params }),
            Effect::TextOps { scope, ops, complete, snap } => {
                let first = self.first_join.take().is_some();
                if !complete {
                    log::warn!("collab: the relay could not keep every change to {scope}");
                    self.notices.push(format!("{scope} has changes this session cannot rebuild; reopen the file"));
                    work.push(Work::TextIncomplete { scope });
                    return;
                }
                match serde_json::from_value::<Vec<String>>(ops) {
                    Ok(ops) => self.merge_updates(scope, &ops, snap && first, work),
                    Err(e) => {
                        log::warn!("collab: replayed operations for {scope} did not decode: {e}");
                        self.notices.push(format!("{scope} could not be caught up; reopen the file"));
                        work.push(Work::TextIncomplete { scope });
                    }
                }
            }
            Effect::Frame { origin, payload } => work.push(Work::Frame { origin, payload }),
            Effect::Ephemeral { origin, kind, payload } => work.push(Work::Ephemeral { origin, kind, payload }),
            Effect::Confirmed { local, seq } => self.confirmed(local, seq, work),
            Effect::Replace { document, seq } => {
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
            Effect::Send { msg } => {
                if let Err(e) = self.hub.send(msg) {
                    log::warn!("collab: a resync request never left: {e}");
                }
            }
        }
    }
}

/// A refused edit, as a transport-level error with the reason kept.
fn refusal(e: LiveError) -> Error {
    Error::malformed("client", e.to_string())
}
