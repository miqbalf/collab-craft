//! Per-document state: the sequence, the locks and the roster.
//!
//! This is the arbitration the relay exists for. A message broker can fan
//! traffic out, and several are already running in this estate, but none can
//! assign a monotonic order or decide who holds a scope. That decision has to
//! live in one place with one lock around it, which is here.
//!
//! No IO. The socket layer calls these methods and emits whatever comes back,
//! so every rule below is testable without a connection.

use std::collections::{HashMap, HashSet};

use serde::Serialize;

/// Ops retained per document for replaying to a peer that fell behind.
///
/// Past this a resync is answered with a snapshot instead. The bound matters:
/// an unbounded log is a memory leak with a document-shaped trigger.
pub const HISTORY: usize = 2048;

/// Bytes of op parameters one document may retain.
///
/// [`HISTORY`] bounds the op count and says nothing about size. A client can put as much JSON
/// in one `params` as the transport will carry, so counting ops alone let one authenticated
/// editor hold an arbitrary multiple of the intended memory per room.
///
/// # Memory per room, and how it relates to the text cap
///
/// A room holds two pools: this op history, and the retained CRDT text a joiner is replayed
/// ([`TEXT_BYTES`] per scope by default, `COLLABCRAFT_TEXT_BYTES` in a deployment, and
/// [`TOTAL_TEXT_BYTES`] or that cap across scopes). A checkpoint snapshot is one op carrying a
/// whole document, so it may be as large as the text cap, which can exceed this budget: a
/// CRDT op up to the text cap is accepted and the history then evicts down to it. Snapshots
/// are counted apart from edits: a scope keeps edits up to the cap plus at most two
/// snapshots, and a room at most two caps of snapshots. So the worst case for one room is
/// about `max(HISTORY_BYTES, cap) + max(TOTAL_TEXT_BYTES, cap) + 2 * cap`: 64 MiB at the
/// default (and maximum) cap of just under 16 MiB. Ephemeral state is a third pool, bounded
/// apart from both: at most [`EPHEMERAL_PEER_BYTES`] per peer, so [`MAX_PEERS`] times that
/// (16 MiB) per room, which brings the worst case to about 80 MiB.
pub const HISTORY_BYTES: usize = 8 * 1024 * 1024;

/// Peers one document may hold at once.
///
/// The value `craft_crdt::Limits` carries. A room is a fan-out set and a roster broadcast, so
/// an unbounded one is quadratic work per join.
pub const MAX_PEERS: usize = 256;

/// Peers one user (one ticket `uid`) may hold in one document at once.
///
/// A ticket mints a fresh peer id per window, so without this one account could open
/// [`MAX_PEERS`] windows and lock everybody else out of the document.
pub const MAX_PEERS_PER_UID: usize = 16;

/// Bytes of retained CRDT operations per scope.
///
/// These are what a joining peer is replayed, so they are kept apart from the op history and
/// outlive eviction from it. Past this the scope is marked incomplete, and a joiner is told
/// rather than left believing the file plus the replay is the whole text.
pub const TEXT_BYTES: usize = 2 * 1024 * 1024;

/// Scopes one document may retain CRDT operations for.
///
/// [`TEXT_BYTES`] bounds the bytes within one scope and says nothing about how
/// many scopes there are. A client naming a fresh scope on every edit would
/// otherwise grow a room without limit, one bounded log at a time.
///
/// Small on purpose. A join sends every retained scope as its own `text_ops` messages, all
/// queued on the socket at once, and the socket holds a bounded count of them (the relay's
/// `SOCKET_BUFFER`). At 4096 scopes one editor naming a fresh scope per edit could make the
/// catch-up larger than that buffer, and every join was then cut and retried for ever. The
/// live document travels under one scope (`craft_collab::DOC_SCOPE`), so 64 leaves room for
/// other kinds of shared state and keeps the catch-up well inside the buffer.
pub const MAX_SCOPES: usize = 64;

/// The most `text_ops` messages one scope's catch-up can take.
///
/// A scope retains at most its edits up to the text cap plus two snapshots each up to the cap,
/// and every piece but the last is at least half the piece budget (an op larger than what is
/// left starts a new piece, and no op is larger than the budget), so three caps fit in at most
/// seven pieces. One more for rounding.
pub const MAX_PIECES_PER_SCOPE: usize = 8;

/// Bytes of retained CRDT operations a document may hold across every scope.
///
/// [`TEXT_BYTES`] bounds one scope, and [`MAX_SCOPES`] bounds how many scopes
/// exist, but their product is nowhere near a sane room budget: a client that
/// spreads its edits across many near-cap scopes instead of one would
/// otherwise retain gigabytes while every individual check still passes. This
/// is the guard that actually bounds total memory. Set to the same figure as
/// [`HISTORY_BYTES`] on purpose: it is the same per-room budget already
/// accepted for the op history, applied to the other unbounded-by-count pool.
pub const TOTAL_TEXT_BYTES: usize = HISTORY_BYTES;

/// Latest-wins values one peer may hold at once.
///
/// A camera and a selection are two kinds. An unbounded set would let one peer grow the room
/// by inventing kind names.
pub const MAX_EPHEMERAL_KINDS: usize = 16;

/// Bytes one ephemeral value may take. A camera, a selection or a caret is a few hundred
/// bytes; this leaves room for a selection over many ranges.
pub const EPHEMERAL_VALUE_BYTES: usize = 16 * 1024;

/// Bytes of ephemeral values one peer may hold across every kind. Allowed to read-only peers,
/// so it is bounded on its own rather than by the text caps.
pub const EPHEMERAL_PEER_BYTES: usize = 64 * 1024;

/// One ordered edit, as the relay recorded it.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Op {
    pub seq: u64,
    pub origin: String,
    /// The sender's own counter, echoed so it can confirm rather than reapply.
    pub local: Option<u64>,
    pub command: String,
    pub params: serde_json::Value,
    pub scope: Option<String>,
}

/// A peer in a document.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Peer {
    pub peer: String,
    pub display: String,
    /// Whether this peer may submit. A reader joins and watches.
    #[serde(skip)]
    pub may_edit: bool,
    /// The ticket's user, for [`MAX_PEERS_PER_UID`]. Never sent: the roster names peers.
    #[serde(skip)]
    pub uid: String,
}

/// One peer's offered edit, before the relay has ordered it.
///
/// Grouped rather than passed as loose arguments, which also stops a caller
/// transposing the two `Option`s or the two strings.
pub struct Submission {
    pub peer_id: String,
    pub may_edit: bool,
    pub command: String,
    pub params: serde_json::Value,
    pub scope: Option<String>,
    pub local: Option<u64>,
}

/// Why a submit, a lock or a join was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The peer holds read only.
    ReadOnly,
    /// Another peer holds the scope.
    Locked { holder: String },
    /// The document already holds [`MAX_PEERS`].
    Full,
    /// This user already holds [`MAX_PEERS_PER_UID`] connections to the document.
    FullForUser,
    /// The message is larger than a document may retain.
    TooLarge { bytes: usize },
    /// A save report that does not sit right before a retained checkpoint snapshot.
    NotACheckpoint { seq: u64 },
    /// A save report whose checkpoint snapshot was ordered but is larger than the text cap,
    /// so it was never retained for joiners. Permanent for that document size.
    TooLargeToRetain { bytes: usize },
    /// A checkpoint snapshot from a peer that is not the service saver.
    NotTheSaver,
    /// The join named another replica schema than the one the room is pinned to.
    Schema { pinned: String },
}

impl Refusal {
    pub fn detail(&self) -> String {
        match self {
            Refusal::ReadOnly => "read-only access to this file".to_owned(),
            Refusal::Locked { holder } => format!("{holder} is editing that region"),
            Refusal::Full => format!("this document already has {MAX_PEERS} editors"),
            Refusal::FullForUser => format!("you already have this document open in {MAX_PEERS_PER_UID} windows"),
            Refusal::TooLarge { bytes } => format!("that change is {bytes} bytes, more than this relay will carry"),
            Refusal::NotACheckpoint { seq } => {
                format!("a save through {seq} is kept only when a checkpoint snapshot follows it; nothing was dropped")
            }
            Refusal::TooLargeToRetain { bytes } => {
                format!("that checkpoint is {bytes} bytes, too large to retain for joining editors; nothing was dropped")
            }
            Refusal::NotTheSaver => "only the document service may checkpoint; editors send edits".to_owned(),
            Refusal::Schema { pinned } => {
                format!("this file is being edited live by another app or version ({pinned}); reopen it once that session has ended")
            }
        }
    }
}

/// What [`Room::leave_as`] released.
#[derive(Debug, PartialEq, Eq)]
pub struct Left {
    /// The scopes the peer held, released.
    pub dropped: Vec<String>,
    /// The peer's last connection left, so it is off the roster.
    pub gone: bool,
}

/// Retained CRDT operations for one scope.
#[derive(Default)]
pub struct TextLog {
    /// The operations, oldest first, as the clients sent them.
    ops: Vec<serde_json::Value>,
    /// The ordered sequence each op in [`TextLog::ops`] was assigned, so a save
    /// can drop the ones it has baked into the file. Same length as `ops`.
    seqs: Vec<u64>,
    /// Which ops are checkpoint snapshots (`"snap": true` on their submit). Same length as `ops`.
    snaps: Vec<bool>,
    bytes: usize,
    /// The part of [`TextLog::bytes`] that is checkpoint snapshots. Ordinary edits are
    /// measured against the cap without it, so typing while a checkpoint is on its way to
    /// being saved does not find the scope full of the snapshot itself.
    snap_bytes: usize,
    /// Whether everything since the file was opened is still here.
    complete: bool,
}

impl TextLog {
    /// The operations to replay to a joining peer.
    pub fn ops(&self) -> &[serde_json::Value] {
        &self.ops
    }

    /// Whether the text can be rebuilt from the file plus [`TextLog::ops`].
    pub fn complete(&self) -> bool {
        self.complete
    }

    /// Whether the first retained op is a checkpoint snapshot, so a joiner starts from it rather
    /// than from the file. The payload stays opaque; this is only the submit's own flag.
    pub fn starts_with_snapshot(&self) -> bool {
        self.snaps.first().copied().unwrap_or(false)
    }

    /// Whether this log would tell a joiner nothing: complete, and holding no op. A save
    /// leaves such logs behind for scopes it emptied.
    pub fn is_idle(&self) -> bool {
        self.complete && self.ops.is_empty()
    }

    /// The ops from the newest retained snapshot ordered after `after` on, when there is one.
    /// A joiner can start from that snapshot whatever came before it.
    pub fn ops_from_snapshot_after(&self, after: u64) -> Option<&[serde_json::Value]> {
        let at = self.seqs.iter().zip(&self.snaps).rposition(|(seq, snap)| *snap && *seq > after)?;
        Some(&self.ops[at..])
    }

    /// Whether the op ordered at `seq` is retained here and is a checkpoint snapshot.
    fn snapshot_at(&self, seq: u64) -> bool {
        self.seqs.iter().zip(&self.snaps).any(|(s, snap)| *s == seq && *snap)
    }

    /// Drop the operations a save has baked into the file.
    ///
    /// Everything ordered at or before `seq` is now in the saved file, so
    /// replaying it to a joiner who loads that file would apply it a second
    /// time. Only the operations after the save are kept.
    ///
    /// A log that now starts at a retained snapshot is complete again, whatever was lost
    /// before: a joiner starts from that snapshot, which a save only follows when nothing was
    /// ordered between it being taken and being ordered.
    fn drop_through(&mut self, seq: u64) {
        self.keep(|op_seq, _, _| op_seq > seq);
        if self.starts_with_snapshot() {
            self.complete = true;
        }
    }

    /// Drop the snapshots that are not the first op. Every op they cover is still retained
    /// (the first op, then the edits after it), so only a save could make them useful, and a
    /// newer snapshot replaces them for that. Returns the bytes freed.
    fn drop_later_snapshots(&mut self) -> usize {
        let before = self.bytes;
        self.keep(|_, snap, index| !snap || index == 0);
        before - self.bytes
    }

    /// The bytes [`TextLog::drop_later_snapshots`] would free.
    fn later_snapshot_bytes(&self) -> usize {
        self.ops.iter().zip(&self.snaps).skip(1).filter(|(_, snap)| **snap).map(|(op, _)| value_bytes(op)).sum()
    }

    /// Keep the ops `keep(seq, is_snapshot, index)` says to, recounting the bytes.
    fn keep(&mut self, keep: impl Fn(u64, bool, usize) -> bool) {
        let (mut ops, mut seqs, mut snaps) = (Vec::new(), Vec::new(), Vec::new());
        let (mut bytes, mut snap_bytes) = (0, 0);
        for (index, ((op, op_seq), snap)) in self.ops.drain(..).zip(self.seqs.drain(..)).zip(self.snaps.drain(..)).enumerate() {
            if keep(op_seq, snap, index) {
                let size = value_bytes(&op);
                bytes += size;
                if snap {
                    snap_bytes += size;
                }
                ops.push(op);
                seqs.push(op_seq);
                snaps.push(snap);
            }
        }
        (self.ops, self.seqs, self.snaps, self.bytes, self.snap_bytes) = (ops, seqs, snaps, bytes, snap_bytes);
    }

    fn clear(&mut self) {
        self.ops.clear();
        self.seqs.clear();
        self.snaps.clear();
        self.bytes = 0;
        self.snap_bytes = 0;
    }
}

/// Replica schemas whose `crdt.*` ops carry yrs updates (base64 in `params.ops`), which
/// [`crdt_ops_sound`] checks before the relay orders them. A room with another schema, or none,
/// is not looked into.
const YRS_SCHEMAS: [&str; 2] = ["wordcraft/", "gridcraft/"];

/// Whether a submit may be ordered: for a room pinned to a yrs schema, every op in a `crdt.*`
/// payload must be base64 of a yrs update that decodes with sane ids
/// ([`craft_collab::update_is_sound`], under `catch_unwind`). A crafted op would otherwise be
/// ordered, replayed to every joiner and abort each browser replica that decodes it (a yrs panic
/// aborts on wasm32). Anything else is let through unread: other schemas, other commands, and a
/// payload without an `ops` list.
#[must_use]
pub fn crdt_ops_sound(schema: Option<&str>, command: &str, params: &serde_json::Value) -> bool {
    if !is_crdt_command(command) || !schema.is_some_and(|s| YRS_SCHEMAS.iter().any(|p| s.starts_with(p))) {
        return true;
    }
    let Some(ops) = params.get("ops").and_then(serde_json::Value::as_array) else {
        return true;
    };
    ops.iter().all(|op| op.as_str().and_then(|t| craft_collab::update_from_wire(t).ok()).is_some_and(|bytes| craft_collab::update_is_sound(&bytes)))
}

/// Commands whose payload is a CRDT operation. They never take a region lock.
#[must_use]
pub fn is_crdt_command(command: &str) -> bool {
    command.starts_with("crdt.")
}

/// One document.
///
/// The sequence is NOT assigned here. It comes from an `INCR` against the
/// shared Valkey, so it survives a relay restart and stays unique across
/// instances. A counter living in this struct would reset to 1 on restart, and
/// every connected client would then read the next op as a replay and silently
/// drop it.
#[derive(Default)]
pub struct Room {
    /// Highest sequence this relay has recorded.
    seq: u64,
    /// Recent ops, oldest first, capped at [`HISTORY`].
    history: Vec<Op>,
    /// Sequence of `history[0]`, so a resync knows how far back it can reach.
    first_in_history: u64,
    /// The sequence the saved file reflects, learned from a client's save.
    ///
    /// A joiner loads the file at this version, so it is the baseline the relay
    /// catches a joiner up from: the file plus every op after this is the
    /// current document. Zero until the first save, which is correct, because
    /// before any save the file a joiner loads is the original.
    saved_seq: u64,
    peers: Vec<Peer>,
    /// peer id -> how many live connections hold it.
    ///
    /// A client that reconnects reuses its token, so the same peer id opens a
    /// second socket before the first is reaped. Keyed on the peer id rather
    /// than the socket, a peer stays in the room while any connection holds it,
    /// and the old socket's disconnect cannot evict the live one. Each connection is named
    /// (the relay uses the socket id), so a leave from a connection the room does not hold
    /// for that peer, a stale one or a repeated one, changes nothing.
    conns: HashMap<String, HashSet<String>>,
    /// Names [`Room::join`] makes up for callers that do not name the connection.
    #[cfg(test)]
    unnamed: u64,
    /// scope -> holder.
    locks: HashMap<String, String>,
    /// Bytes of `params` currently held in [`Room::history`].
    history_bytes: usize,
    /// scope -> the CRDT operations a joining peer needs.
    texts: HashMap<String, TextLog>,
    /// Bytes of `ops` currently held across every [`Room::texts`] entry, kept so
    /// [`Room::retain_text`] can enforce [`TOTAL_TEXT_BYTES`] without summing every
    /// scope on each submit.
    text_bytes: usize,
    /// peer -> kind -> the newest value that peer sent, with its size in bytes.
    ephemeral: HashMap<String, HashMap<String, (serde_json::Value, usize)>>,
    /// The newest `saved` marker from anyone, `(peer, value)`. Kept apart from
    /// [`Room::ephemeral`] because it says which file version is the join baseline, and that
    /// stays true after the peer that saved has left.
    saved: Option<(String, serde_json::Value)>,
    /// Per-scope retained-text cap. `None` means [`TEXT_BYTES`].
    text_cap: Option<usize>,
    /// The newest checkpoint snapshot that was ordered but not retained because it is larger
    /// than the cap, `(seq, bytes)`, so the save report for it is refused as permanent.
    oversized_snapshot: Option<(u64, usize)>,
    /// The part of [`Room::text_bytes`] that is checkpoint snapshots, across every scope.
    snap_bytes: usize,
    /// The sequence the document was already at when this room was made, learned from the bus
    /// on the first [`Room::observe_seq`]. `None` until then.
    ///
    /// Above zero means the history before it lived in a room this process no longer has (a
    /// relay restart, or a room dropped after its grace period): the ops, the snapshot and the
    /// saved marker are gone, and the saved file is not a starting point for this room.
    inherited: Option<u64>,
    /// Bumped each time the room empties, so a timer that would drop it after the grace period
    /// can tell whether it emptied again since (see [`Room::idle_mark`]).
    idle: u64,
    /// The replica schema the first join that named one named (`"wordcraft/1"`), kept while the
    /// room lives. See [`Room::check_schema`].
    schema: Option<String>,
}

/// The ephemeral kind that records a save: `{seq, etag}`.
pub const SAVED_KIND: &str = "saved";

/// Largest `saved` payload the relay keeps.
pub const SAVED_MAX_BYTES: usize = 1024;

impl Room {
    /// A room whose retained CRDT text may reach `cap` bytes per scope.
    ///
    /// The room-wide budget is [`TOTAL_TEXT_BYTES`], or `cap` if that is larger, so a cap above
    /// the default is not defeated by the total.
    pub fn with_text_cap(cap: usize) -> Room {
        Room { text_cap: Some(cap), ..Room::default() }
    }

    fn scope_cap(&self) -> usize {
        self.text_cap.unwrap_or(TEXT_BYTES)
    }

    fn total_cap(&self) -> usize {
        TOTAL_TEXT_BYTES.max(self.scope_cap())
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Raise the recorded sequence to a high-water mark learned from the bus.
    ///
    /// An in-memory room starts at sequence zero. When a relay restarts, or the
    /// last peer left and the room was dropped, the Valkey counter still holds
    /// where the document actually was. Seeding from it on join stops a reopened
    /// document from handing out sequence numbers the clients have already seen,
    /// which is what made a reopen replay or stall edits.
    pub fn observe_seq(&mut self, seq: u64) {
        if self.inherited.is_none() {
            // A submit can reach a new room before the join that reads the bus does; what it
            // recorded is this room's own, so the inherited history ends just before it.
            let before = self.history.first().map_or(seq, |op| op.seq.saturating_sub(1));
            self.inherited = Some(seq.min(before));
        }
        self.seq = self.seq.max(seq);
    }

    /// The sequence this room's own history starts after. See [`Room::inherited`] field.
    pub fn inherited(&self) -> u64 {
        self.inherited.unwrap_or(0)
    }

    /// Whether this room began past history it does not hold and nothing since has replaced it.
    ///
    /// True when the room inherited a sequence above zero, no save has been accepted since,
    /// and no snapshot has been ordered since: a joiner then has nothing to start from that
    /// agrees with the replicas still running, and the file is behind them with other item
    /// identities. A checkpoint by a replica that held the document before clears it.
    pub fn history_lost(&self) -> bool {
        let inherited = self.inherited();
        inherited > 0 && self.saved_seq < inherited && !self.texts.values().any(|log| log.ops_from_snapshot_after(inherited).is_some())
    }

    /// The catch-up for a joiner whose baseline predates this room's own history: per scope,
    /// the ops from the newest snapshot ordered in this room on. Empty when there is none.
    pub fn texts_from_snapshot(&self) -> Vec<(String, Vec<serde_json::Value>)> {
        let inherited = self.inherited();
        let mut out: Vec<(String, Vec<serde_json::Value>)> =
            self.texts.iter().filter_map(|(scope, log)| log.ops_from_snapshot_after(inherited).map(|ops| (scope.clone(), ops.to_vec()))).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Note that the room just emptied. Returns the mark a grace timer checks with
    /// [`Room::still_idle`] before it drops the room.
    pub fn idle_mark(&mut self) -> u64 {
        self.idle += 1;
        self.idle
    }

    /// Whether the room is empty and has not emptied again since `mark` was taken.
    pub fn still_idle(&self, mark: u64) -> bool {
        self.is_empty() && self.idle == mark
    }

    pub fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Who holds a scope, as this relay last saw it.
    ///
    /// The authority is the bus, since another instance can grant a lock this
    /// relay never saw. This mirror exists only so [`Room::submit`] can refuse
    /// an edit without a round trip per keystroke.
    pub fn holder_of(&self, scope: &str) -> Option<&str> {
        self.locks.get(scope).map(String::as_str)
    }

    /// Add a peer, or a second connection for one already here.
    ///
    /// A reconnect opens a new socket under the same peer id before the old one
    /// is reaped. That counts as a second connection rather than a replacement,
    /// so the peer survives the old socket's later disconnect. The roster entry
    /// is refreshed to the newest connection's display and edit right.
    #[cfg(test)]
    pub fn join(&mut self, peer: Peer) -> Result<(), Refusal> {
        self.unnamed += 1;
        let conn = format!("#{}", self.unnamed);
        self.join_as(peer, &conn)
    }

    /// Whether a join naming replica schema `schema` may enter: one naming none always may (a
    /// client from before the field) and pins nothing; one naming the schema the room is pinned
    /// to may; one naming another is refused, so two apps' (or two versions') updates never mix
    /// in one room. Pinning is [`Room::pin_schema`], once the join went through.
    pub fn check_schema(&self, schema: Option<&str>) -> Result<(), Refusal> {
        match (schema, &self.schema) {
            (Some(asked), Some(pinned)) if asked != pinned => Err(Refusal::Schema { pinned: pinned.clone() }),
            _ => Ok(()),
        }
    }

    /// Pin the room to `schema` if it is not pinned yet. A join naming none pins nothing.
    pub fn pin_schema(&mut self, schema: Option<&str>) {
        if let (Some(schema), None) = (schema, &self.schema) {
            self.schema = Some(schema.to_owned());
        }
    }

    /// The replica schema the room is pinned to, if any.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// [`Room::join`] by the connection named `conn`, which [`Room::leave_as`] names again.
    pub fn join_as(&mut self, peer: Peer, conn: &str) -> Result<(), Refusal> {
        let already = self.peers.iter().any(|p| p.peer == peer.peer);
        if !already && self.peers.len() >= MAX_PEERS {
            return Err(Refusal::Full);
        }
        if !already && self.peers.iter().filter(|p| p.uid == peer.uid).count() >= MAX_PEERS_PER_UID {
            return Err(Refusal::FullForUser);
        }
        self.conns.entry(peer.peer.clone()).or_default().insert(conn.to_owned());
        self.peers.retain(|p| p.peer != peer.peer);
        self.peers.push(peer);
        Ok(())
    }

    /// The CRDT operations a joining peer needs, per scope.
    pub fn texts(&self) -> &HashMap<String, TextLog> {
        &self.texts
    }

    /// Every peer's newest latest-wins values, for a joining peer.
    ///
    /// Flattened to `(peer, kind, value)` so a caller emits one message each without walking
    /// two maps.
    pub fn ephemeral(&self) -> Vec<(String, String, serde_json::Value)> {
        let mut out: Vec<(String, String, serde_json::Value)> = self
            .ephemeral
            .iter()
            .flat_map(|(peer, kinds)| kinds.iter().map(move |(kind, (value, _))| (peer.clone(), kind.clone(), value.clone())))
            .collect();
        if let Some((peer, value)) = &self.saved {
            out.push((peer.clone(), SAVED_KIND.to_owned(), value.clone()));
        }
        // Sorted so a join payload does not depend on hash order, which makes it testable.
        out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        out
    }

    /// Record a `saved` marker. Returns whether it was accepted.
    ///
    /// The payload must be `{seq: u64, etag: string}` and at most [`SAVED_MAX_BYTES`]. A `seq`
    /// lower than the stored one is ignored, so a late or replayed marker cannot move the
    /// baseline back.
    pub fn remember_saved(&mut self, peer_id: &str, value: serde_json::Value) -> bool {
        let Some(seq) = value.get("seq").and_then(serde_json::Value::as_u64) else { return false };
        if !value.get("etag").is_some_and(serde_json::Value::is_string) || value_bytes(&value) > SAVED_MAX_BYTES {
            return false;
        }
        let stored = self.saved.as_ref().and_then(|(_, v)| v.get("seq")).and_then(serde_json::Value::as_u64);
        if stored.is_some_and(|stored| seq < stored) {
            return false;
        }
        self.saved = Some((peer_id.to_owned(), value));
        true
    }

    /// Record one peer's newest value for one kind.
    ///
    /// Latest wins, so the previous value is simply replaced. A peer past
    /// [`MAX_EPHEMERAL_KINDS`] is refused, because kind names are the peer's own and an
    /// unbounded set is a way to grow the room. So is a value past [`EPHEMERAL_VALUE_BYTES`],
    /// or one that would take the peer past [`EPHEMERAL_PEER_BYTES`] across its kinds: this is
    /// open to read-only peers, so it is bounded on its own.
    pub fn remember_ephemeral(&mut self, peer_id: &str, kind: &str, value: serde_json::Value) -> Result<(), Refusal> {
        let bytes = value_bytes(&value);
        if bytes > EPHEMERAL_VALUE_BYTES {
            return Err(Refusal::TooLarge { bytes });
        }
        if kind == SAVED_KIND {
            self.remember_saved(peer_id, value);
            return Ok(());
        }
        let kinds = self.ephemeral.entry(peer_id.to_owned()).or_default();
        if !kinds.contains_key(kind) && kinds.len() >= MAX_EPHEMERAL_KINDS {
            return Err(Refusal::TooLarge { bytes });
        }
        let others: usize = kinds.iter().filter(|(k, _)| k.as_str() != kind).map(|(_, (_, size))| size).sum();
        if others + bytes > EPHEMERAL_PEER_BYTES {
            return Err(Refusal::TooLarge { bytes });
        }
        kinds.insert(kind.to_owned(), (value, bytes));
        Ok(())
    }

    /// Drop one of a peer's connections, and everything it held once the last
    /// one goes.
    ///
    /// Releasing on disconnect is the whole reason a lock is safe to take. A
    /// lock that outlived its holder would wedge the region until a restart. But
    /// a peer with a live reconnect still holds its scope, so the roster entry,
    /// its ephemeral state and its locks are released only when the last
    /// connection leaves. An old socket's disconnect decrements the count and
    /// returns nothing.
    #[cfg(test)]
    pub fn leave(&mut self, peer_id: &str) -> Vec<String> {
        let conn = self.conns.get(peer_id).and_then(|conns| conns.iter().next().cloned());
        match conn {
            Some(conn) => self.leave_as(peer_id, &conn).map(|left| left.dropped).unwrap_or_default(),
            None => self.release(peer_id),
        }
    }

    /// Drop the connection `conn` of `peer_id`.
    ///
    /// `None` when the room holds no such connection for that peer: a socket that was
    /// replaced and only now timed out, or one that already left. Nothing changes then, and
    /// nobody should be told the peer left. Otherwise what was released, and whether the
    /// peer is gone from the room (its last connection left).
    pub fn leave_as(&mut self, peer_id: &str, conn: &str) -> Option<Left> {
        let conns = self.conns.get_mut(peer_id)?;
        if !conns.remove(conn) {
            return None;
        }
        if !conns.is_empty() {
            return Some(Left { dropped: Vec::new(), gone: false });
        }
        self.conns.remove(peer_id);
        Some(Left { dropped: self.release(peer_id), gone: true })
    }

    /// Whether `conn` is still one of this peer's connections in the room: false once it left
    /// or was replaced. A lock is recorded only for a connection that passes this, so a peer
    /// that is gone is never left as a holder.
    pub fn has_conn(&self, peer_id: &str, conn: &str) -> bool {
        self.conns.get(peer_id).is_some_and(|conns| conns.contains(conn))
    }

    /// Remove a peer and everything it held. Returns the scopes it held.
    fn release(&mut self, peer_id: &str) -> Vec<String> {
        self.peers.retain(|p| p.peer != peer_id);
        // Their camera and selection go with them. A marker left behind would show somebody
        // looking at a part they are no longer in.
        self.ephemeral.remove(peer_id);
        let dropped: Vec<String> = self.locks.iter().filter(|(_, holder)| holder.as_str() == peer_id).map(|(scope, _)| scope.clone()).collect();
        for scope in &dropped {
            self.locks.remove(scope);
        }
        dropped
    }

    /// Order an edit, or refuse it.
    ///
    /// A scope held by another peer refuses. A scope held by this peer, or held
    /// by nobody, proceeds. The optimistic case is deliberate: requiring a lock
    /// before every edit would make typing wait for a round trip.
    ///
    /// Commands named `crdt.*` never take a lock. Their payload merges, so a
    /// region lock would only serialize what the CRDT is meant to combine. A
    /// scope on those commands is stripped so a buggy client cannot lock a
    /// paragraph by accident.
    pub fn submit(&mut self, seq: u64, s: Submission) -> Result<Op, Refusal> {
        if !s.may_edit {
            return Err(Refusal::ReadOnly);
        }
        let bytes = value_bytes(&s.params);
        // A CRDT op may be a checkpoint snapshot, which the rooms retain up to the text cap.
        let limit = if is_crdt_command(&s.command) { HISTORY_BYTES.max(self.scope_cap()) } else { HISTORY_BYTES };
        if bytes > limit {
            return Err(Refusal::TooLarge { bytes });
        }
        let scope = if is_crdt_command(&s.command) { None } else { s.scope };
        if let Some(scope) = &scope
            && let Some(holder) = self.locks.get(scope)
            && holder != &s.peer_id
        {
            return Err(Refusal::Locked { holder: holder.clone() });
        }

        self.seq = self.seq.max(seq);
        let op = Op { seq, origin: s.peer_id, local: s.local, command: s.command, params: s.params, scope };

        self.retain_text(&op);

        self.history.push(op.clone());
        self.history_bytes += bytes;
        // Both bounds, because either alone leaves the other unbounded: many tiny ops fill the
        // count, and one large op fills the budget.
        while self.history.len() > HISTORY || (self.history_bytes > HISTORY_BYTES && self.history.len() > 1) {
            let gone = self.history.remove(0);
            self.history_bytes = self.history_bytes.saturating_sub(value_bytes(&gone.params));
        }
        // Read from the log itself. Incrementing a mirror assumed the local history was
        // contiguous, and two submits racing for the room lock push out of sequence order.
        self.first_in_history = self.history.first().map_or(0, |op| op.seq);

        Ok(op)
    }

    /// Keep a CRDT operation for a peer that has not joined yet.
    ///
    /// The op history exists to fill a gap in the sequence and is evicted by age. These are
    /// what a joining peer is replayed, so they are kept per scope and survive that eviction.
    /// A scope past [`TEXT_BYTES`], or a document past [`TOTAL_TEXT_BYTES`] across every scope,
    /// is marked incomplete, which is what lets a joiner be told the text cannot be rebuilt
    /// instead of being handed a partial replay.
    fn retain_text(&mut self, op: &Op) {
        if !is_crdt_command(&op.command) {
            return;
        }
        let Some(scope) = op.params.get("scope").and_then(serde_json::Value::as_str) else {
            return;
        };
        // Flattened, one entry per operation. A payload carries a batch, and keeping the
        // batches nested would hand a joining peer an array of arrays that cannot decode as
        // the operation list it expects.
        let Some(ops) = op.params.get("ops").and_then(serde_json::Value::as_array) else {
            return;
        };
        let bytes: usize = ops.iter().map(value_bytes).sum();
        // A fresh scope past the cap is dropped rather than retained. The edit
        // still orders and broadcasts through the op history; what is refused is
        // only the per-scope replay buffer, so a room cannot be grown without
        // limit by inventing scope names.
        if !self.texts.contains_key(scope) && self.texts.len() >= MAX_SCOPES {
            return;
        }
        let (cap, total) = (self.scope_cap(), self.total_cap());
        // A snapshot submit carries one full state; its flag marks that op, not the batch.
        let snap = op.params.get("snap").and_then(serde_json::Value::as_bool).unwrap_or(false);
        let log = self.texts.entry(scope.to_owned()).or_insert_with(|| TextLog { complete: true, ..TextLog::default() });
        if snap {
            // A scope keeps at most two snapshots: the one it starts from after a save, and the
            // newest, which the next save would start from. The room keeps at most two caps of
            // them, so snapshots in many scopes cannot add up either.
            // The older unsaved ones are dropped only once this one is known to be retained, so
            // a refused snapshot never costs the scope the one a pending save needs.
            if bytes > cap || self.snap_bytes - log.later_snapshot_bytes() + bytes > cap.saturating_mul(2) {
                // Not retained, and nothing else is lost: the snapshot holds only what the ops
                // before it already carry. The save report for it is refused as permanent.
                self.oversized_snapshot = Some((op.seq, bytes));
                return;
            }
            let freed = log.drop_later_snapshots();
            self.text_bytes -= freed;
            self.snap_bytes -= freed;
            log.snap_bytes += bytes;
            self.snap_bytes += bytes;
        } else if (log.bytes - log.snap_bytes) + bytes > cap || (self.text_bytes - self.snap_bytes) + bytes > total {
            // Both bounds on edits, because either alone leaves the other kind of growth
            // unbounded: one scope filled past its own cap, or many scopes each under it but
            // summing past what the room as a whole may hold. Snapshots are not counted here;
            // they have their own bound above.
            //
            // Dropping the oldest would leave a replay that cannot be applied, because later
            // operations anchor on characters the dropped ones created. Saying so is the only
            // honest answer, until a save starts the scope at a snapshot again.
            log.complete = false;
            self.text_bytes -= log.bytes;
            self.snap_bytes -= log.snap_bytes;
            log.clear();
            return;
        }
        log.ops.extend(ops.iter().cloned());
        // Every op in one submit shares that submit's sequence, which is what a
        // save checkpoint later compares against.
        log.seqs.extend(std::iter::repeat_n(op.seq, ops.len()));
        log.snaps.extend((0..ops.len()).map(|i| snap && i == 0));
        log.bytes += bytes;
        self.text_bytes += bytes;
    }

    /// Note that the document has been saved up to `seq`.
    ///
    /// Accepted only when the retained op ordered at `seq + 1` is a checkpoint snapshot. A
    /// joiner then starts from that snapshot, so dropping everything at or below `seq` loses
    /// nothing. Any other report is refused and the log is kept whole: a joiner seeding a file
    /// the room has edited would hold different items from the live replicas and diverge.
    ///
    /// Because the snapshot has to be retained, `seq` is always below [`Room::seq`], and a
    /// report at or below an earlier save finds its snapshot already dropped and is refused,
    /// so `saved_seq` only moves forward.
    pub fn checkpoint(&mut self, seq: u64) -> Result<(), Refusal> {
        let snapshot_next = seq.checked_add(1).is_some_and(|next| self.texts.values().any(|log| log.snapshot_at(next)));
        if !snapshot_next {
            return Err(match self.oversized_snapshot {
                Some((at, bytes)) if seq.checked_add(1) == Some(at) => Refusal::TooLargeToRetain { bytes },
                _ => Refusal::NotACheckpoint { seq },
            });
        }
        self.saved_seq = self.saved_seq.max(seq);
        for log in self.texts.values_mut() {
            log.drop_through(seq);
        }
        // Recomputed rather than tracked through the drop, because `drop_through` already
        // walks every surviving op to rebuild `bytes`; summing here is the same cost repeated
        // once per save, not once per op, and cannot drift from what the scopes actually hold.
        self.text_bytes = self.texts.values().map(|log| log.bytes).sum();
        self.snap_bytes = self.texts.values().map(|log| log.snap_bytes).sum();
        Ok(())
    }

    /// The sequence the saved file reflects. See [`Room::saved_seq`] field.
    ///
    /// The relay joins a peer at this baseline and replays every op after it, so
    /// the peer's file load plus the replay is the current document.
    pub fn saved_seq(&self) -> u64 {
        self.saved_seq
    }

    /// Record a grant the bus already made.
    ///
    /// Arbitration happens in Valkey with `SET NX`, because two relay instances
    /// deciding locally would both say yes to the same scope. This table is the
    /// local mirror, kept so a join payload can list current holders without a
    /// round trip per scope.
    pub fn record_lock(&mut self, peer_id: &str, scope: String) {
        self.locks.insert(scope, peer_id.to_owned());
    }

    /// Note that somebody else holds a scope, learned from a refusal.
    pub fn record_holder(&mut self, scope: String, holder: String) {
        self.locks.insert(scope, holder);
    }

    /// Drop a mirrored holder with no peer to match against.
    ///
    /// For when the bus has already confirmed the scope is actually free: a holder's lease
    /// lapsed in Valkey on its own (a crash with no clean unlock), and nothing told this
    /// mirror. Unlike [`Room::unlock`], there is no releasing peer here, because the one who
    /// held it is already gone; the caller already checked the bus itself before reaching for
    /// this.
    pub fn forget_lock(&mut self, scope: &str) {
        self.locks.remove(scope);
    }

    /// Release a scope, but only if this peer holds it.
    ///
    /// Returns whether anything changed, so the caller only broadcasts a real
    /// release. Silently ignoring somebody else's lock is the safe reading of
    /// an unlock for a scope you do not hold.
    pub fn unlock(&mut self, peer_id: &str, scope: &str) -> bool {
        match self.locks.get(scope) {
            Some(holder) if holder == peer_id => {
                self.locks.remove(scope);
                true
            }
            _ => false,
        }
    }

    /// Ops after `have`, or `None` when the log no longer reaches that far.
    ///
    /// `None` is the caller's cue to send a snapshot. Replaying from a gap we
    /// cannot fill would leave the peer permanently out of order, which is
    /// worse than making it start again.
    pub fn since(&self, have: u64) -> Option<Vec<Op>> {
        if have >= self.seq {
            return Some(Vec::new());
        }
        if self.first_in_history == 0 || have + 1 < self.first_in_history {
            return None;
        }
        let ops: Vec<Op> = self.history.iter().filter(|op| op.seq > have).cloned().collect();
        // The run has to be contiguous from `have + 1`. A hole means a sequence
        // number was burned: allocated from the bus, then the submit refused
        // when a lock landed in the race window. That number never arrives, so a
        // peer replaying across the hole waits for it forever. Report the gap as
        // unreplayable, which is the caller's cue to send a snapshot instead.
        for (expected, op) in (have + 1..).zip(ops.iter()) {
            if op.seq != expected {
                return None;
            }
        }
        Some(ops)
    }
}

/// How many bytes a JSON value costs to hold.
///
/// Measured by encoding it. A structural estimate would be cheaper and would also be the
/// number an attacker optimises against, and a submit already pays one serialisation to reach
/// the socket.
fn value_bytes(value: &serde_json::Value) -> usize {
    serde_json::to_vec(value).map_or(0, |v| v.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str) -> Peer {
        Peer { peer: id.to_owned(), display: id.to_owned(), may_edit: true, uid: id.to_owned() }
    }

    /// A replaced socket that closes late (a half-open link that only now timed out) leaves
    /// nothing: the peer stays on the roster with its lock, and a leave the room does not hold
    /// is a no-op however often it comes.
    #[test]
    fn a_replaced_connection_leaving_changes_nothing() {
        let mut r = Room::default();
        r.join_as(peer("alice"), "old").expect("joins");
        r.record_holder("B1".to_owned(), "alice".to_owned());
        r.join_as(peer("alice"), "new").expect("rejoins");
        assert_eq!(r.leave_as("alice", "old"), Some(Left { dropped: Vec::new(), gone: false }));
        assert_eq!(r.leave_as("alice", "old"), None, "a second leave of the same socket");
        assert_eq!(r.leave_as("alice", "never"), None, "a socket the room never held");
        assert_eq!(r.leave_as("bob", "x"), None, "a peer the room never held");
        assert!(r.peers().iter().any(|p| p.peer == "alice") && !r.is_empty());
        assert_eq!(r.holder_of("B1"), Some("alice"));
        assert_eq!(r.leave_as("alice", "new"), Some(Left { dropped: vec!["B1".to_owned()], gone: true }));
        assert!(r.is_empty());
    }

    #[test]
    fn an_old_socket_disconnect_keeps_a_live_reconnect() {
        // A client reconnects under the same peer id before its first socket is
        // reaped. The old socket's later disconnect must not evict the live peer
        // or hand its lock to somebody else.
        let mut r = Room::default();
        r.join(peer("alice")).unwrap(); // socket A
        r.join(peer("bob")).unwrap();
        r.join(peer("alice")).unwrap(); // socket B, same id, A not yet reaped
        r.record_lock("alice", "B2".to_owned()); // held by the live connection

        let dropped = r.leave("alice"); // socket A disconnects
        let roster: Vec<&str> = r.peers().iter().map(|p| p.peer.as_str()).collect();
        assert!(roster.contains(&"alice"), "the live reconnect must stay: roster={roster:?}");
        assert_eq!(r.holder_of("B2"), Some("alice"), "its lock must not be handed away");
        assert!(dropped.is_empty(), "an old socket's close releases nothing");

        // The live socket's own disconnect does release everything.
        let dropped = r.leave("alice");
        assert_eq!(dropped, vec!["B2".to_owned()]);
        assert!(!r.peers().iter().any(|p| p.peer == "alice"), "the last connection leaving removes the peer");
    }

    fn wire(bytes: &[u8]) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// A valid yrs update, the way a client makes one.
    fn good_update() -> String {
        let mut d = craft_collab::LiveDoc::seeded(&[craft_collab::SeedBlock {
            id: 1,
            text: "hi".to_owned(),
            attrs: serde_json::Map::new(),
            runs: Vec::new(),
        }])
        .unwrap();
        d.set_client(3).unwrap();
        wire(&d.insert_text(1, 0, "x", &serde_json::Map::new()).unwrap())
    }

    /// The gate lets valid updates through, refuses junk for a yrs schema, and leaves other
    /// schemas, other commands and other payload shapes alone.
    #[test]
    fn the_crdt_gate_refuses_junk_only_for_yrs_schemas() {
        let ok = |schema, command, ops: serde_json::Value| crdt_ops_sound(schema, command, &serde_json::json!({ "scope": "doc", "ops": ops }));
        let good = good_update();
        for schema in ["wordcraft/1", "gridcraft/1"] {
            assert!(ok(Some(schema), "crdt.text", serde_json::json!([good])), "{schema}: valid");
            assert!(ok(Some(schema), "crdt.text", serde_json::json!([])), "{schema}: empty batch");
            assert!(!ok(Some(schema), "crdt.text", serde_json::json!(["!!not base64!!"])));
            assert!(!ok(Some(schema), "crdt.text", serde_json::json!([wire(&[0xff, 0x01, 0x02])])));
            assert!(!ok(Some(schema), "crdt.text", serde_json::json!([good, 7])), "a non-string op");
            assert!(
                !ok(
                    Some(schema),
                    "crdt.text",
                    serde_json::json!([good, wire(&[1, 1, 1, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x04, 1, 1, b't', 2, b'a', b'b', 0])])
                ),
                "block clock + len overflows"
            );
            assert!(
                !ok(Some(schema), "crdt.text", serde_json::json!([wire(&[0, 1, 1, 1, 0xff, 0xff, 0xff, 0xff, 0x0f, 2])])),
                "delete range overflows"
            );
        }
        assert!(ok(Some("deckcraft/1"), "crdt.text", serde_json::json!(["!!"])), "another schema is not read");
        assert!(ok(None, "crdt.text", serde_json::json!(["!!"])), "a room with no schema is not read");
        assert!(ok(Some("gridcraft/1"), "cell.set", serde_json::json!(["!!"])), "not a crdt command");
        assert!(crdt_ops_sound(Some("gridcraft/1"), "crdt.text", &serde_json::json!({ "scope": "doc" })), "no ops list");
    }

    fn crdt_sub(peer: &str, scope: &str) -> Submission {
        Submission {
            peer_id: peer.to_owned(),
            may_edit: true,
            command: "crdt.text".to_owned(),
            params: serde_json::json!({ "scope": scope, "ops": [{ "op": "x" }] }),
            scope: None,
            local: None,
        }
    }

    fn snap_sub(peer: &str, scope: &str) -> Submission {
        Submission { params: serde_json::json!({ "scope": scope, "snap": true, "ops": ["c25hcA=="] }), ..crdt_sub(peer, scope) }
    }

    #[test]
    fn a_save_checkpoint_drops_the_operations_its_snapshot_covers() {
        // Two text edits, then a snapshot at 3, then an edit. A save through 2 sits right
        // before the snapshot, so the edits it covers are dropped and the snapshot leads.
        let mut r = Room::default();
        r.join(peer("alice")).unwrap();
        r.submit(1, crdt_sub("alice", "/body/0")).unwrap();
        r.submit(2, crdt_sub("alice", "/body/0")).unwrap();
        r.submit(3, snap_sub("alice", "/body/0")).unwrap();
        r.submit(4, crdt_sub("alice", "/body/0")).unwrap();
        assert_eq!(r.texts().get("/body/0").unwrap().ops().len(), 4);

        r.checkpoint(2).unwrap();
        let log = r.texts().get("/body/0").unwrap();
        assert_eq!(log.ops().len(), 2, "the snapshot and the edit after it survive");
        assert!(log.starts_with_snapshot());
        assert_eq!(r.saved_seq(), 2);
    }

    /// A save that is not right before a snapshot is refused and drops nothing, because a
    /// joiner can start from a snapshot but not from a file the room has edited.
    #[test]
    fn a_save_not_followed_by_a_snapshot_is_refused() {
        let mut r = Room::default();
        r.join(peer("alice")).unwrap();
        r.submit(1, crdt_sub("alice", "/body/0")).unwrap();
        r.submit(2, snap_sub("alice", "/body/0")).unwrap();
        r.submit(3, crdt_sub("alice", "/body/0")).unwrap();
        for seq in [0, 2, 3, u64::MAX] {
            assert_eq!(r.checkpoint(seq), Err(Refusal::NotACheckpoint { seq }), "a save through {seq}");
        }
        assert_eq!(r.texts().get("/body/0").unwrap().ops().len(), 3, "the log is kept whole");
        assert_eq!(r.saved_seq(), 0);

        // The accepted save, then an older report: its snapshot is gone, so it is refused and
        // the baseline does not move back.
        r.checkpoint(1).unwrap();
        assert_eq!(r.checkpoint(0), Err(Refusal::NotACheckpoint { seq: 0 }));
        assert_eq!(r.saved_seq(), 1);
    }

    fn plain_sub(peer: &str) -> Submission {
        Submission {
            peer_id: peer.to_owned(),
            may_edit: true,
            command: "cell.set".to_owned(),
            params: serde_json::json!({}),
            scope: None,
            local: None,
        }
    }

    #[test]
    fn since_reports_a_burned_sequence_hole() {
        // Sequence 3 was burned: allocated, then its submit refused. The history
        // holds 1, 2, 4. A replay from 2 cannot be a clean run, so `since`
        // reports the gap rather than handing back [4] that strands a client
        // waiting for 3.
        let mut r = Room::default();
        r.join(peer("alice")).unwrap();
        r.submit(1, plain_sub("alice")).unwrap();
        r.submit(2, plain_sub("alice")).unwrap();
        r.submit(4, plain_sub("alice")).unwrap();
        assert_eq!(r.since(4), Some(Vec::new()), "nothing after the latest");
        let contiguous = r.since(3).map(|ops| ops.iter().map(|o| o.seq).collect::<Vec<_>>());
        assert_eq!(contiguous, Some(vec![4]), "a run with no hole still replays");
        assert_eq!(r.since(2), None, "a hole at 3 is unreplayable");
    }

    #[test]
    fn the_join_baseline_follows_the_save() {
        // A save moves the baseline the relay catches a joiner up from. Before a
        // save it is zero, so a joiner replays from the top of the history. After
        // a save it is the saved sequence, so only the later ops replay.
        let mut r = Room::default();
        r.join(peer("alice")).unwrap();
        r.submit(1, plain_sub("alice")).unwrap();
        r.submit(2, plain_sub("alice")).unwrap();
        assert_eq!(r.saved_seq(), 0);
        let from_top = r.since(r.saved_seq()).map(|ops| ops.iter().map(|o| o.seq).collect::<Vec<_>>());
        assert_eq!(from_top, Some(vec![1, 2]), "with no save, a joiner replays every op");

        r.submit(3, snap_sub("alice", "doc")).unwrap();
        r.checkpoint(2).unwrap();
        assert_eq!(r.saved_seq(), 2);
        let after_save = r.since(r.saved_seq()).map(|ops| ops.iter().map(|o| o.seq).collect::<Vec<_>>());
        assert_eq!(after_save, Some(vec![3]), "after a save, only the later ops replay");
    }

    #[test]
    fn a_checkpoint_past_the_room_s_own_sequence_is_refused() {
        // A save claiming it covers edits the room never ordered would move the join baseline
        // past live edits and strand a later joiner behind them. No snapshot follows it, so it
        // is refused outright.
        let mut r = Room::default();
        r.join(peer("alice")).unwrap();
        r.submit(1, plain_sub("alice")).unwrap();
        assert!(r.checkpoint(u64::MAX).is_err());
        assert_eq!(r.saved_seq(), 0, "the claimed save moves nothing");
    }

    #[test]
    fn total_retained_text_bytes_are_bounded_across_scopes() {
        // TEXT_BYTES bounds one scope and MAX_SCOPES bounds how many exist, but their product
        // is far larger than a room should ever hold. Many scopes each under the per-scope cap
        // must still be bounded in total, or a client spreading its edits across scopes grows
        // the room without limit one bounded log at a time.
        let mut r = Room::default();
        r.join(peer("alice")).unwrap();
        let blob = "x".repeat(TEXT_BYTES - 64);
        let mut seq = 0u64;
        for i in 0.. {
            seq += 1;
            let sub = Submission {
                peer_id: "alice".to_owned(),
                may_edit: true,
                command: "crdt.text".to_owned(),
                params: serde_json::json!({ "scope": format!("/s/{i}"), "ops": [blob.clone()] }),
                scope: None,
                local: None,
            };
            r.submit(seq, sub).unwrap();
            if i >= 2 * (TOTAL_TEXT_BYTES / TEXT_BYTES) {
                break;
            }
        }
        let retained: usize = r.texts().values().map(|log| log.bytes).sum();
        assert!(
            retained <= TOTAL_TEXT_BYTES,
            "retained {retained} bytes across {} scopes, more than the room-wide budget of {TOTAL_TEXT_BYTES}",
            r.texts().len()
        );
    }

    #[test]
    fn retained_scopes_are_bounded() {
        // One editor naming a fresh scope on every edit cannot grow the room
        // without limit. Past the cap, a new scope's replay buffer is dropped.
        let mut r = Room::default();
        r.join(peer("alice")).unwrap();
        for i in 0..(MAX_SCOPES + 10) {
            let sub = Submission {
                peer_id: "alice".to_owned(),
                may_edit: true,
                command: "crdt.text".to_owned(),
                params: serde_json::json!({ "scope": format!("/s/{i}"), "ops": [{ "op": "x" }] }),
                scope: None,
                local: None,
            };
            r.submit((i + 1) as u64, sub).unwrap();
        }
        assert_eq!(r.texts().len(), MAX_SCOPES, "the scope count is capped");
    }

    #[test]
    fn a_reopened_room_seeds_its_sequence_from_the_bus() {
        // A fresh room starts at zero. On reopen the bus still holds where the
        // document was, and observe_seq raises the room to it so the next op is
        // in order rather than a replay the client silently drops.
        let mut r = Room::default();
        assert_eq!(r.seq(), 0);
        r.observe_seq(42);
        assert_eq!(r.seq(), 42, "the room must not hand out a sequence number already seen");
        r.observe_seq(10);
        assert_eq!(r.seq(), 42, "a stale read never pushes the counter backwards");
    }
}
