//! The CollabCraft relay.
//!
//! Peers connect with a token Nextcloud minted. The relay verifies the
//! signature, puts them in a room keyed on the Nextcloud file id, assigns a
//! monotonic sequence to every edit and arbitrates scope locks. It makes no
//! access decisions of its own and never calls back into Nextcloud.
//!
//! Configuration, all from the environment:
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `COLLABCRAFT_SECRET` | required | shared with the Nextcloud app |
//! | `COLLABCRAFT_VALKEY_URL` | `redis://valkey:6379/6` | the bus |
//! | `COLLABCRAFT_BIND` | `0.0.0.0:9444` | listen address |
//! | `COLLABCRAFT_KEYED_LINKS` | on | `off` forbids links for recipients with no account |
//! | `COLLABCRAFT_KEYED_LINK_TTL_HOURS` | 168 | how long one of those links lasts |
//! | `COLLABCRAFT_KEYED_LINK_USES` | 1 | how often one may be opened |
//! | `COLLABCRAFT_PROVIDER_URL` | none | the cloud provider, overridden by the setup page |
//! | `COLLABCRAFT_PROVIDER_KIND` | `nextcloud` | which implementation answers there |
//! | `COLLABCRAFT_ADMIN_GROUPS` | none | groups whose members may change that from the setup page |
//!
//! The provider is the one piece of configuration a page can change. The
//! environment stays the default. A value stored on the bus overrides it, so
//! an existing deployment keeps working with nothing touched.
//!
//! The Valkey URL uses the estate's DNS name rather than an address, so the
//! relay follows the instance if it moves. Database index 6 is free: the stack
//! README allocates 0 through 5 and 8, and the live keyspace also shows 10 and
//! 13 in use.

mod api;
mod auth;
mod bus;
mod esign;
mod git;
mod google;
mod keyed;
mod pages;
mod recipient;
mod room;
mod route;
mod sealed;
mod session;
mod settings;
mod state;
mod store;
mod token;
mod transport;

#[cfg(test)]
mod test_text;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod review_repro;

#[cfg(test)]
mod review_reverify;

#[cfg(test)]
mod e2e_bus;
#[cfg(test)]
mod e2e_fixwave;
#[cfg(test)]
mod e2e_locks;
#[cfg(test)]
mod e2e_reconnect;
#[cfg(test)]
mod e2e_schema;
#[cfg(test)]
mod e2e_space;
#[cfg(test)]
mod review_reverify2;

#[cfg(test)]
mod review_lease_lapse;

#[cfg(test)]
mod review_outbound_expiry;

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use socketioxide::SocketIo;
use socketioxide::extract::{Data, SocketRef, State};
use tokio::sync::Mutex;

use bus::{Bus, LockOutcome, ReleaseOutcome};
use craft_collab::shadow::{Mark, Shadow, ShadowRefusal};
use room::{Op, Peer, Refusal, Room, Submission, crdt_ops_sound, crdt_ops_too_large, is_crdt_command};

/// Bytes one opaque frame may carry, base64 included.
///
/// Frames are passed through unread, so nothing downstream can judge the size. The relay has to.
const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// What a Socket.IO packet carries besides its payload: the event name, the document, the
/// submit's own fields.
const FRAME_HEADROOM: usize = 64 * 1024;

/// The largest Socket.IO payload the transport accepts, on every transport.
///
/// A checkpoint snapshot is one submit carrying a whole document, retained for joiners up to
/// the per-scope text cap, so the transport has to carry one that large or saving a large
/// document would fail at the socket, where the client cannot tell why. Frames keep their own
/// smaller bound (`MAX_FRAME_BYTES`) and other commands the room's (`room::HISTORY_BYTES`),
/// both checked after decoding.
fn transport_cap(text_cap: usize) -> usize {
    text_cap.max(MAX_FRAME_BYTES).saturating_add(FRAME_HEADROOM)
}

/// Apply [`transport_cap`] to every transport limit Socket.IO has.
///
/// `max_payload` bounds only an HTTP long-polling body. A WebSocket message has its own two
/// limits, which default to 16 MiB a frame and 64 MiB a message, so a polling client was held
/// to 1 MiB while a WebSocket one was not. All three now say the same thing.
///
/// The engine.io heartbeat is set here too, so every relay built from this (the binary and
/// the tests) pings the same way.
fn sized(builder: socketioxide::SocketIoBuilder, text_cap: usize) -> socketioxide::SocketIoBuilder {
    let cap = transport_cap(text_cap);
    builder
        .max_payload(cap as u64)
        .ws_max_message_size(cap)
        .ws_max_frame_size(cap)
        .max_buffer_size(SOCKET_BUFFER)
        .ping_interval(PING_INTERVAL)
        .ping_timeout(PING_TIMEOUT)
}

/// Packets a socket may have queued and not yet written. engineioxide's default is 128, and
/// an emit past it fails rather than waits.
///
/// A bound on a COUNT of packets, not on bytes, and not memory set aside: engineioxide queues
/// up to this many packets whatever their size, so the bytes queued are bounded only by what
/// the room retains (its op history and retained text, see [`room::HISTORY_BYTES`]). A join
/// queues the joined message, its whole ordered replay (up to [`room::HISTORY`] ops) or the
/// `text_ops` pieces of every retained scope (at most [`room::MAX_SCOPES`] scopes of at most
/// [`room::MAX_PIECES_PER_SCOPE`] pieces), all at once and before anything is written, so the
/// buffer holds the larger of those with room to spare. Ephemeral state is sent after the
/// socket joined, and one that does not fit is only dropped, never a cut join.
const SOCKET_BUFFER: usize = 1 + max_usize(room::HISTORY, room::MAX_SCOPES * room::MAX_PIECES_PER_SCOPE) + 512;

const fn max_usize(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

/// How long a document's room stays in memory after its last socket left.
///
/// The room holds what a joiner needs and Valkey does not: the op history, the retained text
/// with its checkpoint snapshot, and the saved marker. A reconnect storm, a page reload or a
/// worker restarting would otherwise empty the room for a moment and drop all of that, and the
/// next joiner would find the history gone (see `Joined.history_lost`).
const ROOM_GRACE: std::time::Duration = std::time::Duration::from_secs(120);

/// [`ErrorMsg::code`]: a transient relay failure (the bus did not answer). Send the same thing
/// again after a backoff; nothing was ordered or decided.
const CODE_RETRY: &str = "retry";

/// [`ErrorMsg::code`] on a refused connection: the document, or this user's share of it, is
/// full. Reconnecting at once cannot help.
const CODE_FULL: &str = "full";

/// [`ErrorMsg::code`] on a refused connection: the join named another replica schema than the
/// one its room is pinned to, or a malformed one. Reconnecting with the same build cannot help.
const CODE_SCHEMA: &str = "schema";

/// [`ErrorMsg::code`] on a refused submit whose update is not a valid document update for the
/// room's replica schema. Nothing was ordered or kept; sending it again cannot help.
const CODE_INVALID: &str = "invalid";

/// [`ErrorMsg::code`] on a refused fenced submit (one naming `fence: {scope, token}`): this peer
/// does not hold `scope` under `token` now (the lease lapsed, or was granted again since).
/// Nothing was ordered or kept, and the token never becomes current again, so sending it again
/// cannot help: the sender drops the edit and goes back to the room's state.
const CODE_FENCED: &str = "fenced";
use token::Claims;

const DEFAULT_VALKEY: &str = "redis://valkey:6379/6";
const DEFAULT_BIND: &str = "0.0.0.0:9444";

/// The largest document body a save may carry. Axum's own default is 2 MiB,
/// which refuses real documents, so this is the ceiling the API routes use.
const MAX_SAVE_BYTES: usize = 256 * 1024 * 1024;

/// The protocol version a peer must name in the handshake.
const PROTO: &str = "craft-live/1";

/// The largest WebSocket frame the native client reads. rust_socketio 0.6 connects through
/// tokio-tungstenite with its default config (16 MiB a frame, 64 MiB a message) and does not
/// let a caller change it, and the relay sends every message as one frame. So no message to
/// a client may be larger than this. A browser's WebSocket has no such limit.
const CLIENT_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Default per-scope retained-text cap, overridden by `COLLABCRAFT_TEXT_BYTES`. As large as
/// it can be: one op as large as the cap, broadcast with its wrapper, still fits one client
/// frame.
const DEFAULT_TEXT_BYTES: usize = MAX_TEXT_BYTES;

/// The largest piece of a `text_ops` catch-up, so a scope retaining more than one frame
/// reaches a client in several messages.
const TEXT_OPS_PIECE_BYTES: usize = CLIENT_FRAME_BYTES - FRAME_HEADROOM;

/// Split retained ops into pieces of at most `budget` bytes, in order. An op larger than the
/// budget goes alone; the text cap keeps every op below it.
fn text_pieces(ops: &[serde_json::Value], budget: usize) -> Vec<Vec<serde_json::Value>> {
    let mut pieces: Vec<Vec<serde_json::Value>> = Vec::new();
    let mut used = budget;
    for op in ops {
        let size = serde_json::to_vec(op).map_or(0, |v| v.len()) + 1;
        if used + size > budget {
            pieces.push(Vec::new());
            used = 0;
        }
        used += size;
        if let Some(piece) = pieces.last_mut() {
            piece.push(op.clone());
        }
    }
    if pieces.is_empty() {
        pieces.push(Vec::new());
    }
    pieces
}

/// How often the transport pings. Idle proxies in front of the relay drop a quiet connection
/// at 100 s, so this stays well under that.
const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(25);

/// How long engine.io waits for a pong before it closes the socket. A pong queued behind a
/// large frame (a checkpoint of a few MiB on a slow uplink) arrives only after that frame, and
/// engineioxide's default of 20 s closed such a socket mid transfer, so every retry of the same
/// checkpoint died the same way. 90 s carries a frame at the text cap at
/// `craft_collab::SLOW_LINK_BYTES_PER_SEC`-ish rates, and still reaps a dead socket in under two
/// minutes; the client's own heartbeat notices a dead link much sooner.
const PING_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// Shared relay state.
struct Relay {
    /// Per-scope cap on retained CRDT text for every room this relay creates.
    text_cap: usize,
    /// The ticket `uid` of the one peer allowed to emit `saved`.
    service_uid: String,
    secret: Vec<u8>,
    bus: Mutex<Bus>,
    /// file id -> room. One lock, because ordering needs a single decision
    /// point and a per-room lock would not make the hot path meaningfully
    /// cheaper at this scale. Held only for room bookkeeping, never while a payload is
    /// serialized onto a socket.
    rooms: Mutex<HashMap<String, Room>>,
    /// file id -> that room's broadcast gate, and whether ops are tried on a shadow copy.
    /// See [`Relay::gate`].
    gates: Gates,
    /// How long an emptied room is kept. [`ROOM_GRACE`] in the binary.
    room_grace: std::time::Duration,
}

/// What a peer sends in the Socket.IO handshake.
#[derive(Debug, Deserialize)]
struct Handshake {
    token: String,
    /// The sequence this connection has already applied, when it is resuming a session rather
    /// than loading the document fresh.
    ///
    /// Absent, or sent as `null`, on a fresh load: an older client that never sends the field
    /// decodes the same way, so this is a compatible extension rather than a breaking one. The
    /// join then baselines on the saved file's sequence as it always did. When it is present,
    /// the join baselines there instead, because that connection already knows what it has
    /// applied and resuming anywhere else either strands it behind a save it never loaded, or
    /// re-sends it ops it already has.
    #[serde(default)]
    applied: Option<u64>,
    /// The protocol version the peer speaks. Required: a missing or different value is
    /// refused with `version mismatch`, so an old client fails loudly instead of misreading
    /// the relay.
    #[serde(default)]
    proto: Option<String>,
    /// The replica schema the peer edits with (`"wordcraft/1"`, `"gridcraft/1"`). Optional: the
    /// room is pinned to the first one a join names, and a join naming another is refused with
    /// [`CODE_SCHEMA`]. A join naming none (a client from before the field) is not checked and
    /// pins nothing.
    #[serde(default)]
    schema: Option<String>,
    /// The id space a rejoining replica already mints in (it keeps it across reconnects). The
    /// space counter is raised to at least this before the grant ([`Bus::next_space`]), so no
    /// later connection is granted it. Absent on a fresh load and from older clients. A value
    /// that is not a `u32` is ignored rather than refusing the connection.
    #[serde(default, deserialize_with = "lenient_space")]
    space: Option<u32>,
}

/// [`Handshake::space`]: any JSON value, kept only when it is a `u32`.
fn lenient_space<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
    let v = serde_json::Value::deserialize(d)?;
    Ok(v.as_u64().and_then(|n| u32::try_from(n).ok()))
}

/// The longest replica schema name a handshake may carry.
const MAX_SCHEMA_BYTES: usize = 64;

/// Whether the handshake's replica schema, when it names one, is a short plain name
/// (`app/version`: ASCII letters, digits, `.`, `-`, `_`, `/`).
fn check_schema(handshake: &Handshake) -> Result<(), &'static str> {
    match handshake.schema.as_deref() {
        None => Ok(()),
        Some(s)
            if !s.is_empty() && s.len() <= MAX_SCHEMA_BYTES && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '/')) =>
        {
            Ok(())
        }
        Some(_) => Err("the replica schema named in the handshake is not a valid name"),
    }
}

/// Whether the handshake names the protocol this relay speaks.
fn check_proto(handshake: &Handshake) -> Result<(), &'static str> {
    match handshake.proto.as_deref() {
        Some(PROTO) => Ok(()),
        _ => Err("version mismatch"),
    }
}

/// `COLLABCRAFT_BASE` as a mount path: empty, or `/something` with no trailing slash.
fn normalise_base(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if !trimmed.starts_with('/') || trimmed.contains("//") || trimmed.contains(char::is_whitespace) || trimmed.contains(['?', '#', '%']) {
        return Err(format!("COLLABCRAFT_BASE must look like /craft, got {raw:?}"));
    }
    Ok(trimmed.to_owned())
}

/// Whether this peer may emit the `saved` marker: the service peer, with edit right.
fn saved_allowed(claims: &Claims, service_uid: &str) -> bool {
    claims.may_edit() && claims.uid == service_uid
}

/// Whether a submit is a checkpoint snapshot (`"snap": true` on a CRDT op). Only the service
/// peer may send one, by the same rule as [`saved_allowed`]: a snapshot becomes where every
/// joiner starts, and an editor's would let any peer reset the room or crowd out the saver's.
fn is_snapshot(command: &str, params: &serde_json::Value) -> bool {
    is_crdt_command(command) && params.get("snap").and_then(serde_json::Value::as_bool).unwrap_or(false)
}

/// Upper bound for `COLLABCRAFT_TEXT_BYTES`: the native client's frame limit, less the
/// wrapper an op travels in (see [`CLIENT_FRAME_BYTES`]).
const MAX_TEXT_BYTES: usize = CLIENT_FRAME_BYTES - FRAME_HEADROOM;

/// Parse `COLLABCRAFT_TEXT_BYTES`: empty means the default, 0 is refused, large values clamp.
fn parse_text_bytes(raw: &str) -> Result<usize, &'static str> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(DEFAULT_TEXT_BYTES);
    }
    match raw.parse::<usize>() {
        Ok(0) => Err("COLLABCRAFT_TEXT_BYTES must be greater than 0"),
        Ok(n) => Ok(n.min(MAX_TEXT_BYTES)),
        Err(_) => Err("COLLABCRAFT_TEXT_BYTES must be a number of bytes"),
    }
}

/// The rooms' broadcast gates. See [`Relay::gate`].
struct Gates {
    map: std::sync::Mutex<HashMap<String, Arc<Mutex<Gate>>>>,
    /// Whether each `crdt.*` op is tried on a shadow copy of its room's document before it is
    /// ordered (`COLLABCRAFT_SHADOW`, on unless set to `0`). See [`admit_on_shadow`].
    shadow: bool,
    /// What all rooms' shadow copies hold, about ([`Shadow::bytes`]), and how many copies
    /// hold more than the floor.
    shadow_bytes: std::sync::atomic::AtomicUsize,
    over_floor: std::sync::atomic::AtomicUsize,
    room_cap: usize,
    global_cap: usize,
    /// A copy at or under this many bytes is never refused for the relay-wide cap: a room of
    /// ordinary size goes on whatever other rooms hold (`COLLABCRAFT_SHADOW_FLOOR_BYTES`).
    floor: usize,
    /// Past this many bytes in all copies, the relay drops copies instead of growing them, and
    /// never refuses an op for it (`COLLABCRAFT_SHADOW_CEILING_BYTES`; by default half again
    /// the relay-wide cap, and never under it, so the floor and share rules act first). It
    /// bounds what the floor leaves open (N rooms at the floor each). Idle copies go first,
    /// then the largest over the floor ([`make_room`]); only when neither is left does the
    /// room at hand lose its copy and its op go through unjudged ([`ceiling_drop`]).
    ceiling: usize,
    /// Per (room, user id): ops that cost a copy a rebuild. Kept here, not in the gate or by
    /// socket, so neither a reconnect nor an emptied room resets them. See [`MAX_STRIKES`].
    strikes: std::sync::Mutex<HashMap<(String, String), Strikes>>,
}

impl Default for Gates {
    fn default() -> Self {
        Gates::new(true, DEFAULT_SHADOW_ROOM_BYTES, DEFAULT_SHADOW_BYTES)
    }
}

impl Gates {
    fn new(shadow: bool, room_cap: usize, global_cap: usize) -> Gates {
        Gates {
            map: std::sync::Mutex::new(HashMap::new()),
            shadow,
            shadow_bytes: std::sync::atomic::AtomicUsize::new(0),
            over_floor: std::sync::atomic::AtomicUsize::new(0),
            room_cap,
            global_cap,
            floor: DEFAULT_SHADOW_FLOOR_BYTES,
            ceiling: default_ceiling(global_cap),
            strikes: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn with_floor(self, floor: usize) -> Gates {
        Gates { floor, ..self }
    }

    fn with_ceiling(self, ceiling: usize) -> Gates {
        Gates { ceiling, ..self }
    }

    /// One copy's share of the relay-wide cap, among the copies over the floor only, so
    /// opening many small rooms shrinks nobody's share.
    fn fair_share(&self) -> usize {
        self.global_cap / self.over_floor.load(std::sync::atomic::Ordering::Relaxed).max(1)
    }

    fn strikes(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), Strikes>> {
        self.strikes.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Default cap on one room's shadow copy (`COLLABCRAFT_SHADOW_ROOM_BYTES`): its document, base,
/// log and index, about.
const DEFAULT_SHADOW_ROOM_BYTES: usize = 256 * 1024 * 1024;
/// Default cap on all rooms' shadow copies together (`COLLABCRAFT_SHADOW_BYTES`).
const DEFAULT_SHADOW_BYTES: usize = 2 * 1024 * 1024 * 1024;
/// Default floor under which a copy is never refused for the relay-wide cap
/// (`COLLABCRAFT_SHADOW_FLOOR_BYTES`). A 1 MiB document's copy is about 2-3 MiB.
const DEFAULT_SHADOW_FLOOR_BYTES: usize = 16 * 1024 * 1024;
/// A copy whose room sent no op for this long is idle: the first to go at the ceiling (see
/// [`make_room`]). An active room types every few seconds; a room that sent nothing for a
/// minute loses only its judging of a first op after a while, and its next checkpoint brings
/// its document back.
const SHADOW_IDLE: std::time::Duration = std::time::Duration::from_secs(60);

/// The default hard ceiling on all rooms' shadow copies together: half again the relay-wide
/// cap (3 GiB at the default 2 GiB). See [`Gates::ceiling`].
fn default_ceiling(global_cap: usize) -> usize {
    global_cap.saturating_add(global_cap / 2)
}

/// Parse `COLLABCRAFT_SHADOW_CEILING_BYTES` against the relay-wide cap: unset means the
/// default; a ceiling under the cap is refused, because then copies would be dropped (and ops
/// let through unjudged) before the floor and share rules ever act.
fn parse_ceiling(raw: &str, global_cap: usize) -> Result<usize, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default_ceiling(global_cap));
    }
    match raw.parse::<usize>() {
        Ok(n) if n >= global_cap => Ok(n),
        Ok(n) => Err(format!("COLLABCRAFT_SHADOW_CEILING_BYTES ({n}) must be at least COLLABCRAFT_SHADOW_BYTES ({global_cap})")),
        Err(_) => Err("COLLABCRAFT_SHADOW_CEILING_BYTES must be a number of bytes".to_owned()),
    }
}

/// Ops one user may send to one room that cost its copy a rebuild (yrs panicked on them)
/// within [`STRIKE_WINDOW`], before that user's CRDT ops in that room are refused for
/// [`STRIKE_PAUSE`]. Only that user: the room's other editors and its checkpoints go on. A
/// rollback because the bus did not answer, or the room refused the op, counts against
/// nobody. Honest clients never strike.
const MAX_STRIKES: usize = 4;
const STRIKE_WINDOW: std::time::Duration = std::time::Duration::from_secs(60);
const STRIKE_PAUSE: std::time::Duration = std::time::Duration::from_secs(30);

/// One user's rebuild-costing ops in one room, and until when its ops are refused.
#[derive(Default)]
struct Strikes {
    at: std::collections::VecDeque<std::time::Instant>,
    paused_until: Option<std::time::Instant>,
}

/// What a room's gate guards besides the order of its broadcasts: the shadow copy of the
/// room's document (the [`craft_collab::DOC_SCOPE`] scope, the only one replicas read), which
/// only a holder of the gate touches, so the copy takes ops in the order they are numbered.
#[derive(Default)]
struct Gate {
    shadow: Option<Shadow>,
    /// What the copy held at its last count, as added to [`Gates::shadow_bytes`].
    bytes: usize,
    /// When the room last sent an op to the copy ([`SHADOW_IDLE`]).
    last_op: Option<std::time::Instant>,
}

/// Why the relay refused an op at the shadow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShadowStop {
    /// The copy refused it, or yrs panicked on it, or it carries more than one update.
    Refused(ShadowRefusal),
    /// For now: the room's copy is past its byte cap, or among the largest while all copies
    /// are past theirs, or this user is paused for its rebuilds. Sent as `retry`.
    Busy,
    /// The op alone is past the room's copy cap, or has more items than a check takes:
    /// sending it again cannot help, and it says nothing about its content.
    TooLarge,
}

/// Times the blocking task of a shadow copy failed (the copy is lost and started again).
static SHADOW_LOST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Copies dropped at the relay-wide ceiling ([`Gates::ceiling`]).
static SHADOW_CEILING_DROPS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A byte count from the environment, or `default` when unset or unreadable.
fn parse_bytes_or(raw: &str, default: usize) -> usize {
    raw.trim().parse().ok().filter(|&n: &usize| n > 0).unwrap_or(default)
}

/// Parse `COLLABCRAFT_SHADOW`: on unless it is `0`, `false` or `off`.
fn parse_shadow(raw: &str) -> bool {
    !matches!(raw.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off")
}

/// Ops whose payload is past this many bytes are checked off the async runtime: the gate
/// takes about 15 ms on a 1 MiB checkpoint (release), long enough to stall other rooms.
const BLOCKING_CHECK_BYTES: usize = 64 * 1024;

/// The yrs updates of a `crdt.*` submit to the document scope, decoded. `None` when it carries
/// none, or is for another scope: replicas read only [`craft_collab::DOC_SCOPE`] and ignore the
/// rest, so ops there cannot reach a replica's yrs and need no copy (one per invented scope
/// name would let a peer crowd the real one out).
fn crdt_updates(command: &str, params: &serde_json::Value) -> Option<Vec<Vec<u8>>> {
    if !is_crdt_command(command) || params.get("scope").and_then(serde_json::Value::as_str) != Some(craft_collab::DOC_SCOPE) {
        return None;
    }
    let ops = params.get("ops")?.as_array()?;
    ops.iter().map(|op| op.as_str().and_then(|t| craft_collab::update_from_wire(t).ok())).collect()
}

/// Try a submit's update on the room's shadow copy (`gate` is held by the caller; `who` is the
/// room and the sender's user id). Refused when the copy's replica refuses it (it does not fit
/// the document) or yrs panics applying or encoding it; the copy then stays as it was. Returns
/// where the copy stood before, for the caller to roll back to if the op is not ordered after
/// all. Run off the async runtime, because a checkpoint costs tens of milliseconds on a large
/// document.
///
/// One update per document-scope submit: every client sends one (an edit, an offline bundle it
/// merged itself, or a checkpoint), and refusing the second of two would cost a rebuild to take
/// the first out again. A refused single update costs none: the replica refuses it before yrs
/// sees it. `state`: the op is a checkpoint snapshot from the document service, checked under
/// the higher state item cap.
///
/// Bounded, and never by switching judgement off: a copy past the room cap is compacted, and
/// if still past it, the op is refused for now (`retry`). Past the relay-wide cap, a copy at
/// or under the floor always goes on; among the copies over it, only one past its fair share
/// is refused for now, after compacting. A copy is judged on its own bytes against the floor,
/// not with the op: a room under the floor goes on whatever size its checkpoints are. Past the
/// hard [ceiling](Gates::ceiling) the relay stops judging instead: the op goes through
/// unjudged and the room's copy is dropped (logged). A user whose ops cost [`MAX_STRIKES`]
/// rebuilds in a room in [`STRIKE_WINDOW`] is refused there for [`STRIKE_PAUSE`].
async fn admit_on_shadow(
    gates: &Gates,
    gate: &mut Gate,
    who: (&str, &str),
    schema: Option<String>,
    updates: Vec<Vec<u8>>,
    state: bool,
) -> Result<Option<Mark>, ShadowStop> {
    use std::sync::atomic::Ordering::Relaxed;
    let now = std::time::Instant::now();
    let key = (who.0.to_owned(), who.1.to_owned());
    if gates.strikes().get(&key).and_then(|s| s.paused_until).is_some_and(|until| now < until) {
        return Err(ShadowStop::Busy);
    }
    let update = match <[Vec<u8>; 1]>::try_from(updates) {
        Ok([update]) => update,
        Err(none) if none.is_empty() => return Ok(None),
        Err(_) => return Err(ShadowStop::Refused(ShadowRefusal::Invalid)),
    };
    if update.len() > gates.room_cap {
        return Err(ShadowStop::TooLarge);
    }
    gate.last_op = Some(now);
    // Past the ceiling, other rooms' idle and larger copies go before this one.
    make_room(gates, who.0, gate.bytes, gate.bytes + 2 * update.len(), now);
    let (room_cap, global_cap, share, floor, ceiling) = (gates.room_cap, gates.global_cap, gates.fair_share(), gates.floor, gates.ceiling);
    let others = gates.shadow_bytes.load(Relaxed).saturating_sub(gate.bytes);
    if gate.shadow.is_none() && others + 2 * update.len() > ceiling {
        // Past the ceiling, no new copy is started: the op goes through unjudged.
        tracing::debug!(room = %who.0, "the shadow copies are past their ceiling; an op goes through unjudged");
        return Ok(None);
    }
    let mut shadow = gate.shadow.take().unwrap_or_else(|| Shadow::new(schema.as_deref()));
    let empty_before = shadow.empty_rebuilds();
    let judged = tokio::task::spawn_blocking(move || {
        // The op before this one was ordered (or rolled back), so its mark is spent.
        shadow.settle();
        shadow.set_schema(schema.as_deref());
        let need = |s: &Shadow| s.bytes() + 2 * update.len();
        // The floor is on the copy alone (R3-M1): a large op into a copy under the floor goes on.
        let over = |s: &Shadow| need(s) > room_cap || (others + need(s) > global_cap && s.bytes() > floor && need(s) > share);
        let past_ceiling = |s: &Shadow| others + need(s) > ceiling;
        if over(&shadow) || past_ceiling(&shadow) {
            shadow.compact_now();
        }
        if over(&shadow) {
            return (Some(shadow), Err(ShadowStop::Busy), None, 0);
        }
        if past_ceiling(&shadow) {
            // Stop judging rather than refuse an honest room: the copy goes.
            return (None, Ok(()), None, 0);
        }
        let (mark, before) = (shadow.mark(), shadow.rebuilds());
        let admitted = if state { shadow.admit_state(&update) } else { shadow.admit(&update) };
        let verdict = admitted.map_err(|r| if r == ShadowRefusal::TooLarge { ShadowStop::TooLarge } else { ShadowStop::Refused(r) });
        let cost = shadow.rebuilds() - before;
        (Some(shadow), verdict, Some(mark), cost)
    })
    .await;
    match judged {
        Ok((None, ..)) => {
            ceiling_drop(gates, gate, who.0, others);
            Ok(None)
        }
        Ok((Some(shadow), verdict, mark, cost)) => {
            warn_empty_rebuild(who.0, &shadow, empty_before);
            if shadow.is_waiting() {
                // Expected until the room's first checkpoint (the copy lacks the seed), after a
                // relay restart, and when two submits race for the gate. Logged, not refused:
                // honest clients send such ops.
                tracing::debug!("an op waits at the shadow copy for something it lacks");
            }
            account(gates, gate, &shadow);
            gate.shadow = Some(shadow);
            if cost > 0 {
                strike(gates, key, now);
            }
            verdict.map(|()| mark)
        }
        // The copy is caught under `catch_unwind` inside, so this is a cancelled task; the room
        // starts a fresh copy and the op is let through, as with the shadow off.
        Err(e) => {
            lose_copy(gates, gate, &e.to_string());
            Ok(None)
        }
    }
}

/// The room's copy is gone (its blocking task failed): count it, log it, and free its bytes, so
/// the next op starts a fresh copy. The op at hand is let through unjudged, as with the shadow
/// off; [`SHADOW_LOST`] says how often that happened.
fn lose_copy(gates: &Gates, gate: &mut Gate, error: &str) {
    let lost = SHADOW_LOST.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    tracing::warn!(error, lost, "the shadow copy was lost; starting a new one");
    gate.shadow = None;
    forget_bytes(gates, gate);
}

/// Bring the copies under the [ceiling](Gates::ceiling) for an op of room `room` whose copy
/// holds `own` bytes and will need about `need`, by dropping other rooms' copies: idle ones
/// first ([`SHADOW_IDLE`], largest first), then active ones over the floor and larger than
/// this room's, largest first. A room never loses its copy to the ceiling while a copy that
/// is idle or larger exists, so an honest small active room keeps being judged. A gate held
/// right now (its room is mid-op) is passed over, never waited for.
fn make_room(gates: &Gates, room: &str, own: usize, need: usize, now: std::time::Instant) {
    use std::sync::atomic::Ordering::Relaxed;
    let past = || gates.shadow_bytes.load(Relaxed).saturating_sub(own).saturating_add(need) > gates.ceiling;
    if !past() {
        return;
    }
    let all: Vec<(String, Arc<Mutex<Gate>>)> = {
        let map = gates.map.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        map.iter().filter(|(key, _)| key.as_str() != room).map(|(key, gate)| (key.clone(), Arc::clone(gate))).collect()
    };
    // (idle, bytes, room, gate) of every copy that may go before this room's.
    let mut victims: Vec<(bool, usize, String, Arc<Mutex<Gate>>)> = all
        .into_iter()
        .filter_map(|(key, gate)| {
            let g = gate.try_lock().ok()?;
            g.shadow.as_ref()?;
            let idle = g.last_op.is_none_or(|t| now.duration_since(t) >= SHADOW_IDLE);
            let (bytes, larger) = (g.bytes, g.bytes > gates.floor && g.bytes > own);
            drop(g);
            (idle || larger).then_some((idle, bytes, key, gate))
        })
        .collect();
    victims.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
    for (_, _, key, gate) in victims {
        if !past() {
            break;
        }
        if let Ok(mut g) = gate.try_lock()
            && g.shadow.is_some()
        {
            let others = gates.shadow_bytes.load(Relaxed).saturating_sub(g.bytes);
            ceiling_drop(gates, &mut g, &key, others);
        }
    }
}

/// The relay-wide [ceiling](Gates::ceiling) is reached: drop a room's copy (another room's, an
/// idle or larger one, from [`make_room`]; or, when none is left, the room at hand's, whose op
/// then goes through unjudged, as with the shadow off). Logged and counted
/// ([`SHADOW_CEILING_DROPS`]); the room's next op starts a new copy once the total is back
/// under the ceiling.
fn ceiling_drop(gates: &Gates, gate: &mut Gate, room: &str, others: usize) {
    let dropped = SHADOW_CEILING_DROPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    tracing::warn!(
        room,
        copy = gate.bytes,
        others,
        ceiling = gates.ceiling,
        dropped,
        "the shadow copies reached their ceiling; a room's copy is dropped, and its ops go through unjudged until its next checkpoint"
    );
    gate.shadow = None;
    forget_bytes(gates, gate);
}

/// R3-M4: a rebuild left the copy empty because it could not take its own base (a document
/// past the state item cap). It fails open (later ops wait and go through unjudged); say so in
/// the relay's log, which does not show the library's own warning by default.
fn warn_empty_rebuild(room: &str, shadow: &Shadow, before: u64) {
    if shadow.empty_rebuilds() > before {
        tracing::warn!(room, "a shadow copy was rebuilt empty: its document is past the state item cap; its ops go through unjudged");
    }
}

/// Take a copy's bytes out of the relay-wide counts (it is gone).
fn forget_bytes(gates: &Gates, gate: &mut Gate) {
    use std::sync::atomic::Ordering::Relaxed;
    let bytes = std::mem::take(&mut gate.bytes);
    gates.shadow_bytes.fetch_sub(bytes, Relaxed);
    if bytes > gates.floor {
        gates.over_floor.fetch_sub(1, Relaxed);
    }
}

/// Count the copy's bytes into the relay-wide counts.
fn account(gates: &Gates, gate: &mut Gate, shadow: &Shadow) {
    use std::sync::atomic::Ordering::Relaxed;
    let bytes = shadow.bytes();
    gates.shadow_bytes.fetch_add(bytes, Relaxed);
    gates.shadow_bytes.fetch_sub(gate.bytes, Relaxed);
    match (gate.bytes > gates.floor, bytes > gates.floor) {
        (false, true) => drop(gates.over_floor.fetch_add(1, Relaxed)),
        (true, false) => drop(gates.over_floor.fetch_sub(1, Relaxed)),
        _ => {}
    }
    gate.bytes = bytes;
}

/// Take an op back out of the room's copy when it is not ordered after all (the bus could not
/// number it, or the room refused it). A rebuild, but not the sender's doing, so not counted
/// against it.
fn unadmit(gates: &Gates, gate: &mut Gate, mark: Option<Mark>) {
    if let (Some(mark), Some(mut shadow)) = (mark, gate.shadow.take()) {
        let empty_before = shadow.empty_rebuilds();
        shadow.rollback(mark);
        warn_empty_rebuild("(rollback)", &shadow, empty_before);
        account(gates, gate, &shadow);
        gate.shadow = Some(shadow);
    }
}

/// Count an op of user `key.1` in room `key.0` that cost the copy a rebuild, and pause that
/// user there when it is the [`MAX_STRIKES`]th within [`STRIKE_WINDOW`].
fn strike(gates: &Gates, key: (String, String), now: std::time::Instant) {
    let mut strikes = gates.strikes();
    // Bounded by who struck lately: forget those with nothing recent.
    strikes.retain(|_, s| s.paused_until.is_some_and(|u| now < u) || s.at.back().is_some_and(|t| now.duration_since(*t) <= STRIKE_WINDOW));
    let s = strikes.entry(key.clone()).or_default();
    while s.at.front().is_some_and(|t| now.duration_since(*t) > STRIKE_WINDOW) {
        s.at.pop_front();
    }
    s.at.push_back(now);
    if s.at.len() >= MAX_STRIKES {
        s.at.clear();
        s.paused_until = Some(now + STRIKE_PAUSE);
        tracing::warn!(room = %key.0, uid = %key.1, "a user's ops cost the shadow copy {MAX_STRIKES} rebuilds within a minute; its ops there are refused for a while");
    }
}

impl Relay {
    /// The gate that orders a room's broadcasts against its joins.
    ///
    /// A submit holds it from recording its op to broadcasting it, and a join from reading
    /// its replay to joining the broadcast room. So an op is either in a joiner's replay or
    /// broadcast after the joiner is in the room and has been sent `joined`, and the long
    /// part, serialising a large replay onto the socket, blocks only that room rather than
    /// the global [`Relay::rooms`] lock.
    fn gate(&self, key: &str) -> Arc<Mutex<Gate>> {
        let mut gates = self.gates.map.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(gates.entry(key.to_owned()).or_default())
    }

    /// Drop a room's gate once nobody holds or waits on it.
    fn forget_gate(&self, key: &str) {
        let mut gates = self.gates.map.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if gates.get(key).is_some_and(|gate| Arc::strong_count(gate) == 1)
            && let Some(gate) = gates.remove(key)
            && let Ok(mut gate) = gate.try_lock()
        {
            forget_bytes(&self.gates, &mut gate);
        }
    }

    /// The room for `key`, created with this relay's text cap if it does not exist yet.
    fn room<'a>(&self, rooms: &'a mut HashMap<String, Room>, key: &str) -> &'a mut Room {
        rooms.entry(key.to_owned()).or_insert_with(|| Room::with_text_cap(self.text_cap))
    }
}

// ---- payloads, matching craft-collab's proto ------------------------------

#[derive(Debug, Deserialize)]
struct SubmitMsg {
    local: Option<u64>,
    command: String,
    #[serde(default)]
    params: serde_json::Value,
    #[serde(default)]
    scope: Option<String>,
    /// An ordered edit's fence: ordered only while this peer holds `scope` under `token`.
    #[serde(default)]
    fence: Option<FenceMsg>,
}

/// The lock an ordered edit is made under, and the grant's fencing token.
#[derive(Debug, Deserialize)]
struct FenceMsg {
    scope: String,
    token: u64,
}

#[derive(Debug, Deserialize)]
struct ScopeMsg {
    scope: String,
}

/// An unlock. `token` is the grant's fencing token; with it only that grant is released, so a
/// stale unlock cannot release a newer grant. Without it (a client from before fencing) the
/// peer alone is compared.
#[derive(Debug, Deserialize)]
struct UnlockMsg {
    scope: String,
    #[serde(default)]
    token: Option<u64>,
}

/// A holder extending its lease while its ordered run goes on.
#[derive(Debug, Deserialize)]
struct RefreshMsg {
    scope: String,
    token: u64,
}

#[derive(Debug, Deserialize)]
struct ResyncMsg {
    have: u64,
}

#[derive(Debug, Deserialize)]
struct FrameMsg {
    payload: String,
}

#[derive(Debug, Deserialize)]
struct EphemeralMsg {
    kind: String,
    #[serde(default)]
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct SavedMsg {
    seq: u64,
}

/// Stashed on a socket once it has actually reached `Room::join`.
///
/// See the comment where it is inserted, in `on_connect`, and where it is read, in
/// `on_disconnect`. `Clone` because `Extensions::insert`/`get` require it, not because a copy
/// of this ever means anything beyond its own presence.
#[derive(Clone)]
struct JoinedRoom;

#[derive(Debug, Serialize)]
struct JoinedMsg<'a> {
    doc: String,
    peer: &'a str,
    seq: u64,
    /// The CRDT id space this connection may mint character ids in. Never 0.
    space: u32,
    roster: &'a [Peer],
    locks: Vec<LockEntry>,
    /// The last sequence of the ordered replay that follows, so a client can tell it from
    /// live ops that overtake it.
    replay_until: u64,
    /// The catch-up is the `text_ops` that follow, not an ordered replay.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    fallback: bool,
    /// The largest checkpoint snapshot, in wire bytes, this relay retains for a joiner, so a
    /// saver can refuse a larger one itself rather than have the transport drop it.
    text_cap: usize,
    /// This relay acknowledges the `alive` heartbeat (see `on_alive`).
    heartbeat: bool,
    /// The room began past history this relay no longer holds (it restarted, or the room was
    /// dropped after its grace period) and nobody has checkpointed since. A joiner that holds
    /// nothing from before must not take the saved file as current: it waits for a checkpoint
    /// snapshot. A replica that held the document before may checkpoint at once.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    history_lost: bool,
}

#[derive(Debug, Serialize)]
struct LockEntry {
    scope: String,
    holder: String,
}

#[derive(Debug, Serialize)]
struct RefusedMsg {
    doc: String,
    detail: String,
    /// What a client must change before it connects again; see `craft_collab::REFUSED_AUTH`.
    /// A field an older client does not know, so it still reads the error as before.
    code: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorMsg {
    doc: String,
    detail: String,
    /// The `local` counter of the submit this refuses, when it refuses one.
    #[serde(skip_serializing_if = "Option::is_none")]
    local: Option<u64>,
    /// [`CODE_RETRY`] for a transient failure, [`CODE_FULL`] for a refused connection, absent
    /// otherwise. An older client ignores the field and reads the error as before.
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<&'static str>,
}

/// Key space for sessions and account links, kept apart from the relay's
/// own room keys so one can be cleared without touching the other.
const PREFIX_SERVICE: &str = "collabcraft:svc";

/// Crates whose `debug` traces carry the handshake, and so the ticket. Always held at `info`.
const CAPPED_LOG_TARGETS: [&str; 2] = ["socketioxide", "engineioxide"];

/// The log filter for a `RUST_LOG` value. The transport crates are held at `info` whatever it
/// says: at `debug` they trace handshakes, and a handshake's auth carries the ticket. A
/// directive naming one of them or a module inside one, or filtering by span (which can enable
/// events of any target inside that span), is dropped before the caps are added, so no
/// directive can be more specific than the cap.
fn log_filter(rust_log: &str) -> Result<tracing_subscriber::EnvFilter, tracing_subscriber::filter::ParseError> {
    let kept: Vec<&str> = rust_log
        .split(',')
        .map(str::trim)
        .filter(|d| !d.is_empty() && !d.contains('['))
        .filter(|d| {
            let target = d.split('=').next().unwrap_or_default();
            !CAPPED_LOG_TARGETS.iter().any(|capped| target.starts_with(capped))
        })
        .collect();
    let mut filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::ERROR.into())
        .parse_lossy(kept.join(","))
        .add_directive("collabcraft_relay=info".parse()?);
    for capped in CAPPED_LOG_TARGETS {
        filter = filter.add_directive(format!("{capped}=info").parse()?);
    }
    Ok(filter)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter(log_filter(&std::env::var("RUST_LOG").unwrap_or_default())?).init();

    let secret = std::env::var("COLLABCRAFT_SECRET").map_err(|_| {
        "COLLABCRAFT_SECRET is not set. It must match the collabcraft app's relay_secret, \
         which the Nextcloud app generates on first use."
    })?;
    if secret.len() < 32 {
        return Err("COLLABCRAFT_SECRET is shorter than 32 characters; the app generates 48".into());
    }

    let valkey_url = std::env::var("COLLABCRAFT_VALKEY_URL").unwrap_or_else(|_| DEFAULT_VALKEY.to_owned());
    let bind = std::env::var("COLLABCRAFT_BIND").unwrap_or_else(|_| DEFAULT_BIND.to_owned());

    let mut bus = Bus::connect(&valkey_url).await?;
    // Fail at startup, not on the first edit. A relay that accepts peers and
    // then refuses every submit is harder to diagnose than one that will not
    // start.
    bus.check_writable().await?;
    // Redacted, because a deployment may embed a password in the URL and a log
    // is read by people who were never given it.
    tracing::info!(valkey_url = %redact_url(&valkey_url), "bus ready");

    let secret_bytes = secret.clone().into_bytes();
    let text_cap = parse_text_bytes(&std::env::var("COLLABCRAFT_TEXT_BYTES").unwrap_or_default())?;
    let service_uid = std::env::var("COLLABCRAFT_SERVICE_UID").ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| "craft_office".to_owned());
    let base = normalise_base(&std::env::var("COLLABCRAFT_BASE").unwrap_or_default())?;
    let public_apps = std::env::var("COLLABCRAFT_PUBLIC_APPS").is_ok_and(|v| v.trim() == "1");
    let shadow = parse_shadow(&std::env::var("COLLABCRAFT_SHADOW").unwrap_or_default());
    let shadow_bytes = parse_bytes_or(&std::env::var("COLLABCRAFT_SHADOW_BYTES").unwrap_or_default(), DEFAULT_SHADOW_BYTES);
    let shadow_ceiling = parse_ceiling(&std::env::var("COLLABCRAFT_SHADOW_CEILING_BYTES").unwrap_or_default(), shadow_bytes)?;
    tracing::info!(text_cap, base = %base, public_apps, shadow, shadow_bytes, shadow_ceiling, "relay options");
    let relay = Arc::new(Relay {
        text_cap,
        service_uid,
        secret: secret_bytes.clone(),
        bus: Mutex::new(bus),
        rooms: Mutex::new(HashMap::new()),
        gates: Gates::new(
            shadow,
            parse_bytes_or(&std::env::var("COLLABCRAFT_SHADOW_ROOM_BYTES").unwrap_or_default(), DEFAULT_SHADOW_ROOM_BYTES),
            shadow_bytes,
        )
        .with_floor(parse_bytes_or(&std::env::var("COLLABCRAFT_SHADOW_FLOOR_BYTES").unwrap_or_default(), DEFAULT_SHADOW_FLOOR_BYTES))
        .with_ceiling(shadow_ceiling),
        room_grace: ROOM_GRACE,
    });
    // For the health route, which outlives the move into the socket layer.
    let health_relay = relay.clone();

    // Cap the transport too. Room::submit refuses an oversized `params` after decoding, and a
    // cap there still means the bytes were read and parsed first. This refuses them at the
    // socket, which is the only place that costs nothing. Sized for the largest snapshot the
    // rooms retain; see `transport_cap`.
    let (layer, io) = sized(SocketIo::builder(), text_cap).req_path(format!("{base}/socket.io")).with_state(relay).build_layer();
    // ns returns () for the local adapter, so there is nothing to await here.
    io.ns("/", on_connect);

    // The API half. It signs the tokens the relay above verifies, which is why
    // both halves hold the same secret and neither has to ask the other
    // anything at runtime.
    let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(20)).build().unwrap_or_default();
    let oidc = session::Oidc::from_env(&http).await;
    if oidc.is_none() {
        tracing::warn!("no identity provider configured: every API route will refuse");
    }
    let store = store::Store::new(&valkey_url, PREFIX_SERVICE)?.sealing_with(&secret_bytes);
    let web_root = std::env::var("COLLABCRAFT_WEB_ROOT").unwrap_or_default();
    // Read once. A directory listing per request would be a disk hit on every
    // asset, and the set only changes on deploy.
    let hosted: Vec<String> = if web_root.is_empty() {
        Vec::new()
    } else {
        std::fs::read_dir(&web_root)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect()
            })
            .unwrap_or_default()
    };
    if web_root.is_empty() {
        // No root, no file service. ServeDir over an empty root resolves
        // against the working directory, and a direct launch from a stack
        // directory would then serve whatever sits there, `.env` included.
        tracing::warn!("COLLABCRAFT_WEB_ROOT is not set: no browser builds are served");
    } else {
        tracing::info!(count = hosted.len(), "browser builds hosted");
    }
    let esign = esign::EsignRuntime::from_env(&secret_bytes).await?;
    let app_state = Arc::new(state::App {
        // Precompressed on disk, sent with the Content-Encoding header.
        // Brotli at a high level costs minutes per wasm bundle, which is
        // unaffordable per request and free once at deploy.
        web: (!web_root.is_empty()).then(|| tower_http::services::ServeDir::new(&web_root).precompressed_br().precompressed_gzip()),
        hosted,
        secret: secret_bytes,
        relay_url: std::env::var("COLLABCRAFT_PUBLIC_RELAY").unwrap_or_else(|_| format!("http://{bind}")),
        settings: settings::Settings::from_env(),
        permissions: state::Permissions::from_env(),
        keyed: keyed::Policy::from_env(),
        store,
        oidc,
        http,
        esign,
        public_apps,
    });
    // What the deployment is actually on, which is the stored value where
    // there is one. Logged at startup because a deployment reading its own log
    // should not have to guess which of the two won.
    match app_state.provider_config().await {
        Some((config, settings::Source::Stored)) => {
            tracing::info!(kind = %config.kind, base = %config.base, "cloud provider, set on the setup page");
        }
        Some((config, settings::Source::Environment)) => {
            tracing::info!(kind = %config.kind, base = %config.base, "cloud provider, from the environment");
        }
        None => tracing::warn!("no cloud provider is configured: no account can be linked"),
    }
    if app_state.settings.env_default().is_none() && std::env::var("COLLABCRAFT_PROVIDER_URL").is_ok_and(|v| !v.trim().is_empty()) {
        tracing::warn!("COLLABCRAFT_PROVIDER_URL is set to something that is not a usable server address");
    }
    if app_state.settings.admin_groups().is_empty() {
        tracing::info!("COLLABCRAFT_ADMIN_GROUPS is not set: nobody may change the cloud provider from the setup page");
    }
    if app_state.keyed.enabled() {
        tracing::info!(ttl_secs = app_state.keyed.ttl_secs, uses = app_state.keyed.max_uses, "keyed links are enabled");
    } else {
        tracing::info!("keyed links are off: a share to an address with no account will mint nothing");
    }

    let api = axum::Router::new()
        .route("/me", axum::routing::get(api::me))
        .route("/files", axum::routing::get(api::list))
        .route("/files/shared", axum::routing::get(api::shared_with_me))
        .route("/files/content", axum::routing::get(api::read).put(api::write))
        .route("/files/meta", axum::routing::get(api::file_meta))
        .route("/files/new", axum::routing::post(api::create))
        .route("/contacts", axum::routing::get(api::contacts))
        .route("/conversation", axum::routing::get(api::conversation))
        .route("/shares", axum::routing::get(api::shares).post(api::share))
        .route("/shares/{id}", axum::routing::delete(api::unshare))
        .route("/token", axum::routing::get(api::token))
        .route("/apps", axum::routing::get(api::apps))
        // The recipient of a keyed link. Separate routes rather than the ones
        // above, because those act as the signed-in user against the provider
        // and a recipient is not one.
        .route("/invite/token", axum::routing::get(recipient::token))
        .route("/invite/content", axum::routing::get(recipient::read).put(recipient::write))
        .nest("/esign", esign::router())
        // Axum caps a request body at 2 MiB by default, so a save of a real
        // document (a drawing, a sheet with images) was refused with 413 before
        // it ever reached the provider. Raise it to a document-sized ceiling.
        // The provider's own transport still bounds what it will read back.
        .layer(axum::extract::DefaultBodyLimit::max(MAX_SAVE_BYTES));

    let auth_routes = axum::Router::new()
        .route("/login", axum::routing::get(auth::login))
        .route("/callback", axum::routing::get(auth::callback))
        .route("/logout", axum::routing::post(auth::logout))
        .route("/device/start", axum::routing::post(auth::device_start))
        .route("/device/poll", axum::routing::post(auth::device_poll))
        .route("/link/start", axum::routing::post(auth::link_start))
        .route("/link/poll", axum::routing::post(auth::link_poll))
        .route("/link/password", axum::routing::post(auth::link_password))
        .route("/link", axum::routing::delete(auth::unlink));

    // The web face. Server rendered out of this binary, so the service answers
    // a browser as well as an app. These are real routes, so each one wins
    // over the static fallback below.
    let web = axum::Router::new()
        .route("/", axum::routing::get(pages::launcher))
        // Where a hand-written entry page used to sit in the web root.
        .route("/index.html", axum::routing::get(pages::index_html))
        .route("/account", axum::routing::get(pages::account))
        .route("/account/link", axum::routing::post(pages::link_begin))
        .route("/account/link/finish", axum::routing::post(pages::link_finish))
        .route("/account/link/password", axum::routing::post(pages::link_password))
        .route("/account/link/google", axum::routing::get(pages::google_finish))
        .route("/account/unlink", axum::routing::post(pages::unlink))
        .route("/account/connection/default", axum::routing::post(pages::connection_default))
        .route("/account/signout", axum::routing::post(pages::signout))
        // Where a desktop sign-in is approved by its user code.
        .route("/device", axum::routing::get(auth::device_page).post(auth::device_approve))
        .route("/settings/provider", axum::routing::get(pages::provider).post(pages::provider_save))
        .route("/settings/apps", axum::routing::get(pages::apps).post(pages::apps_save));

    // with_state goes last, after every route including the fallback.
    // Applying it earlier leaves the fallback still wanting state, and the
    // router then will not serve at all.
    let app = axum::Router::new()
        // Health includes the bus. A static "ok" kept answering after Valkey
        // went away, and a deploy gate reading it then called a relay healthy
        // that could not order a single edit.
        .route(
            "/healthz",
            axum::routing::get(move || {
                let relay = health_relay.clone();
                async move { health(&relay).await }
            }),
        )
        .merge(web)
        .nest("/api/v1", api)
        .nest("/auth", auth_routes)
        // Where a keyed link lands. A real route, so it wins over the static
        // fallback below.
        .route("/invite/{key}", axum::routing::get(recipient::offer).post(recipient::open))
        .route("/esign/invite/{key}", axum::routing::get(esign::land_signer))
        .fallback(api::web)
        .with_state(app_state);

    // Everything, health and static builds included, lives under the base path when one is
    // set, so a reverse proxy can forward `/craft` without rewriting anything.
    let app = if base.is_empty() { app } else { axum::Router::new().nest(&base, app) };
    let app = app.layer(layer);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "relay listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    Ok(())
}

/// Admit a peer, or disconnect it.
async fn on_connect(socket: SocketRef, Data(handshake): Data<Handshake>, State(relay): State<Arc<Relay>>) {
    if let Err(detail) = check_proto(&handshake) {
        tracing::warn!("rejecting a connection: protocol version mismatch");
        let _ = socket.emit("error", &RefusedMsg { doc: String::new(), detail: detail.to_owned(), code: "proto" });
        let _ = socket.disconnect();
        return;
    }
    if let Err(detail) = check_schema(&handshake) {
        tracing::warn!("rejecting a connection: malformed replica schema");
        let _ = socket.emit("error", &RefusedMsg { doc: String::new(), detail: detail.to_owned(), code: CODE_SCHEMA });
        let _ = socket.disconnect();
        return;
    }
    let now = now_unix();
    let claims = match token::verify(&handshake.token, &relay.secret, now) {
        Ok(c) => c,
        Err(e) => {
            // The reason goes to the log, not to the peer. A caller learning
            // which check failed learns something about the secret.
            tracing::warn!(error = %e, "rejecting a connection");
            let _ = socket.emit("error", &RefusedMsg { doc: String::new(), detail: "token rejected".to_owned(), code: "auth" });
            let _ = socket.disconnect();
            return;
        }
    };

    let room_name = claims.room();
    let room_key = claims.rm.clone();
    let peer_id = claims.pid.clone();

    // Register the handlers and stash the claims BEFORE any await below.
    //
    // This ordering is the whole correctness of the join. The old code emitted
    // `joined` and then awaited a presence broadcast and a Valkey SCAN before
    // registering anything. A client does not wait: it receives `joined` and
    // immediately sends `lock` and `submit`. Those packets arrived at a socket
    // with no handler for them and Socket.IO dropped them silently, with no
    // error anywhere, so an edit simply never happened. Three runs in a row
    // lost the submit that way.
    //
    // Registering first costs nothing. A handler that fires before the room
    // entry exists still works, because every handler uses `entry().or_default()`
    // and reads the claims it needs from the socket's own extensions.
    socket.extensions.insert(claims.clone());
    socket.on("submit", on_submit);
    socket.on("lock", on_lock);
    socket.on("unlock", on_unlock);
    socket.on("refresh", on_refresh);
    socket.on("resync", on_resync);
    socket.on("frame", on_frame);
    socket.on("ephemeral", on_ephemeral);
    socket.on("saved", on_saved);
    socket.on("alive", on_alive);
    socket.on_disconnect(on_disconnect);
    // The socket joins the broadcast room only below, under the rooms lock, once its replay
    // is fixed. Joined earlier, a live op broadcast during the bus calls in between reached it
    // before `joined`, and the session it reached had nowhere to put it.

    let locks = {
        let mut bus = relay.bus.lock().await;
        bus.holders(&room_key).await.unwrap_or_else(|e| {
            tracing::error!(error = %e, room = %room_key, "could not read lock holders");
            Vec::new()
        })
    };

    // One id space per connection, from the bus, so two relays cannot grant the same one and a
    // restart cannot hand one out twice. A client that derived its own from its peer id could
    // collide with another peer's, and the two would then mint the same character ids and drop
    // each other's text.
    let space = {
        let mut bus = relay.bus.lock().await;
        match bus.next_space(&room_key, handshake.space.unwrap_or(0)).await {
            Ok(s) => s,
            Err(e) => {
                // Refuse rather than admit a peer with no space. Admitting it would mean
                // either silent divergence or a window that cannot type, and neither is
                // something a user could act on.
                tracing::error!(error = %e, room = %room_key, "could not grant an id space");
                let _ = socket.emit(
                    "error",
                    &ErrorMsg { doc: room_name.clone(), detail: "relay could not start a session".to_owned(), local: None, code: Some(CODE_RETRY) },
                );
                let _ = socket.disconnect();
                return;
            }
        }
    };

    // Where the document already was, read from the bus before the first peer
    // joins. A room rebuilt in memory starts at sequence zero, and handing out
    // a number a client has already seen is what made a reopen replay or stall.
    //
    // A failed read refuses the connection as transient rather than reading as zero: zero
    // would make a room rebuilt after a restart look like a brand new document, and a joiner
    // would then take the saved file as current next to replicas that are not on it.
    //
    // A rejoining replica that has applied more than the counter holds raises it to what it
    // applied (R2, the sequence floor): the counter was lost or went backwards (a wiped volume,
    // a Valkey restart inside the `everysec` window), and numbering on from below would make
    // every replica in the session drop the next ops as replays, without any error. It is
    // raised [`bus::SEQ_RAISE_MARGIN`] past the claim, so no number handed out before the loss
    // is handed out again, and every replica from before it (this one included) rejoins
    // through the lost history with the retained text. The room is lifted to that sequence
    // (see below).
    //
    // Only a peer that may edit raises it: a read-only ticket must not be able to move a
    // document's ordering. A claim past [`bus::MAX_SEQ_CLAIM`] (which leaves room for the margin
    // below [`bus::MAX_SEQ_FLOOR`]) is refused, and the join is then a fresh load.
    let applied = handshake.applied.filter(|&applied| applied <= bus::MAX_SEQ_CLAIM);
    let claimed = if claims.may_edit() { applied.unwrap_or(0) } else { 0 };
    //
    // Which join may raise, and from what (fix round 3):
    //
    // * A join that rebuilds the room (none in memory) asks the bus, which also raises when
    //   Valkey restarted since the room was last rebuilt ([`Bus::rebuild_seq`]): the counter
    //   may then trail numbers replicas from before the restart still hold, even when this
    //   joiner claims nothing past it.
    // * A join to a live room raises only when the claim is past both the counter and the live
    //   room's own sequence. The live room holds the true sequence, so a counter that merely
    //   trails it (Valkey hard-killed under a running relay) is healed by the next `next_seq`
    //   and needs no lift.
    let live_seq = relay.rooms.lock().await.get(&room_key).map(Room::seq);
    let (retained_seq, raised) = {
        let mut bus = relay.bus.lock().await;
        let read = match live_seq {
            None => bus.rebuild_seq(&room_key, claimed).await,
            Some(live) => match bus.current_seq(&room_key).await {
                Ok(seq) if claimed > seq && claimed > live => {
                    let target = claimed + bus::SEQ_RAISE_MARGIN;
                    bus.raise_seq(&room_key, target).await.map(|raised| (raised.max(target), true))
                }
                other => other.map(|seq| (seq, false)),
            },
        };
        if let Ok((seq, true)) = read {
            tracing::warn!(room = %room_key, seq, applied = claimed, rebuilt = live_seq.is_none(), "the sequence counter could not be trusted; raised it");
        }
        match read {
            Ok(read) => read,
            Err(e) => {
                tracing::error!(error = %e, room = %room_key, "could not read the document's sequence");
                let _ = socket.emit(
                    "error",
                    &ErrorMsg { doc: room_name.clone(), detail: "relay could not start a session".to_owned(), local: None, code: Some(CODE_RETRY) },
                );
                let _ = socket.disconnect();
                return;
            }
        }
    };

    // The room's gate is held until the socket has joined the broadcast room, after the
    // replay. A submit holds it from recording its op to broadcasting it, so every op is
    // either in this replay (and maybe broadcast again, which the client drops by its
    // sequence) or broadcast after this socket joined and was sent `joined`. None is lost,
    // none lands twice. The global rooms lock is released before anything is serialised.
    let gate = relay.gate(&room_key);
    let ordered = gate.lock().await;
    let mut rooms = relay.rooms.lock().await;
    let (joined, lifted) = {
        let room = relay.room(&mut rooms, &room_key);
        // A raise lifts the room past every number it has (its own ops, or the old sequence its
        // members joined at: a fresh loader, a viewer, an earlier rejoiner), so neither a
        // replay nor the room's members can carry on across it. The room is lifted into a lost
        // history: see the catch-up below and `lifted` after it.
        let lifted = if raised {
            room.lift_seq(retained_seq)
        } else {
            room.observe_seq(retained_seq);
            false
        };
        let peer = Peer { peer: peer_id.clone(), display: claims.dsp.clone(), may_edit: claims.may_edit(), uid: claims.uid.clone() };
        let schema = handshake.schema.as_deref();
        let joined = match room.check_schema(schema).and_then(|()| room.join_as(peer, &socket.id.to_string())) {
            Ok(()) => {
                room.pin_schema(schema);
                for (scope, holder) in &locks {
                    room.record_holder(scope.clone(), holder.clone());
                }
                // See `catch_up` for which baseline and why. A read-only peer that claims more
                // than the document holds is caught up as a fresh load: it may not raise the
                // sequence, and resuming past it would make it drop the next ops as replays.
                let applied = applied.filter(|&applied| claims.may_edit() || applied <= room.seq());
                let baseline = applied.unwrap_or_else(|| room.saved_seq());
                let catch = if lifted {
                    // The ops the room ordered before the lift are in no replay this joiner
                    // can take; its retained text is, and the lost history makes the saver
                    // checkpoint.
                    CatchUp { seq: room.seq(), replay: Vec::new(), texts: retained_texts(room), history_lost: true }
                } else {
                    catch_up(room, baseline)
                };
                Ok((catch, room.peers().to_vec(), room.ephemeral()))
            }
            Err(refusal) => Err(refusal),
        };
        (joined, lifted)
    };

    drop(rooms);
    let (CatchUp { seq, replay, texts, history_lost }, roster, ephemeral) = match joined {
        Ok(joined) => joined,
        Err(refusal) => {
            drop(ordered);
            let code = if matches!(refusal, Refusal::Schema { .. }) {
                tracing::warn!(room = %room_key, "refusing a peer: it edits with another replica schema than the room's");
                CODE_SCHEMA
            } else {
                tracing::warn!(room = %room_key, "refusing a peer: the document is full");
                CODE_FULL
            };
            let _ = socket.emit("error", &ErrorMsg { doc: room_name.clone(), detail: refusal.detail(), local: None, code: Some(code) });
            let _ = socket.disconnect();
            return;
        }
    };

    // Mark this socket as having actually reached `Room::join`. `on_disconnect` is registered
    // near the top of this function, well before the bus calls that can still refuse the
    // connection (`next_space`, above). A socket disconnected for one of those reasons never
    // joined the room at all, and without this marker its disconnect would still call
    // `Room::leave` for its peer id, which can evict a different, live connection sharing that
    // id and drop its locks.
    socket.extensions.insert(JoinedRoom);

    let _ = socket.emit(
        "joined",
        &JoinedMsg {
            doc: room_name.clone(),
            peer: &peer_id,
            seq,
            space,
            roster: &roster,
            locks: locks.iter().map(|(s, h)| LockEntry { scope: s.clone(), holder: h.clone() }).collect(),
            replay_until: replay.last().map_or(seq, |op| op.seq),
            fallback: !texts.is_empty(),
            text_cap: relay.text_cap,
            heartbeat: true,
            history_lost,
        },
    );

    // The ordered ops since the saved version. The joiner applies these on top
    // of the file it loads, exactly as it would a live edit, so the edits made
    // since the last save are not lost. Empty on the fallback path below.
    // Every one queued on the socket before any is written, so the socket's buffer is sized
    // to hold the longest replay (see [`SOCKET_BUFFER`]). Should one still not fit, the
    // join is cut below rather than left with a gap.
    let mut caught_up = true;
    for op in &replay {
        caught_up &= socket.emit("op", &OpMsg::new(&claims, op)).is_ok();
    }

    // The fallback catch-up for text, used only when the ordered replay above
    // could not be built. The file the joiner opens is behind the edits made
    // since it was written, and the sequence is already past those edits, so
    // these carry no sequence.
    // In pieces no larger than a client can read in one WebSocket frame: a scope can retain
    // up to three caps (edits plus two snapshots). Only the first piece can start with the
    // snapshot, and a client treats the rest as ordinary updates.
    // `more` on every piece but the last, so a client knows when the catch-up is over and a
    // link lost midway re-arms it on the rejoin.
    for (scope, ops, complete, snap) in texts {
        let pieces = text_pieces(&ops, TEXT_OPS_PIECE_BYTES);
        let last = pieces.len().saturating_sub(1);
        for (i, piece) in pieces.into_iter().enumerate() {
            let sent = socket.emit(
                "text_ops",
                &serde_json::json!({ "doc": room_name.clone(), "scope": scope, "ops": piece, "complete": complete, "snap": snap && i == 0, "more": i < last }),
            );
            caught_up &= sent.is_ok();
        }
    }
    if !caught_up {
        // A catch-up with a hole in it would leave the joiner believing it is current. Cut
        // the link instead: the client reconnects and the join sends it all again.
        drop(ordered);
        tracing::warn!(room = %room_key, "a join's catch-up did not fit the socket's buffer; disconnecting it");
        let _ = socket.disconnect();
        return;
    }
    socket.join(room_name.clone());
    drop(ordered);

    if lifted {
        // The room's other members are on the ops ordered before the lift, and the next op is
        // numbered past it, so each would hold every later op and wait in silence for the gap
        // to fill. Closed now, they reconnect at once and rejoin through the lost history,
        // with their unconfirmed edits sent again.
        tracing::warn!(room = %room_key, seq, "the sequence was lifted past the room's own ops; reconnecting its other members");
        let _ = socket.to(room_name.clone()).disconnect().await;
    }

    // And on where everybody is, so somebody arriving late does not have to wait for each
    // person to move before their marker appears.
    for (peer, kind, payload) in ephemeral {
        let _ = socket.emit("ephemeral", &serde_json::json!({ "doc": room_name.clone(), "origin": peer, "kind": kind, "payload": payload }));
    }

    emit_live(socket.to(room_name.clone()), "presence", &serde_json::json!({ "doc": room_name, "roster": roster }));

    tracing::info!(uid = %claims.uid, peer = %peer_id, room = %room_key, may_edit = claims.may_edit(), history_lost, "peer joined");
}

/// The health route's answer: 200 while the bus answers a `PING`, 503 while it does not.
///
/// Bounded, so a bus that hangs reads as down rather than holding the route open.
async fn health(relay: &Relay) -> (axum::http::StatusCode, &'static str) {
    let pong = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut bus = relay.bus.lock().await;
        bus.ping().await
    })
    .await;
    match pong {
        Ok(Ok(())) => (axum::http::StatusCode::OK, "ok"),
        _ => (axum::http::StatusCode::SERVICE_UNAVAILABLE, "the bus is unreachable"),
    }
}

/// What a join is caught up with.
#[derive(Debug, PartialEq)]
struct CatchUp {
    /// `Joined.seq`: the sequence the catch-up brings the joiner to before its replay.
    seq: u64,
    /// The ordered replay after `seq`.
    replay: Vec<Op>,
    /// The fallback instead of a replay, per scope: `(scope, ops, complete, starts with a snapshot)`.
    texts: Vec<(String, Vec<serde_json::Value>, bool, bool)>,
    /// `Joined.history_lost`, see [`Room::history_lost`].
    history_lost: bool,
}

/// The catch-up for a joiner at `baseline`.
///
/// Catch the joiner up from where IT already is, not from where the saved file is. A
/// connection resuming a session knows what it has already applied, and that is the only
/// baseline that is correct for it: the saved file's sequence can be ahead of it (a save
/// happened while it was gone, and resuming there would skip the edits made before that save
/// that it never saw) or behind it (it is already caught up, and resuming at the file's
/// sequence would re-send ops it has). A fresh load has no applied sequence of its own to
/// offer, and the file it just read is at `saved_seq`, so that is the only baseline available
/// to it. Either way the ops after the baseline bring it current, replayed as ordered ops so
/// the path is the same one a live edit already takes. When the history no longer reaches
/// that baseline, or a burned sequence broke the run, there is nothing clean to replay, so the
/// join falls back to the current sequence and the retained text: the joiner then leans on the
/// file alone, the older behaviour.
///
/// A baseline before this room's own history (the relay restarted, or the room was dropped
/// after its grace period) is different: what this room retained builds on ops it no longer
/// has, so it cannot be laid over the file. Such a joiner starts from a snapshot ordered in
/// this room when there is one, and otherwise gets the room's own retained text as the
/// fallback and is told the history is lost: a replica that held the document merges it, and a
/// fresh one waits for the snapshot.
///
/// Scopes that would tell a joiner nothing (complete and empty, as a save leaves them) are not
/// sent, so they do not take a message each.
fn catch_up(room: &Room, baseline: u64) -> CatchUp {
    let history_lost = room.history_lost();
    if baseline < room.inherited() && room.saved_seq() < room.inherited() {
        // With no snapshot yet, the room's own ops go out as the ordinary fallback text. A
        // replica that held the document before merges them (they build on what it has); a
        // fresh one waiting for the snapshot drops them.
        let texts = if history_lost {
            retained_texts(room)
        } else {
            room.texts_from_snapshot().into_iter().map(|(scope, ops)| (scope, ops, true, true)).collect()
        };
        return CatchUp { seq: room.seq(), replay: Vec::new(), texts, history_lost };
    }
    match room.since(baseline) {
        Some(replay) => CatchUp { seq: baseline, replay, texts: Vec::new(), history_lost },
        None => CatchUp { seq: room.seq(), replay: Vec::new(), texts: retained_texts(room), history_lost },
    }
}

/// The room's retained text per scope, as the fallback catch-up sends it, without the scopes
/// that would tell a joiner nothing.
fn retained_texts(room: &Room) -> Vec<(String, Vec<serde_json::Value>, bool, bool)> {
    let mut texts: Vec<(String, Vec<serde_json::Value>, bool, bool)> = room
        .texts()
        .iter()
        .filter(|(_, log)| !log.is_idle())
        .map(|(scope, log)| (scope.clone(), log.ops().to_vec(), log.complete(), log.starts_with_snapshot()))
        .collect();
    texts.sort_by(|a, b| a.0.cmp(&b.0));
    texts
}

async fn on_submit(socket: SocketRef, Data(msg): Data<SubmitMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    let room_name = claims.room();
    // Echoed on a refusal, so the sender can tell which of its submits was refused.
    let local = msg.local;

    // Arbitrate BEFORE taking a sequence number. INCR is not refundable, so a
    // submit refused after the increment would burn that number and leave a
    // permanent gap. Every client holds an out-of-order op waiting for its
    // predecessor, so a gap that never arrives stalls the document until
    // somebody asks for a resync.
    //
    // This pre-flight reads the local lock mirror. The authoritative check is
    // still Room::submit below, so a scope another instance locked without this
    // relay seeing it can get past here. That residual case burns one number
    // and recovers by resync, which is the rare path rather than the common
    // one.
    // crdt.* commands merge; a scope on them is a client bug and is stripped
    // before the lock check so a held region cannot refuse the edit.
    let scope = if is_crdt_command(&msg.command) { None } else { msg.scope };
    if is_snapshot(&msg.command, &msg.params) && !saved_allowed(&claims, &relay.service_uid) {
        let refusal = if claims.may_edit() { Refusal::NotTheSaver } else { Refusal::ReadOnly };
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: refusal.detail(), local, code: None });
        return;
    }
    let preflight = match &scope {
        Some(scope_name) => {
            let mirrored = {
                let rooms = relay.rooms.lock().await;
                rooms.get(&claims.rm).and_then(|r| r.holder_of(scope_name)).map(str::to_owned)
            };
            match mirrored {
                Some(holder) if holder != claims.pid => {
                    // The mirror is refreshed only on a local grant, refusal or release, so a
                    // lease that lapsed naturally in Valkey (its holder's socket went away
                    // without unlocking) leaves the mirror pointing at a holder who no longer
                    // has anything. Confirmed against the bus before refusing on it, rather
                    // than trusted outright: unlike the residual case above, a stale refusal
                    // here is a region the room stays wedged on until something else happens
                    // to touch the mirror, not a one-off burned sequence a resync clears up.
                    let current = {
                        let mut bus = relay.bus.lock().await;
                        bus.current_holder(&claims.rm, scope_name).await
                    };
                    match current {
                        Ok(Some(still)) if still != claims.pid => {
                            let mut rooms = relay.rooms.lock().await;
                            relay.room(&mut rooms, &claims.rm).record_holder(scope_name.clone(), still.clone());
                            Err(Refusal::Locked { holder: still })
                        }
                        Ok(current_holder) => {
                            // Either the bus now agrees this peer holds it, or the lease
                            // lapsed and nobody does. Either way the mirror was wrong; forget
                            // it so later submits to this scope do not pay this round trip
                            // again.
                            {
                                let mut rooms = relay.rooms.lock().await;
                                relay.room(&mut rooms, &claims.rm).forget_lock(scope_name);
                            }
                            if current_holder.is_none() {
                                // The lease lapsed on its own and nobody holds the scope now.
                                // The room still believes `holder` does, from the grant it was
                                // told about with no matching release ever heard, so the lapse
                                // is announced the same way a real unlock is.
                                emit_live(
                                    socket.within(room_name.clone()),
                                    "lock_released",
                                    &serde_json::json!({ "doc": room_name, "scope": scope_name }),
                                );
                            }
                            if claims.may_edit() { Ok(()) } else { Err(Refusal::ReadOnly) }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, room = %claims.rm, scope = %scope_name, "could not confirm a mirrored lock; trusting the mirror");
                            Err(Refusal::Locked { holder })
                        }
                    }
                }
                _ if !claims.may_edit() => Err(Refusal::ReadOnly),
                _ => Ok(()),
            }
        }
        None if !claims.may_edit() => Err(Refusal::ReadOnly),
        None => Ok(()),
    };
    if let Err(refusal) = preflight {
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: refusal.detail(), local, code: None });
        return;
    }

    // A yrs update that does not decode, or whose ids overflow, is refused here: ordered, it
    // would be replayed to every joiner and abort the browser replicas that decode it. Before
    // the sequence number, so nothing is burned.
    // Counted first, without decoding: past the block cap an op is too large to check.
    let sound = if room::value_bytes(&msg.params) > BLOCKING_CHECK_BYTES {
        let (command, params) = (msg.command.clone(), msg.params.clone());
        tokio::task::spawn_blocking(move || if crdt_ops_too_large(&command, &params) { None } else { Some(crdt_ops_sound(&command, &params)) })
            .await
            .unwrap_or(Some(false))
    } else if crdt_ops_too_large(&msg.command, &msg.params) {
        None
    } else {
        Some(crdt_ops_sound(&msg.command, &msg.params))
    };
    let Some(sound) = sound else {
        tracing::warn!(room = %claims.rm, peer = %claims.pid, "refused an op with more blocks than a check takes");
        let detail = Refusal::TooLarge { bytes: room::value_bytes(&msg.params) }.detail();
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail, local, code: None });
        return;
    };
    if !sound {
        tracing::warn!(room = %claims.rm, peer = %claims.pid, "refused an op that is not a valid document update");
        let _ = socket.emit(
            "error",
            &ErrorMsg { doc: room_name, detail: "that change is not a valid document update".to_owned(), local, code: Some(CODE_INVALID) },
        );
        return;
    }

    // Taken before the sequence, and held only while the op is numbered and recorded.
    //
    // Numbering inside the gate keeps the room recording ops in sequence order. Numbered
    // outside it, seq 10 could be recorded after seq 11 and after a join that read the room at
    // 11: the joiner's replay ends at 11, it drops the broadcast of 10 as already seen, and
    // the op is lost to it. With both inside, the op a join reads last is the highest number
    // handed out so far, and every later one is recorded after the join is done.
    //
    // A join holds the gate from reading its replay until its socket is in the broadcast
    // room, so an op recorded before the join read is in its replay (a later broadcast copy
    // is dropped by sequence), and one recorded after had to wait for the join to finish, so
    // its broadcast, whenever it goes out, reaches the joined socket. The lock refresh and the
    // broadcast below run without the gate.
    let gate = relay.gate(&claims.rm);
    let mut ordered = gate.lock().await;

    // A fenced submit (an ordered edit) is ordered only while its sender holds the scope under
    // the grant's token. Checked on the bus, inside the gate and before the sequence: a holder
    // that was paused or cut off past its lease, and whose scope was granted again since, is
    // refused here and its edit is never numbered, ordered or replayed. The check extends the
    // lease, so nobody can be granted the scope between it and the op being recorded.
    if let Some(fence) = &msg.fence {
        let held = {
            let mut bus = relay.bus.lock().await;
            bus.check_fence(&claims.rm, &fence.scope, &claims.pid, fence.token).await
        };
        match held {
            Ok(true) => {}
            Ok(false) => {
                drop(ordered);
                tracing::info!(room = %claims.rm, peer = %claims.pid, scope = %fence.scope, token = fence.token, "refused a fenced edit from a peer that no longer holds the lock");
                let detail = format!("you no longer hold {}; that change was not shared", fence.scope);
                let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail, local, code: Some(CODE_FENCED) });
                return;
            }
            Err(e) => {
                drop(ordered);
                tracing::error!(error = %e, room = %claims.rm, "could not check a fence");
                let _ = socket
                    .emit("error", &ErrorMsg { doc: room_name, detail: "relay could not order that edit".to_owned(), local, code: Some(CODE_RETRY) });
                return;
            }
        }
    }

    // Tried on the room's shadow copy under the gate, so the copy takes ops in the order they
    // are numbered, and before the sequence, so a refusal burns nothing. The gate above reads an
    // op alone; this is what catches an op that decodes but breaks yrs in the document it lands
    // in. See [`craft_collab::shadow`]. Only an op the room will record is tried (the room is
    // asked first), and the copy is rolled back if the op is not ordered after all, so the copy
    // holds exactly what peers are sent.
    let mut mark = None;
    if relay.gates.shadow
        && let Some(updates) = crdt_updates(&msg.command, &msg.params)
    {
        let (schema, room_says) = {
            let rooms = relay.rooms.lock().await;
            let room = rooms.get(&claims.rm);
            let says = room.map_or(Ok(()), |r| r.precheck(claims.may_edit(), &msg.command, room::value_bytes(&msg.params)));
            (room.and_then(Room::schema).map(str::to_owned), says)
        };
        // Refused by the room below, with its own message; the copy is not asked.
        if room_says.is_ok() {
            let state = is_snapshot(&msg.command, &msg.params);
            match admit_on_shadow(&relay.gates, &mut ordered, (&claims.rm, &claims.uid), schema, updates, state).await {
                Ok(m) => mark = m,
                Err(stop) => {
                    drop(ordered);
                    let (detail, code) = match stop {
                        ShadowStop::Refused(refusal) => {
                            tracing::warn!(room = %claims.rm, peer = %claims.pid, reason = %refusal, "refused an op the shadow copy could not take");
                            ("that change is not a valid document update".to_owned(), Some(CODE_INVALID))
                        }
                        ShadowStop::Busy => {
                            tracing::info!(room = %claims.rm, peer = %claims.pid, "refused an op for now: the shadow copy is at its cap, or this peer is paused");
                            ("relay is busy checking this document; try again shortly".to_owned(), Some(CODE_RETRY))
                        }
                        ShadowStop::TooLarge => {
                            tracing::warn!(room = %claims.rm, peer = %claims.pid, "refused an op too large for the shadow copy to check");
                            (Refusal::TooLarge { bytes: room::value_bytes(&msg.params) }.detail(), None)
                        }
                    };
                    let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail, local, code });
                    return;
                }
            }
        }
    }
    // The sequence comes from Valkey, so two relay instances cannot hand out
    // the same number.
    let floor = {
        let rooms = relay.rooms.lock().await;
        rooms.get(&claims.rm).map(Room::seq).unwrap_or(0)
    };
    let seq = {
        let mut bus = relay.bus.lock().await;
        match bus.next_seq(&claims.rm, floor).await {
            Ok(s) => s,
            Err(e) => {
                unadmit(&relay.gates, &mut ordered, mark);
                drop(ordered);
                tracing::error!(error = %e, room = %claims.rm, "could not assign a sequence");
                let _ = socket
                    .emit("error", &ErrorMsg { doc: room_name, detail: "relay could not order that edit".to_owned(), local, code: Some(CODE_RETRY) });
                return;
            }
        }
    };

    let outcome = {
        let mut rooms = relay.rooms.lock().await;
        let room = relay.room(&mut rooms, &claims.rm);
        room.submit(
            seq,
            Submission {
                peer_id: claims.pid.clone(),
                may_edit: claims.may_edit(),
                command: msg.command,
                params: msg.params,
                scope,
                local: msg.local,
            },
        )
    };
    if outcome.is_err() {
        unadmit(&relay.gates, &mut ordered, mark);
    }
    drop(ordered);

    match outcome {
        Ok(op) => {
            // Editing a scope you hold refreshes the lock. The TTL is 60s, so
            // without this a holder who keeps typing would lose the lock under
            // themselves and another peer could take the region mid-edit. Doing
            // it here needs no timer, because an active holder is by definition
            // submitting.
            if let Some(scope) = op.scope.as_deref() {
                let mut bus = relay.bus.lock().await;
                if let Err(e) = bus.refresh_lock(&claims.rm, scope, &claims.pid).await {
                    tracing::warn!(error = %e, scope, "could not refresh a held lock");
                }
            }
            // To the whole room, sender included. The sender recognises its own
            // op by the echoed local counter and confirms rather than reapplies.
            emit_live(socket.within(room_name), "op", &OpMsg::new(&claims, &op));
        }
        Err(refusal) => {
            let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: refusal.detail(), local, code: None });
        }
    }
}

#[derive(Debug, Serialize)]
struct OpMsg<'a> {
    doc: String,
    seq: u64,
    origin: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    local: Option<u64>,
    command: &'a str,
    params: &'a serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'a str>,
}

impl<'a> OpMsg<'a> {
    fn new(claims: &Claims, op: &'a Op) -> OpMsg<'a> {
        OpMsg {
            doc: claims.room(),
            seq: op.seq,
            origin: &op.origin,
            local: op.local,
            command: &op.command,
            params: &op.params,
            scope: op.scope.as_deref(),
        }
    }
}

async fn on_lock(socket: SocketRef, Data(msg): Data<ScopeMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    let room_name = claims.room();

    if !claims.may_edit() {
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: Refusal::ReadOnly.detail(), local: None, code: None });
        return;
    }

    let (outcome, lease) = {
        let mut bus = relay.bus.lock().await;
        (bus.try_lock(&claims.rm, &msg.scope, &claims.pid).await, bus.lock_ttl())
    };

    match outcome {
        Ok(LockOutcome::Granted { token }) => {
            // The room's sequence at the moment of the grant, read in the same rooms lock that
            // records the holder. The ordering gate is held across the read, so an op that was
            // numbered before this grant is recorded, and counted, before the seq is read: the
            // holder knows how far to apply before it edits on what the lock protects. The wait
            // can be as long as one op's number plus one join's replay, since both hold the gate.
            //
            // The requester may have left while it waited. The holder is recorded only if its
            // connection is still in the room, checked under the rooms lock that `on_disconnect`
            // also takes, so a disconnect either finds the record and releases it, or happens
            // first and is seen here. Otherwise the lease taken above is given back now.
            let seq = {
                let gate = relay.gate(&claims.rm);
                let _ordered = gate.lock().await;
                let mut rooms = relay.rooms.lock().await;
                match rooms.get_mut(&claims.rm).filter(|room| room.has_conn(&claims.pid, &socket.id.to_string())) {
                    Some(room) => {
                        room.record_lock(&claims.pid, msg.scope.clone());
                        Some(room.seq())
                    }
                    None => None,
                }
            };
            let Some(seq) = seq else {
                let mut bus = relay.bus.lock().await;
                let _ = bus.release_fenced(&claims.rm, &msg.scope, &claims.pid, token).await;
                return;
            };
            // `token` fences the holder's ordered edits (see `on_submit`); `lease_ms` is how long
            // the lease lasts unrefreshed, so the holder knows when to refresh it.
            let lease_ms = u64::try_from(lease.as_millis()).unwrap_or(u64::MAX);
            let _ = socket
                .emit("lock_granted", &serde_json::json!({ "doc": room_name, "scope": msg.scope, "seq": seq, "token": token, "lease_ms": lease_ms }));
            emit_live(
                socket.to(room_name.clone()),
                "presence_lock",
                &serde_json::json!({ "doc": room_name, "scope": msg.scope, "holder": claims.pid }),
            );
        }
        Ok(LockOutcome::Held { holder }) => {
            {
                let mut rooms = relay.rooms.lock().await;
                relay.room(&mut rooms, &claims.rm).record_holder(msg.scope.clone(), holder.clone());
            }
            let _ = socket.emit("lock_denied", &serde_json::json!({ "doc": room_name, "scope": msg.scope, "holder": holder }));
        }
        Err(e) => {
            tracing::error!(error = %e, room = %claims.rm, "lock failed");
            let _ = socket.emit(
                "error",
                &ErrorMsg { doc: room_name, detail: "relay could not take that lock".to_owned(), local: None, code: Some(CODE_RETRY) },
            );
        }
    }
}

async fn on_unlock(socket: SocketRef, Data(msg): Data<UnlockMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    let room_name = claims.room();

    let bus_released = {
        let mut bus = relay.bus.lock().await;
        match msg.token {
            // Fenced: only the grant named. A stale unlock for a scope this peer holds again
            // under a newer token releases nothing, on the bus or in the mirror.
            Some(token) => match bus.release_fenced(&claims.rm, &msg.scope, &claims.pid, token).await {
                Ok(ReleaseOutcome::Released) => true,
                Ok(ReleaseOutcome::Absent) => false,
                Ok(ReleaseOutcome::NotYours) => return,
                // Not known whether it was released: fail closed (the lease is the backstop,
                // and announcing a release that did not happen would send others to a busy lock).
                Err(e) => {
                    tracing::warn!(error = %e, room = %claims.rm, scope = %msg.scope, token, "could not release a fenced lock");
                    return;
                }
            },
            None => bus.release(&claims.rm, &msg.scope, &claims.pid).await.unwrap_or(false),
        }
    };
    let mirror_released = {
        let mut rooms = relay.rooms.lock().await;
        relay.room(&mut rooms, &claims.rm).unlock(&claims.pid, &msg.scope)
    };

    // A real release on the bus is announced, as before. One where the lease had already
    // lapsed on its own in Valkey, so there was nothing left for the bus to delete, but the
    // room's mirror still recorded this peer as the scope's holder, is announced too: the
    // mirror changing here is the room's only chance to hear the scope is free, since the
    // lapse itself told nobody. Neither flag announces somebody else's unlock: a stale or
    // wrong peer id matches neither the bus's compare-and-delete nor Room::unlock's own holder
    // check, so both stay false and nothing broadcasts, which is what keeps a late unlock for a
    // scope somebody else has since claimed a no-op.
    if bus_released || mirror_released {
        emit_live(socket.within(room_name.clone()), "lock_released", &serde_json::json!({ "doc": room_name, "scope": msg.scope }));
    }
}

/// A holder extends its lease while an ordered run goes on, so a long run (a macro) does not
/// lose the lock. Fenced like a submit: extended only while this peer holds the scope under
/// `token`. When it does not, the holder is told (`lock_lost`), so it stops treating the run as
/// protected before its next edit is refused.
async fn on_refresh(socket: SocketRef, Data(msg): Data<RefreshMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    // A read ticket never holds a lock, so it has nothing to refresh: no bus call for it.
    if !claims.may_edit() {
        return;
    }
    let held = {
        let mut bus = relay.bus.lock().await;
        bus.check_fence(&claims.rm, &msg.scope, &claims.pid, msg.token).await
    };
    match held {
        Ok(true) => {}
        Ok(false) => {
            let _ = socket.emit("lock_lost", &serde_json::json!({ "doc": claims.room(), "scope": msg.scope, "token": msg.token }));
        }
        // Not known either way. The next refresh, or the fenced submit, asks again.
        Err(e) => tracing::warn!(error = %e, room = %claims.rm, scope = %msg.scope, "could not refresh a lease"),
    }
}

async fn on_resync(socket: SocketRef, Data(msg): Data<ResyncMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    let room_name = claims.room();

    let replay = {
        let rooms = relay.rooms.lock().await;
        rooms.get(&claims.rm).and_then(|r| r.since(msg.have))
    };

    match replay {
        Some(ops) => {
            for op in &ops {
                let _ = socket.emit("op", &OpMsg::new(&claims, op));
            }
        }
        None => {
            // The log no longer reaches back that far. A snapshot is the only
            // honest answer, and the relay holds no document, so the peer is
            // told to fetch it from Nextcloud over WebDAV.
            let seq = {
                let rooms = relay.rooms.lock().await;
                rooms.get(&claims.rm).map(Room::seq).unwrap_or(0)
            };
            let _ = socket.emit("snapshot", &serde_json::json!({ "doc": room_name, "seq": seq, "document": serde_json::Value::Null }));
        }
    }
}

/// Pass one opaque frame to the rest of the room.
///
/// Nothing here decodes the payload, assigns it a sequence or keeps a copy. That is the whole
/// point: a caller that signs its own frames and chains each to the previous hash keeps both
/// properties end to end, and a relay that re-read or re-ordered them would break the chain.
/// Not echoed to the sender, which already has it.
async fn on_frame(socket: SocketRef, Data(msg): Data<FrameMsg>) {
    // No relay state: a frame is neither sequenced nor stored, so there is nothing to reach
    // for. Every other handler takes it, and taking it here would suggest otherwise.
    let Some(claims) = live_claims(&socket) else { return };
    let room_name = claims.room();

    if !claims.may_edit() {
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: Refusal::ReadOnly.detail(), local: None, code: None });
        return;
    }
    if msg.payload.len() > MAX_FRAME_BYTES {
        let _ = socket
            .emit("error", &ErrorMsg { doc: room_name, detail: Refusal::TooLarge { bytes: msg.payload.len() }.detail(), local: None, code: None });
        return;
    }

    emit_live(socket.to(room_name.clone()), "frame", &serde_json::json!({ "doc": room_name, "origin": claims.pid, "payload": msg.payload }));
}

/// Record and pass on one latest-wins value about this peer.
///
/// A camera, a selection, a caret. Allowed without edit permission, because somebody watching a
/// design should still be able to say where they are looking. The newest value per peer and kind
/// is kept so a joining peer can be told where everybody is, and nothing here takes a sequence
/// number: ordering ephemeral state would make a stale camera wait behind an edit.
async fn on_ephemeral(socket: SocketRef, Data(msg): Data<EphemeralMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    let room_name = claims.room();
    // The saved marker names the join baseline for everybody, so only the service peer may
    // set it. Nothing about the payload is logged.
    if msg.kind == room::SAVED_KIND {
        if !saved_allowed(&claims, &relay.service_uid) {
            tracing::debug!(room = %claims.rm, "dropping a saved marker from a peer that is not the service");
            return;
        }
        let accepted = {
            let mut rooms = relay.rooms.lock().await;
            relay.room(&mut rooms, &claims.rm).remember_saved(&claims.pid, msg.payload.clone())
        };
        if accepted {
            emit_live(
                socket.to(room_name.clone()),
                "ephemeral",
                &serde_json::json!({ "doc": room_name, "origin": claims.pid, "kind": msg.kind, "payload": msg.payload }),
            );
        }
        return;
    }

    let remembered = {
        let mut rooms = relay.rooms.lock().await;
        relay.room(&mut rooms, &claims.rm).remember_ephemeral(&claims.pid, &msg.kind, msg.payload.clone())
    };
    if let Err(refusal) = remembered {
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: refusal.detail(), local: None, code: None });
        return;
    }

    emit_live(
        socket.to(room_name.clone()),
        "ephemeral",
        &serde_json::json!({ "doc": room_name, "origin": claims.pid, "kind": msg.kind, "payload": msg.payload }),
    );
}

/// Record that the document was saved, so the file's own content is no longer
/// also replayed as operations.
///
/// The client reports the sequence the save reflects. Every retained CRDT
/// operation at or below it is now in the file, so the relay drops it: a joiner
/// that loads the saved file and then receives those operations would otherwise
/// apply them a second time. Requires edit permission: a reader never wrote the
/// file, so it cannot know what the file reflects, and trusting its report would
/// prune another editor's unsaved work out of the replay with no way to tell the
/// two cases apart afterwards. [`Room::checkpoint`] additionally caps the claimed
/// sequence at the room's own, so a save cannot move the join baseline ahead of
/// edits the room has actually ordered.
/// The client's heartbeat. Acknowledged with nothing and not logged: it exists so a client
/// can tell a live link from a half-open one (laptop sleep, a Wi-Fi roam, a NAT that dropped
/// the mapping), where its sends succeed into a socket nothing reads.
///
/// Not acknowledged once the socket's token has lapsed: [`live_claims`] drops the socket
/// instead, as every other handler does, so a socket that only listens and beats cannot outlive
/// its ticket.
async fn on_alive(socket: SocketRef, ack: socketioxide::extract::AckSender) {
    if live_claims(&socket).is_none() {
        return;
    }
    let _ = ack.send(&());
}

async fn on_saved(socket: SocketRef, Data(msg): Data<SavedMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    let doc = claims.room();
    // Answered either way, so the saver knows whether it may write the file: the baseline
    // moved (`saved_ack`) or it did not (`saved_refused`, with the same `seq`).
    //
    // Only the service peer may move the baseline, by the same rule as the snapshot it names
    // ([`saved_allowed`]): an editor never wrote the file, so its report would only ever be a
    // way to prune another editor's unsaved work out of the replay.
    let outcome = if saved_allowed(&claims, &relay.service_uid) {
        let mut rooms = relay.rooms.lock().await;
        match rooms.get_mut(&claims.rm) {
            Some(room) => room.checkpoint(msg.seq).map_err(|refusal| refusal.detail()),
            None => Err("this document is not open on the relay".to_owned()),
        }
    } else if claims.may_edit() {
        Err(Refusal::NotTheSaver.detail())
    } else {
        Err(Refusal::ReadOnly.detail())
    };
    match outcome {
        Ok(()) => {
            let _ = socket.emit("saved_ack", &serde_json::json!({ "doc": doc, "seq": msg.seq }));
        }
        Err(reason) => {
            let _ = socket.emit("saved_refused", &serde_json::json!({ "doc": doc, "seq": msg.seq, "reason": reason }));
            let _ = socket.emit("error", &ErrorMsg { doc, detail: reason, local: None, code: None });
        }
    }
}

async fn on_disconnect(socket: SocketRef, State(relay): State<Arc<Relay>>) {
    let Some(claims) = socket.extensions.get::<Claims>() else { return };
    // This socket never reached `Room::join` (refused at `next_space`, or the room was full),
    // so it was never counted into the room and has nothing to release. Calling `Room::leave`
    // anyway would still find the live connection sharing this peer id and evict it, taking
    // its locks with it.
    if socket.extensions.get::<JoinedRoom>().is_none() {
        return;
    }
    let room_name = claims.room();

    let (dropped, gone, roster, empty) = {
        let mut rooms = relay.rooms.lock().await;
        let Some(room) = rooms.get_mut(&claims.rm) else { return };
        // Keyed by this socket: a socket the peer has already replaced (a half-open link that
        // only now timed out) leaves nothing and tells nobody, so the roster never loses a
        // peer who is still connected, and a room is never closed with editors in it.
        let Some(left) = room.leave_as(&claims.pid, &socket.id.to_string()) else {
            tracing::debug!(peer = %claims.pid, room = %claims.rm, "a replaced socket closed");
            return;
        };
        let (dropped, gone) = (left.dropped, left.gone);
        let roster = room.peers().to_vec();
        let empty = room.is_empty();
        if empty {
            // Kept for the grace period, then dropped if nobody came back: dropping frees the
            // op history and the retained text, which only this room holds. The sequence stays
            // in Valkey, so a later peer continues from where this one left off rather than
            // starting again at 1, but a joiner after the drop finds the history gone.
            let mark = room.idle_mark();
            let (relay, key) = (Arc::clone(&relay), claims.rm.clone());
            tokio::spawn(async move {
                tokio::time::sleep(relay.room_grace).await;
                let mut rooms = relay.rooms.lock().await;
                if rooms.get(&key).is_some_and(|room| room.still_idle(mark)) {
                    rooms.remove(&key);
                    drop(rooms);
                    relay.forget_gate(&key);
                    tracing::debug!(room = %key, "an idle room was dropped after its grace period");
                }
            });
        }
        (dropped, gone, roster, empty)
    };

    // Releasing on disconnect is what makes a lock safe to take at all. One
    // that outlived its holder would wedge the region until the TTL lapsed.
    //
    // `dropped` already came from the room's own mirror, which only reports a scope here when
    // it believed this peer held it, so every entry is a real release from the room's point of
    // view and is announced unconditionally. The bus call is best effort cleanup alongside
    // that: when the lease had already lapsed on its own before the disconnect, there is
    // nothing left in Valkey to delete, and that must not suppress telling the room, which is
    // exactly the silent-lapse gap this announcement exists to close.
    for scope in &dropped {
        {
            let mut bus = relay.bus.lock().await;
            let _ = bus.release(&claims.rm, scope, &claims.pid).await;
        }
        emit_live(socket.to(room_name.clone()), "lock_released", &serde_json::json!({ "doc": room_name, "scope": scope }));
    }

    if gone && !empty {
        emit_live(socket.to(room_name.clone()), "presence", &serde_json::json!({ "doc": room_name, "roster": roster }));
    }

    tracing::info!(peer = %claims.pid, room = %claims.rm, locks_dropped = dropped.len(), "peer left");
}

fn now_unix() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// A URL with any userinfo replaced, fit for a log line.
///
/// `redis://user:password@valkey:6379/6` logs as `redis://…@valkey:6379/6`.
/// String surgery rather than a parser, because the input came straight from
/// the deployment's own environment and only ever needs its secret removed.
fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else { return url.to_owned() };
    let rest = &url[scheme_end + 3..];
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => format!("{}…@{}", &url[..scheme_end + 3], &rest[at + 1..]),
        None => url.to_owned(),
    }
}

/// The socket's claims, but only while its token still stands.
///
/// Authorisation is decided once, at connect, and cached on the socket. That
/// alone would let a withdrawn or downgraded share keep editing for as long as
/// the socket stays open, which has no bound. Re-reading expiry on every
/// message that changes the document and on every delivery that
/// [`emit_live`] fans out bounds it to the token's own lifetime:
/// once the token lapses the peer is dropped and has to fetch another, and
/// minting one re-stats the provider and re-decides the grant. A client that
/// refreshes its token before then sees no interruption. Disconnecting is safe
/// from a handler, because `on_disconnect` releases every scope the peer held.
fn live_claims(socket: &SocketRef) -> Option<Claims> {
    let claims = socket.extensions.get::<Claims>()?;
    if claims.exp <= now_unix() {
        tracing::info!(room = %claims.rm, "a socket's token expired; dropping it so it must re-authorise");
        // `disconnect` consumes the handle, and this one is borrowed. The handle
        // is a cheap clonable reference to the same socket, so a clone drops it.
        let _ = socket.clone().disconnect();
        return None;
    }
    Some(claims)
}

/// Deliver one event to every targeted room member whose token still stands.
///
/// The adapter's own broadcast hands the payload to each member with no look
/// at who is receiving. The inbound recheck in [`live_claims`] never fires for
/// a member that only listens. Such a member used to keep receiving every
/// edit for as long as its socket stayed open. Fanning out here puts the same
/// recheck on the delivery path. A lapsed member never sees the event and is
/// disconnected the first time one would have reached it. The disconnect runs
/// `on_disconnect`, releasing the member's locks and correcting the roster
/// the same way an inbound-side drop does.
fn emit_live<T: ?Sized + serde::Serialize>(targets: socketioxide::operators::BroadcastOperators, event: &str, data: &T) {
    for member in targets.sockets() {
        if live_claims(&member).is_none() {
            continue;
        }
        let _ = member.emit(event, data);
    }
}
