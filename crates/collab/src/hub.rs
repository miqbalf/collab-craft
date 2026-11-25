//! The transport seam.
//!
//! A [`Hub`] is whatever carries the protocol. The crate never opens a connection, exactly as
//! `craft-nextcloud` never opens a socket, so one session implementation serves every target.
//!
//! The intended implementations are Socket.IO on both sides, which is where the Hub and
//! subscriber shape comes from. A native app drives `rust_socketio`. A browser build drives the
//! official `socket.io-client` through `wasm-bindgen`, because `rust_socketio` depends on
//! `native-tls` with no target-gated alternative and therefore cannot build for `wasm32`.
//!
//! Rooms do the subscribing. One room per document, which is how a relay fans an edit out to
//! exactly the peers holding that document open.
//!
//! [`emit`] and [`receive`] are the whole of the encoding, and both hubs use them. A host writing
//! a hub of its own uses them too, rather than working out which event name carries which message
//! a second time.

use crate::error::Result;
use crate::proto::{ClientMsg, ServerMsg};

/// A connection to the relay.
///
/// `send` is fire and forget. Ordering is the relay's job, and the session tolerates arrival out
/// of order, so an implementation needs no queue of its own.
///
/// `poll` is non-blocking and returns what has arrived since the last call. That suits an egui
/// app, which wants to drain messages once per frame rather than be called back on another
/// thread.
pub trait Hub {
    fn send(&mut self, msg: ClientMsg) -> Result<()>;

    /// Messages received since the previous call, oldest first.
    fn poll(&mut self) -> Vec<ServerMsg>;

    /// Whether the transport believes it is connected. A session stays usable across a brief
    /// disconnect, because the relay re-sends on rejoin.
    fn connected(&self) -> bool;

    /// Use `token` for the next (re)connect. Tickets are short-lived, so a host refreshes them
    /// while connected; after the relay refused the connection itself (an expired ticket), a
    /// hub that stopped reconnecting starts again with this one. No-op by default.
    fn set_token(&mut self, token: String) {
        let _ = token;
    }

    /// The replica on this link mints in id `space` and keeps it across reconnects. Every
    /// rejoin must name it (handshake `space`): the relay raises its space counter to at least
    /// that before it grants a space, so no other connection is granted it after the counter
    /// went backwards and the relay lost what it granted (a restart together with a Valkey
    /// flush). While the relay process runs, it also keeps its own floor of every space it
    /// granted, which covers a counter that lost only its latest grants.
    ///
    /// Required, with no default, so that a wrapper hub (one that boxes or taps another) cannot
    /// drop it silently: forward it to the inner hub. A hub with no handshake to carry it in
    /// writes an empty body, and leaves the guarantee to the relay's own floor.
    ///
    /// **Not covered** even with it: the relay restarts *and* Valkey loses recent grants, and a
    /// newcomer joins before the holder of a lost space rejoins. That case depends on how
    /// Valkey persists and is not fixed in M1.
    fn set_space(&mut self, space: u32);

    /// Close the link for good: no reconnect, nothing sent or received after it. A hub that
    /// closes on drop (the web and native hubs) need not do more. No-op by default.
    fn close(&mut self) {}
}

/// How long a hub waits before its next reconnect attempt: 250 ms, doubling to 5 s, and back
/// to 250 ms once the relay has joined it again. Kept across attempts, so a relay that accepts
/// the socket and then closes it is not reconnected to in a tight loop.
#[derive(Debug)]
#[cfg(any(feature = "native-hub", feature = "web-hub"))]
pub(crate) struct Backoff {
    next: std::time::Duration,
}

#[cfg(any(feature = "native-hub", feature = "web-hub"))]
impl Backoff {
    const FIRST: std::time::Duration = std::time::Duration::from_millis(250);
    const MOST: std::time::Duration = std::time::Duration::from_secs(5);

    /// The wait before this attempt; the next one waits twice as long, up to the most.
    pub(crate) fn next(&mut self) -> std::time::Duration {
        let wait = self.next;
        self.next = (wait * 2).min(Self::MOST);
        wait
    }

    pub(crate) fn reset(&mut self) {
        self.next = Self::FIRST;
    }
}

#[cfg(any(feature = "native-hub", feature = "web-hub"))]
impl Default for Backoff {
    fn default() -> Backoff {
        Backoff { next: Self::FIRST }
    }
}

/// Where a relay URL points a Socket.IO client: the server, and the transport path on it.
///
/// A relay mounted under a base path (`COLLABCRAFT_BASE=/craft`) serves Socket.IO at
/// `/craft/socket.io/`. Apps are given the relay URL as the user sees it, `https://host/craft`,
/// and a Socket.IO client would read that path as a namespace or as the transport path itself,
/// so the two are split here once for both hubs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// Scheme and authority, `https://host:port`.
    pub origin: String,
    /// The transport path, always ending in `/socket.io/`.
    pub path: String,
    /// The query string without its `?`, when the URL had one.
    pub query: Option<String>,
}

impl Endpoint {
    /// Split a relay URL. A path already naming `socket.io` is kept as it is; a fragment is
    /// dropped.
    #[must_use]
    pub fn of(url: &str) -> Endpoint {
        let url = url.split('#').next().unwrap_or(url);
        let (rest, query) = match url.split_once('?') {
            Some((rest, query)) => (rest, Some(query.to_owned())),
            None => (url, None),
        };
        let authority_at = rest.find("://").map_or(0, |at| at + 3);
        let (scheme, after) = rest.split_at(authority_at);
        let (authority, path) = match after.find('/') {
            Some(at) => after.split_at(at),
            None => (after, ""),
        };
        let mut path = path.trim_end_matches('/').to_owned();
        if !path.ends_with("/socket.io") {
            path.push_str("/socket.io");
        }
        path.push('/');
        Endpoint { origin: format!("{scheme}{authority}"), path, query }
    }

    /// The one URL a client that takes the transport path inside the URL connects to.
    #[must_use]
    pub fn url(&self) -> String {
        match &self.query {
            Some(query) => format!("{}{}?{query}", self.origin, self.path),
            None => format!("{}{}", self.origin, self.path),
        }
    }
}

/// Every event the relay can send.
///
/// Listed explicitly because a Socket.IO client registers one callback per event name, so a hub
/// has to know the whole set before anything arrives. An event missing from here is an event
/// nobody is listening for, which looks like a relay that connects and then says nothing.
pub const EVENTS: [&str; 15] = [
    "joined",
    "op",
    "lock_granted",
    "lock_denied",
    "lock_released",
    "lock_lost",
    "presence_lock",
    "presence",
    "snapshot",
    "text_ops",
    "frame",
    "ephemeral",
    "error",
    "saved_ack",
    "saved_refused",
];

/// Encode a client message as the JSON a Socket.IO event carries.
pub fn encode(msg: &ClientMsg) -> Result<String> {
    serde_json::to_string(msg).map_err(|e| crate::error::Error::malformed("client", e.to_string()))
}

/// Decode one relay payload.
pub fn decode(payload: &str) -> Result<ServerMsg> {
    serde_json::from_str(payload).map_err(|e| crate::error::Error::malformed("server", e.to_string()))
}

/// The slowest link a transfer is given time for, in bytes a second.
///
/// A deadline that ignores the size of what is moving counts a large transfer on a slow link
/// as a dead link, and the reconnect then sends the same transfer again into the same deadline.
/// Every transfer deadline is a fixed part plus [`transfer_time`] of its bytes at this rate.
pub const SLOW_LINK_BYTES_PER_SEC: u64 = 64 * 1024;

/// How long `bytes` take to move at [`SLOW_LINK_BYTES_PER_SEC`].
pub fn transfer_time(bytes: usize) -> std::time::Duration {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    std::time::Duration::from_millis(bytes.saturating_mul(1000) / SLOW_LINK_BYTES_PER_SEC)
}

/// The event name and body for one client message.
///
/// The relay keys handlers on the event name and reads the document from the peer's token, so the
/// body carries only what the event needs.
pub fn emit(msg: &ClientMsg) -> (&'static str, serde_json::Value) {
    match msg {
        ClientMsg::Join { display, .. } => ("join", serde_json::json!({ "display": display })),
        ClientMsg::Leave { .. } => ("leave", serde_json::Value::Null),
        ClientMsg::Submit { local, command, params, scope, fence, .. } => match fence {
            Some(fence) => ("submit", serde_json::json!({ "local": local, "command": command, "params": params, "scope": scope, "fence": fence })),
            None => ("submit", serde_json::json!({ "local": local, "command": command, "params": params, "scope": scope })),
        },
        ClientMsg::Lock { scope, .. } => ("lock", serde_json::json!({ "scope": scope })),
        ClientMsg::Unlock { scope, token: Some(token), .. } => ("unlock", serde_json::json!({ "scope": scope, "token": token })),
        ClientMsg::Unlock { scope, token: None, .. } => ("unlock", serde_json::json!({ "scope": scope })),
        ClientMsg::Refresh { scope, token, .. } => ("refresh", serde_json::json!({ "scope": scope, "token": token })),
        ClientMsg::Resync { have, .. } => ("resync", serde_json::json!({ "have": have })),
        ClientMsg::Frame { payload, .. } => ("frame", serde_json::json!({ "payload": payload })),
        ClientMsg::Ephemeral { kind, payload, .. } => ("ephemeral", serde_json::json!({ "kind": kind, "payload": payload })),
        ClientMsg::Saved { seq, .. } => ("saved", serde_json::json!({ "seq": seq })),
    }
}

/// One relay payload, read as the message its event name says it is.
///
/// **The event name is the discriminator.** [`ServerMsg`] is a serde-tagged enum on `t`, and the
/// relay emits a bare body under an event name rather than repeating the tag inside it. Socket.IO
/// exists to carry that name, so duplicating it in the payload would be redundant on every
/// message. The tag is injected here before deserialising, which works because every event name
/// is exactly the snake_case name of its variant.
///
/// `None` for anything unreadable. Dropping a message we cannot read beats tearing down a live
/// session over it, and a caller logs it.
pub fn receive(event: &str, mut value: serde_json::Value) -> Option<ServerMsg> {
    match value.as_object_mut() {
        Some(map) => {
            map.insert("t".to_owned(), serde_json::Value::String(event.to_owned()));
        }
        None => {
            log::warn!("{event} payload was not an object");
            return None;
        }
    }
    match serde_json::from_value::<ServerMsg>(value) {
        Ok(msg) => Some(msg),
        Err(e) => {
            log::warn!("unreadable {event} message: {e}");
            None
        }
    }
}

/// What happens to a message sent while the link is down.
#[cfg(any(feature = "native-hub", feature = "web-hub"))]
pub(crate) enum WhileDown {
    /// Dropped: something else covers it. Edit submits are sent again by
    /// [`crate::Live`] after the rejoin, merged; a checkpoint snapshot and its save report
    /// are reported as raced on the rejoin, and must not reach the relay after that, or the
    /// baseline would move for a save the saver was told did not happen; presence is resent
    /// by the app with its newest value; a resync request is moot, because the rejoin replays.
    Drop,
    /// Sent once the link is back: queued by the native hub until the relay has joined the
    /// connection again (lock commands coalesced per scope, other submits), buffered by
    /// socket.io-client in a browser.
    Queue,
}

#[cfg(any(feature = "native-hub", feature = "web-hub"))]
pub(crate) fn while_down(msg: &ClientMsg) -> WhileDown {
    match msg {
        ClientMsg::Submit { command, .. } if crate::proto::is_crdt_command(command) => WhileDown::Drop,
        // A refresh is for the link it was sent on: the relay releases a holder's locks with its
        // link, and the next refresh is due anyway.
        ClientMsg::Saved { .. } | ClientMsg::Ephemeral { .. } | ClientMsg::Frame { .. } | ClientMsg::Resync { .. } | ClientMsg::Refresh { .. } => {
            WhileDown::Drop
        }
        _ => WhileDown::Queue,
    }
}
