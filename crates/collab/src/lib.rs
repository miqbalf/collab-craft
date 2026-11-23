//! Shared editing for the Craft apps.
//!
//! The relay assigns a strictly increasing sequence to every edit, and each client applies edits
//! in that order. Concurrent work in different regions proceeds freely. Concurrent work in the
//! same region is serialised by a scope lock, unless the command is `crdt.*`. A `crdt.text`
//! command carries [yrs](https://docs.rs/yrs) updates for one shared [`LiveDoc`] and skips the
//! lock: two people can type in the same paragraph and both insertions survive. CAD booleans
//! and renders still take a lock, because they do not commute.
//!
//! Sequence numbers are a delivery order for catch-up. Merge correctness for `crdt.*` comes
//! from the CRDT's item identities, not from seq.
//!
//! Four pieces:
//!
//! * [`proto`] is the wire protocol, two enums and nothing else.
//! * [`Session`] orders, deduplicates and tracks locks. It performs no IO and returns [`Effect`]
//!   values describing what the app should do.
//! * [`Hub`] is the transport seam a caller implements, intended for Socket.IO at both ends.
//! * [`LiveDoc`] is the shared block document, and [`Live`] pumps a hub plus that replica. Any
//!   other schema implements [`Replica`] (most of it on [`YReplica`]) and [`Live`] pumps it the
//!   same way.
//!   That is the shared solution. It does not live in `craft-cloud` (files) and it does not
//!   live in one app's desktop binary.
//!
//! A command is a name plus opaque JSON, matching the `CommandSpec` shape every Craft app already
//! has, and a lock scope is an opaque string each app reads its own way. So nothing here is tied
//! to one app. WordCraft maps a paragraph path. GridCraft maps a cell address. CAD booleans still
//! take a lock.
//!
//! ```
//! use craft_collab::{Effect, Session};
//! use serde_json::json;
//!
//! let mut s = Session::new("doc-42");
//! let _join = s.join("Alice");
//! // …the relay answers, the app feeds messages in, and acts on the effects it gets back.
//! let _ = s.submit("cell.set", json!({"cell": "B2", "input": "7"}), Some("B2".into()));
//! assert_eq!(s.inflight(), 1);
//! # let _ = Effect::Notice { detail: String::new() };
//! ```
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unimplemented, clippy::todo, clippy::unreachable)]

mod error;
mod fits;
#[cfg(test)]
mod fuzz;
mod hub;
mod live;
pub mod proto;
pub mod replica;
mod session;
pub mod shadow;
mod token;
mod walk;
mod ydoc;

/// A [`Hub`] over `rust_socketio`, for native apps.
///
/// Behind a feature, because `rust_socketio` depends on `native-tls` and cannot
/// build for `wasm32`. A browser build leaves it off.
#[cfg(feature = "native-hub")]
pub mod native;

/// A [`Hub`] over the JS `socket.io-client`, for a browser.
///
/// Behind a feature, because it reaches a global the page is expected to have
/// put there and binds nothing at link time. A native build leaves it off.
#[cfg(feature = "web-hub")]
pub mod web;

#[cfg(test)]
mod tests;

pub use error::{Error, Result};
pub use hub::{EVENTS, Endpoint, Hub, SLOW_LINK_BYTES_PER_SEC, decode, emit, encode, receive, transfer_time};
pub use live::{
    CHECKPOINT_TIMEOUT, DOC_SCOPE, Live, LockEvent, MAX_UNCONFIRMED_BYTES, MAX_UNCONFIRMED_EDITS, OFFLINE_MAX_BYTES, PAST_BOUND, TextPayload, Work,
    refusal_is_permanent, update_from_wire, update_to_wire,
};
pub use proto::{
    CRDT_POS, CRDT_TEXT, ClientMsg, DocId, FENCED, Fence, LockState, PROTO, PeerId, Presence, REFUSED_AUTH, REFUSED_FULL, REFUSED_PROTO,
    REFUSED_SCHEMA, RETRY, Scope, ServerMsg, is_crdt_command,
};
pub use replica::{MAX_OP_BLOCKS, MAX_STATE_BLOCKS, Replica, YReplica, update_blocks, update_exceeds_blocks, update_is_sound};
pub use session::{Effect, Session};
pub use token::doc_from_token;
pub use ydoc::{Attributes, BlockChange, LiveDoc, LiveError, SeedBlock};
/// The CRDT under every replica, for an app that writes its own schema on [`YReplica`].
pub use yrs;
