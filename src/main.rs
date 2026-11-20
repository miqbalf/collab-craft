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

use bus::{Bus, LockOutcome};
use room::{Op, Peer, Refusal, Room, Submission, is_crdt_command};

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
fn sized(builder: socketioxide::SocketIoBuilder, text_cap: usize) -> socketioxide::SocketIoBuilder {
    let cap = transport_cap(text_cap);
    builder.max_payload(cap as u64).ws_max_message_size(cap).ws_max_frame_size(cap)
}
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
    /// file id -> that room's broadcast gate. See [`Relay::gate`].
    gates: std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>,
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

impl Relay {
    /// The gate that orders a room's broadcasts against its joins.
    ///
    /// A submit holds it from recording its op to broadcasting it, and a join from reading
    /// its replay to joining the broadcast room. So an op is either in a joiner's replay or
    /// broadcast after the joiner is in the room and has been sent `joined`, and the long
    /// part, serialising a large replay onto the socket, blocks only that room rather than
    /// the global [`Relay::rooms`] lock.
    fn gate(&self, key: &str) -> Arc<Mutex<()>> {
        let mut gates = self.gates.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(gates.entry(key.to_owned()).or_default())
    }

    /// Drop a room's gate once nobody holds or waits on it.
    fn forget_gate(&self, key: &str) {
        let mut gates = self.gates.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if gates.get(key).is_some_and(|gate| Arc::strong_count(gate) == 1) {
            gates.remove(key);
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
}

#[derive(Debug, Deserialize)]
struct ScopeMsg {
    scope: String,
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
}

#[derive(Debug, Serialize)]
struct LockEntry {
    scope: String,
    holder: String,
}

#[derive(Debug, Serialize)]
struct ErrorMsg {
    doc: String,
    detail: String,
    /// The `local` counter of the submit this refuses, when it refuses one.
    #[serde(skip_serializing_if = "Option::is_none")]
    local: Option<u64>,
}

/// Key space for sessions and account links, kept apart from the relay's
/// own room keys so one can be cleared without touching the other.
const PREFIX_SERVICE: &str = "collabcraft:svc";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("collabcraft_relay=info".parse()?))
        .init();

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
    tracing::info!(text_cap, base = %base, public_apps, "relay options");
    let relay = Arc::new(Relay {
        text_cap,
        service_uid,
        secret: secret_bytes.clone(),
        bus: Mutex::new(bus),
        rooms: Mutex::new(HashMap::new()),
        gates: std::sync::Mutex::new(HashMap::new()),
    });
    // For the health route, which outlives the move into the socket layer.
    let health_relay = relay.clone();

    // Cap the transport too. Room::submit refuses an oversized `params` after decoding, and a
    // cap there still means the bytes were read and parsed first. This refuses them at the
    // socket, which is the only place that costs nothing. Sized for the largest snapshot the
    // rooms retain; see `transport_cap`.
    let (layer, io) =
        sized(SocketIo::builder(), text_cap).req_path(format!("{base}/socket.io")).ping_interval(PING_INTERVAL).with_state(relay).build_layer();
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
                async move {
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
        let _ = socket.emit("error", &ErrorMsg { doc: String::new(), detail: detail.to_owned(), local: None });
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
            let _ = socket.emit("error", &ErrorMsg { doc: String::new(), detail: "token rejected".to_owned(), local: None });
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
    socket.on("resync", on_resync);
    socket.on("frame", on_frame);
    socket.on("ephemeral", on_ephemeral);
    socket.on("saved", on_saved);
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
        match bus.next_space(&room_key).await {
            Ok(s) => s,
            Err(e) => {
                // Refuse rather than admit a peer with no space. Admitting it would mean
                // either silent divergence or a window that cannot type, and neither is
                // something a user could act on.
                tracing::error!(error = %e, room = %room_key, "could not grant an id space");
                let _ = socket.emit("error", &ErrorMsg { doc: room_name.clone(), detail: "relay could not start a session".to_owned(), local: None });
                let _ = socket.disconnect();
                return;
            }
        }
    };

    // Where the document already was, read from the bus before the first peer
    // joins. A room rebuilt in memory starts at sequence zero, and handing out
    // a number a client has already seen is what made a reopen replay or stall.
    let retained_seq = {
        let mut bus = relay.bus.lock().await;
        bus.current_seq(&room_key).await.unwrap_or(0)
    };

    // The room's gate is held until the socket has joined the broadcast room, after the
    // replay. A submit holds it from recording its op to broadcasting it, so every op is
    // either in this replay (and maybe broadcast again, which the client drops by its
    // sequence) or broadcast after this socket joined and was sent `joined`. None is lost,
    // none lands twice. The global rooms lock is released before anything is serialised.
    let gate = relay.gate(&room_key);
    let ordered = gate.lock().await;
    let mut rooms = relay.rooms.lock().await;
    let joined = {
        let room = relay.room(&mut rooms, &room_key);
        room.observe_seq(retained_seq);
        match room.join(Peer { peer: peer_id.clone(), display: claims.dsp.clone(), may_edit: claims.may_edit() }) {
            Ok(()) => {
                for (scope, holder) in &locks {
                    room.record_holder(scope.clone(), holder.clone());
                }
                // Catch the joiner up from where IT already is, not from where the
                // saved file is. A connection resuming a session knows what it has
                // already applied, and that is the only baseline that is correct
                // for it: the saved file's sequence can be ahead of it (a save
                // happened while it was gone, and resuming there would skip the
                // edits made before that save that it never saw) or behind it (it
                // is already caught up, and resuming at the file's sequence would
                // re-send ops it has). A fresh load has no applied sequence of its
                // own to offer, and the file it just read is at `saved_seq`, so
                // that is the only baseline available to it. Either way the ops
                // after the baseline bring it current, replayed as ordered ops so
                // the path is the same one a live edit already takes. When the
                // history no longer reaches that baseline, or a burned sequence
                // broke the run, there is nothing clean to replay, so the join
                // falls back to the current sequence and the retained text: the
                // joiner then leans on the file alone, the older behaviour.
                let baseline = handshake.applied.unwrap_or_else(|| room.saved_seq());
                match room.since(baseline) {
                    Some(replay) => Some((baseline, replay, Vec::new(), room.peers().to_vec(), room.ephemeral())),
                    None => {
                        let texts: Vec<(String, Vec<serde_json::Value>, bool, bool)> = room
                            .texts()
                            .iter()
                            .map(|(scope, log)| (scope.clone(), log.ops().to_vec(), log.complete(), log.starts_with_snapshot()))
                            .collect();
                        Some((room.seq(), Vec::new(), texts, room.peers().to_vec(), room.ephemeral()))
                    }
                }
            }
            Err(_) => None,
        }
    };

    drop(rooms);
    let Some((seq, replay, texts, roster, ephemeral)) = joined else {
        drop(ordered);
        tracing::warn!(room = %room_key, "refusing a peer: the document is full");
        let _ = socket.emit("error", &ErrorMsg { doc: room_name.clone(), detail: Refusal::Full.detail(), local: None });
        let _ = socket.disconnect();
        return;
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
        },
    );

    // The ordered ops since the saved version. The joiner applies these on top
    // of the file it loads, exactly as it would a live edit, so the edits made
    // since the last save are not lost. Empty on the fallback path below.
    for op in &replay {
        let _ = socket.emit("op", &OpMsg::new(&claims, op));
    }

    // The fallback catch-up for text, used only when the ordered replay above
    // could not be built. The file the joiner opens is behind the edits made
    // since it was written, and the sequence is already past those edits, so
    // these carry no sequence.
    // In pieces no larger than a client can read in one WebSocket frame: a scope can retain
    // up to three caps (edits plus two snapshots). Only the first piece can start with the
    // snapshot, and a client treats the rest as ordinary updates.
    for (scope, ops, complete, snap) in texts {
        for (i, piece) in text_pieces(&ops, TEXT_OPS_PIECE_BYTES).into_iter().enumerate() {
            let _ = socket.emit(
                "text_ops",
                &serde_json::json!({ "doc": room_name.clone(), "scope": scope, "ops": piece, "complete": complete, "snap": snap && i == 0 }),
            );
        }
    }
    socket.join(room_name.clone());
    drop(ordered);

    // And on where everybody is, so somebody arriving late does not have to wait for each
    // person to move before their marker appears.
    for (peer, kind, payload) in ephemeral {
        let _ = socket.emit("ephemeral", &serde_json::json!({ "doc": room_name.clone(), "origin": peer, "kind": kind, "payload": payload }));
    }

    emit_live(socket.to(room_name.clone()), "presence", &serde_json::json!({ "doc": room_name, "roster": roster }));

    tracing::info!(uid = %claims.uid, peer = %peer_id, room = %room_key, may_edit = claims.may_edit(), "peer joined");
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
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: refusal.detail(), local });
        return;
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
                tracing::error!(error = %e, room = %claims.rm, "could not assign a sequence");
                let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: "relay could not order that edit".to_owned(), local });
                return;
            }
        }
    };

    // Held only while the op is recorded. That is enough: a join holds the gate from reading
    // its replay until its socket is in the broadcast room, so an op recorded before the join
    // read is in its replay (a later broadcast copy is dropped by sequence), and one recorded
    // after had to wait for the join to finish, so its broadcast, whenever it goes out,
    // reaches the joined socket. The Valkey round trip below runs without it.
    let gate = relay.gate(&claims.rm);
    let ordered = gate.lock().await;
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
            let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: refusal.detail(), local });
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
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: Refusal::ReadOnly.detail(), local: None });
        return;
    }

    let outcome = {
        let mut bus = relay.bus.lock().await;
        bus.try_lock(&claims.rm, &msg.scope, &claims.pid).await
    };

    match outcome {
        Ok(LockOutcome::Granted) => {
            {
                let mut rooms = relay.rooms.lock().await;
                relay.room(&mut rooms, &claims.rm).record_lock(&claims.pid, msg.scope.clone());
            }
            let _ = socket.emit("lock_granted", &serde_json::json!({ "doc": room_name, "scope": msg.scope }));
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
            let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: "relay could not take that lock".to_owned(), local: None });
        }
    }
}

async fn on_unlock(socket: SocketRef, Data(msg): Data<ScopeMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    let room_name = claims.room();

    let bus_released = {
        let mut bus = relay.bus.lock().await;
        bus.release(&claims.rm, &msg.scope, &claims.pid).await.unwrap_or(false)
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
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: Refusal::ReadOnly.detail(), local: None });
        return;
    }
    if msg.payload.len() > MAX_FRAME_BYTES {
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: Refusal::TooLarge { bytes: msg.payload.len() }.detail(), local: None });
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
        let _ = socket.emit("error", &ErrorMsg { doc: room_name, detail: refusal.detail(), local: None });
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
async fn on_saved(socket: SocketRef, Data(msg): Data<SavedMsg>, State(relay): State<Arc<Relay>>) {
    let Some(claims) = live_claims(&socket) else { return };
    let doc = claims.room();
    // Answered either way, so the saver knows whether it may write the file: the baseline
    // moved (`saved_ack`) or it did not (`saved_refused`, with the same `seq`).
    let outcome = if claims.may_edit() {
        let mut rooms = relay.rooms.lock().await;
        match rooms.get_mut(&claims.rm) {
            Some(room) => room.checkpoint(msg.seq).map_err(|refusal| refusal.detail()),
            None => Err("this document is not open on the relay".to_owned()),
        }
    } else {
        Err(Refusal::ReadOnly.detail())
    };
    match outcome {
        Ok(()) => {
            let _ = socket.emit("saved_ack", &serde_json::json!({ "doc": doc, "seq": msg.seq }));
        }
        Err(reason) => {
            let _ = socket.emit("saved_refused", &serde_json::json!({ "doc": doc, "seq": msg.seq, "reason": reason }));
            let _ = socket.emit("error", &ErrorMsg { doc, detail: reason, local: None });
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

    let (dropped, roster, empty) = {
        let mut rooms = relay.rooms.lock().await;
        let Some(room) = rooms.get_mut(&claims.rm) else { return };
        let dropped = room.leave(&claims.pid);
        let roster = room.peers().to_vec();
        let empty = room.is_empty();
        if empty {
            // Dropping the room frees the op history. The sequence stays in
            // Valkey, so a later peer continues from where this one left off
            // rather than starting again at 1.
            rooms.remove(&claims.rm);
            relay.forget_gate(&claims.rm);
        }
        (dropped, roster, empty)
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

    if !empty {
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
