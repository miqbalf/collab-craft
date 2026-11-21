//! A [`Hub`] for native apps, over `rust_socketio`.
//!
//! Behind the `native-hub` feature, because `rust_socketio` depends on
//! `native-tls` unconditionally and therefore cannot build for `wasm32`. A
//! browser build leaves this off and uses [`crate::web`] instead.
//!
//! The bridge is the whole point of this module. `rust_socketio` is async and
//! callback driven. [`Hub`] is synchronous and polled, because an egui app wants
//! to drain messages once per frame rather than be called back on another
//! thread. So callbacks push into a channel, [`NativeHub::poll`] drains it, and
//! sends go out through a task that owns the socket.
//!
//! The protocol itself is not here. [`emit`] and [`receive`] are the same
//! functions the browser hub calls, so the two peers cannot drift apart over
//! an event name.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rust_socketio::asynchronous::{Client, ClientBuilder};
use rust_socketio::{Event, Payload, TransportType};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::hub::{Backoff, EVENTS, Endpoint, Hub, WhileDown, emit, receive, transfer_time, while_down};
use crate::proto::{ClientMsg, ServerMsg};

/// A connection to the relay from a native app.
pub struct NativeHub {
    outbox: UnboundedSender<ClientMsg>,
    inbox: UnboundedReceiver<ServerMsg>,
    control: UnboundedSender<Control>,
    shared: Arc<Shared>,
    /// Dropped with the hub, which ends the socket task wherever it is, mid outage included.
    _stop: oneshot::Sender<()>,
}

/// What the socket callbacks and the hub share with the task that owns the socket.
struct Shared {
    /// Set by the relay's connect acknowledgement, cleared by a close or a dropped link.
    connected: AtomicBool,
    /// Which client the callbacks belong to. A callback of a client already replaced is
    /// ignored, so a late close from the old socket cannot take down the new one.
    generation: AtomicU64,
    /// The token the next (re)connect joins with, and how many times it has been replaced.
    /// [`Hub::set_token`] replaces it.
    token: Mutex<(String, u64)>,
}

impl Shared {
    fn token(&self) -> (String, u64) {
        self.token.lock().map(|t| t.clone()).unwrap_or_default()
    }

    /// The auth a reconnect sends, with the token's generation: the current token, and never
    /// `applied`, because what a connection applied may be mid catch-up and is not a baseline
    /// the relay can replay from.
    fn rejoin(&self) -> (serde_json::Value, u64) {
        let (token, generation) = self.token();
        (serde_json::json!({ "token": token, "proto": crate::proto::PROTO }), generation)
    }

    fn current(&self, generation: u64) -> bool {
        self.generation.load(Ordering::SeqCst) == generation
    }
}

/// What the callbacks and the hub tell the socket task.
enum Control {
    /// The socket of this generation is gone: the relay closed it.
    Closed(u64),
    /// The relay joined the connection of this generation, and says whether it acknowledges
    /// heartbeats and how long its largest frame may take to arrive. The backoff starts over
    /// and what was queued while the link was down goes out, before anything sent after.
    Joined(u64, bool, Duration),
    /// A message arrived on the connection of this generation: the link is alive. `true` when
    /// it is a catch-up piece with more to follow.
    Heard(u64, bool),
    /// The relay acknowledged heartbeat `n` on the connection of this generation.
    Alive(u64, u64),
    /// The relay refused the connection of this generation itself (a bad or expired token,
    /// another protocol). Reconnecting with the same auth cannot help, so the task stops until
    /// a new token, unless one arrived since that connection was made.
    Refused(u64),
    /// A new token: reconnect with it if stopped.
    Token,
}

impl NativeHub {
    /// Connect, and keep the socket on a task of its own.
    ///
    /// Must be called from inside a Tokio runtime, which a native Craft app has
    /// because the relay client is the only async thing it owns.
    ///
    /// `url` is the relay as the deployment publishes it, base path included
    /// (`https://host/craft`); the Socket.IO path is derived from it ([`Endpoint`]).
    /// `token` is what the Nextcloud app minted; it
    /// travels in the Socket.IO handshake auth, so it never appears in a query
    /// string and therefore never lands in an access log.
    ///
    /// `applied` is the sequence this connection has already applied, when it is resuming a
    /// [`crate::Session`] rather than opening the document fresh. Pass `Session::applied()`
    /// when reconnecting with a session that survived the drop, and `None` on a first join or
    /// after reloading the file. Only this first connection sends it.
    ///
    /// The hub reconnects on its own when the link drops or the relay closes it, backing off
    /// from 250 ms to 5 s until the relay joins it again, and [`Hub::connected`] follows the
    /// relay's acknowledgements. A reconnect joins with the newest token
    /// ([`Hub::set_token`]) and without `applied`. A relay that refuses the connection itself
    /// (`ServerMsg::Error` with a `code`) stops the reconnecting until [`Hub::set_token`].
    /// Messages sent while the link is down are dropped; a [`crate::Live`] sends its
    /// unconfirmed edits again once it has rejoined.
    ///
    /// The connection is WebSocket only, with no long-polling fallback, so every proxy between
    /// the app and the relay must pass the WebSocket upgrade on the Socket.IO path.
    pub async fn connect(url: &str, token: &str, applied: Option<u64>) -> Result<NativeHub> {
        let (in_tx, inbox) = unbounded_channel::<ServerMsg>();
        let (outbox, out_rx) = unbounded_channel::<ClientMsg>();
        let (control, control_rx) = unbounded_channel::<Control>();
        let shared = Arc::new(Shared { connected: AtomicBool::new(false), generation: AtomicU64::new(0), token: Mutex::new((token.to_owned(), 0)) });
        let link = Link { url: Endpoint::of(url).url(), in_tx, control: control.clone(), shared: shared.clone() };

        let first = serde_json::json!({ "token": token, "applied": applied, "proto": crate::proto::PROTO });
        let client = link.builder(0).auth(first).connect().await.map_err(|e| Error::Hub(e.to_string()))?;
        let (stop, stopped) = oneshot::channel();
        tokio::spawn(link.run(client, out_rx, control_rx, stopped));
        Ok(NativeHub { outbox, inbox, control, shared, _stop: stop })
    }
}

/// How often the task sends a heartbeat.
///
/// rust_socketio's own reconnect is off, and with it the only place it notices a closed
/// transport, so the hub notices a dead link itself. A relay that says so in `Joined`
/// (`heartbeat`) acknowledges each `alive`; one not acknowledged within [`HEARTBEAT_TIMEOUT`]
/// means a dead link, which also catches a half-open one (laptop sleep, a Wi-Fi roam, a NAT
/// that dropped the mapping) where sends succeed into a socket nothing reads. With an older
/// relay, which has no handler and would never acknowledge, only an emit that fails or hangs
/// counts, so it never sees false drops, but a half-open link there waits for the transport.
///
/// Any message from the relay is proof of life as well, and moves the deadline. An
/// acknowledgement queued behind a large frame arrives only after it, so while a join's
/// catch-up is arriving (from `Joined` until the first acknowledgement after it, and while
/// catch-up pieces say more follow) the deadline also allows one frame at the relay's text cap
/// at [`crate::SLOW_LINK_BYTES_PER_SEC`]. The same goes the other way: a heartbeat written
/// after a large emit leaves only once that emit has, so its deadline starts after the emit's
/// transfer time.
const HEARTBEAT: Duration = Duration::from_secs(5);

/// How long a heartbeat may go unacknowledged.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long one connection attempt, or one emit on top of its own transfer time at
/// [`crate::SLOW_LINK_BYTES_PER_SEC`], may take before the link counts as dead. An emit into a
/// stalled transport can otherwise hang the task.
const EMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest frame the native client reads, for a relay that does not advertise its text
/// cap: rust_socketio's (tungstenite's) default WebSocket frame limit.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// How long rust_socketio keeps a heartbeat's acknowledgement callback: far longer than any
/// deadline [`Connection::overdue`] sets, so a late acknowledgement (one queued behind a large
/// transfer) still counts. The task decides when the link is dead, not the callback's expiry.
const ACK_WINDOW: Duration = Duration::from_secs(600);

/// The heartbeat's event.
const HEARTBEAT_EVENT: &str = "alive";

/// Messages queued while the link is down, at most.
const MAX_QUEUED: usize = 256;

/// What a socket to the relay is built from, kept so the task can build another.
struct Link {
    url: String,
    in_tx: UnboundedSender<ServerMsg>,
    control: UnboundedSender<Control>,
    shared: Arc<Shared>,
}

/// How an emit went.
enum Emitted {
    /// Handed to the transport, with its size in about as many bytes.
    Sent(usize),
    /// It failed or did not finish in time: the link is dead.
    Dead,
    /// The hub was dropped meanwhile.
    Stopped,
}

/// One emit, bounded by [`EMIT_TIMEOUT`] plus its transfer time, and abandoned when the hub
/// is dropped.
async fn emit_on(socket: &Client, msg: &ClientMsg, stopped: &mut oneshot::Receiver<()>) -> Emitted {
    let (event, body) = emit(msg);
    let bytes = json_len(&body);
    let deadline = EMIT_TIMEOUT + transfer_time(bytes);
    tokio::select! {
        _ = stopped => Emitted::Stopped,
        sent = tokio::time::timeout(deadline, socket.emit(event, body)) => match sent {
            Ok(Ok(())) => Emitted::Sent(bytes),
            Ok(Err(e)) => {
                log::warn!("relay send failed: {e}");
                Emitted::Dead
            }
            Err(_) => {
                log::warn!("relay send did not finish in {deadline:?}");
                Emitted::Dead
            }
        },
    }
}

/// About how many bytes `value` takes as JSON, without serializing it: strings by length (the
/// large ones are base64, which needs no escapes), numbers at their longest.
fn json_len(value: &serde_json::Value) -> usize {
    use serde_json::Value;
    match value {
        Value::Null | Value::Bool(_) => 5,
        Value::Number(_) => 24,
        Value::String(s) => s.len() + 2,
        Value::Array(items) => items.iter().map(|v| json_len(v) + 1).sum::<usize>() + 2,
        Value::Object(map) => map.iter().map(|(k, v)| k.len() + 3 + json_len(v) + 1).sum::<usize>() + 2,
    }
}

/// The connection the task holds, and what it knows about it.
struct Connection {
    client: Client,
    generation: u64,
    /// The token generation it was made with.
    token: u64,
    /// The relay joined it. Until then sends are held, so what was queued goes out first.
    joined: bool,
    /// The relay acknowledges heartbeats on it.
    acks: bool,
    /// The heartbeat in flight, and when it was sent.
    beat: Option<(u64, tokio::time::Instant)>,
    beats: u64,
    /// When the relay last sent anything on it.
    heard: tokio::time::Instant,
    /// A join's catch-up may be arriving on it, behind which an acknowledgement waits.
    catching_up: bool,
    /// How long one frame at the relay's cap takes on a slow link.
    frame_time: Duration,
    /// Until when what was written may still be in the socket's buffers: an emit returns once
    /// the transport took it, and a heartbeat written after it waits behind it on the link.
    busy_until: tokio::time::Instant,
}

impl Connection {
    fn new(client: Client, generation: u64, token: u64) -> Connection {
        Connection {
            client,
            generation,
            token,
            joined: false,
            acks: false,
            beat: None,
            beats: 0,
            heard: tokio::time::Instant::now(),
            catching_up: false,
            frame_time: transfer_time(MAX_FRAME_BYTES),
            busy_until: tokio::time::Instant::now(),
        }
    }

    /// `bytes` were handed to the transport: allow them their transfer time on a slow link.
    fn wrote(&mut self, bytes: usize) {
        self.busy_until = self.busy_until.max(tokio::time::Instant::now()) + transfer_time(bytes);
    }

    /// When the heartbeat in flight is overdue, on a relay that acknowledges them: counted
    /// from the latest of its send, the end of the transfer time of what was written before
    /// it, and the last message heard, with a frame's transfer time added while a catch-up may
    /// be arriving.
    fn overdue(&self) -> Option<tokio::time::Instant> {
        let (_, sent) = self.beat?;
        Some(beat_deadline(sent.max(self.busy_until), self.heard, self.catching_up, self.frame_time))
    }
}

/// When a heartbeat sent at `sent` is overdue, with the relay last heard at `heard`.
fn beat_deadline(sent: tokio::time::Instant, heard: tokio::time::Instant, catching_up: bool, frame_time: Duration) -> tokio::time::Instant {
    let slack = if catching_up { HEARTBEAT_TIMEOUT + frame_time } else { HEARTBEAT_TIMEOUT };
    sent.max(heard) + slack
}

impl Link {
    /// A client of `generation` that reports into this hub, without its auth.
    ///
    /// rust_socketio's own reconnect is off: it would reconnect with its own backoff and
    /// generation, past a refused ticket, and after the hub is gone. The hub's task reconnects
    /// instead, through a new client.
    fn builder(&self, generation: u64) -> ClientBuilder {
        // rust_socketio takes a non-root URL path as the transport path, so it is given the
        // derived one: `https://host/craft` connects to `https://host/craft/socket.io/`.
        // Straight to WebSocket. The default starts on long-polling and upgrades, and an emit
        // made during that upgrade can return Ok and never reach the relay; right after a
        // rejoin, when the unconfirmed edits go out, that lost them.
        let mut builder = ClientBuilder::new(self.url.clone()).reconnect(false).transport_type(TransportType::Websocket);

        // "error" is in the list because the relay reports its own refusals
        // that way, and an app needs to show them rather than have them
        // swallowed into a log.
        for event in EVENTS {
            let (tx, control, shared) = (self.in_tx.clone(), self.control.clone(), self.shared.clone());
            builder = builder.on(event, move |payload, _client| {
                let (tx, control) = (tx.clone(), control.clone());
                // A client already replaced speaks for nobody.
                let current = shared.current(generation);
                Box::pin(async move {
                    if !current {
                        return;
                    }
                    for msg in decode_payload(event, payload) {
                        let more = matches!(msg, ServerMsg::TextOps { more: true, .. });
                        let _ = control.send(Control::Heard(generation, more));
                        match &msg {
                            ServerMsg::Joined { heartbeat, text_cap, .. } => {
                                // A frame holds one op at the cap with its wrapper.
                                let frame =
                                    text_cap.and_then(|cap| usize::try_from(cap).ok()).map_or(MAX_FRAME_BYTES, |cap| cap.saturating_add(64 * 1024));
                                let _ = control.send(Control::Joined(generation, *heartbeat, transfer_time(frame.min(MAX_FRAME_BYTES))));
                            }
                            msg if msg.refuses_connection().is_some() => {
                                let _ = control.send(Control::Refused(generation));
                            }
                            _ => {}
                        }
                        // A closed receiver means the app dropped the hub. Not
                        // an error, just nothing left to deliver to.
                        let _ = tx.send(msg);
                    }
                })
            });
        }

        // The relay acknowledged a connection (rust_socketio raises Connect on the server's
        // CONNECT packet).
        let shared = self.shared.clone();
        builder = builder.on(Event::Connect, move |_, _| {
            if shared.current(generation) {
                shared.connected.store(true, Ordering::SeqCst);
            }
            Box::pin(async {})
        });
        let (shared, control) = (self.shared.clone(), self.control.clone());
        builder.on(Event::Close, move |_, _| {
            if shared.current(generation) {
                shared.connected.store(false, Ordering::SeqCst);
                let _ = control.send(Control::Closed(generation));
            }
            Box::pin(async {})
        })
    }

    /// Hold `msg` until the relay has joined the connection again, or drop it when the rejoin
    /// covers it ([`while_down`]). Lock commands keep only the newest per scope. A submit
    /// that does not fit is refused back to the session, so it is not in flight forever.
    fn hold(&self, queued: &mut VecDeque<ClientMsg>, msg: ClientMsg) {
        if matches!(while_down(&msg), WhileDown::Drop) {
            log::debug!("relay link down; dropping a message the rejoin covers");
            return;
        }
        if let ClientMsg::Lock { scope, .. } | ClientMsg::Unlock { scope, .. } = &msg {
            let scope = scope.clone();
            queued.retain(|m| !matches!(m, ClientMsg::Lock { scope: s, .. } | ClientMsg::Unlock { scope: s, .. } if *s == scope));
        }
        if queued.len() < MAX_QUEUED {
            queued.push_back(msg);
            return;
        }
        match msg {
            ClientMsg::Submit { doc, local, .. } => {
                let _ = self.in_tx.send(ServerMsg::Error {
                    doc,
                    detail: "not connected to the relay for too long; that change was not sent".to_owned(),
                    local: Some(local),
                    code: None,
                });
            }
            other => {
                log::warn!("relay link down too long; dropping the oldest queued message");
                queued.pop_front();
                queued.push_back(other);
            }
        }
    }

    /// The link of `conn` is dead: count it as closed.
    fn dead(&self, conn: &Connection) {
        if self.shared.current(conn.generation) {
            self.shared.connected.store(false, Ordering::SeqCst);
            let _ = self.control.send(Control::Closed(conn.generation));
        }
    }

    /// Own the socket: send what the hub queues, and replace the socket when it is gone. Ends
    /// when the hub is dropped, also in the middle of an outage or of an emit.
    async fn run(
        self,
        client: Client,
        mut out_rx: UnboundedReceiver<ClientMsg>,
        mut control_rx: UnboundedReceiver<Control>,
        mut stopped: oneshot::Receiver<()>,
    ) {
        let mut conn = Some(Connection::new(client, 0, 0));
        let mut queued: VecDeque<ClientMsg> = VecDeque::new();
        let mut backoff = Backoff::default();
        let mut refused = false;
        // When the next connection attempt is due, while there is no socket.
        let mut retry: Option<tokio::time::Instant> = None;
        let mut heartbeat = tokio::time::interval(HEARTBEAT);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        'run: loop {
            let due = retry.unwrap_or_else(tokio::time::Instant::now);
            // When the heartbeat in flight is overdue, on a relay that acknowledges them.
            let overdue = conn.as_ref().and_then(Connection::overdue);
            // Biased: a control message was sent before the inbox message the app reacts to,
            // so handling it first means a `Joined` is seen before anything sent because of it.
            tokio::select! {
                biased;
                _ = &mut stopped => break,
                control = control_rx.recv() => {
                    let Some(control) = control else { break };
                    match control {
                        Control::Joined(generation, acks, frame_time) if self.shared.current(generation) => {
                            backoff.reset();
                            // Never before the client is in hand: rust_socketio starts reading
                            // the socket only as `connect()` returns, and this task handles
                            // controls only after storing what it returned.
                            if let Some(c) = conn.as_mut().filter(|c| c.generation == generation) {
                                c.joined = true;
                                c.acks = acks;
                                c.heard = tokio::time::Instant::now();
                                c.catching_up = true;
                                c.frame_time = frame_time;
                                while let Some(msg) = queued.pop_front() {
                                    match emit_on(&c.client, &msg, &mut stopped).await {
                                        Emitted::Sent(bytes) => c.wrote(bytes),
                                        Emitted::Dead => {
                                            self.dead(c);
                                            break;
                                        }
                                        Emitted::Stopped => break 'run,
                                    }
                                }
                            }
                        }
                        Control::Alive(generation, n) => {
                            if let Some(c) = conn.as_mut().filter(|c| c.generation == generation) {
                                c.heard = tokio::time::Instant::now();
                                // Acknowledged behind whatever the relay queued before it, so
                                // the join's catch-up, if any, is in.
                                c.catching_up = false;
                                if c.beat.is_some_and(|(b, _)| b == n) {
                                    c.beat = None;
                                }
                            }
                        }
                        Control::Heard(generation, more) => {
                            if let Some(c) = conn.as_mut().filter(|c| c.generation == generation) {
                                c.heard = tokio::time::Instant::now();
                                c.catching_up |= more;
                            }
                        }
                        Control::Refused(generation) if self.shared.current(generation) => {
                            // Refused for a ticket that has been replaced since: try the new one
                            // as soon as this socket is gone.
                            if conn.as_ref().is_some_and(|c| c.token == self.shared.token().1) {
                                refused = true;
                            } else {
                                backoff.reset();
                            }
                        }
                        Control::Token => {
                            if refused {
                                refused = false;
                                backoff.reset();
                                retry = conn.is_none().then(tokio::time::Instant::now);
                            }
                        }
                        Control::Closed(generation) if self.shared.current(generation) && conn.is_some() => {
                            if let Some(old) = conn.take() {
                                let _ = tokio::time::timeout(EMIT_TIMEOUT, old.client.disconnect()).await;
                            }
                            retry = (!refused).then(|| tokio::time::Instant::now() + backoff.next());
                        }
                        Control::Closed(_) | Control::Joined(..) | Control::Refused(_) => {}
                    }
                }
                msg = out_rx.recv() => {
                    let Some(msg) = msg else { break };
                    match conn.as_mut().filter(|c| c.joined && self.shared.connected.load(Ordering::SeqCst)) {
                        Some(c) => match emit_on(&c.client, &msg, &mut stopped).await {
                            Emitted::Sent(bytes) => c.wrote(bytes),
                            Emitted::Dead => self.dead(c),
                            Emitted::Stopped => break 'run,
                        },
                        None => self.hold(&mut queued, msg),
                    }
                }
                _ = heartbeat.tick(), if conn.as_ref().is_some_and(|c| c.joined) => {
                    let Some(c) = conn.as_mut() else { continue };
                    if c.beat.is_some() {
                        continue;
                    }
                    c.beats += 1;
                    let (n, generation, control) = (c.beats, c.generation, self.control.clone());
                    let beat = c.client.emit_with_ack(HEARTBEAT_EVENT, serde_json::json!({}), ACK_WINDOW, move |_, _| {
                        let _ = control.send(Control::Alive(generation, n));
                        Box::pin(async {})
                    });
                    let sent = tokio::select! {
                        _ = &mut stopped => break,
                        sent = tokio::time::timeout(EMIT_TIMEOUT, beat) => matches!(sent, Ok(Ok(()))),
                    };
                    if sent {
                        // Only a relay that acknowledges is waited on; for an older one the
                        // emit succeeding is all there is to know.
                        c.beat = c.acks.then(|| (n, tokio::time::Instant::now()));
                    } else {
                        self.dead(c);
                    }
                }
                () = tokio::time::sleep_until(overdue.unwrap_or_else(tokio::time::Instant::now)), if overdue.is_some() => {
                    if let Some(c) = conn.as_mut() {
                        log::warn!("relay did not acknowledge a heartbeat in {HEARTBEAT_TIMEOUT:?}; the link is dead");
                        c.beat = None;
                        self.dead(c);
                    }
                }
                () = tokio::time::sleep_until(due), if retry.is_some() && conn.is_none() && !refused => {
                    retry = None;
                    let generation = self.shared.generation.fetch_add(1, Ordering::SeqCst) + 1;
                    let (auth, token) = self.shared.rejoin();
                    // A cancelled attempt (the timeout, or the hub dropped) leaves nothing behind:
                    // rust_socketio 0.6 spawns the task that reads the socket as the last step of
                    // `connect()`, with no await after it, so before that every handle to the
                    // socket lives in the future and is dropped with it, closing the socket.
                    let attempt = tokio::select! {
                        // Dropped while connecting: no join on behalf of a hub that is gone.
                        _ = &mut stopped => break,
                        attempt = tokio::time::timeout(EMIT_TIMEOUT, self.builder(generation).auth(auth).connect()) => attempt,
                    };
                    match attempt {
                        Ok(Ok(client)) => {
                            conn = Some(Connection::new(client, generation, token));
                        }
                        failed => {
                            let wait = backoff.next();
                            let why = match failed {
                                Ok(Err(e)) => e.to_string(),
                                _ => format!("no answer in {EMIT_TIMEOUT:?}"),
                            };
                            log::info!("relay reconnect failed, trying again in {wait:?}: {why}");
                            retry = Some(tokio::time::Instant::now() + wait);
                        }
                    }
                }
            }
        }
        // The hub is gone. Close the socket rather than leaking it for the process lifetime.
        if let Some(c) = conn {
            let _ = tokio::time::timeout(EMIT_TIMEOUT, c.client.disconnect()).await;
        }
    }
}

impl Hub for NativeHub {
    fn send(&mut self, msg: ClientMsg) -> Result<()> {
        self.outbox.send(msg).map_err(|_| Error::Hub("relay connection is gone".to_owned()))
    }

    fn poll(&mut self) -> Vec<ServerMsg> {
        let mut out = Vec::new();
        // try_recv, never recv. A frame must not block waiting for the relay.
        while let Ok(msg) = self.inbox.try_recv() {
            out.push(msg);
        }
        out
    }

    fn connected(&self) -> bool {
        self.shared.connected.load(Ordering::SeqCst)
    }

    fn set_token(&mut self, token: String) {
        if let Ok(mut current) = self.shared.token.lock() {
            *current = (token, current.1 + 1);
        }
        let _ = self.control.send(Control::Token);
    }
}

/// Turn one Socket.IO payload into protocol messages.
///
/// A payload can carry several values, and a malformed one is skipped with a
/// log line. Dropping a message we cannot read beats tearing down a live
/// session over it.
fn decode_payload(event: &str, payload: Payload) -> Vec<ServerMsg> {
    let values: Vec<serde_json::Value> = match payload {
        Payload::Text(values) => values,
        Payload::Binary(_) => {
            log::debug!("ignoring a binary payload; the protocol is JSON");
            return Vec::new();
        }
        // Deprecated upstream in favour of Payload::Text, and still matched so
        // a relay on an older protocol does not go silently unread. The allow
        // is narrow on purpose: dropping the arm would turn an old server into
        // a session that connects and then receives nothing.
        #[allow(deprecated)]
        Payload::String(s) => match serde_json::from_str(&s) {
            Ok(v) => vec![v],
            Err(e) => {
                log::warn!("unreadable string payload on {event}: {e}");
                return Vec::new();
            }
        },
    };

    values.into_iter().filter_map(|v| receive(event, v)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anything_heard_moves_the_heartbeat_deadline() {
        let sent = tokio::time::Instant::now();
        let frame = transfer_time(MAX_FRAME_BYTES);
        assert_eq!(beat_deadline(sent, sent - Duration::from_secs(3), false, frame), sent + HEARTBEAT_TIMEOUT);
        let later = sent + Duration::from_secs(8);
        assert_eq!(beat_deadline(sent, later, false, frame), later + HEARTBEAT_TIMEOUT);
    }

    #[test]
    fn a_catch_up_in_flight_allows_one_frame_on_a_slow_link() {
        let sent = tokio::time::Instant::now();
        let frame = transfer_time(4 * 1024 * 1024);
        assert_eq!(frame, Duration::from_secs(64));
        assert_eq!(beat_deadline(sent, sent, true, frame), sent + HEARTBEAT_TIMEOUT + frame);
        assert!(ACK_WINDOW > HEARTBEAT_TIMEOUT + transfer_time(MAX_FRAME_BYTES), "a late acknowledgement is still delivered");
    }

    #[test]
    fn an_emit_deadline_grows_with_its_body() {
        let body = serde_json::json!({ "local": 1, "params": { "u": "x".repeat(640 * 1024) } });
        let estimate = json_len(&body);
        let exact = serde_json::to_vec(&body).map(|v| v.len()).unwrap_or_default();
        assert!(estimate >= exact && estimate < exact + 128, "{estimate} for {exact}");
        assert_eq!(transfer_time(estimate).as_secs(), 10);
    }
}
