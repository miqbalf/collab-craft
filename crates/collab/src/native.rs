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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rust_socketio::asynchronous::{Client, ClientBuilder};
use rust_socketio::{Event, Payload};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::hub::{Backoff, EVENTS, Endpoint, Hub, WhileDown, emit, receive, while_down};
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
    /// The socket of this generation is gone: the relay closed it, or a heartbeat could not be
    /// sent on it.
    Closed(u64),
    /// The relay joined the connection of this generation; the backoff starts over and what
    /// was queued while the link was down goes out.
    Joined(u64),
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

/// How often the task sends a heartbeat on an idle socket. rust_socketio's own reconnect is
/// off, and with it the only place it notices a dropped link, so the hub notices it here:
/// an emit on a socket whose transport has closed fails.
const HEARTBEAT: Duration = Duration::from_secs(2);

/// The heartbeat's event. The relay has no handler for it and drops it.
const HEARTBEAT_EVENT: &str = "alive";

/// Messages queued while the link is down, at most. Older ones are dropped past it.
const MAX_QUEUED: usize = 256;

/// What a socket to the relay is built from, kept so the task can build another.
struct Link {
    url: String,
    in_tx: UnboundedSender<ServerMsg>,
    control: UnboundedSender<Control>,
    shared: Arc<Shared>,
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
        let mut builder = ClientBuilder::new(self.url.clone()).reconnect(false);

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
                        match &msg {
                            ServerMsg::Joined { .. } => {
                                let _ = control.send(Control::Joined(generation));
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

    /// Own the socket: send what the hub queues, and replace the socket when it is gone. Ends
    /// when the hub is dropped, also in the middle of an outage.
    async fn run(
        self,
        client: Client,
        mut out_rx: UnboundedReceiver<ClientMsg>,
        mut control_rx: UnboundedReceiver<Control>,
        mut stopped: oneshot::Receiver<()>,
    ) {
        let mut client = Some(client);
        // The token generation the current connection was made with.
        let mut client_token = 0;
        let mut queued: std::collections::VecDeque<ClientMsg> = std::collections::VecDeque::new();
        let mut backoff = Backoff::default();
        let mut refused = false;
        // When the next connection attempt is due, while there is no socket.
        let mut retry: Option<tokio::time::Instant> = None;
        let mut heartbeat = tokio::time::interval(HEARTBEAT);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let due = retry.unwrap_or_else(tokio::time::Instant::now);
            tokio::select! {
                _ = &mut stopped => break,
                msg = out_rx.recv() => {
                    let Some(msg) = msg else { break };
                    match client.as_ref().filter(|_| self.shared.connected.load(Ordering::SeqCst)) {
                        Some(socket) => send(socket, &msg).await,
                        None => match while_down(&msg) {
                            WhileDown::Drop => log::debug!("relay link down; dropping a message the rejoin covers"),
                            WhileDown::Queue => {
                                if queued.len() >= MAX_QUEUED {
                                    log::warn!("relay link down too long; dropping the oldest queued message");
                                    queued.pop_front();
                                }
                                queued.push_back(msg);
                            }
                        },
                    }
                }
                _ = heartbeat.tick(), if client.is_some() && self.shared.connected.load(Ordering::SeqCst) => {
                    if let Some(socket) = client.as_ref()
                        && socket.emit(HEARTBEAT_EVENT, serde_json::json!({})).await.is_err()
                    {
                        let generation = self.shared.generation.load(Ordering::SeqCst);
                        self.shared.connected.store(false, Ordering::SeqCst);
                        let _ = self.control.send(Control::Closed(generation));
                    }
                }
                control = control_rx.recv() => {
                    let Some(control) = control else { break };
                    match control {
                        Control::Joined(generation) if self.shared.current(generation) => {
                            backoff.reset();
                            if let Some(socket) = client.as_ref() {
                                while let Some(msg) = queued.pop_front() {
                                    send(socket, &msg).await;
                                }
                            }
                        }
                        Control::Refused(generation) if self.shared.current(generation) => {
                            // Refused for a ticket that has been replaced since: try the new one
                            // as soon as this socket is gone.
                            if self.shared.token().1 == client_token {
                                refused = true;
                            } else {
                                backoff.reset();
                            }
                        }
                        Control::Token => {
                            if refused {
                                refused = false;
                                backoff.reset();
                                retry = client.is_none().then(tokio::time::Instant::now);
                            }
                        }
                        Control::Closed(generation) if self.shared.current(generation) && client.is_some() => {
                            if let Some(old) = client.take() {
                                let _ = old.disconnect().await;
                            }
                            retry = (!refused).then(|| tokio::time::Instant::now() + backoff.next());
                        }
                        Control::Closed(_) | Control::Joined(_) | Control::Refused(_) => {}
                    }
                }
                () = tokio::time::sleep_until(due), if retry.is_some() && client.is_none() && !refused => {
                    retry = None;
                    let generation = self.shared.generation.fetch_add(1, Ordering::SeqCst) + 1;
                    let (auth, token) = self.shared.rejoin();
                    let attempt = tokio::select! {
                        // Dropped while connecting: no join on behalf of a hub that is gone.
                        _ = &mut stopped => break,
                        attempt = self.builder(generation).auth(auth).connect() => attempt,
                    };
                    match attempt {
                        Ok(socket) => {
                            client = Some(socket);
                            client_token = token;
                        }
                        Err(e) => {
                            let wait = backoff.next();
                            log::info!("relay reconnect failed, trying again in {wait:?}: {e}");
                            retry = Some(tokio::time::Instant::now() + wait);
                        }
                    }
                }
            }
        }
        // The hub is gone. Close the socket rather than leaking it for the process lifetime.
        if let Some(socket) = client {
            let _ = socket.disconnect().await;
        }
    }
}

/// Send one message. A failure is logged and left to the heartbeat and the relay's close,
/// which are what move the link state.
async fn send(socket: &Client, msg: &ClientMsg) {
    let (event, body) = emit(msg);
    if let Err(e) = socket.emit(event, body).await {
        log::warn!("relay send failed: {e}");
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
