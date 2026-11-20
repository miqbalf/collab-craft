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

use rust_socketio::asynchronous::{Client, ClientBuilder, ReconnectSettings};
use rust_socketio::{Event, Payload};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::hub::{Backoff, EVENTS, Endpoint, Hub, emit, receive};
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
    /// The token the next (re)connect joins with. [`Hub::set_token`] replaces it.
    token: Mutex<String>,
}

impl Shared {
    fn token(&self) -> String {
        self.token.lock().map(|t| t.clone()).unwrap_or_default()
    }

    /// The auth a reconnect sends: the current token, and never `applied`, because what a
    /// connection applied may be mid catch-up and is not a baseline the relay can replay from.
    fn rejoin(&self) -> serde_json::Value {
        serde_json::json!({ "token": self.token(), "proto": crate::proto::PROTO })
    }

    fn current(&self, generation: u64) -> bool {
        self.generation.load(Ordering::SeqCst) == generation
    }
}

/// What the callbacks and the hub tell the socket task.
enum Control {
    /// The relay closed the socket of this generation.
    Closed(u64),
    /// The relay joined this connection; the reconnect backoff starts over.
    Joined,
    /// The relay refused the connection itself (a bad or expired token, another protocol).
    /// Reconnecting with the same auth cannot help, so the task stops until a new token.
    Refused,
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
        let shared = Arc::new(Shared { connected: AtomicBool::new(false), generation: AtomicU64::new(0), token: Mutex::new(token.to_owned()) });
        let link = Link { url: Endpoint::of(url).url(), in_tx, control: control.clone(), shared: shared.clone() };

        let first = serde_json::json!({ "token": token, "applied": applied, "proto": crate::proto::PROTO });
        let client = link.builder(0, first).connect().await.map_err(|e| Error::Hub(e.to_string()))?;
        let (stop, stopped) = oneshot::channel();
        tokio::spawn(link.run(client, out_rx, control_rx, stopped));
        Ok(NativeHub { outbox, inbox, control, shared, _stop: stop })
    }
}

/// Where rust_socketio's own reconnect attempt is sent: a closed port, so it fails at once.
const NOWHERE: &str = "http://127.0.0.1:1/socket.io/";

/// What a socket to the relay is built from, kept so the task can build another.
struct Link {
    url: String,
    in_tx: UnboundedSender<ServerMsg>,
    control: UnboundedSender<Control>,
    shared: Arc<Shared>,
}

impl Link {
    /// A client of `generation` that joins with `auth` and reports into this hub.
    fn builder(&self, generation: u64, auth: serde_json::Value) -> ClientBuilder {
        // rust_socketio takes a non-root URL path as the transport path, so it is given the
        // derived one: `https://host/craft` connects to `https://host/craft/socket.io/`.
        let (shared, control) = (self.shared.clone(), self.control.clone());
        let mut builder = ClientBuilder::new(self.url.clone())
            .auth(auth)
            // rust_socketio notices a dropped link only inside its own reconnect loop, which
            // calls this before its attempt. That attempt is sent nowhere (a closed local
            // port) and is its only one: the hub's task reconnects instead, so that every
            // reconnect shares one backoff, one generation count, and stops with the hub.
            .max_reconnect_attempts(1)
            .on_reconnect(move || {
                if shared.current(generation) {
                    shared.connected.store(false, Ordering::SeqCst);
                    let _ = control.send(Control::Closed(generation));
                }
                let mut settings = ReconnectSettings::new();
                settings.address(NOWHERE);
                Box::pin(async move { settings })
            });

        // "error" is in the list because the relay reports its own refusals
        // that way, and an app needs to show them rather than have them
        // swallowed into a log.
        for event in EVENTS {
            let (tx, control) = (self.in_tx.clone(), self.control.clone());
            builder = builder.on(event, move |payload, _client| {
                let (tx, control) = (tx.clone(), control.clone());
                Box::pin(async move {
                    for msg in decode_payload(event, payload) {
                        match &msg {
                            ServerMsg::Joined { .. } => {
                                let _ = control.send(Control::Joined);
                            }
                            msg if msg.refuses_connection().is_some() => {
                                let _ = control.send(Control::Refused);
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
        // CONNECT packet), the first or a reconnect.
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

    /// Own the socket: send what the hub queues, and replace the socket when the relay closes
    /// it. Ends when the hub is dropped, also in the middle of an outage.
    async fn run(
        self,
        client: Client,
        mut out_rx: UnboundedReceiver<ClientMsg>,
        mut control_rx: UnboundedReceiver<Control>,
        mut stopped: oneshot::Receiver<()>,
    ) {
        let mut client = Some(client);
        let mut backoff = Backoff::default();
        let mut refused = false;
        // When the next connection attempt is due, while there is no socket.
        let mut retry: Option<tokio::time::Instant> = None;
        loop {
            let due = retry.unwrap_or_else(tokio::time::Instant::now);
            tokio::select! {
                _ = &mut stopped => break,
                msg = out_rx.recv() => {
                    let Some(msg) = msg else { break };
                    let Some(socket) = client.as_ref().filter(|_| self.shared.connected.load(Ordering::SeqCst)) else {
                        log::debug!("relay link down; dropping a message");
                        continue;
                    };
                    let (event, body) = emit(&msg);
                    if let Err(e) = socket.emit(event, body).await {
                        // The link state is the relay's to say: a dead socket closes or
                        // reconnects, and that is what moves `connected`.
                        log::warn!("relay send failed: {e}");
                    }
                }
                control = control_rx.recv() => {
                    let Some(control) = control else { break };
                    match control {
                        Control::Joined => backoff.reset(),
                        Control::Refused => refused = true,
                        Control::Token => {
                            if refused {
                                refused = false;
                                backoff.reset();
                                retry = client.is_none().then(tokio::time::Instant::now);
                            }
                        }
                        Control::Closed(generation) if self.shared.current(generation) && client.is_some() => {
                            // A dropped link, or a socket the relay closed (which rust_socketio
                            // would leave closed). Either way it is replaced here, after the
                            // backoff, unless the relay refused this connection's ticket.
                            if let Some(old) = client.take() {
                                let _ = old.disconnect().await;
                            }
                            retry = (!refused).then(|| tokio::time::Instant::now() + backoff.next());
                        }
                        Control::Closed(_) => {}
                    }
                }
                () = tokio::time::sleep_until(due), if retry.is_some() && client.is_none() => {
                    retry = None;
                    let generation = self.shared.generation.fetch_add(1, Ordering::SeqCst) + 1;
                    let attempt = tokio::select! {
                        // Dropped while connecting: no join on behalf of a hub that is gone.
                        _ = &mut stopped => break,
                        attempt = self.builder(generation, self.shared.rejoin()).connect() => attempt,
                    };
                    match attempt {
                        Ok(socket) => client = Some(socket),
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
            *current = token;
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
