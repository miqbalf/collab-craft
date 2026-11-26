//! A [`Hub`] for a browser, over the JS `socket.io-client`.
//!
//! Behind the `web-hub` feature. `rust_socketio` depends on `native-tls` unconditionally and
//! therefore cannot build for `wasm32`, so a browser build leaves [`crate::native`] off and uses
//! this instead.
//!
//! The page supplies the client. `socket.io-client` is loaded as a script and leaves `io` on the
//! global object, which is what this reaches for by name. Nothing is bound at link time, so a
//! page that forgot the script gets a sentence saying so rather than a module that will not
//! instantiate.
//!
//! Callbacks push into a queue and [`WebHub::poll`] drains it, exactly as the native hub does,
//! because [`Hub`] is polled rather than called back. There is no channel and no lock: a browser
//! runs this on one thread, and a queue behind a [`RefCell`] is the honest description of that.
//!
//! An app that draws frames only when asked (egui) would still see a message only on its next
//! poll. [`WebHub::set_wake`] gives the handlers a callback that runs after each message is
//! queued, and after the link connects or drops, so the app polls at once ([`crate::Wake`]).
//!
//! The protocol itself is not repeated here. [`emit`] and [`receive`] are the same functions the
//! native hub calls, so a browser peer and a desktop peer cannot drift apart over an event name.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use js_sys::{Function, JSON, Object, Reflect};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};

use crate::error::{Error, Result};
use crate::hub::{Backoff, EVENTS, Endpoint, Hub, WhileDown, emit, receive, while_down};

/// Reconnect state the socket's handlers share with the hub.
#[derive(Default)]
struct Link {
    backoff: Backoff,
    /// The relay refused the connection itself; nothing reconnects until a new token.
    refused: bool,
    /// The pending reconnect timer, from `setTimeout`.
    timer: Option<JsValue>,
    /// The hub was dropped: nothing may connect on its behalf.
    dropped: bool,
}
use crate::proto::{ClientMsg, ServerMsg};
use crate::wake::Wake;

/// A connection to the relay from a browser.
pub struct WebHub {
    socket: JsValue,
    /// The handshake auth socket.io-client sends on every attempt; [`Hub::set_token`] edits it.
    auth: Object,
    link: Rc<RefCell<Link>>,
    inbox: Rc<RefCell<VecDeque<ServerMsg>>>,
    /// What the handlers call once something arrived ([`WebHub::set_wake`]).
    wake: Wake,
    /// The handlers, kept alive while the socket can still call them. Dropping a [`Closure`]
    /// invalidates the function JavaScript is holding, and the next event would then tear the
    /// page down rather than deliver a message.
    _handlers: Vec<Closure<dyn FnMut(JsValue)>>,
}

impl WebHub {
    /// Connect, through the `socket.io-client` the page loaded.
    ///
    /// `url` is the relay as the deployment publishes it, base path included
    /// (`https://host/craft`); the Socket.IO path is derived from it ([`Endpoint`]).
    /// `token` is what the service minted. It travels in the Socket.IO
    /// handshake auth, so it never appears in a query string and therefore never lands in an
    /// access log.
    ///
    /// `applied` is the sequence this connection has already applied, when it is resuming a
    /// [`crate::Session`] rather than opening the document fresh. Pass `Session::applied()`
    /// when reconnecting with a session that survived the drop, and `None` on a first join or
    /// after reloading the file. The relay joins at `applied` when it is given and at the
    /// saved file's sequence otherwise, so a reconnect is caught up on exactly what it missed
    /// rather than on what the file happened to be saved at.
    ///
    /// Returns as soon as the socket exists. Socket.IO connects in the background and queues what
    /// is sent meanwhile, and [`Hub::connected`] says whether it has arrived.
    pub fn connect(url: &str, token: &str, applied: Option<u64>) -> Result<WebHub> {
        WebHub::connect_as(url, token, applied, None)
    }

    /// [`WebHub::connect`] naming the replica schema the app edits with (`"gridcraft/1"`) in
    /// the handshake, which socket.io-client sends again on every reconnect. The relay pins the
    /// room to it and refuses a join naming another ([`crate::REFUSED_SCHEMA`]).
    pub fn connect_as(url: &str, token: &str, applied: Option<u64>, schema: Option<&str>) -> Result<WebHub> {
        let global = js_sys::global();
        let io = function(global.as_ref(), "io").ok_or_else(|| Error::Hub("this page has not loaded socket.io-client".to_owned()))?;

        let auth = Object::new();
        Reflect::set(&auth, &JsValue::from_str("token"), &JsValue::from_str(token)).map_err(js)?;
        let applied_js = match applied {
            Some(applied) => JsValue::from_f64(applied as f64),
            None => JsValue::NULL,
        };
        Reflect::set(&auth, &JsValue::from_str("applied"), &applied_js).map_err(js)?;
        Reflect::set(&auth, &JsValue::from_str("proto"), &JsValue::from_str(crate::proto::PROTO)).map_err(js)?;
        if let Some(schema) = schema {
            Reflect::set(&auth, &JsValue::from_str("schema"), &JsValue::from_str(schema)).map_err(js)?;
        }
        let options = Object::new();
        Reflect::set(&options, &JsValue::from_str("auth"), &auth).map_err(js)?;
        // socket.io-client reads a URL path as a namespace, so the relay's base path goes in
        // `path` and only the origin (and any query) in the URL.
        let endpoint = Endpoint::of(url);
        Reflect::set(&options, &JsValue::from_str("path"), &JsValue::from_str(&endpoint.path)).map_err(js)?;
        let server = match &endpoint.query {
            Some(query) => format!("{}?{query}", endpoint.origin),
            None => endpoint.origin.clone(),
        };

        let socket = io.call2(global.as_ref(), &JsValue::from_str(&server), &options).map_err(js)?;
        let on = function(&socket, "on").ok_or_else(|| Error::Hub("socket.io gave back something that is not a socket".to_owned()))?;

        let inbox = Rc::new(RefCell::new(VecDeque::new()));
        let mut handlers = Vec::with_capacity(EVENTS.len());
        // "error" is in the set because the relay reports its own refusals that way, and an app
        // has to show them rather than have them swallowed into a log.
        let link = Rc::new(RefCell::new(Link::default()));
        let wake = Wake::default();
        for event in EVENTS {
            let queue = Rc::clone(&inbox);
            let state = Rc::clone(&link);
            let woken = wake.clone();
            let handler = Closure::<dyn FnMut(JsValue)>::new(move |payload: JsValue| {
                if let Some(msg) = read(event, &payload) {
                    if matches!(msg, ServerMsg::Joined { .. }) {
                        state.borrow_mut().backoff.reset();
                    } else if msg.refuses_connection().is_some() {
                        state.borrow_mut().refused = true;
                    }
                    queue.borrow_mut().push_back(msg);
                    woken.fire();
                }
            });
            on.call2(&socket, &JsValue::from_str(event), handler.as_ref()).map_err(js)?;
            handlers.push(handler);
        }

        // A reconnect joins without `applied`: what this connection applied may be mid
        // catch-up, so the relay joins it at the saved sequence instead. socket.io-client
        // sends the same `auth` object on every attempt, so it is cleared once the first
        // connection is up.
        let first = auth.clone();
        let woken = wake.clone();
        let cleared = Closure::<dyn FnMut(JsValue)>::new(move |_: JsValue| {
            let _ = Reflect::set(&first, &JsValue::from_str("applied"), &JsValue::NULL);
            // Connected: the app's status line changes.
            woken.fire();
        });
        on.call2(&socket, &JsValue::from_str("connect"), cleared.as_ref()).map_err(js)?;
        handlers.push(cleared);
        // socket.io-client reconnects a dropped link on its own, but not one the server closed
        // (a relay restart, or a join it had to cut). That one is reconnected here, after a
        // backoff kept across attempts, and not at all once the relay refused the connection
        // itself: the same ticket would be refused again.
        let reopen = socket.clone();
        let state = Rc::clone(&link);
        let woken = wake.clone();
        let reopened = Closure::<dyn FnMut(JsValue)>::new(move |reason: JsValue| {
            // Dropped: the app's status line changes.
            woken.fire();
            if reason.as_string().as_deref() != Some("io server disconnect") {
                return;
            }
            let wait = {
                let mut link = state.borrow_mut();
                if link.refused || link.dropped || link.timer.is_some() {
                    return;
                }
                link.backoff.next()
            };
            let (socket, fired) = (reopen.clone(), Rc::clone(&state));
            let later = Closure::once_into_js(move || {
                fired.borrow_mut().timer = None;
                connect_now(&socket, &fired.borrow());
            });
            if let Some(set_timeout) = function(js_sys::global().as_ref(), "setTimeout") {
                state.borrow_mut().timer = set_timeout.call2(js_sys::global().as_ref(), &later, &JsValue::from_f64(wait.as_millis() as f64)).ok();
            }
        });
        on.call2(&socket, &JsValue::from_str("disconnect"), reopened.as_ref()).map_err(js)?;
        handlers.push(reopened);
        // A failed connect or reconnect attempt changes what the app's status line says.
        let woken = wake.clone();
        let failed = Closure::<dyn FnMut(JsValue)>::new(move |_: JsValue| woken.fire());
        on.call2(&socket, &JsValue::from_str("connect_error"), failed.as_ref()).map_err(js)?;
        handlers.push(failed);

        Ok(WebHub { socket, auth, link, inbox, wake, _handlers: handlers })
    }

    /// Call `wake` whenever a message from the relay is queued for [`Hub::poll`], and when the
    /// link connects or drops, in place of any earlier callback. An egui app passes
    /// `move || ctx.request_repaint()`, so a remote edit is drawn on the next frame rather than
    /// when the app next happens to poll.
    pub fn set_wake(&self, wake: impl Fn() + 'static) {
        self.wake.set(wake);
    }
}

impl Hub for WebHub {
    fn send(&mut self, msg: ClientMsg) -> Result<()> {
        // socket.io-client buffers what is sent while disconnected and flushes it on the
        // reconnect. Edit submits are dropped instead, as the native hub drops them: a
        // [`crate::Live`] sends them again after the rejoin, merged, and the buffered copy
        // would go out a second time. So are save reports, which must not reach the relay
        // after the rejoin reported their checkpoint raced.
        if !self.connected() && matches!(while_down(&msg), WhileDown::Drop) {
            log::debug!("relay link down; dropping a message the rejoin covers");
            return Ok(());
        }
        let (event, body) = emit(&msg);
        let payload = to_js(&body)?;
        let emitting = function(&self.socket, "emit").ok_or_else(|| Error::Hub("the relay connection is gone".to_owned()))?;
        emitting.call2(&self.socket, &JsValue::from_str(event), &payload).map_err(js)?;
        Ok(())
    }

    fn poll(&mut self) -> Vec<ServerMsg> {
        // Drained whole. A frame must not block waiting for the relay, and nothing here can
        // block: the queue is already full of what the callbacks delivered.
        self.inbox.borrow_mut().drain(..).collect()
    }

    fn set_wake(&mut self, wake: &Wake) {
        let wake = wake.clone();
        self.wake.set(move || wake.fire());
    }

    fn connected(&self) -> bool {
        // Read off the socket each time rather than tracked here. Socket.IO reconnects on its
        // own, and a flag of ours would have to be told about every one of those.
        Reflect::get(&self.socket, &JsValue::from_str("connected")).ok().and_then(|value| value.as_bool()).unwrap_or(false)
    }

    fn set_space(&mut self, space: u32) {
        // socket.io-client sends this same `auth` object on every reconnect attempt.
        let _ = Reflect::set(&self.auth, &JsValue::from_str("space"), &JsValue::from_f64(f64::from(space)));
    }

    fn set_token(&mut self, token: String) {
        let _ = Reflect::set(&self.auth, &JsValue::from_str("token"), &JsValue::from_str(&token));
        let restart = {
            let mut link = self.link.borrow_mut();
            let restart = std::mem::take(&mut link.refused);
            if restart {
                link.backoff.reset();
            }
            restart
        };
        if restart {
            connect_now(&self.socket, &self.link.borrow());
        }
    }
}

impl Drop for WebHub {
    /// Close the socket rather than leaving it open for the life of the page.
    fn drop(&mut self) {
        self.wake.clear();
        let timer = {
            let mut link = self.link.borrow_mut();
            link.dropped = true;
            link.timer.take()
        };
        if let (Some(timer), Some(clear)) = (timer, function(js_sys::global().as_ref(), "clearTimeout")) {
            let _ = clear.call1(js_sys::global().as_ref(), &timer);
        }
        if let Some(disconnect) = function(&self.socket, "disconnect") {
            let _ = disconnect.call0(&self.socket);
        }
    }
}

/// One Socket.IO payload, as a protocol message.
///
/// The payload crosses as JSON text. `JSON.stringify` and `serde_json` agree on that, which is
/// one conversion with no schema of its own in the middle.
fn read(event: &str, payload: &JsValue) -> Option<ServerMsg> {
    let text = match JSON::stringify(payload).ok().and_then(|text| text.as_string()) {
        Some(text) => text,
        None => {
            log::warn!("a {event} payload could not be read as JSON");
            return None;
        }
    };
    match serde_json::from_str(&text) {
        Ok(value) => receive(event, value),
        Err(e) => {
            log::warn!("unreadable {event} payload: {e}");
            None
        }
    }
}

/// One JSON value, as something a Socket.IO client can send.
fn to_js(value: &serde_json::Value) -> Result<JsValue> {
    if value.is_null() {
        return Ok(JsValue::NULL);
    }
    let text = serde_json::to_string(value).map_err(|e| Error::malformed("client", e.to_string()))?;
    JSON::parse(&text).map_err(js)
}

/// Ask socket.io-client to connect the socket again, unless the hub is gone or its ticket was
/// refused.
fn connect_now(socket: &JsValue, link: &Link) {
    if link.dropped || link.refused {
        return;
    }
    if let Some(connect) = function(socket, "connect") {
        let _ = connect.call0(socket);
    }
}

/// One function off an object, by name.
fn function(object: &JsValue, name: &str) -> Option<Function> {
    Reflect::get(object, &JsValue::from_str(name)).ok()?.dyn_into().ok()
}

fn js(error: JsValue) -> Error {
    Error::Hub(describe(&error))
}

/// A JavaScript error in words.
///
/// An `Error` object serialises to `{}`, so its message is read off it rather than stringified.
/// Without this every browser failure reads the same.
fn describe(value: &JsValue) -> String {
    if let Some(text) = value.as_string() {
        return text;
    }
    if let Ok(message) = Reflect::get(value, &JsValue::from_str("message"))
        && let Some(text) = message.as_string()
    {
        return text;
    }
    JSON::stringify(value).ok().and_then(|text| text.as_string()).unwrap_or_else(|| "the browser gave an error it could not describe".to_owned())
}
