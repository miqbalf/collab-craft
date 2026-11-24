//! End-to-end tests of a hub's reconnect: a dropped link, a socket the relay closes, a ticket
//! the relay refuses and the token refresh that recovers from it. Ignored by default; they need
//! a throwaway Valkey in `REVIEW_VALKEY_URL`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use craft_collab::native::NativeHub;
use craft_collab::{Hub, Live, Work};
use socketioxide::SocketIo;
use tokio::sync::{Mutex, Notify};

use crate::test_text::LivePeer;
use crate::{Relay, bus::Bus, on_connect, token};

pub(crate) const SECRET: &[u8] = b"a-shared-secret-at-least-32-chars-long";

pub(crate) struct Harness {
    pub(crate) relay: Arc<Relay>,
    pub(crate) url: String,
    pub(crate) io: SocketIo,
}

pub(crate) async fn start() -> Option<Harness> {
    start_pinging(None).await
}

/// The relay, with engine.io pinging every `ping` and giving up after as long again, so a
/// test controls when a dead socket is reaped.
async fn start_pinging(ping: Option<Duration>) -> Option<Harness> {
    start_capped(ping, crate::room::TEXT_BYTES).await
}

/// The relay with `text_cap` for every room and the transport sized from it.
pub(crate) async fn start_capped(ping: Option<Duration>, text_cap: usize) -> Option<Harness> {
    start_with(ping, text_cap, crate::ROOM_GRACE).await
}

/// The relay as [`start_capped`] builds it, keeping an emptied room for `grace`.
pub(crate) async fn start_with(ping: Option<Duration>, text_cap: usize, grace: Duration) -> Option<Harness> {
    let valkey = std::env::var("REVIEW_VALKEY_URL").ok()?;
    let mut bus = Bus::connect(&valkey).await.expect("valkey");
    bus.check_writable().await.expect("writable");
    let relay = Arc::new(Relay {
        text_cap,
        service_uid: "craft_office".to_owned(),
        secret: SECRET.to_vec(),
        bus: Mutex::new(bus),
        rooms: Mutex::new(Default::default()),
        gates: Default::default(),
        room_grace: grace,
    });
    let mut builder = crate::sized(SocketIo::builder(), text_cap);
    if let Some(ping) = ping {
        builder = builder.ping_interval(ping).ping_timeout(ping);
    }
    let (layer, io) = builder.with_state(relay.clone()).build_layer();
    io.ns("/", on_connect);
    let app = axum::Router::new().layer(layer);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Some(Harness { relay, url: format!("http://{addr}"), io })
}

pub(crate) fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}

pub(crate) fn unique_room(tag: &str) -> String {
    format!("{tag}-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
}

pub(crate) fn ticket(room: &str, pid: &str, at: i64) -> String {
    token::sign_at(SECRET, room, pid, pid, true, pid, at).unwrap()
}

/// Wait until `peer`'s hub says `up`.
pub(crate) async fn link_is(peer: &mut LivePeer, up: bool) -> bool {
    peer.until(Duration::from_secs(15), |live, _| live.hub().connected() == up).await
}

pub(crate) fn shows(live: &Live<NativeHub>, text: &str) -> bool {
    live.doc().and_then(|d| d.block(0)).is_some_and(|p| p.text == text)
}

/// Poll both peers until `done` holds for them, as an app pumps every frame. A link counts as
/// up on the relay's CONNECT, and the Joined that makes a replica send its unconfirmed edits
/// again may still be in its inbox then: only a replica that is polled handles it.
pub(crate) async fn until_both(a: &mut LivePeer, b: &mut LivePeer, d: Duration, done: impl Fn(&LivePeer, &LivePeer) -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + d;
    loop {
        a.until(Duration::from_millis(25), |_, _| false).await;
        b.until(Duration::from_millis(25), |_, _| false).await;
        if done(a, b) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
    }
}

pub(crate) async fn seq_of(h: &Harness, room: &str) -> u64 {
    h.relay.rooms.lock().await.get(room).map(crate::room::Room::seq).unwrap_or(0)
}

/// A TCP forwarder to the relay that can cut every connection and refuse new ones for a
/// while, as a network drop or a relay restart looks to a client. Counts the connections it
/// accepted while up.
pub(crate) struct Proxy {
    pub(crate) url: String,
    pub(crate) cut: Arc<Notify>,
    pub(crate) accepted: Arc<AtomicU64>,
}

pub(crate) async fn proxy(to: SocketAddr, down_for: Duration) -> Proxy {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cut = Arc::new(Notify::new());
    let accepted = Arc::new(AtomicU64::new(0));
    let generation = Arc::new(AtomicU64::new(0));
    let down = Arc::new(AtomicBool::new(false));
    let (on_cut, bump, going_down) = (cut.clone(), generation.clone(), down.clone());
    tokio::spawn(async move {
        loop {
            on_cut.notified().await;
            going_down.store(true, Ordering::SeqCst);
            bump.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(down_for).await;
            going_down.store(false, Ordering::SeqCst);
        }
    });
    let counted = accepted.clone();
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            if down.load(Ordering::SeqCst) {
                continue;
            }
            counted.fetch_add(1, Ordering::SeqCst);
            let born = generation.load(Ordering::SeqCst);
            let generation = generation.clone();
            tokio::spawn(async move {
                let Ok(mut outbound) = tokio::net::TcpStream::connect(to).await else { return };
                let copy = tokio::io::copy_bidirectional(&mut inbound, &mut outbound);
                tokio::pin!(copy);
                loop {
                    tokio::select! {
                        _ = &mut copy => return,
                        () = tokio::time::sleep(Duration::from_millis(20)) => {
                            if generation.load(Ordering::SeqCst) != born {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    Proxy { url: format!("http://{addr}"), cut, accepted }
}

/// A TCP forwarder to the relay that moves at most `rate` bytes a second each way, as a slow
/// link does. Counts the connections it accepted.
pub(crate) async fn throttled(to: SocketAddr, rate: usize) -> (String, Arc<AtomicU64>) {
    async fn pump(mut from: tokio::net::tcp::OwnedReadHalf, mut to: tokio::net::tcp::OwnedWriteHalf, rate: usize) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 8 * 1024];
        loop {
            let n = match from.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            if to.write_all(&buf[..n]).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_secs_f64(n as f64 / rate as f64)).await;
        }
        let _ = to.shutdown().await;
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicU64::new(0));
    let counted = accepted.clone();
    tokio::spawn(async move {
        while let Ok((inbound, _)) = listener.accept().await {
            counted.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(outbound) = tokio::net::TcpStream::connect(to).await else { return };
                let (in_read, in_write) = inbound.into_split();
                let (out_read, out_write) = outbound.into_split();
                tokio::join!(pump(in_read, out_write, rate), pump(out_read, in_write, rate));
            });
        }
    });
    (format!("http://{addr}"), accepted)
}

/// A ticket for the document service, the one peer that may checkpoint.
pub(crate) fn saver(room: &str) -> String {
    token::sign_at(SECRET, room, "craft_office", "alice", true, "alice", now()).unwrap()
}

/// About 256 KiB a second: a slow uplink, where a few MiB take longer than any fixed deadline.
pub(crate) const SLOW_LINK: usize = 256 * 1024;

/// A text whose checkpoint snapshot is about 3.5 MiB on the wire: some 14 s on [`SLOW_LINK`],
/// past the hub's fixed 10 s deadlines, and under the relay's 20 s engine.io ping timeout,
/// which bounds how long any one frame may take in either direction.
pub(crate) fn large_file() -> String {
    "ab".repeat(1_300_000)
}

/// A checkpoint of a few MiB over a slow link: the emit and the checkpoint get time for their
/// bytes, so the snapshot is acknowledged on the first connection and the link never counts
/// as dead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_large_checkpoint_on_a_slow_link_is_acked_without_a_reconnect() {
    let Some(h) = start_capped(None, 8 * 1024 * 1024).await else { return };
    let (url, accepted) = throttled(relay_addr(&h), SLOW_LINK).await;
    let room = unique_room("slow-save");
    let mut a = LivePeer::open(&url, &saver(&room), &format!("doc:{room}"), &large_file()).await;
    let started = tokio::time::Instant::now();
    assert!(a.save().await, "the checkpoint was not acknowledged: {:?}", a.work);
    assert!(started.elapsed() > Duration::from_secs(10), "the link was not slow: {:?}", started.elapsed());
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "the hub reconnected");
    assert!(a.live.hub().connected());
    a.until(Duration::from_secs(12), |_, _| false).await;
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "the hub reconnected after the checkpoint");
    assert!(a.live.hub().connected());
}

/// A join whose catch-up is a few MiB over a slow link: the heartbeat's acknowledgement waits
/// behind it, and the link still counts as alive, so the joiner settles on its first
/// connection instead of looping on reconnects that each restart the same catch-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_large_catch_up_on_a_slow_link_settles_without_a_reconnect() {
    let Some(h) = start_capped(None, 8 * 1024 * 1024).await else { return };
    let room = unique_room("slow-join");
    let doc = format!("doc:{room}");
    let file = large_file();
    let mut a = LivePeer::open(&h.url, &saver(&room), &doc, &file).await;
    a.insert(0, ">");
    a.until(Duration::from_millis(300), |_, _| false).await;
    assert!(a.save().await, "the checkpoint was not acknowledged: {:?}", a.work);

    let (url, accepted) = throttled(relay_addr(&h), SLOW_LINK).await;
    let mut b = LivePeer::connect(&url, &ticket(&room, "bob", now()), &doc).await;
    let started = tokio::time::Instant::now();
    let arrived = b.until(Duration::from_secs(60), |_, work| work.iter().any(|w| matches!(w, Work::SeedWanted { .. }))).await;
    assert!(arrived, "the catch-up never arrived: {:?}", b.work);
    assert!(started.elapsed() > Duration::from_secs(10), "the link was not slow: {:?}", started.elapsed());
    b.live.seed(&crate::test_text::para(&file)).expect("restores");
    assert!(b.text() == a.text(), "the joiner did not converge");
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "the joiner reconnected");
    b.until(Duration::from_secs(12), |_, _| false).await;
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "the joiner reconnected after settling");
    assert!(b.live.hub().connected());
}

/// A TCP forwarder that, once `hole` is notified, stops forwarding on the connections it has
/// without closing them, as a sleeping laptop or a NAT that dropped the mapping looks to both
/// ends. Connections made after that are forwarded normally.
pub(crate) async fn black_hole(to: SocketAddr) -> (String, Arc<Notify>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hole = Arc::new(Notify::new());
    let generation = Arc::new(AtomicU64::new(0));
    let (on_hole, bump) = (hole.clone(), generation.clone());
    tokio::spawn(async move {
        loop {
            on_hole.notified().await;
            bump.fetch_add(1, Ordering::SeqCst);
        }
    });
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            let born = generation.load(Ordering::SeqCst);
            let generation = generation.clone();
            tokio::spawn(async move {
                let Ok(mut outbound) = tokio::net::TcpStream::connect(to).await else { return };
                {
                    let copy = tokio::io::copy_bidirectional(&mut inbound, &mut outbound);
                    tokio::pin!(copy);
                    loop {
                        tokio::select! {
                            _ = &mut copy => return,
                            () = tokio::time::sleep(Duration::from_millis(20)) => {
                                if generation.load(Ordering::SeqCst) != born {
                                    break;
                                }
                            }
                        }
                    }
                }
                // Held open, and nothing goes through.
                let _held = (inbound, outbound);
                std::future::pending::<()>().await;
            });
        }
    });
    (format!("http://{addr}"), hole)
}

/// A half-open link: sends succeed into a socket nothing reads. The missing heartbeat
/// acknowledgement takes `connected()` false, the hub reconnects, and edits converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_half_open_link_is_noticed_and_reconnects() {
    let Some(h) = start().await else { return };
    let (url, hole) = black_hole(relay_addr(&h)).await;
    let room = unique_room("half-open");
    let doc = format!("doc:{room}");
    let mut a = LivePeer::open(&url, &ticket(&room, "alice", now()), &doc, "hello").await;
    let mut b = LivePeer::open(&h.url, &ticket(&room, "bob", now()), &doc, "hello").await;

    hole.notify_one();
    let went = tokio::time::Instant::now();
    a.insert(5, "!");
    assert!(a.until(Duration::from_secs(20), |live, _| !live.hub().connected()).await, "the half-open link was never noticed");
    assert!(went.elapsed() <= Duration::from_secs(17), "noticed only after {:?}", went.elapsed());
    assert!(link_is(&mut a, true).await, "alice never came back");
    b.insert(0, ">");
    assert!(b.until(Duration::from_secs(10), |live, _| shows(live, ">hello!")).await, "bob has {:?}", b.text());
    assert!(a.until(Duration::from_secs(10), |live, _| shows(live, ">hello!")).await, "alice has {:?}", a.text());
}

/// The socket a half-open link left behind is reaped only after the hub has reconnected. Its
/// close must not take the peer off the roster (a room whose roster empties is saved and
/// closed), nor tell the others the peer left; and the peer still edits and receives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_stale_socket_closing_late_leaves_the_peer_in_the_room() {
    let Some(h) = start_pinging(Some(Duration::from_secs(12))).await else { return };
    let (url, hole) = black_hole(relay_addr(&h)).await;
    let room = unique_room("stale-socket");
    let doc = format!("doc:{room}");
    let mut a = LivePeer::open(&url, &ticket(&room, "alice", now()), &doc, "hello").await;
    let mut b = LivePeer::open(&h.url, &ticket(&room, "bob", now()), &doc, "hello").await;
    assert!(b.until(Duration::from_secs(5), |live, _| live.roster.iter().any(|p| p.peer == "alice")).await);

    hole.notify_one();
    assert!(a.until(Duration::from_secs(20), |live, _| !live.hub().connected()).await, "the half-open link was never noticed");
    assert!(link_is(&mut a, true).await, "alice never came back");
    // The relay now holds alice twice: the new socket, and the old one until it is reaped.
    // Reaped after two pings of 12 s, so after alice's hub noticed and reconnected (at most
    // 15 s): that is the order a half-open link takes in a deployment as well.
    let reaped = tokio::time::Instant::now() + Duration::from_secs(40);
    while h.io.sockets().len() > 2 && tokio::time::Instant::now() < reaped {
        b.until(Duration::from_millis(100), |_, _| false).await;
        assert!(b.live.roster.iter().any(|p| p.peer == "alice"), "bob was told alice left");
    }
    assert_eq!(h.io.sockets().len(), 2, "the stale socket was reaped");
    b.until(Duration::from_millis(500), |_, _| false).await;
    assert!(b.live.roster.iter().any(|p| p.peer == "alice"), "bob was told alice left");
    let peers: Vec<String> = h.relay.rooms.lock().await.get(&room).map(|r| r.peers().iter().map(|p| p.peer.clone()).collect()).unwrap_or_default();
    assert!(peers.contains(&"alice".to_owned()), "the relay dropped alice: {peers:?}");

    a.insert(5, "!");
    assert!(b.until(Duration::from_secs(10), |live, _| shows(live, "hello!")).await, "bob has {:?}", b.text());
    b.insert(0, ">");
    assert!(a.until(Duration::from_secs(10), |live, _| shows(live, ">hello!")).await, "alice has {:?}", a.text());
}

pub(crate) fn relay_addr(h: &Harness) -> SocketAddr {
    h.url.trim_start_matches("http://").parse().unwrap()
}

/// A dropped link: `connected()` goes false and true again, and an edit made while it was
/// down reaches the room exactly once (one ordered op, not a queued copy plus the resend).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_dropped_link_reconnects_and_the_outage_edit_lands_once() {
    let Some(h) = start().await else { return };
    let p = proxy(relay_addr(&h), Duration::from_millis(1500)).await;
    let room = unique_room("link-drop");
    let doc = format!("doc:{room}");
    let mut a = LivePeer::open(&p.url, &ticket(&room, "alice", now()), &doc, "hello").await;
    let mut b = LivePeer::open(&h.url, &ticket(&room, "bob", now()), &doc, "hello").await;
    let before = seq_of(&h, &room).await;
    // Nothing but the relay may be reached on a reconnect (an older build sent its internal
    // attempt, ticket included, to 127.0.0.1:1). Binding port 1 needs root on Linux (not on
    // macOS or Windows, which is what made the decoy a leak); skipped when it cannot be bound.
    let decoy = tokio::net::TcpListener::bind("127.0.0.1:1").await.ok();

    p.cut.notify_one();
    assert!(link_is(&mut a, false).await, "alice never saw the link drop");
    a.insert(5, "!");
    assert!(link_is(&mut a, true).await, "alice never came back");
    assert!(until_both(&mut a, &mut b, Duration::from_secs(10), |_, b| shows(&b.live, "hello!")).await, "bob has {:?}", b.text());
    assert!(a.until(Duration::from_secs(5), |live, _| shows(live, "hello!")).await, "alice has {:?}", a.text());
    a.until(Duration::from_millis(500), |_, _| false).await;
    assert_eq!(seq_of(&h, &room).await - before, 1, "the outage edit was ordered exactly once");
    if let Some(decoy) = decoy {
        let reached = tokio::time::timeout(Duration::from_millis(100), decoy.accept()).await;
        assert!(reached.is_err(), "a reconnect reached somewhere other than the relay");
    }
}

/// A socket the relay closes is reconnected to, and edits on both sides converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_hub_closed_by_the_relay_reconnects_and_converges() {
    let Some(h) = start().await else { return };
    let room = unique_room("server-close");
    let doc = format!("doc:{room}");
    let mut a = LivePeer::open(&h.url, &ticket(&room, "alice", now()), &doc, "hello").await;
    let mut b = LivePeer::open(&h.url, &ticket(&room, "bob", now()), &doc, "hello").await;
    a.insert(5, "!");
    assert!(b.until(Duration::from_secs(5), |live, _| shows(live, "hello!")).await);

    h.io.disconnect().await.expect("the relay closes every socket");
    a.insert(0, ">");
    assert!(link_is(&mut a, false).await || a.live.hub().connected(), "alice's link state");
    let back = tokio::time::Instant::now() + Duration::from_secs(15);
    while h.io.sockets().len() < 2 && tokio::time::Instant::now() < back {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(h.io.sockets().len(), 2, "both hubs reconnected");
    assert!(link_is(&mut a, true).await && link_is(&mut b, true).await);
    b.insert(6, "?");
    until_both(&mut a, &mut b, Duration::from_secs(10), |a, b| a.text().chars().count() == 8 && b.text().chars().count() == 8).await;
    assert!(a.text() == b.text() && (a.text() == ">hello?!" || a.text() == ">hello!?"), "alice {:?}, bob {:?}", a.text(), b.text());
}

/// An expired ticket on a reconnect: the relay refuses it, the hub stops (no loop of
/// accept-refuse-close), the app is told with a typed event, and a fresh ticket through
/// `set_token` brings the link back, with the edit made meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_refused_ticket_stops_reconnecting_until_a_new_one() {
    let Some(h) = start().await else { return };
    let p = proxy(relay_addr(&h), Duration::from_millis(300)).await;
    let room = unique_room("ticket");
    let doc = format!("doc:{room}");
    let mut a = LivePeer::open(&p.url, &ticket(&room, "alice", now()), &doc, "hello").await;
    let mut b = LivePeer::open(&h.url, &ticket(&room, "bob", now()), &doc, "hello").await;

    // The ticket the hub will reconnect with has long expired.
    a.live.hub_mut().set_token(ticket(&room, "alice", now() - 24 * 3600));
    let from = a.work.len();
    p.cut.notify_one();
    assert!(
        a.until(Duration::from_secs(20), |_, work| work[from..]
            .iter()
            .any(|w| matches!(w, Work::ConnectionRefused { code, .. } if code == craft_collab::REFUSED_AUTH)))
            .await,
        "no typed refusal: {:?}",
        &a.work[from..]
    );
    let attempts = p.accepted.load(Ordering::SeqCst);
    a.until(Duration::from_secs(4), |_, _| false).await;
    assert_eq!(p.accepted.load(Ordering::SeqCst), attempts, "the hub kept reconnecting with a refused ticket");
    assert!(!a.live.hub().connected());
    a.insert(5, "!");

    a.live.hub_mut().set_token(ticket(&room, "alice", now()));
    let from = a.work.len();
    assert!(link_is(&mut a, true).await, "a fresh ticket did not bring the link back");
    let arrived = until_both(&mut a, &mut b, Duration::from_secs(10), |_, b| shows(&b.live, "hello!")).await;
    assert!(
        arrived,
        "bob has {:?} (connected {}); alice has {:?}, relay seq {}, sockets {}, alice's work since the new ticket: {:?}",
        b.text(),
        b.live.hub().connected(),
        a.text(),
        seq_of(&h, &room).await,
        h.io.sockets().len(),
        &a.work[from..]
    );
}

/// A hub dropped during an outage ends its task: nothing joins on its behalf once the link
/// would be back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn e2e_a_hub_dropped_mid_outage_never_rejoins() {
    let Some(h) = start().await else { return };
    let p = proxy(relay_addr(&h), Duration::from_millis(1500)).await;
    let room = unique_room("dropped-hub");
    let doc = format!("doc:{room}");
    let mut a = LivePeer::open(&p.url, &ticket(&room, "alice", now()), &doc, "hello").await;
    p.cut.notify_one();
    assert!(link_is(&mut a, false).await);
    drop(a);
    let attempts = p.accepted.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(p.accepted.load(Ordering::SeqCst), attempts, "a dropped hub connected again");
    let peers: Vec<String> = h.relay.rooms.lock().await.get(&room).map(|r| r.peers().iter().map(|p| p.peer.clone()).collect()).unwrap_or_default();
    assert!(!peers.contains(&"alice".to_owned()), "a ghost join: {peers:?}");
}
