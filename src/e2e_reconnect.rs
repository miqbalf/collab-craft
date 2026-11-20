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

const SECRET: &[u8] = b"a-shared-secret-at-least-32-chars-long";

struct Harness {
    relay: Arc<Relay>,
    url: String,
    io: SocketIo,
}

async fn start() -> Option<Harness> {
    let valkey = std::env::var("REVIEW_VALKEY_URL").ok()?;
    let mut bus = Bus::connect(&valkey).await.expect("valkey");
    bus.check_writable().await.expect("writable");
    let relay = Arc::new(Relay {
        text_cap: crate::room::TEXT_BYTES,
        service_uid: "craft_office".to_owned(),
        secret: SECRET.to_vec(),
        bus: Mutex::new(bus),
        rooms: Mutex::new(Default::default()),
        gates: Default::default(),
    });
    let (layer, io) = crate::sized(SocketIo::builder(), crate::room::TEXT_BYTES).with_state(relay.clone()).build_layer();
    io.ns("/", on_connect);
    let app = axum::Router::new().layer(layer);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Some(Harness { relay, url: format!("http://{addr}"), io })
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}

fn unique_room(tag: &str) -> String {
    format!("{tag}-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
}

fn ticket(room: &str, pid: &str, at: i64) -> String {
    token::sign_at(SECRET, room, pid, pid, true, pid, at).unwrap()
}

/// Wait until `peer`'s hub says `up`.
async fn link_is(peer: &mut LivePeer, up: bool) -> bool {
    peer.until(Duration::from_secs(15), |live, _| live.hub().connected() == up).await
}

fn shows(live: &Live<NativeHub>, text: &str) -> bool {
    live.doc().and_then(|d| d.block(0)).is_some_and(|p| p.text == text)
}

async fn seq_of(h: &Harness, room: &str) -> u64 {
    h.relay.rooms.lock().await.get(room).map(crate::room::Room::seq).unwrap_or(0)
}

/// A TCP forwarder to the relay that can cut every connection and refuse new ones for a
/// while, as a network drop or a relay restart looks to a client. Counts the connections it
/// accepted while up.
struct Proxy {
    url: String,
    cut: Arc<Notify>,
    accepted: Arc<AtomicU64>,
}

async fn proxy(to: SocketAddr, down_for: Duration) -> Proxy {
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

fn relay_addr(h: &Harness) -> SocketAddr {
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
    // attempt, ticket included, to 127.0.0.1:1). The port needs root to bind; skipped without.
    let decoy = tokio::net::TcpListener::bind("127.0.0.1:1").await.ok();

    p.cut.notify_one();
    assert!(link_is(&mut a, false).await, "alice never saw the link drop");
    a.insert(5, "!");
    assert!(link_is(&mut a, true).await, "alice never came back");
    assert!(b.until(Duration::from_secs(10), |live, _| shows(live, "hello!")).await, "bob has {:?}", b.text());
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
    for peer in [&mut a, &mut b] {
        peer.until(Duration::from_secs(10), |live, _| live.doc().and_then(|d| d.block(0)).is_some_and(|p| p.text.chars().count() == 8)).await;
    }
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
        a.until(Duration::from_secs(10), |_, work| work[from..]
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
    assert!(link_is(&mut a, true).await, "a fresh ticket did not bring the link back");
    assert!(b.until(Duration::from_secs(10), |live, _| shows(live, "hello!")).await, "bob has {:?}", b.text());
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
