//! End-to-end tests of the id space floor: a rejoining replica names the space it keeps, and the
//! relay raises its space counter past it before the grant, so no other connection is ever
//! granted that space, even after the counter went backwards. Ignored by default; they need a
//! throwaway Valkey in `REVIEW_VALKEY_URL`.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use futures_util::FutureExt;
use rust_socketio::asynchronous::{Client, ClientBuilder};
use rust_socketio::{Payload, TransportType};
use serde_json::{Value, json};
use tokio::sync::mpsc::unbounded_channel;

use crate::bus::MAX_SPACE_FLOOR;
use crate::e2e_reconnect::{now, start, ticket, unique_room};

/// Join `room` as `pid`, naming `space` as the one its replica keeps, and read the space the
/// join grants. The client is returned so the connection stays up.
async fn join(url: &str, room: &str, pid: &str, space: Option<u32>) -> (Client, u32) {
    let (tx, mut joined) = unbounded_channel::<Value>();
    let builder = ClientBuilder::new(url.to_owned()).reconnect(false).transport_type(TransportType::Websocket).on("joined", move |payload, _| {
        let tx = tx.clone();
        async move {
            if let Payload::Text(values) = payload {
                for v in values {
                    let _ = tx.send(v);
                }
            }
        }
        .boxed()
    });
    let mut auth = json!({ "token": ticket(room, pid, now()), "proto": craft_collab::PROTO });
    if let Some(space) = space {
        auth["space"] = json!(space);
    }
    let client = builder.auth(auth).connect().await.expect("connects");
    let v = tokio::time::timeout(Duration::from_secs(10), joined.recv()).await.expect("joined in time").expect("joined");
    let granted = v.get("space").and_then(Value::as_u64).and_then(|s| u32::try_from(s).ok()).expect("a space");
    (client, granted)
}

/// The space counter is lost (a flush) and the relay restarts, so it remembers nothing it
/// granted. A replica that rejoins names the space it keeps, and from then on nobody is granted
/// that space or one below it.
#[tokio::test]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_rejoin_raises_the_space_counter_past_the_space_it_keeps() {
    let Some(h) = start().await else { return };
    let room = unique_room("space-floor");
    let (_a, first) = join(&h.url, &room, "alice", None).await;
    let (_b, _) = join(&h.url, &room, "bob", None).await;
    let (_c, kept) = join(&h.url, &room, "carol", None).await;
    assert!(kept > first);
    {
        let mut bus = h.relay.bus.lock().await;
        bus.rewind_spaces(&room, 0).await;
        bus.forget_granted_spaces();
    }
    // Carol's replica rejoins keeping `kept`; then a newcomer joins.
    let (_c2, rejoined) = join(&h.url, &room, "carol", Some(kept)).await;
    let (_d, newcomer) = join(&h.url, &room, "dave", None).await;
    assert!(rejoined > kept, "the rejoin was granted {rejoined}, not past the kept {kept}");
    assert!(newcomer > kept, "a newcomer was granted {newcomer} after the counter went back below the kept {kept}");
    // A floor no real replica holds is not honoured: the counter does not jump towards its wrap.
    let (_e, after) = join(&h.url, &room, "eve", Some(u32::MAX)).await;
    assert!(after > newcomer && after < MAX_SPACE_FLOOR, "a huge floor moved the counter to {after}");
}

/// The counter loses its latest grant (a crash inside the `everysec` AOF window, a failover to a
/// replica that lagged) while the relay runs and the holder stays connected, so it does not
/// rejoin. A newcomer is still never granted the holder's space.
#[tokio::test]
#[ignore = "needs REVIEW_VALKEY_URL"]
async fn a_lost_grant_is_never_granted_again_while_the_relay_runs() {
    let Some(h) = start().await else { return };
    let room = unique_room("space-lost-grant");
    let (_a, first) = join(&h.url, &room, "alice", None).await;
    let (_b, held) = join(&h.url, &room, "bob", None).await;
    assert!(held > first);
    h.relay.bus.lock().await.rewind_spaces(&room, held - 1).await;
    let (_c, newcomer) = join(&h.url, &room, "carol", None).await;
    assert!(newcomer > held, "a newcomer was granted {newcomer}; bob still holds {held}");
}

/// A `space` that is not a `u32` (a buggy client) is ignored, and the handshake still decodes.
#[test]
fn a_malformed_space_does_not_refuse_the_handshake() {
    for bad in [json!(-1), json!(1.5), json!(u64::from(u32::MAX) + 1), json!("7"), Value::Null] {
        let h: crate::Handshake = serde_json::from_value(json!({ "token": "t", "space": bad })).expect("decodes");
        assert_eq!(h.space, None, "{bad}");
    }
    let h: crate::Handshake = serde_json::from_value(json!({ "token": "t", "space": 7 })).expect("decodes");
    assert_eq!(h.space, Some(7));
}
