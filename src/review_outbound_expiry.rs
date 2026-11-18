//! Outbound expiry tests. A passive socket never sends an application
//! message. The inbound recheck in `live_claims` therefore never fires for
//! it. Before `emit_live` such a socket kept receiving every broadcast after
//! its token lapsed. These tests pin the fix: delivery itself now rechecks the
//! recipient. Same harness as review_repro::e2e (real handlers, real Valkey,
//! real NativeHub clients).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod e2e {
    use std::sync::Arc;
    use std::time::Duration;

    use craft_collab::native::NativeHub;
    use craft_collab::{Effect, Hub, Session};
    use serde_json::json;
    use socketioxide::SocketIo;
    use tokio::sync::Mutex;

    use crate::{MAX_FRAME_BYTES, Relay, bus::Bus, on_connect, token};

    const SECRET: &[u8] = b"a-shared-secret-at-least-32-chars-long";

    struct Harness {
        relay: Arc<Relay>,
        url: String,
    }

    async fn start() -> Option<Harness> {
        let valkey = std::env::var("REVIEW_VALKEY_URL").ok()?;
        let mut bus = Bus::connect(&valkey).await.expect("valkey");
        bus.check_writable().await.expect("writable");
        let relay = Arc::new(Relay { secret: SECRET.to_vec(), bus: Mutex::new(bus), rooms: Mutex::new(Default::default()) });
        let (layer, io) = SocketIo::builder().max_payload(MAX_FRAME_BYTES as u64).with_state(relay.clone()).build_layer();
        io.ns("/", on_connect);
        let app = axum::Router::new().layer(layer);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Some(Harness { relay, url: format!("http://{addr}") })
    }

    fn now() -> i64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
    }

    fn unique_room(tag: &str) -> String {
        format!("{tag}-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
    }

    struct Client {
        hub: NativeHub,
        s: Session,
        fx: Vec<Effect>,
    }

    impl Client {
        /// Connect with a token signed as of `minted_at`. The token's expiry is
        /// `minted_at` plus the fixed lifetime. A `minted_at` in the near past
        /// therefore yields a token that is valid at connect and lapses soon
        /// after.
        async fn connect_minted_at(h: &Harness, room: &str, pid: &str, may_edit: bool, minted_at: i64) -> Client {
            let t = token::sign_at(SECRET, room, pid, pid, may_edit, pid, minted_at).unwrap();
            let hub = NativeHub::connect(&h.url, &t, None).await.expect("connect");
            let mut c = Client { hub, s: Session::new(format!("doc:{room}")), fx: Vec::new() };
            c.until(Duration::from_secs(5), |s, _| s.peer().is_some()).await;
            assert!(c.s.peer().is_some(), "{pid} never got joined");
            c
        }
        async fn connect(h: &Harness, room: &str, pid: &str, may_edit: bool) -> Client {
            Client::connect_minted_at(h, room, pid, may_edit, now()).await
        }
        fn send(&mut self, m: craft_collab::ClientMsg) {
            self.hub.send(m).unwrap();
        }
        async fn until(&mut self, d: Duration, done: impl Fn(&Session, &[Effect]) -> bool) -> bool {
            let end = tokio::time::Instant::now() + d;
            loop {
                for m in self.hub.poll() {
                    let fx = self.s.handle(m);
                    for e in fx {
                        if let Effect::Send { msg } = &e {
                            let _ = self.hub.send(msg.clone());
                        }
                        self.fx.push(e);
                    }
                }
                if done(&self.s, &self.fx) {
                    return true;
                }
                if tokio::time::Instant::now() > end {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        async fn settle(&mut self, d: Duration) {
            self.until(d, |_, _| false).await;
        }
        fn applies(&self) -> usize {
            self.fx.iter().filter(|e| matches!(e, Effect::Apply { .. })).count()
        }
    }

    fn confirmed(fx: &[Effect], local: u64) -> bool {
        fx.iter().any(|e| matches!(e, Effect::Confirmed { local: l, .. } if *l == local))
    }

    /// The finding: a viewer that only listens kept receiving edits after its
    /// token lapsed. Nothing between connect and disconnect ever read its
    /// expiry again. The viewer's token here lapses two seconds in. Alice
    /// edits after that. The edit must reach alice through her own confirm
    /// and must never reach the viewer. The delivery recheck must also drop
    /// the viewer from the room.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_lapsed_passive_viewer_receives_nothing() {
        let Some(h) = start().await else { return };
        let room = unique_room("lapse-passive");
        // Valid for two more seconds: minted almost a full lifetime ago.
        let mut viewer = Client::connect_minted_at(&h, &room, "viewer", false, now() - token::LIFETIME_SECS + 2).await;
        let mut a = Client::connect(&h, &room, "alice", true).await;
        viewer.settle(Duration::from_millis(200)).await;
        let seen_before_lapse = viewer.applies();

        // Let the viewer's token lapse while both sockets sit idle.
        tokio::time::sleep(Duration::from_secs(3)).await;

        let m = a.s.submit("cell.set", json!({"cell": "A1", "v": 1}), None);
        a.send(m);
        assert!(a.until(Duration::from_secs(5), |_, fx| confirmed(fx, 1)).await, "alice's edit never confirmed: {:?}", a.fx);
        viewer.settle(Duration::from_millis(500)).await;

        let roster: Vec<String> =
            h.relay.rooms.lock().await.get(&room).map(|r| r.peers().iter().map(|p| p.peer.clone()).collect()).unwrap_or_default();
        assert_eq!(viewer.applies(), seen_before_lapse, "the lapsed viewer still received an edit; fx={:?}", viewer.fx);
        assert!(!roster.contains(&"viewer".to_owned()), "the lapsed viewer is still on the roster: {roster:?}");
        assert!(roster.contains(&"alice".to_owned()), "alice was dropped by the delivery recheck: {roster:?}");
    }

    /// The guard the fix must not break: a passive viewer whose token still
    /// stands receives the edit exactly as before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_live_passive_viewer_still_receives() {
        let Some(h) = start().await else { return };
        let room = unique_room("live-passive");
        let mut viewer = Client::connect(&h, &room, "viewer", false).await;
        let mut a = Client::connect(&h, &room, "alice", true).await;

        let m = a.s.submit("cell.set", json!({"cell": "A1", "v": 1}), None);
        a.send(m);
        assert!(a.until(Duration::from_secs(5), |_, fx| confirmed(fx, 1)).await, "alice's edit never confirmed: {:?}", a.fx);
        assert!(
            viewer.until(Duration::from_secs(5), |_, fx| fx.iter().any(|e| matches!(e, Effect::Apply { .. }))).await,
            "the live viewer never received the edit; fx={:?}",
            viewer.fx
        );
    }
}
