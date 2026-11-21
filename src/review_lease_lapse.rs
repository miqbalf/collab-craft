//! Recheck tests for the item left open on issue 24 after `3d3855a`: a lock whose Valkey lease
//! has lapsed on its own was not announced to the room, and bob's own late unlock for it, sent
//! after the lapse, was never heard either. Same harness as `review_repro::e2e` (real handlers,
//! a real Valkey, real `NativeHub` clients).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod e2e {
    use std::sync::Arc;
    use std::time::Duration;

    use craft_collab::native::NativeHub;
    use craft_collab::{ClientMsg, Effect, Hub, Session};
    use serde_json::json;
    use socketioxide::SocketIo;
    use tokio::sync::Mutex;

    use crate::{Relay, bus::Bus, on_connect, token};

    const SECRET: &[u8] = b"a-shared-secret-at-least-32-chars-long";

    struct Harness {
        relay: Arc<Relay>,
        url: String,
        valkey: String,
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
            room_grace: crate::ROOM_GRACE,
        });
        let (layer, io) = crate::sized(SocketIo::builder(), crate::room::TEXT_BYTES).with_state(relay.clone()).build_layer();
        io.ns("/", on_connect);
        let app = axum::Router::new().layer(layer);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Some(Harness { relay, url: format!("http://{addr}"), valkey })
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
        async fn connect(h: &Harness, room: &str, pid: &str) -> Client {
            let t = token::sign_at(SECRET, room, pid, pid, true, pid, now()).unwrap();
            let hub = NativeHub::connect(&h.url, &t, None).await.expect("connect");
            let mut c = Client { hub, s: Session::new(format!("doc:{room}")), fx: Vec::new() };
            c.until(Duration::from_secs(5), |s, _| s.peer().is_some()).await;
            assert!(c.s.peer().is_some(), "{pid} never got joined");
            c
        }
        fn send(&mut self, m: ClientMsg) {
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
    }

    fn confirmed(fx: &[Effect], local: u64) -> Option<u64> {
        fx.iter().find_map(|e| match e {
            Effect::Confirmed { local: l, seq } if *l == local => Some(*seq),
            _ => None,
        })
    }

    /// Force one scope's lock key to expire right now, rather than waiting out the real 60 s
    /// TTL. The key naming has to match `Bus::lock_key` exactly for this to touch the same
    /// record the relay reads.
    async fn expire_lock_now(valkey: &str, room: &str, scope: &str) {
        let client = redis::Client::open(valkey).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let key = format!("collabcraft:doc:{room}:lock:{scope}");
        let _: i64 = redis::cmd("PEXPIRE").arg(&key).arg(1).query_async(&mut conn).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    /// Issue 24 item 6, second half, positive case. Bob's lease on B2 lapses in Valkey with
    /// nobody told. Alice's submit into B2 hits the preflight's stale mirror ("bob holds it"),
    /// the mirror rechecks the bus, finds the lease gone, and must both let her edit through
    /// AND announce the lapse to the room the same way a real unlock is, since the lapse itself
    /// never told anyone. A fresh lock request for the same scope right after must also
    /// succeed, rather than the stale refusal somehow persisting.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_lapsed_lease_is_announced_and_the_next_lock_succeeds() {
        let Some(h) = start().await else { return };
        let room = unique_room("lease-lapse-announce");
        let mut alice = Client::connect(&h, &room, "alice").await;
        let mut bob = Client::connect(&h, &room, "bob").await;

        let m = bob.s.request_lock("B2");
        bob.send(m);
        assert!(bob.until(Duration::from_secs(5), |s, _| s.holds("B2")).await, "bob never got B2: {:?}", bob.fx);
        assert!(alice.until(Duration::from_secs(5), |s, _| s.blocked_by("B2").is_some()).await, "alice never learned bob holds B2: {:?}", alice.fx);

        expire_lock_now(&h.valkey, &room, "B2").await;

        let m = alice.s.submit("cell.set", json!({"cell": "B2"}), Some("B2".into()));
        alice.send(m);
        let submitted = alice.until(Duration::from_secs(3), |_, fx| confirmed(fx, 1).is_some()).await;
        alice.settle(Duration::from_millis(500)).await;
        let announced = alice.fx.iter().any(|e| matches!(e, Effect::LockDropped { scope } if scope == "B2"));

        assert!(
            submitted && announced,
            "bob's B2 lease lapsed with nobody told; alice's edit into it confirmed={submitted}; \
             the room's announcement of the lapse (a lock_released/LockDropped) reached alice={announced}; alice fx={:?}",
            alice.fx
        );

        // The stale refusal must not persist: a fresh lock on the same scope now succeeds.
        let m = alice.s.request_lock("B2");
        alice.send(m);
        assert!(
            alice.until(Duration::from_secs(3), |s, _| s.holds("B2")).await,
            "a lock request for B2 right after the announced lapse was not granted: {:?}",
            alice.fx
        );
    }

    /// Issue 24 item 6, second half, idempotency case. Bob's lease on B2 lapses, alice then
    /// claims B2 for herself through a normal lock (both the bus and the mirror now point to
    /// her), and only after that does bob's own late unlock for B2 arrive, exactly as a delayed
    /// client retry would send one. It must be a no-op: alice's hold must survive untouched,
    /// and she must not be told the scope released, which `ServerMsg::LockReleased` would
    /// otherwise have her drop unconditionally, corrupting her state over somebody else's stale
    /// message for a lock she actually still holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_late_unlock_after_lapse_does_not_disturb_the_new_holder() {
        let Some(h) = start().await else { return };
        let room = unique_room("lease-lapse-late-unlock");
        let mut alice = Client::connect(&h, &room, "alice").await;
        let mut bob = Client::connect(&h, &room, "bob").await;

        let m = bob.s.request_lock("B2");
        bob.send(m);
        assert!(bob.until(Duration::from_secs(5), |s, _| s.holds("B2")).await, "bob never got B2: {:?}", bob.fx);

        expire_lock_now(&h.valkey, &room, "B2").await;

        let m = alice.s.request_lock("B2");
        alice.send(m);
        assert!(alice.until(Duration::from_secs(5), |s, _| s.holds("B2")).await, "alice never got B2 after bob's lease lapsed: {:?}", alice.fx);
        bob.settle(Duration::from_millis(300)).await;

        // Bob's late unlock: his client only now catches up to its own lease having already
        // died, and sends the release it always meant to.
        let m = bob.s.release_lock("B2");
        bob.send(m);
        alice.settle(Duration::from_millis(500)).await;

        let alice_lost_it = alice.fx.iter().any(|e| matches!(e, Effect::LockDropped { scope } if scope == "B2"));
        let room_holder = h.relay.rooms.lock().await.get(&room).and_then(|r| r.holder_of("B2")).map(str::to_owned);
        assert!(
            alice.s.holds("B2") && !alice_lost_it && room_holder.as_deref() == Some("alice"),
            "bob's late unlock for B2, sent after his own lease had already lapsed and alice had since claimed it, \
             must be a no-op: alice still holds it={}, she was told it released={alice_lost_it}, room mirror holder={room_holder:?}",
            alice.s.holds("B2")
        );
    }
}
