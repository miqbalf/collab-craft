//! Re-verification tests for 8cae690 (new, not in the gist). Same harness as review_reverify.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod unit {
    use craft_collab::{ClientMsg, Effect, ServerMsg, Session};
    use serde_json::json;

    fn op(doc: &str, seq: u64) -> ServerMsg {
        ServerMsg::Op {
            doc: doc.to_owned(),
            seq,
            origin: "alice".to_owned(),
            local: None,
            command: "cell.set".to_owned(),
            params: json!({"n": seq}),
            scope: None,
        }
    }

    /// Item 5, the client half. `since()` reports a burned-sequence hole, but a client only
    /// asked for it past the old MAX_PENDING (512) held ops, so a long-lived connected session
    /// that fell behind a hole sat frozen for far longer than normal reordering ever needs.
    /// Distinct from the sequence-hole *reporting* the reviewer already confirmed passes
    /// (`Room::since` itself, covered by `review_repro::unit_burned_sequence_is_recoverable`
    /// and `room::tests::since_reports_a_burned_sequence_hole`): this is the client noticing.
    /// Fixed by lowering `craft_collab::session::MAX_PENDING` to 64; this test's 97 held ops
    /// (seq 4 through 100, behind the hole at 3) comfortably cross that.
    #[test]
    fn unit_client_behind_hole_recovers_promptly() {
        const DOC: &str = "doc:hole";
        let mut carol = Session::new(DOC);
        carol.handle(ServerMsg::Joined {
            doc: DOC.to_owned(),
            peer: "carol".to_owned(),
            seq: 2,
            space: 3,
            roster: vec![],
            locks: vec![],
            replay_until: None,
            fallback: false,
            text_cap: None,
            heartbeat: false,
            history_lost: false,
        });
        let mut fx = Vec::new();
        for seq in 4..=100u64 {
            fx.extend(carol.handle(op(DOC, seq)));
        }
        let resync = fx.iter().any(|e| matches!(e, Effect::Send { msg: ClientMsg::Resync { .. } }));
        let applies = fx.iter().filter(|e| matches!(e, Effect::Apply { .. })).count();
        assert!(
            carol.applied() == 100 || resync,
            "seq 3 burned; carol got ops 4..=100: applied()={}, Apply effects={applies}, resync requested={resync}",
            carol.applied()
        );
    }

    /// Item 7, bytes. MAX_SCOPES bounds the count, but each scope may hold TEXT_BYTES, so the
    /// per-room retained text is bounded only by MAX_SCOPES * TEXT_BYTES without a room-wide
    /// budget.
    ///
    /// ADAPTATION: checked against `room::TOTAL_TEXT_BYTES`, the dedicated room-wide budget the
    /// fix introduces, rather than `HISTORY_BYTES`. The two happen to share a value by design
    /// (see `TOTAL_TEXT_BYTES`'s own doc comment), but `HISTORY_BYTES` names a different pool
    /// (the op history) and asserting against it here would pass by coincidence rather than by
    /// naming the guard that is actually doing the work.
    #[test]
    fn unit_retained_text_bytes_are_bounded_per_room() {
        use crate::room::{MAX_SCOPES, Room, Submission, TEXT_BYTES, TOTAL_TEXT_BYTES};
        let mut r = Room::default();
        r.join(crate::room::Peer { peer: "mallory".into(), display: "mallory".into(), may_edit: true, uid: "mallory".into() }).unwrap();
        let blob = "x".repeat(TEXT_BYTES - 64);
        for i in 0..16u64 {
            let sub = Submission {
                peer_id: "mallory".into(),
                may_edit: true,
                command: "crdt.text".into(),
                params: json!({"scope": format!("big{i}"), "ops": [blob.clone()]}),
                scope: None,
                local: None,
            };
            r.submit(i + 1, sub).unwrap();
        }
        let retained: usize = r.texts().values().flat_map(|l| l.ops().iter()).map(|v| serde_json::to_vec(v).unwrap().len()).sum();
        assert!(
            retained <= TOTAL_TEXT_BYTES,
            "16 near-cap scopes retain {retained} bytes ({} MiB) in one room; TOTAL_TEXT_BYTES={TOTAL_TEXT_BYTES}, TEXT_BYTES={TEXT_BYTES}, MAX_SCOPES={MAX_SCOPES} => worst case {} MiB",
            retained / (1024 * 1024),
            (MAX_SCOPES * TEXT_BYTES) / (1024 * 1024)
        );
    }
}

mod e2e {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::test_text::{LivePeer, Replica, SCOPE};
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
    }

    async fn start() -> Option<Harness> {
        start_under("").await
    }

    /// The relay with Socket.IO mounted under `base`, as `COLLABCRAFT_BASE` mounts it.
    async fn start_under(base: &str) -> Option<Harness> {
        start_with(base, crate::room::TEXT_BYTES).await
    }

    /// The relay as `main` builds it for a per-scope text cap of `text_cap`.
    async fn start_with(base: &str, text_cap: usize) -> Option<Harness> {
        // Unlike review_reverify.rs's harness, nothing in this file reaches Valkey directly
        // (no lease-expiry fault injection here), so the URL is read only to gate on its
        // presence and is not kept.
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
            room_grace: crate::ROOM_GRACE,
        });
        let (layer, io) = crate::sized(SocketIo::builder(), text_cap).req_path(format!("{base}/socket.io")).with_state(relay.clone()).build_layer();
        io.ns("/", on_connect);
        let app = axum::Router::new().layer(layer);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Some(Harness { relay, url: format!("http://{addr}{base}") })
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
        async fn connect_with(h: &Harness, room: &str, pid: &str, may_edit: bool, s: Session) -> Client {
            let t = token::sign_at(SECRET, room, pid, pid, may_edit, pid, now()).unwrap();
            // A session that has already joined knows what it applied, and resuming there is
            // what lets a reconnect pick up exactly what it missed. A fresh session has
            // nothing of its own to offer, so the join falls back to the saved file's sequence.
            let applied = s.peer().is_some().then(|| s.applied());
            let hub = NativeHub::connect(&h.url, &t, applied).await.expect("connect");
            let mut c = Client { hub, s, fx: Vec::new() };
            let joined_before = c.fx.len();
            c.until(Duration::from_secs(5), |s, fx| s.peer().is_some() && fx.len() > joined_before).await;
            assert!(c.s.peer().is_some(), "{pid} never got joined");
            c
        }
        async fn connect(h: &Harness, room: &str, pid: &str, may_edit: bool) -> Client {
            Client::connect_with(h, room, pid, may_edit, Session::new(format!("doc:{room}"))).await
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
        /// Every replayed text op the relay sent this client for `scope`.
        fn replay(&self, scope: &str) -> (Vec<serde_json::Value>, Vec<bool>) {
            let mut ops = Vec::new();
            let mut complete = Vec::new();
            for e in &self.fx {
                if let Effect::TextOps { scope: s, ops: o, complete: c, .. } = e
                    && s == scope
                {
                    let v: Vec<serde_json::Value> = serde_json::from_value(o.clone()).unwrap();
                    ops.extend(v);
                    complete.push(*c);
                }
                // ADAPTATION (8cae690): the join now replays text as ordered `op` events
                // (Effect::Apply of `crdt.text`) instead of `text_ops`. Count both, so the
                // assertions see every replayed text op the joiner would apply.
                if let Effect::Apply { command, params, .. } = e
                    && command == "crdt.text"
                    && params.get("scope").and_then(|s| s.as_str()) == Some(scope)
                {
                    let v: Vec<serde_json::Value> = serde_json::from_value(params["ops"].clone()).unwrap();
                    ops.extend(v);
                    complete.push(true);
                }
            }
            (ops, complete)
        }
    }

    fn confirmed(fx: &[Effect], local: u64) -> Option<u64> {
        fx.iter().find_map(|e| match e {
            Effect::Confirmed { local: l, seq } if *l == local => Some(*seq),
            _ => None,
        })
    }

    /// Alice types "!" into "hello". Returns her replica and the params she submitted.
    async fn alice_types_bang(a: &mut Client) -> (Replica, serde_json::Value) {
        let mut t = Replica::seeded(a.s.space().expect("space granted"), "hello");
        let params = t.insert(5, "!");
        (t, params)
    }

    fn applies(fx: &[Effect]) -> usize {
        fx.iter().filter(|e| matches!(e, Effect::Apply { .. })).count()
    }

    async fn cell_edits(a: &mut Client, vals: &[i64]) {
        for v in vals {
            let before = a.s.inflight();
            let m = a.s.submit("cell.set", json!({"cell": "A1", "v": v}), None);
            a.send(m);
            let _ = before;
            assert!(a.until(Duration::from_secs(5), |s, _| s.inflight() == 0).await, "edit {v} never confirmed");
        }
    }

    /// Item 4 after a save. Bob drops at seq 0; alice makes 2 edits, saves (Saved{2}), makes a
    /// third. Bob's app keeps its Session (it did not reload the file). He must receive all 3
    /// missed ops, or a snapshot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_reconnect_after_save_receives_missed_ops() {
        let Some(h) = start().await else { return };
        let room = unique_room("missed-save");
        let mut a = Client::connect(&h, &room, "alice", true).await;
        let b = Client::connect(&h, &room, "bob", true).await;
        let Client { hub, s: bob_session, .. } = b;
        drop(hub);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let before = bob_session.applied();
        cell_edits(&mut a, &[1, 2]).await;
        // The save goes through a checkpoint, the only save the relay accepts. Carol is the
        // saver: a separate replica of the document, as the service worker is.
        let t = token::sign_at(SECRET, &room, "craft_office", "carol", true, "carol", now()).unwrap();
        let mut saver = LivePeer::open(&h.url, &t, &format!("doc:{room}"), "hello").await;
        assert!(saver.save().await, "the checkpoint was not accepted: {:?}", saver.work);
        saver.until(Duration::from_millis(200), |_, _| false).await;
        cell_edits(&mut a, &[3]).await;

        let mut b = Client::connect_with(&h, &room, "bob", true, bob_session).await;
        b.settle(Duration::from_millis(800)).await;
        let n = b.fx.iter().filter(|e| matches!(e, Effect::Apply { command, .. } if command == "cell.set")).count();
        let replaced = b.fx.iter().any(|e| matches!(e, Effect::Replace { .. }));
        assert!(
            n == 3 || replaced,
            "bob applied seq {before} before dropping; alice made 3 edits and saved at 2; after reconnect bob.applied={} but Apply effects={n}, snapshot={replaced}",
            b.s.applied()
        );
    }

    /// Regression check for the new join. Bob is online and applies alice's 2 edits, then
    /// reconnects with the same Session. He must not be handed them again as new Applies.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_reconnect_does_not_reapply_seen_ops() {
        let Some(h) = start().await else { return };
        let room = unique_room("reapply");
        let mut a = Client::connect(&h, &room, "alice", true).await;
        let mut b = Client::connect(&h, &room, "bob", true).await;
        cell_edits(&mut a, &[1, 2]).await;
        assert!(b.until(Duration::from_secs(5), |s, _| s.applied() == 2).await);
        let first = applies(&b.fx);
        let Client { hub, s: bob_session, .. } = b;
        drop(hub);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut b = Client::connect_with(&h, &room, "bob", true, bob_session).await;
        b.settle(Duration::from_millis(800)).await;
        let again = applies(&b.fx);
        assert_eq!(
            again, 0,
            "bob applied {first} ops while online (applied=2); after reconnecting with the same Session he got {again} more Apply effects for ops he already had"
        );
    }

    /// The same, with a text edit and a real replica: does the re-delivery change what bob sees?
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_reconnect_text_is_not_doubled() {
        let Some(h) = start().await else { return };
        let room = unique_room("reapply-text");
        let mut a = Client::connect(&h, &room, "alice", true).await;
        let mut b = Client::connect(&h, &room, "bob", true).await;
        let (t, params) = alice_types_bang(&mut a).await;
        let m = a.s.submit("crdt.text", params, None);
        a.send(m);
        assert!(a.until(Duration::from_secs(5), |_, fx| confirmed(fx, 1).is_some()).await);
        assert!(b.until(Duration::from_secs(5), |s, _| s.applied() == 1).await);
        let mut bt = Replica::seeded(b.s.space().unwrap(), "hello");
        let (r1, _) = b.replay(SCOPE);
        bt.apply(&r1);
        let online = bt.render();
        let Client { hub, s: bob_session, .. } = b;
        drop(hub);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut b = Client::connect_with(&h, &room, "bob", true, bob_session).await;
        b.settle(Duration::from_millis(800)).await;
        let (r2, _) = b.replay(SCOPE);
        bt.apply(&r2);
        let after = bt.render();
        assert_eq!(after, t.render(), "bob saw {online:?} online; after reconnect he was re-sent {} text ops and sees {after:?}", r2.len());
    }

    /// A read-only viewer reports a save slightly ahead of the room. `saved_seq` is now the
    /// join baseline (monotonic max), so a later joiner is told it has applied past the room
    /// and drops the next live edits as already seen.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_viewer_saved_ahead_does_not_strand_joiner() {
        let Some(h) = start().await else { return };
        let room = unique_room("viewer-ahead");
        let mut a = Client::connect(&h, &room, "alice", true).await;
        cell_edits(&mut a, &[1]).await;
        let mut viewer = Client::connect(&h, &room, "viewer", false).await;
        let room_seq = h.relay.rooms.lock().await.get(&room).map(|r| r.seq()).unwrap();
        viewer.send(ClientMsg::Saved { doc: format!("doc:{room}"), seq: room_seq + 3 });
        viewer.settle(Duration::from_millis(300)).await;

        let mut b = Client::connect(&h, &room, "bob", true).await;
        let joined_at = b.s.applied();
        cell_edits(&mut a, &[2, 3]).await;
        b.settle(Duration::from_millis(800)).await;
        let n = applies(&b.fx);
        assert!(
            n >= 2,
            "viewer (may_edit=false) sent Saved{{seq: {}}} at room seq {room_seq}; bob joined with applied={joined_at}; alice then made 2 edits; bob Apply effects={n}, applied={}",
            room_seq + 3,
            b.s.applied()
        );
    }

    /// Fix round 2, item 5: a live op broadcast while a peer is joining reaches it exactly
    /// once. Alice types without pause while joiners keep arriving; every joiner must end at
    /// the room's last sequence with each op applied once and no resync needed to get there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_an_op_during_the_join_window_reaches_the_joiner_once() {
        let Some(h) = start().await else { return };
        let room = unique_room("join-window");
        let mut a = Client::connect(&h, &room, "alice", true).await;
        const EDITS: u64 = 200;
        let typing = tokio::spawn(async move {
            for v in 0..EDITS {
                let m = a.s.submit("cell.set", json!({"cell": "A1", "v": v}), None);
                a.send(m);
                a.until(Duration::from_millis(2), |_, _| false).await;
            }
            a.until(Duration::from_secs(10), |s, _| s.inflight() == 0).await;
            a
        });
        let mut joiners = Vec::new();
        for i in 0..12 {
            joiners.push(Client::connect(&h, &room, &format!("j{i}"), true).await);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        let a = typing.await.unwrap();
        let last = a.s.applied();
        assert_eq!(last, EDITS, "alice's edits were all ordered");
        for (i, j) in joiners.iter_mut().enumerate() {
            j.until(Duration::from_secs(5), |s, _| s.applied() == last).await;
            let mut seqs: Vec<u64> = j.fx.iter().filter_map(|e| if let Effect::Apply { seq, .. } = e { Some(*seq) } else { None }).collect();
            let resynced = j.fx.iter().any(|e| matches!(e, Effect::Send { msg: ClientMsg::Resync { .. } }));
            let n = seqs.len();
            seqs.dedup();
            assert_eq!(seqs.len(), n, "joiner {i} applied an op twice");
            assert!(!resynced && j.s.applied() == last, "joiner {i} at {} of {last}, resync needed: {resynced}", j.s.applied());
            assert_eq!(seqs, (1..=last).collect::<Vec<_>>(), "joiner {i} did not apply every op once");
        }
    }

    /// Round 2b: a hub given the relay URL with its base path (`http://host/craft`) reaches
    /// Socket.IO at `/craft/socket.io/`, joins, and saves with an acknowledged checkpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_a_relay_under_a_base_path_is_reached_and_acks_a_save() {
        let Some(h) = start_under("/craft").await else { return };
        let room = unique_room("base-path");
        let t = token::sign_at(SECRET, &room, "craft_office", "alice", true, "alice", now()).unwrap();
        let mut a = LivePeer::open(&h.url, &t, &format!("doc:{room}"), "hello").await;
        a.insert(5, "!");
        // Ordered before the checkpoint: two submits in flight from one socket are handled
        // concurrently by the relay and may be ordered either way.
        a.until(Duration::from_millis(500), |_, _| false).await;
        assert!(a.save().await, "the checkpoint was not acknowledged: {:?}", a.work);
        assert_eq!(h.relay.rooms.lock().await.get(&room).map(|r| r.saved_seq()), Some(1));
    }

    /// Round 3: a snapshot of about 2 MiB goes through the transport and is acknowledged at
    /// the default text cap, with every transport limit sized from that cap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_a_two_mib_snapshot_checkpoints_at_the_default_cap() {
        let Some(h) = start_with("", crate::DEFAULT_TEXT_BYTES).await else { return };
        let room = unique_room("big-snap");
        let t = token::sign_at(SECRET, &room, "craft_office", "alice", true, "alice", now()).unwrap();
        let file = "ab".repeat(800 * 1024);
        let mut a = LivePeer::open(&h.url, &t, &format!("doc:{room}"), &file).await;
        a.insert(0, "!");
        assert!(a.save().await, "a large checkpoint was not acknowledged: {:?}", a.work);
        assert_eq!(h.relay.rooms.lock().await.get(&room).map(|r| r.saved_seq()), Some(1));
    }

    /// Round 3: a snapshot past the relay's text cap is refused for good before it is sent,
    /// so a saver does not loop on retries the transport would drop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_a_snapshot_over_the_cap_is_refused_not_retried() {
        let Some(h) = start_with("", 1 << 20).await else { return };
        let room = unique_room("over-cap");
        let t = token::sign_at(SECRET, &room, "craft_office", "alice", true, "alice", now()).unwrap();
        let file = "ab".repeat(600 * 1024);
        let mut a = LivePeer::open(&h.url, &t, &format!("doc:{room}"), &file).await;
        let refused = a.live.checkpoint();
        assert!(matches!(&refused, Err(craft_collab::Error::Refused(reason)) if craft_collab::refusal_is_permanent(reason)), "{refused:?}");
    }

    /// Round 4, 4: a scope retaining more than one native client frame (a near-cap snapshot
    /// plus the edits after it) reaches a fallback joiner in pieces it can read, and the joiner
    /// converges. The snapshot pushes everything else out of the op history, so the join has to
    /// fall back to the retained text.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_a_scope_past_one_client_frame_reaches_a_fallback_joiner() {
        let Some(h) = start_with("", crate::DEFAULT_TEXT_BYTES).await else { return };
        let room = unique_room("full-scope");
        let doc = format!("doc:{room}");
        let t = token::sign_at(SECRET, &room, "craft_office", "alice", true, "alice", now()).unwrap();
        let file = "ab".repeat(5 * 1024 * 1024);
        let mut a = LivePeer::open(&h.url, &t, &doc, &file).await;
        a.insert(0, &"1".repeat(1024 * 1024));
        assert!(a.save().await, "the near-cap checkpoint was not acknowledged: {:?}", a.work);
        a.insert(0, &"2".repeat(1024 * 1024));
        a.until(Duration::from_secs(3), |_, _| false).await;
        let retained: usize = h
            .relay
            .rooms
            .lock()
            .await
            .get(&room)
            .map(|r| r.texts().values().map(|l| l.ops().iter().map(|o| o.to_string().len()).sum::<usize>()).sum())
            .unwrap_or(0);
        assert!(retained > 16 * 1024 * 1024, "the scope holds more than one client frame: {retained}");

        let t = token::sign_at(SECRET, &room, "bob", "bob", true, "bob", now()).unwrap();
        let mut b = LivePeer::connect(&h.url, &t, &doc).await;
        let pieces = |work: &[craft_collab::Work]| work.iter().filter(|w| matches!(w, craft_collab::Work::SeedWanted { .. })).count();
        assert!(b.until(Duration::from_secs(20), |_, work| pieces(work) >= 2).await, "the catch-up did not arrive in pieces: {:?}", b.work);
        b.live.seed(&crate::test_text::para(&file)).expect("restores");
        assert_eq!(b.text().len(), a.text().len());
        assert!(b.text() == a.text(), "the fallback joiner did not converge");
    }

    /// Round 5, 6: a join whose replay is close to the whole op history queues every op on the
    /// socket at once. engineioxide's default buffer holds 128 packets and an emit past it
    /// fails, which used to leave the joiner with a gap; the buffer now holds the longest
    /// replay and the joiner converges.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_a_near_max_replay_reaches_a_late_joiner_whole() {
        let Some(h) = start().await else { return };
        let room = unique_room("long-replay");
        let doc = format!("doc:{room}");
        let t = token::sign_at(SECRET, &room, "alice", "alice", true, "alice", now()).unwrap();
        let mut a = LivePeer::open(&h.url, &t, &doc, "hello").await;
        let edits = crate::room::HISTORY - 48;
        for i in 0..edits {
            a.insert(0, if i % 2 == 0 { "a" } else { "b" });
            if i % 200 == 0 {
                a.until(Duration::from_millis(20), |_, _| false).await;
            }
        }
        let seq = || async { h.relay.rooms.lock().await.get(&room).map(crate::room::Room::seq).unwrap_or(0) };
        let end = tokio::time::Instant::now() + Duration::from_secs(30);
        while seq().await < edits as u64 && tokio::time::Instant::now() < end {
            a.until(Duration::from_millis(100), |_, _| false).await;
        }
        assert_eq!(seq().await, edits as u64, "every edit was ordered");

        let t = token::sign_at(SECRET, &room, "bob", "bob", true, "bob", now()).unwrap();
        let mut b = LivePeer::join_late(&h.url, &t, &doc, "hello").await;
        let want = a.text();
        assert_eq!(want.len(), edits + 5);
        let target = want.clone();
        b.until(Duration::from_secs(20), move |live, _| live.doc().and_then(|d| d.block(0)).is_some_and(|p| p.text == target)).await;
        assert!(b.text() == want, "the late joiner has {} of {} chars", b.text().len(), want.len());
    }

    /// Round 5, 3: a checkpoint from an editor that is not the service saver is refused for
    /// good, so it neither moves the room's baseline nor loops on retries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "needs REVIEW_VALKEY_URL"]
    async fn e2e_an_editors_checkpoint_is_refused_for_good() {
        let Some(h) = start().await else { return };
        let room = unique_room("not-saver");
        let t = token::sign_at(SECRET, &room, "alice", "alice", true, "alice", now()).unwrap();
        let mut a = LivePeer::open(&h.url, &t, &format!("doc:{room}"), "hello").await;
        a.insert(5, "!");
        assert!(!a.save().await);
        assert!(a.work.iter().any(|w| matches!(w, craft_collab::Work::CheckpointRefused { .. })), "{:?}", a.work);
        let room_state = h.relay.rooms.lock().await;
        let r = room_state.get(&room).expect("the room");
        assert_eq!(r.saved_seq(), 0);
        assert!(r.texts().values().all(|l| !l.starts_with_snapshot() && l.ops().len() == 1), "no snapshot was retained");
    }
}
