//! A one-paragraph `craft_collab::LiveDoc` replica for the relay's tests.
//!
//! The relay never reads `crdt.text` payloads. These tests drive a real client document against
//! what `Room` retained, so the round trip is checked against the code that has to decode it
//! rather than against an assumption about the shape.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use craft_collab::{DOC_SCOPE, LiveDoc, SeedBlock, TextPayload, update_from_wire};
use serde_json::{Map, Value};

/// The paragraph every test edits.
const PARA: u64 = 0;

pub struct Replica(LiveDoc);

impl Replica {
    /// Seed one paragraph holding `text`, as the file would, and mint in `space`.
    pub fn seeded(space: u32, text: &str) -> Replica {
        let mut doc =
            LiveDoc::seeded(&[SeedBlock { id: PARA, text: text.to_owned(), attrs: Map::new(), runs: Vec::new() }]).expect("one block seeds");
        doc.set_client(space).expect("the client id is adopted");
        Replica(doc)
    }

    /// Restore from a retained snapshot op (one base64 full state), as a joiner does, and mint
    /// in `space`.
    pub fn from_snapshot(space: u32, op: &Value) -> Replica {
        let state = update_from_wire(op.as_str().expect("an op is a string")).expect("an op is base64");
        let mut doc = LiveDoc::from_state(&state).expect("a snapshot restores");
        doc.set_client(space).expect("the client id is adopted");
        Replica(doc)
    }

    /// The `crdt.text` params of a checkpoint snapshot of this replica, as the saver submits.
    pub fn snapshot(&self) -> Value {
        serde_json::to_value(TextPayload::snapshot(&self.0.state())).expect("payload encodes")
    }

    /// Type `text` at char `at` and return the `crdt.text` params to submit.
    pub fn insert(&mut self, at: usize, text: &str) -> Value {
        let update = self.0.insert_text(PARA, at, text, &Map::new()).expect("seeded and in a space");
        serde_json::to_value(TextPayload::new(&update)).expect("payload encodes")
    }

    /// Apply replayed or broadcast ops, each one base64 update.
    pub fn apply(&mut self, ops: &[Value]) {
        for op in ops {
            let update = update_from_wire(op.as_str().expect("an op is a string")).expect("an op is base64");
            self.0.apply(&update).expect("an op is an update");
        }
    }

    /// The paragraph's text.
    pub fn render(&self) -> String {
        self.0.block(PARA).expect("the paragraph exists").text
    }
}

/// The scope every update travels under.
pub const SCOPE: &str = DOC_SCOPE;

/// The paragraph every test edits, as a seed block for a [`craft_collab::Live`].
pub fn para(text: &str) -> Vec<SeedBlock> {
    vec![SeedBlock { id: PARA, text: text.to_owned(), attrs: Map::new(), runs: Vec::new() }]
}

/// A [`craft_collab::Live`] over a real socket, for the end-to-end tests: the path an app
/// takes, so a save goes through [`craft_collab::Live::checkpoint`] and nothing else.
pub struct LivePeer {
    pub live: craft_collab::Live<craft_collab::native::NativeHub>,
    pub work: Vec<craft_collab::Work>,
}

impl LivePeer {
    pub async fn connect(url: &str, token: &str, doc: &str) -> LivePeer {
        let hub = craft_collab::native::NativeHub::connect(url, token, None).await.expect("connect");
        LivePeer { live: craft_collab::Live::new(hub, doc), work: Vec::new() }
    }

    /// Connect, seed `file` and wait until the relay granted an id space.
    pub async fn open(url: &str, token: &str, doc: &str, file: &str) -> LivePeer {
        let mut peer = LivePeer::connect(url, token, doc).await;
        peer.live.seed(&para(file)).expect("the file seeds");
        assert!(peer.until(std::time::Duration::from_secs(5), |live, _| live.ready_to_type()).await, "never ready to type");
        peer
    }

    /// Connect without the file, wait for the catch-up, then load `file` as an app does.
    pub async fn join_late(url: &str, token: &str, doc: &str, file: &str) -> LivePeer {
        let mut peer = LivePeer::connect(url, token, doc).await;
        peer.until(std::time::Duration::from_millis(800), |_, _| false).await;
        peer.live.seed(&para(file)).expect("the file seeds");
        peer
    }

    /// Pump until `done` holds or `d` passes. Whether it held.
    pub async fn until(
        &mut self,
        d: std::time::Duration,
        done: impl Fn(&craft_collab::Live<craft_collab::native::NativeHub>, &[craft_collab::Work]) -> bool,
    ) -> bool {
        let end = tokio::time::Instant::now() + d;
        loop {
            let work = self.live.pump();
            self.work.extend(work);
            if done(&self.live, &self.work) {
                return true;
            }
            if tokio::time::Instant::now() > end {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// The paragraph as this peer shows it.
    pub fn text(&self) -> String {
        self.live.doc().and_then(|d| d.block(PARA)).map(|b| b.text).unwrap_or_default()
    }

    /// Type `text` at char `at`.
    pub fn insert(&mut self, at: usize, text: &str) {
        self.live.edit(|d| d.insert_text(PARA, at, text, &Map::new())).expect("an edit goes out");
    }

    /// Checkpoint and wait for the relay to order it. Whether it was accepted.
    pub async fn save(&mut self) -> bool {
        self.live.checkpoint().expect("a checkpoint goes out");
        let from = self.work.len();
        self.until(std::time::Duration::from_secs(5), |_, work| {
            work[from..].iter().any(|w| {
                matches!(
                    w,
                    craft_collab::Work::Checkpointed { .. } | craft_collab::Work::CheckpointRaced | craft_collab::Work::CheckpointRefused { .. }
                )
            })
        })
        .await;
        self.work[from..].iter().any(|w| matches!(w, craft_collab::Work::Checkpointed { .. }))
    }
}
