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
        let mut doc = LiveDoc::seeded(&[SeedBlock { id: PARA, text: text.to_owned(), attrs: Map::new(), runs: Vec::new() }]);
        doc.set_client(space);
        Replica(doc)
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
