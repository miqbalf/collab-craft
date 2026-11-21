//! The worker's room open path against a fake Nextcloud (axum, in the test) and a fake
//! document service that records the bytes it was opened with.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod fake;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use craft_live::testing::MemRelay;
use craft_live_worker::{Clock, DocumentService, FileError, Flow, Room, Secret, pinned_source, serve_grant, worker_signature};
use fake::{FakeNextcloud, FakeService, Window, files_for, sha_hex};

const FILE_BYTES: &[u8] = b"file bytes A";
const BASE_BYTES: &[u8] = b"base bytes B";
const FILE_ETAG: &str = "etag-of-the-file";

struct TestClock(AtomicU64);

impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn unix_ms(&self) -> i64 {
        1_795_269_780_000
    }
}

fn clock() -> Arc<TestClock> {
    Arc::new(TestClock(AtomicU64::new(0)))
}

/// Open a room on `service`, add an editor who types, and step until the worker has written.
async fn open_and_save(nc: &FakeNextcloud, service: Arc<FakeService>) -> Vec<(String, Vec<u8>)> {
    let relay = MemRelay::new();
    let hub = relay.connect(craft_live_worker::SERVICE_UID, "service", true);
    let clock = clock();
    let files = files_for(nc);
    let mut room = Room::open_with(service.clone(), hub, files, clock.clone()).await.expect("room opens");
    let bytes = service.opened_with().expect("the service was opened");
    let mut alice = Window::join(&relay, "alice", &bytes);
    for _ in 0..8 {
        room.step().await;
        alice.pump();
    }
    alice.type_text("edited");
    for _ in 0..40 {
        clock.0.fetch_add(1_000, Ordering::SeqCst);
        alice.pump();
        if room.step().await == Flow::Done || !nc.puts().is_empty() {
            break;
        }
    }
    nc.puts()
}

#[tokio::test]
async fn pinned_service_opens_from_the_base() {
    let nc = FakeNextcloud::start(FILE_BYTES, FILE_ETAG, Some(BASE_BYTES), None).await;
    let service = Arc::new(FakeService::new(true));
    let puts = open_and_save(&nc, service.clone()).await;
    assert_eq!(service.opened_with().as_deref(), Some(BASE_BYTES), "opened from the base");
    assert_ne!(service.opened_with().as_deref(), Some(FILE_BYTES));
    let (if_match, _) = puts.first().expect("the room wrote the file");
    assert_eq!(if_match.trim_matches('"'), FILE_ETAG, "the first write is over the file's ETag, not the base's");
}

#[tokio::test]
async fn unpinned_service_opens_from_the_file() {
    let nc = FakeNextcloud::start(FILE_BYTES, FILE_ETAG, Some(BASE_BYTES), None).await;
    let service = Arc::new(FakeService::new(false));
    let puts = open_and_save(&nc, service.clone()).await;
    assert_eq!(service.opened_with().as_deref(), Some(FILE_BYTES));
    assert_eq!(nc.base_requests(), 0, "an unpinned service never asks for the base");
    assert!(!puts.is_empty());
}

/// A refused base: the room is not opened, the app is told to close it, and what the worker
/// would log names no ticket, signature or secret.
async fn refused_base(nc: FakeNextcloud) {
    let service = Arc::new(FakeService::new(true));
    let secret = Secret::new("CRAFT_RELAY_SECRET", Some(&"r".repeat(64))).unwrap();
    let connects = Arc::new(AtomicU64::new(0));
    let seen = connects.clone();
    let connect = move |_ticket: String| {
        seen.fetch_add(1, Ordering::SeqCst);
        async { Err::<craft_live::testing::MemHub, String>("no relay in this test".into()) }
    };
    serve_grant(service.clone(), &secret, files_for(&nc), clock(), connect).await;
    assert_eq!(service.opened_with(), None, "the service was opened");
    assert_eq!(connects.load(Ordering::SeqCst), 0, "the relay was reached for a room that cannot open");
    assert_eq!(nc.closes(), 1, "the room was closed");

    let got = files_for(&nc).get().await.unwrap();
    let Err(err) = pinned_source(service.as_ref(), &files_for(&nc), got).await else { panic!("the base was accepted") };
    let line = format!("{err}");
    for secret in [fake::TOKEN, &worker_signature(&Secret::new("S", Some(&"w".repeat(64))).unwrap(), fake::ROOM, fake::TOKEN).unwrap()] {
        assert!(!line.contains(secret), "the log line names a secret: {line}");
    }
    assert!(!line.contains("127.0.0.1"), "the log line names the server: {line}");
}

#[tokio::test]
async fn base_hash_mismatch_is_refused() {
    let nc = FakeNextcloud::start(FILE_BYTES, FILE_ETAG, Some(BASE_BYTES), Some(sha_hex(b"something else"))).await;
    refused_base(nc).await;
}

#[tokio::test]
async fn missing_base_ends_the_room() {
    let nc = FakeNextcloud::start(FILE_BYTES, FILE_ETAG, None, None).await;
    refused_base(nc).await;
    // And the error is the server's answer, not a hash problem.
    let nc = FakeNextcloud::start(FILE_BYTES, FILE_ETAG, None, None).await;
    let err = files_for(&nc).base().await.expect_err("404");
    assert_ne!(err, FileError::BaseMismatch);
}

#[test]
fn the_default_is_not_pinned() {
    assert!(!FakeService::new(false).pinned_base());
}

#[tokio::test]
async fn every_worker_call_is_signed() {
    let nc = FakeNextcloud::start(FILE_BYTES, FILE_ETAG, Some(BASE_BYTES), None).await;
    let service = Arc::new(FakeService::new(true));
    open_and_save(&nc, service).await;
    assert!(nc.all_signed());
}
