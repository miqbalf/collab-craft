//! The fakes: a Nextcloud with the app's worker routes, and a document service whose document
//! is one line of text, built from the bytes it is opened with.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use craft_collab::{BlockChange, ClientMsg, Hub, LiveDoc, SeedBlock, ServerMsg};
use craft_live::testing::{MemHub, MemRelay, ROOM as RELAY_ROOM};
use craft_live::{Access, Change, Landed, LiveApp, LiveEditing, LiveKind, LocalError};
use craft_live_worker::{DocumentService, LiveDocument, Nextcloud, RoomFiles, Secret};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::watch;

pub const ROOM: &str = "dev:42:1";
pub const TOKEN: &str = "grantTokenAbc123";
const FILE_NAME: &str = "Budget.xlsx";
const PREFIX: &str = "/index.php/apps/craft_office/api/v1";

pub fn sha_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

// ---- the fake Nextcloud ----------------------------------------------------------------------

#[derive(Default)]
struct State_ {
    file: Vec<u8>,
    etag: String,
    base: Option<(Vec<u8>, String)>,
    puts: Vec<(String, Vec<u8>)>,
    closes: usize,
    base_requests: usize,
    signed: bool,
}

type Shared = Arc<Mutex<State_>>;

pub struct FakeNextcloud {
    addr: SocketAddr,
    state: Shared,
}

impl FakeNextcloud {
    /// `base`: the pinned base's bytes (none: the route answers 404); `claimed`: the hash the
    /// route sends (none: the true one).
    pub async fn start(file: &[u8], etag: &str, base: Option<&[u8]>, claimed: Option<String>) -> FakeNextcloud {
        let state: Shared = Arc::new(Mutex::new(State_ {
            file: file.to_vec(),
            etag: etag.to_owned(),
            base: base.map(|b| (b.to_vec(), claimed.unwrap_or_else(|| sha_hex(b)))),
            signed: true,
            ..State_::default()
        }));
        let app = Router::new()
            .route(&format!("{PREFIX}/file/{{room}}"), get(get_file).put(put_file))
            .route(&format!("{PREFIX}/base/{{room}}"), get(get_base))
            .route(&format!("{PREFIX}/room/{{room}}/heartbeat"), post(heartbeat))
            .route(&format!("{PREFIX}/room/{{room}}/close"), post(close))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        FakeNextcloud { addr, state }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State_> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Every write so far: its `If-Match` and its body.
    pub fn puts(&self) -> Vec<(String, Vec<u8>)> {
        self.lock().puts.clone()
    }

    pub fn closes(&self) -> usize {
        self.lock().closes
    }

    pub fn base_requests(&self) -> usize {
        self.lock().base_requests
    }

    /// Whether every call carried a worker signature.
    pub fn all_signed(&self) -> bool {
        self.lock().signed
    }
}

pub fn files_for(nc: &FakeNextcloud) -> RoomFiles {
    let secret = Secret::new("CRAFT_WORKER_SECRET", Some(&"w".repeat(64))).expect("secret");
    let client = Nextcloud::new(&format!("http://{}", nc.addr), Arc::new(secret)).expect("client");
    let (_tx, rx) = watch::channel(TOKEN.to_owned());
    // The sender is dropped; the receiver keeps the last value.
    RoomFiles::new(Arc::new(client), ROOM.to_owned(), rx)
}

fn note_signature(st: &Shared, headers: &HeaderMap) {
    if !headers.get("X-Craft-Worker").is_some_and(|v| v.len() == 64) {
        st.lock().unwrap_or_else(|e| e.into_inner()).signed = false;
    }
}

fn reply(status: StatusCode, headers: &[(&'static str, String)], body: Vec<u8>) -> Response {
    let mut r = (status, body).into_response();
    for (k, v) in headers {
        if let Ok(v) = HeaderValue::from_str(v) {
            r.headers_mut().insert(*k, v);
        }
    }
    r
}

async fn get_file(State(st): State<Shared>, headers: HeaderMap, Path(_room): Path<String>) -> Response {
    note_signature(&st, &headers);
    let s = st.lock().unwrap_or_else(|e| e.into_inner());
    reply(
        StatusCode::OK,
        &[("ETag", format!("\"{}\"", s.etag)), ("Content-Disposition", format!("attachment; filename=\"{FILE_NAME}\""))],
        s.file.clone(),
    )
}

async fn put_file(State(st): State<Shared>, headers: HeaderMap, Path(_room): Path<String>, body: Bytes) -> Response {
    note_signature(&st, &headers);
    let if_match = headers.get("If-Match").and_then(|v| v.to_str().ok()).unwrap_or("").to_owned();
    let mut s = st.lock().unwrap_or_else(|e| e.into_inner());
    if if_match.trim_matches('"') != s.etag {
        return reply(StatusCode::PRECONDITION_FAILED, &[], Vec::new());
    }
    s.puts.push((if_match, body.to_vec()));
    s.etag = format!("etag-after-{}", s.puts.len());
    let answer = json!({ "etag": format!("\"{}\"", s.etag) }).to_string().into_bytes();
    reply(StatusCode::OK, &[("Content-Type", "application/json".into())], answer)
}

async fn get_base(State(st): State<Shared>, headers: HeaderMap, Path(_room): Path<String>) -> Response {
    note_signature(&st, &headers);
    let mut s = st.lock().unwrap_or_else(|e| e.into_inner());
    s.base_requests += 1;
    match s.base.clone() {
        Some((bytes, sha)) => reply(StatusCode::OK, &[("X-Craft-Base-Sha256", sha), ("ETag", "\"etag-of-the-base\"".into())], bytes),
        None => reply(StatusCode::NOT_FOUND, &[], Vec::new()),
    }
}

async fn heartbeat(State(st): State<Shared>, headers: HeaderMap, Path(_room): Path<String>) -> Response {
    note_signature(&st, &headers);
    reply(StatusCode::OK, &[], Vec::new())
}

async fn close(State(st): State<Shared>, headers: HeaderMap, Path(_room): Path<String>) -> Response {
    note_signature(&st, &headers);
    st.lock().unwrap_or_else(|e| e.into_inner()).closes += 1;
    reply(StatusCode::OK, &[], Vec::new())
}

// ---- a document of one line ------------------------------------------------------------------

pub struct Lines;

pub type Doc = Vec<(u64, String)>;

impl LiveKind for Lines {
    type Replica = LiveDoc;
    type Edit = (Doc, Doc);
    type Caret = (u64, usize);
    type Pos = (u64, usize);
    fn pos(caret: &(u64, usize)) -> (u64, usize) {
        *caret
    }
}

#[derive(Default)]
pub struct Engine {
    pub doc: Doc,
}

impl LiveApp<Lines> for Engine {
    fn local(&mut self, replica: &mut LiveDoc, edits: &[(Doc, Doc)]) -> Result<Vec<Vec<u8>>, LocalError> {
        let (Some((before, _)), Some((_, after))) = (edits.first(), edits.last()) else { return Ok(Vec::new()) };
        let mut out = Vec::new();
        for ((id, was), (_, now)) in before.iter().zip(after) {
            if was != now {
                out.push(replica.delete_text(*id, 0, was.chars().count()).map_err(|e| LocalError::Failed(e.to_string()))?);
                out.push(replica.insert_text(*id, 0, now, &Map::new()).map_err(|e| LocalError::Failed(e.to_string()))?);
            }
        }
        Ok(out)
    }
    fn landed(&mut self) -> Landed {
        Landed::Done
    }
    fn put_back(&mut self, edits: Vec<(Doc, Doc)>) -> Result<(), String> {
        self.doc = edits.into_iter().next().ok_or("nothing to put back")?.0;
        Ok(())
    }
    fn received(&mut self, _: &mut LiveDoc, changes: Vec<BlockChange>) -> (Vec<BlockChange>, Vec<Vec<u8>>) {
        (changes, Vec::new())
    }
    fn apply(&mut self, replica: &LiveDoc, _: &[Change<Lines>]) -> Result<(), String> {
        self.resync(replica)
    }
    fn resync(&mut self, replica: &LiveDoc) -> Result<(), String> {
        self.doc = replica.order().into_iter().map(|id| (id, replica.block(id).map(|b| b.text).unwrap_or_default())).collect();
        Ok(())
    }
    fn reset(&mut self, _: u32) {}
    fn set_space(&mut self, _: u32) {}
    fn caret(&self, _: &Value) -> Option<(u64, usize)> {
        None
    }
    fn follow(&self, _: &mut HashMap<String, (u64, usize)>) {}
}

fn seed(bytes: &[u8]) -> Vec<SeedBlock> {
    vec![SeedBlock { id: 1, text: String::from_utf8_lossy(bytes).into_owned(), ..SeedBlock::default() }]
}

fn lines(bytes: &[u8]) -> Doc {
    vec![(1, String::from_utf8_lossy(bytes).into_owned())]
}

/// An editor's window.
pub struct Window {
    engine: Engine,
    le: LiveEditing<MemHub, Lines>,
}

impl Window {
    pub fn join(relay: &MemRelay, uid: &str, bytes: &[u8]) -> Window {
        let hub = relay.connect(uid, uid, true);
        let le = LiveEditing::start(hub, RELAY_ROOM, seed(bytes).as_slice(), Access::Edit).expect("start");
        Window { engine: Engine { doc: lines(bytes) }, le }
    }

    pub fn pump(&mut self) {
        self.le.pump(&mut self.engine);
    }

    pub fn type_text(&mut self, text: &str) {
        let before = self.engine.doc.clone();
        if let Some((_, t)) = self.engine.doc.first_mut() {
            text.clone_into(t);
        }
        self.le.queue().push((before, self.engine.doc.clone()));
        self.le.flush(&mut self.engine);
    }
}

// ---- the service -----------------------------------------------------------------------------

pub struct FakeService {
    pinned: bool,
    opened: Mutex<Option<Vec<u8>>>,
}

impl FakeService {
    pub fn new(pinned: bool) -> FakeService {
        FakeService { pinned, opened: Mutex::new(None) }
    }

    /// The bytes the service was opened with, if it was opened.
    pub fn opened_with(&self) -> Option<Vec<u8>> {
        self.opened.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

pub struct FakeDoc<H: Hub> {
    engine: Engine,
    le: LiveEditing<H, Lines>,
}

impl<H: Hub> LiveDocument<H> for FakeDoc<H> {
    type Kind = Lines;
    type Snapshot = Doc;
    fn live(&self) -> &LiveEditing<H, Lines> {
        &self.le
    }
    fn live_mut(&mut self) -> &mut LiveEditing<H, Lines> {
        &mut self.le
    }
    fn pump(&mut self) -> usize {
        self.le.pump(&mut self.engine).len()
    }
    fn snapshot(&self) -> Doc {
        self.engine.doc.clone()
    }
    fn unchanged(&self, a: &Doc, b: &Doc) -> bool {
        a == b
    }
    fn same(&self, a: &Doc, b: &Doc) -> bool {
        a == b
    }
    fn save(&self, _state: &[u8], _name: &str) -> Result<Vec<u8>, String> {
        Ok(self.engine.doc.iter().map(|(_, t)| t.as_str()).collect::<Vec<_>>().join("\n").into_bytes())
    }
}

impl DocumentService for FakeService {
    type Doc<H: Hub> = FakeDoc<H>;
    fn accepts(&self, file_name: &str) -> bool {
        file_name.ends_with(".xlsx")
    }
    fn open<H: Hub>(&self, hub: H, _room: &str, _file_name: &str, bytes: &[u8]) -> Result<FakeDoc<H>, String> {
        *self.opened.lock().unwrap_or_else(|e| e.into_inner()) = Some(bytes.to_vec());
        let le = LiveEditing::start(hub, RELAY_ROOM, seed(bytes).as_slice(), Access::Edit).map_err(|e| e.to_string())?;
        Ok(FakeDoc { engine: Engine { doc: lines(bytes) }, le })
    }
    fn pinned_base(&self) -> bool {
        self.pinned
    }
}

/// The service's link: the in-memory relay does not acknowledge save reports (the real relay
/// does), so this one does, which is what lets a checkpoint finish.
pub struct AckHub {
    inner: MemHub,
    answers: Vec<ServerMsg>,
}

impl AckHub {
    pub fn new(inner: MemHub) -> AckHub {
        AckHub { inner, answers: Vec::new() }
    }
}

impl Hub for AckHub {
    fn send(&mut self, msg: ClientMsg) -> craft_collab::Result<()> {
        if let ClientMsg::Saved { doc, seq } = &msg {
            self.answers.push(ServerMsg::SavedAck { doc: doc.clone(), seq: *seq });
        }
        self.inner.send(msg)
    }
    fn poll(&mut self) -> Vec<ServerMsg> {
        let mut out = self.inner.poll();
        out.append(&mut self.answers);
        out
    }
    fn connected(&self) -> bool {
        self.inner.connected()
    }
    fn set_token(&mut self, token: String) {
        self.inner.set_token(token);
    }
}
