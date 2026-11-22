# CollabCraft

Cloud documents and shared editing for the Craft apps.

One service holds four jobs: signing people in, reaching whatever cloud their
documents live in, relaying live edits between them, serving the browser
builds. They are together because they share a secret, a bus and a provider.
Splitting them would mean three deployments agreeing on all three.

An app talks only to CollabCraft. It holds no provider credential, knows no
provider URL and implements no provider protocol. That is the whole design: a
desktop binary carrying a cloud password leaks one the moment somebody copies
the file. A plugin per provider reimplements authentication, file authorisation
and token minting every time.

## What it needs

| Dependency | Why | Notes |
|---|---|---|
| A Redis-compatible server | per-document sequence, scope locks, sessions | Valkey or Redis. Must not evict: see below |
| An OIDC provider | signing people in | Tested against Authentik and Keycloak |
| A cloud provider | where documents live | Nextcloud today. See *Providers* |
| A reverse proxy | TLS | Optional, but the OIDC redirect wants https. Must pass WebSocket upgrades: see below |

Nothing else. The service writes nothing to disk and keeps no database of its
own.

**The bus must not evict.** The sequence counters are not a cache. Losing one
silently reorders every edit in that document. Clients then hold their edits,
waiting for numbers that never arrive. Run it with
`--maxmemory-policy noeviction`. The bus overlay below already does.

**Proxies must pass WebSocket upgrades.** Native clients (`NativeHub`) connect
to the relay's Socket.IO endpoint over WebSocket only, with no long-polling
fallback. Every proxy between a client and the relay has to forward the
`Upgrade: websocket` handshake on `<base>/socket.io/`, and keep idle
connections open longer than the relay's 25 s ping. With nginx, that means
`proxy_http_version 1.1` plus the `Upgrade` and `Connection` headers. A proxy
that only allows polling leaves native clients unable to connect.

## Deploying

```sh
cp .env.example .env
$EDITOR .env          # every variable is documented in place
docker compose up -d
curl -fsS http://127.0.0.1:9444/healthz
```

That assumes `COLLABCRAFT_VALKEY_URL` names a Redis-compatible server you
already run. A deployment with no bus of its own layers one on:

```sh
docker compose -f docker-compose.yml -f docker-compose.valkey.yml up -d
```

Nothing else differs between the two arrangements.

`/healthz` answers only once the bus is writable. A service that is up but
cannot order an edit therefore fails the check.

Register a confidential OIDC client with the redirect URI
`${COLLABCRAFT_PUBLIC_URL}/auth/callback` and the scopes `openid profile email
groups`. Entitlement is decided from group membership, so without `groups`
every user looks ungrouped.

### Behind an internal certificate authority

A stock image trusts neither an internal identity provider nor an internal
cloud provider. The failure reads as *could not reach the identity provider*.
That says nothing about a certificate. Mount a bundle carrying the
public roots **alongside** your own CA and point `COLLABCRAFT_CA_BUNDLE` at it.
Replacing the system bundle with only your CA breaks every public call.

```sh
cat /etc/ssl/certs/ca-certificates.crt your-root-ca.crt > ca-bundle.pem
```

## The web face

The service answers a browser as well as an application. The pages are
rendered by the service itself. No framework, no build step, no external
asset. One request draws a whole page.

`/` lists the applications the signed-in user may open. The set is their
entitlement crossed with what this deployment hosts. An entitlement to an
application nobody deployed draws nothing. A card over a 404 is worse than no
card. An anonymous visitor is offered a sign-in.

`/account` says who somebody is and whether their storage is linked. It lists
the applications they may open. It carries the actions for linking storage,
unlinking it and signing out. The desktop application's Account settings link
already points here.

`/settings/provider` is where a deployment says which cloud it uses. See
*Configuring the provider* below.

Every page draws what it draws from entitlement and capability. A control that
cannot work does not appear. A deployment with no identity provider offers no
sign-in button. A deployment with no cloud provider offers no Link storage
button. Somebody outside the configuring group sees no link to the setup page.

Light and dark follow the browser's own setting through
`prefers-color-scheme`. There is nothing to choose and nothing stored.

## Configuring the provider

`COLLABCRAFT_PROVIDER_URL` is the default. A value saved on
`/settings/provider` overrides it. That value lives on the bus with no expiry.
A deployment that saves nothing keeps working exactly as it did.

Saving tests first. The test asks the provider to begin its own desktop
sign-in. That needs no credential and no account. A server that answers is
reachable, speaks the protocol and is willing to issue a credential. An
address that does not answer is never saved. A stored address nobody proved
fails for every user at once.

`COLLABCRAFT_ADMIN_GROUPS` names the groups whose members may change it. The
identity provider reports membership. Somebody added to a group there has the
whole of the permission. A deployment that names no group has nobody who may
change it. The page then refuses everybody.

The page names the groups that may change it. Somebody refused therefore knows
who to ask. The provider's address appears on that page and on no other, since
that group is the one entitled to change it. One exception stands. A user
linking storage authenticates at their own provider and has to be sent there.

## Providers

`craft-cloud` defines what a provider offers: files, sharing, contacts,
notifications, mail, accounts. None of it names a vendor. None of it has to. Files are WebDAV, contacts are CardDAV, calendars are CalDAV. Only
sharing and notification are vendor-shaped. Both are small.

Three kinds ship, chosen with `COLLABCRAFT_PROVIDER_KIND` or on
`/settings/provider`:

| Kind | Address | Linking | What works |
|---|---|---|---|
| `nextcloud` | server root | Nextcloud's own browser sign-in | everything: files, sharing, contacts, Talk, Mail |
| `owncloud` | server root | login name and app password | files, sharing, shared-with-me, contacts. No Talk, Mail or notifications |
| `webdav` | the files address, `{user}` where the login name goes | login name and app password or token | files with etag-checked saves and co-editing. Contacts where a CardDAV address is given. No sharing |

`webdav` is any server speaking [RFC 4918]: SabreDAV, Apache `mod_dav`, nginx,
Seafile's SeafDAV, a Synology or QNAP, ownCloud Infinite Scale, `rclone serve
webdav`. Its contacts come from any CardDAV server, Baïkal and Radicale
included, through `COLLABCRAFT_PROVIDER_CONTACTS_URL`. A few addresses to start
from:

| Server | Files address |
|---|---|
| ownCloud Infinite Scale | `https://ocis.example.com/remote.php/dav/files/{user}` |
| Seafile | `https://seafile.example.com/seafdav` |
| Synology | `https://nas.example.com:5006/home` |
| SabreDAV, Apache, nginx, rclone | wherever the server mounts it |

Plain WebDAV keys a co-editing room on the path unless the server reports a
file id. Renaming a document while people are in it splits the room there.
A save refuses to overwrite a newer version only where the server honours
`If-Match`. Apache, SabreDAV and Nextcloud do. `rclone serve webdav` does not,
so a save there can overwrite somebody else's.

A kind without a browser sign-in is linked with a login name and an app
password. The service asks the server to accept them before storing anything.
That means this service holds the credential, as it does the Nextcloud one. Use
a password the server lets you revoke on its own.

`craft-nextcloud` implements the first two. `craft-webdav` implements the
third over the same WebDAV and CardDAV code. Git is a fourth kind: files go
through the GitHub or Gitea/Forgejo contents API for a named repository,
linked with a login and a token. Sharing, mail and contacts are absent, the
same way they are on plain WebDAV. Another kind is an implementation of six
traits and one line in `state::provider_from`, with no change to any app.

[RFC 4918]: https://www.rfc-editor.org/rfc/rfc4918

What a provider can actually do is reported, never assumed. An administrator
who forbids public links gets no Copy link button. The alternative is a button
that fails on use.

## Entitlement

`COLLABCRAFT_APPS_DEFAULT` is what everyone who signs in may open.
`COLLABCRAFT_APPS_<GROUP>` adds to that for members of a group from the
identity provider. Serving an app directory checks it. Adding somebody to a group is therefore the
whole of granting access.

A recipient who followed a keyed link carries no group and may open the one
app that opens the one document they were sent.

A client is never asked whether it may run. A client asking itself is no check
at all.

`/settings/apps` changes all of that without a redeploy. A stored entitlement
overrides the environment, the same way a stored provider address does. The
page is gated on `COLLABCRAFT_ADMIN_GROUPS`. The people who decide where
documents live are the people who decide who may open them. Dropping the stored
record puts the deployment back on whatever its environment names.

The page lists what this deployment hosts beside what it grants. It names
anything granted that nobody deployed. That combination is the one mistake
whose only symptom is a card that never appears.

## Sharing with somebody who has no account

A share aimed at an address the provider holds no account for produces a link
carrying a one-time key. The provider is asked to mail it, through the
`Notifications` or `Mail` contract. This service therefore opens no connection
to a mail server and holds no mail credential. Where a provider sends no mail
of its own the link comes back to the sharer, who passes it on.

Following that link puts the recipient in the document. An installed Craft app
is handed the document through its `craft+<app>:` scheme. Where none is
installed the browser build opens, with a scoped join token already in hand. A
recipient therefore lands in the document and not on a sign-in page.

The key in the URL is a lookup handle and a secret. It is never a signed blob
carrying the permission. The authority stays here. What is stored is an HMAC
over the key material under the service secret. A copy of the keyspace
therefore opens no document. The permission is re-read from the provider on
every request a recipient makes. A share withdrawn or downgraded five minutes
into a session therefore takes effect in that session.

A recipient holds no account and is entitled to one room at one permission
through one app. Every entitlement question in the service comes through one
method. That bound therefore holds on every route at once.

A key expires. It can be spent. It can be withdrawn. Setting
`COLLABCRAFT_KEYED_LINKS=off` turns the whole mechanism off. Nothing is then
minted. Every link answers 404. The capability is reported absent on
`/api/v1/me`. The share dialog therefore offers no button.

## The browser builds

Point `COLLABCRAFT_WEB_DIR` at a directory of per-app WebAssembly builds, one
directory per app. They are served behind the same permission check as
everything else.

Serve them precompressed: put `foo.wasm.br` beside `foo.wasm` and the service
sends it with the right `Content-Encoding`. Brotli at a high level costs
minutes per bundle. That is unaffordable per request and free once at deploy.
A 16 MB bundle becomes about 5 MB.

Leave the directory empty to run as an API and relay only.

## One relay per document

A document is served by exactly one relay process. Nothing enforces that, and
getting it wrong is quiet rather than loud, so it belongs in front of the
deployment notes.

The shared Valkey makes two pieces safe across instances. `INCR` means two
relays cannot hand out the same sequence number, and `SET NX` means two cannot
grant the same scope lock. Fan-out is not one of those pieces. An ordered edit
reaches the room through the Socket.IO in-process adapter, so a second instance
serving the same document would take sequence numbers from the shared counter
and deliver only its own half of them. Every client on both sides would sit on a
permanent gap in the sequence, ask for a resync, and be told to reopen the file.

Run one relay, or route a document to a fixed instance. A load balancer in front
of several relays needs session affinity on the document, not on the client.

Carrying edits between instances needs a Socket.IO adapter with a broker behind
it, which is a real change rather than a configuration flag: the adapter has to
be installed on the builder before `io.ns`, every broadcast path has to become
fallible and awaited, and the result has to be tested against two relays and a
Valkey together. `socketioxide-redis` was a declared dependency that nothing
used, which read as though this already worked. It is gone until the work is
done.

## How an edit travels

The API asks the provider whether this user may open this document, signs a
short-lived token carrying the answer, then hands it over. The relay verifies
that signature locally and never calls back. Authorisation costs one round trip
per document, not one per keystroke.

Each edit gets a sequence number from the bus and is fanned out in that order.
Concurrent work in different regions proceeds freely. Concurrent work in the
same region is serialised by a scope lock.

Text is the exception, because a lock on a paragraph serialises exactly what two
people writing in it want merged. Commands named `crdt.*` carry per-scope RGA
operations from `craft-crdt`, and the relay strips any scope from them. Two
people type in one paragraph and both insertions survive, with no lock and no
authority deciding between them. Structured commands that do not commute, such
as a boolean or a render, still take a lock.

Two rules keep the character identities honest. Every scope allocates from its
own counters, so the ids a paragraph receives cannot move when another paragraph
is loaded first. The relay grants each connection its own id space in the join
payload, and a client mints nothing before that arrives. A replica keeps the first space it was
granted across reconnects (nobody else is ever granted it), so its own items, its undo history and
the edits it made while the link was down carry on unchanged; it takes a new one only when the
room lost its history.

A peer joining a live session cannot reach the room's text by reading the file,
because the file is behind every edit made since it was written. The relay
replays the `crdt.*` operations it still holds, per scope, outside the sequence.
The peer loads the file and the replay lands on top, in either order. A scope the
relay could not keep whole is reported as incomplete rather than left to look
current.

## Pass-through and ephemeral messages

Two message kinds carry nothing the relay reads.

A **frame** is opaque bytes. No sequence, no history, no decoding, so a caller
that signs and orders its own traffic keeps both properties end to end. It needs
edit permission.

An **ephemeral** message is latest-wins state about one peer, such as a camera, a
selection or a caret. The relay keeps only the newest value per peer and kind,
and hands those to a joining peer so somebody arriving late sees where everyone
is without waiting for them to move. It needs no edit permission, because
somebody watching a design should still be able to say where they are looking.
Nothing ephemeral is ordered, replayed or journaled.

## Live API notes: scope locks and the pinned base (M1)

Public items added to the Rust crates, additive on the wire (an older relay or client ignores them); in Rust, `Effect::LockHeld` gained a field and `Effect` gained `LockBusy`, so exhaustive matches need updating:

- **Wire.** `lock_granted` carries `seq`, the room's sequence when the lock was granted. A
  missing `seq` parses as `None`. The relay no longer records a grant for a peer that left while
  it waited.
- **`craft-collab`.** `Effect::LockHeld { scope, seq }` (was `LockHeld { scope }`) and
  `Effect::LockBusy { scope, holder }`, raised with `RemoteLocks` when somebody else just took
  a scope. `Live::unconfirmed_edits()` counts edit updates the relay has not
  ordered yet, a resent update counted once, and edits dropped when the replica broke or a relay
  snapshot replaced it stay counted. `LockEvent` (`Granted`, `Denied`, `Released`,
  `HeldByOther`) is exported.
- **`craft-live`.** `LiveEditing::lock(scope)`, `unlock(scope)`, `take_lock_events()` and
  `local_settled()`. The `ordered` module: `Ordered<P>` runs one queued item at a time under one
  scope (`request`, `on_lock`, `poll`, `finish`, `rejoined`), `Polled` (`Nothing`, `Run`, `GaveUp`,
  `LockLost { scope }`), `Action` and `GIVE_UP_MS`. `Ordered::replica_broken()` ends a run whose replica broke as
  `LockLost` and returns the best-effort `SendUnlock` (the relay's lease is the backstop). `edits_saved()` now reads the
  replica's unconfirmed count, so a submit refused for now no longer reads as unsaved after its
  resend is ordered. `testing::MemRelay` acknowledges `Saved` with `SavedAck`, as the real relay
  does.
- **`craft-live-worker`.** `DocumentService::pinned_base()` and `pinned_source()`: a pinned
  service opens a room from the base the Nextcloud app pinned for the grant
  (`X-Craft-Base-Sha256` is checked), keeping the file's name and ETag so the first write stays
  `If-Match` on the file. A missing or mismatched base closes the room.

## Live API notes: fenced ordered edits (C8)

A lock holder that is paused or cut off past its lease (60 s) must never order an edit after
somebody else was granted the lock. Every grant now carries a fencing token, and edits made under
it are ordered only while it is still the current grant. Additive on the wire; in Rust, struct
variants gained fields (see below), so literals and exhaustive matches need updating.

- **Wire.** `lock_granted` carries `token` (per room and scope, strictly increasing, a re-grant to
  the same holder included) and `lease_ms`. `submit` may carry `fence: {scope, token}`: the relay
  orders it only while the sender holds `scope` under `token` (checked on the bus, inside the
  room's gate, before the sequence; the check extends the lease), and otherwise refuses it with
  `error {code: "fenced", local}`, never numbered, retained or replayed. `unlock` may carry
  `token`: then only that grant is released, so a stale unlock is a no-op. New `refresh {scope,
  token}` extends the lease; when the grant is not current it is answered with `lock_lost {scope,
  token}` to the sender. Submits without `fence` (free edits) are unchanged.
- **`craft-collab`.** `Fence { scope, token }`, `FENCED`. `ClientMsg::Submit` gained `fence`,
  `ClientMsg::Unlock` gained `token`, new `ClientMsg::Refresh`. `ServerMsg::LockGranted` gained
  `token` and `lease_ms`, new `ServerMsg::LockLost`. `Effect::LockHeld` gained `token` and
  `lease_ms`, new `Effect::LockLost`. `LockEvent::Granted` gained `token`, new `LockEvent::Lost {
  scope, token }`. `Session::submit_fenced`, `release_grant`, `refresh_lock`. `Live` fences by
  itself: from a fenced grant until `Live::unlock(scope)` (which sends the token) every local edit
  goes out under the fence; a fenced refusal takes the run back (the replica is rebuilt from its
  state at the grant plus everything the room ordered since, `Work::Reset`) and reports
  `LockEvent::Lost`. `Live::fence()` and `Live::refresh_lease(now_ms)`.
  Under the fence edits go out one submit at a time (the next waits for the relay's answer to the
  one before, so a refusal never leaves a later edit ordered behind a hole); an edit is only ever
  sent under the token it was made under. After a loss the fence stays, dead, until
  `Live::unlock(scope)`: edits made meanwhile are never sent and are taken back, so a lost run
  fails closed. `Live::unlock` while fenced edits are unanswered waits for them before the unlock
  goes out (edits made in between wait too, then go free).
- **`craft-live`.** `LiveEditing::pump` refreshes the lease at half its length on the host clock
  (`set_clock`) and passes `LockEvent::Lost` through `take_lock_events()`. `Ordered`: a `Lost`
  for its grant before the run gives the dead grant back and asks again, during the run ends it as
  `Polled::LockLost`, and `finish` still returns `SendUnlock` for a fenced run (the unlock ends the
  client's dead fence and releases nothing at the relay); a rejoin
  no longer loses a fenced run by itself (the relay's answer to its edits decides). `MemRelay`
  fences like the relay, and `MemRelay::lapse(scope)`, `refreshes()` and `holder(scope)` let a
  test run a lease out.

## Known limits

Phase 1 of live co-editing ships with these. Each is a decision, not an oversight.

- **Fencing tokens restart with Valkey's data.** The per-scope token counter lives in Valkey
  (`collabcraft:doc:<room>:fence:<scope>`, 30 days idle). A Valkey restart without persistence,
  a `FLUSHDB`, or a scope idle past that resets it, so tokens can repeat. Safety holds because the
  fence also requires the holder to be the same peer id, so only that peer could reuse a token.
  The lock scripts also touch two keys without a hash tag, so a Valkey Cluster would refuse them
  (`CROSSSLOT`), which fails closed: every lock becomes `retry`.

- **Room state lives in relay memory only.** The op history, the retained text with its
  checkpoint snapshot, and the saved marker are not written to Valkey (only the sequence, the id
  space counter and the locks are). A room whose last socket left is kept for a grace period of
  120 s (`ROOM_GRACE`), then dropped. After a relay restart, or once a room was dropped, the
  relay marks every join to it `history_lost` until somebody checkpoints: a replica that held
  nothing before refuses edits and waits for a snapshot, and the document service's saver
  checkpoints at once if it still holds the document. A replica that held the document and
  rejoins before that snapshot still gets the room's own edits since the restart. The unsaved
  edits are lost if the saver did not survive. Editors' windows still hold them; they go Broken
  and can download a copy. Persisting
  the snapshot and the tail in Valkey is the real fix, for a later phase.
- **The service identity is a ticket `uid`.** The relay trusts any ticket whose `uid` is
  `COLLABCRAFT_SERVICE_UID` (default `craft_office`) to checkpoint and to report saves. The
  Nextcloud app refuses to mint a ticket for a real user with that uid; a separate signed claim
  would be stronger and is left for later.
- **The id space counter has no floor.** It lives in Valkey with a 30-day TTL refreshed on every
  connection. If it is lost (a flush, a different database) while the sequence survives, spaces
  start again at 1 and a new connection can share a space with a replica still open from before
  (a replica keeps its space across reconnects, unless the join says the room lost its history).
  Reopening every window after a Valkey flush avoids it.
- **A crafted yrs update can abort a wasm build.** Native builds catch a yrs panic and mark the
  replica damaged. A browser build targets `wasm32`, where a panic aborts and cannot be
  caught, so a hostile update from a peer with edit rights can stop that tab; the peer already
  had edit rights to the document.
- **A refreshed ticket does not reach a live socket.** A ticket is read at connect, and the relay
  drops the socket when it expires (the default lifetime is 15 minutes). The hub reconnects with
  the newest ticket it was given, so the cost is one reconnect and its catch-up per lifetime.
- **A join copies the retained text under the relay-wide rooms lock.** Bounded by the room's text
  budget, but a join to a room near it briefly delays other rooms' bookkeeping.
- **Transport crates are held at `info` logging.** At `debug` socketioxide and engineioxide trace
  handshakes, which carry the ticket, so the relay caps both at `info` whatever `RUST_LOG` says:
  a `RUST_LOG` directive naming either crate or a module inside one, or filtering by span, is
  dropped before the cap is added (`log_filter` in `src/main.rs`, with tests). A build that
  removes that cap must not run at `debug` in production.
- **A peer that vanished is reaped at the ping timeout.** engine.io pings every 25 s and waits
  90 s for the answer, so that a pong queued behind a large checkpoint on a slow uplink still
  arrives. A socket whose TCP connection died without a close is noticed by the relay only then.
- **One user holds at most 16 connections to one document**, and a document at most 256.
- **Single relay process only.** Room state is per process, so several relay processes behind
  one Valkey are unsupported, and `history_lost` misfires there.
- **A sequence number burned in Valkey leaves a gap.** When `INCR` succeeded but its reply timed
  out, nothing is ordered at that number. Peers stall until `MAX_PENDING` and are told to reopen.
- **A room dropped while its grant is still alive gives the next reopen `history_lost`.** When
  the room was dropped after the 120 s grace (the worker crashed, everyone left), the fresh
  editor and worker go Broken after 20 s. The file is complete, so nothing is lost, but the user
  must reopen once the app's stale job closes the grant.
- **A refused socket can leave an empty room that no grace timer drops.** A small leak, bounded
  by the rooms such sockets name.
- **A joiner at baseline 0 keeps its file seed even if seq 1 is a snapshot.** A first
  checkpoint on an untouched room is ordered at seq 1 and reported as `Saved{0}`, which leaves
  the join baseline at 0, the same as "never saved". Services should checkpoint twice on an
  untouched room, or accept a baseline of at least 1.

## Licence

See `LICENSE-APACHE` and `LICENSE-MIT`.
