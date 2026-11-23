//! Valkey, used as the bus and the arbiter.
//!
//! No new message queue. The relay uses a Valkey (on the Craft Office deployments, one of its
//! own; it may also share an instance, which is why every key is namespaced).
//!
//! These two make the relay safe to run beside another instance. Fan-out does not: an ordered
//! edit reaches the room through the in-process Socket.IO adapter, so a document is served by
//! exactly one relay. The README says so under "One relay per document".
//!
//! Two things live here that cannot live in relay memory:
//!
//! * **The sequence.** `INCR` is atomic, so two relay instances cannot hand out
//!   the same number, and the value survives a relay restart. A counter in
//!   process memory would reset to 1 on restart, and every connected client
//!   would then read the next op as a replay and silently drop it.
//! * **The lock table.** `SET NX` is atomic, so two instances cannot both grant
//!   the same scope. Deciding locally would let them.
//!
//! Everything else stays in relay memory on purpose. A Valkey run with `maxmemory` and
//! `maxmemory-policy noeviction` fails writes loudly at the ceiling (for every app on it, when
//! it is shared). So the op history is bounded in process and never written here, and every key
//! family carries a TTL.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use redis::AsyncCommands;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};

/// Key prefix. Namespaced because the instance is shared, and a bare `doc:1`
/// would be a fine key for somebody else too.
const PREFIX: &str = "collabcraft";

/// The highest space a rejoining replica may name as its floor ([`Bus::next_space`]). Spaces are
/// granted one per connection, so a real one is far below; a larger claim is ignored, so a
/// client cannot push the counter towards its wrap, where grants would collide again.
pub const MAX_SPACE_FLOOR: u32 = 1 << 31;

/// The highest sequence a rejoining replica may name as what it applied (R2). Far above any real
/// document's, and below the limit of `INCR` and of a JavaScript number, so a buggy client
/// cannot push the counter towards either.
pub const MAX_SEQ_FLOOR: u64 = 1 << 52;

/// How long a sequence counter outlives its last edit.
///
/// Long, because losing it is worse than keeping it. See [`Bus::next_seq`] for
/// why, and for the reseed that makes expiry survivable anyway.
const SEQ_TTL: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// How long a scope lock survives without a refresh.
///
/// Short, because a crashed relay must not wedge a region. A holder that is
/// still alive refreshes well inside this.
pub const LOCK_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("valkey: {0}")]
    Redis(#[from] redis::RedisError),

    /// The connection landed on a read-only replica.
    ///
    /// Called out separately because it must never pass quietly. The DNS name
    /// resolves to the primary today, and if a second answer is ever added a
    /// write could land on the replica. A sequence that silently fails to
    /// advance corrupts every client's ordering.
    #[error("valkey is a read-only replica; the relay needs the primary for INCR and SET NX")]
    ReadOnlyReplica,
}

/// Whether a scope was granted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockOutcome {
    /// Granted, with the grant's fencing token: higher than every token granted for this scope
    /// before, re-grants to the same holder included.
    Granted {
        token: u64,
    },
    Held {
        holder: String,
    },
}

/// What a fenced release ([`Bus::release_fenced`]) found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The lease was this peer's under this token, and is gone now.
    Released,
    /// Somebody holds the scope, but not this peer under this token (another peer, or this
    /// peer under a newer grant). Nothing changed.
    NotYours,
    /// Nobody holds the scope: the lease lapsed on its own.
    Absent,
}

/// A connection to the shared Valkey.
///
/// A [`ConnectionManager`], so the bus survives a Valkey restart. A single connection opened at
/// startup did not: once Valkey went away it was never replaced, every command failed from then
/// on, and the relay stayed down (health 503, every edit refused) until it was restarted itself.
/// The manager replaces a connection that failed: the command that found it broken fails (and
/// a submit is refused as transient, `code: "retry"`), and the next one runs on a new
/// connection.
#[derive(Clone)]
pub struct Bus {
    conn: ConnectionManager,
    /// How long a lease lasts without a refresh. [`LOCK_TTL`] unless a test shortens it.
    lock_ttl: Duration,
    /// Fail this many [`Bus::next_seq`] calls, as a bus that did not answer in time, before
    /// working again. For the tests of what a transient failure does.
    #[cfg(test)]
    pub fail_next_seq: u32,
    /// room -> the highest id space this relay granted or saw kept there, and when. The floor
    /// [`Bus::next_space`] adds to the counter, so a counter that lost its latest `INCR`s
    /// (an `everysec` AOF crash window, a lagging failover) never grants one again while this
    /// process runs. Shared by clones, as the connection is.
    spaces: Arc<std::sync::Mutex<HashMap<String, (u32, Instant)>>>,
}

/// How long one bus command may take before it fails.
///
/// Every command runs with the relay's bus lock held, and an ordering one also holds the room's
/// gate, so a Valkey that stopped answering would otherwise freeze the document (and, through
/// the bus lock, every document) until the TCP connection died, which can take many minutes.
/// A command that times out fails like any other, and the submit is refused as transient
/// (`code: "retry"`), which a client sends again.
pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);

/// How long connecting to the bus may take, at startup and on each reconnect.
///
/// A reconnect runs while a command waits for it, with the relay's bus lock held, so it is
/// bounded like a command ([`RESPONSE_TIMEOUT`]), and made once per failed command rather than
/// retried with a backoff inside it: a Valkey that is still down then costs each command one
/// short attempt, not a stall of every document.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

impl Bus {
    /// Connect.
    ///
    /// `url` carries the database index, as in
    /// `redis://valkey:6379/6`. Index 6 is free: the stack README
    /// allocates 0 through 5 and 8, and the live keyspace also shows 10 and 13
    /// in use. Sharing an index with another app would mean one `FLUSHDB` away
    /// from losing their data or ours.
    pub async fn connect(url: &str) -> Result<Bus, BusError> {
        let client = redis::Client::open(url)?;
        // Set here rather than left to the client's defaults, which have changed between
        // releases: the bound matters to the relay, so it is the relay's to state.
        let config = ConnectionManagerConfig::new()
            .set_response_timeout(Some(RESPONSE_TIMEOUT))
            .set_connection_timeout(Some(CONNECT_TIMEOUT))
            .set_number_of_retries(0);
        let conn = ConnectionManager::new_with_config(client, config).await?;
        Ok(Bus {
            conn,
            lock_ttl: LOCK_TTL,
            #[cfg(test)]
            fail_next_seq: 0,
            spaces: Arc::default(),
        })
    }

    /// Whether the bus still answers. The health route's whole question.
    ///
    /// Asked twice when the first `PING` finds the connection broken: that failure is what makes
    /// the manager reconnect, and the second asks the new connection. So health reads 200 as
    /// soon as Valkey is back, and 503 for as long as it is not. A `PING` changes nothing, so
    /// asking again is safe; a write is never retried this way.
    pub async fn ping(&mut self) -> Result<(), BusError> {
        match redis::cmd("PING").query_async::<()>(&mut self.conn).await {
            Ok(()) => Ok(()),
            Err(e) if e.is_io_error() || e.is_unrecoverable_error() => {
                redis::cmd("PING").query_async::<()>(&mut self.conn).await?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Fail now if this connection cannot write.
    ///
    /// Called at startup so a misrouted relay dies immediately with a clear
    /// message, rather than accepting peers and then refusing every edit.
    pub async fn check_writable(&mut self) -> Result<(), BusError> {
        let probe = format!("{PREFIX}:startup-probe");
        match redis::cmd("SET").arg(&probe).arg("1").arg("EX").arg(10i64).query_async::<()>(&mut self.conn).await {
            Ok(()) => Ok(()),
            Err(e) if is_readonly(&e) => Err(BusError::ReadOnlyReplica),
            Err(e) => Err(e.into()),
        }
    }

    /// The next sequence for a document.
    ///
    /// `floor` is the relay's own high-water mark for this document. The counter is raised to
    /// at least `floor` before the increment, in the same script, so the number handed out is
    /// always past every number the room has already ordered. Without it, a counter that is
    /// absent (first use, a lapsed TTL, a wiped volume) would hand out 1 again, and one that
    /// went backwards (a Valkey restart inside the `everysec` AOF window, which the relay now
    /// survives, see [`Bus`]) would hand out a number already used; a client holding sequence
    /// 900 would then treat the next ops as replays and drop them all. A live counter above
    /// `floor` is never pushed backwards. A counter that is not a number (a stray write) is
    /// treated as lost and reset to `floor`, rather than refusing every submit for good.
    pub async fn next_seq(&mut self, room: &str, floor: u64) -> Result<u64, BusError> {
        const SCRIPT: &str = r#"
            local floor = tonumber(ARGV[1])
            local raw = redis.call('GET', KEYS[1])
            local current = 0
            if raw then current = tonumber(raw) end
            if not current or current < floor then
                redis.call('SET', KEYS[1], ARGV[1])
            end
            local next = redis.call('INCR', KEYS[1])
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            return next
        "#;
        #[cfg(test)]
        if self.fail_next_seq > 0 {
            self.fail_next_seq -= 1;
            return Err(BusError::Redis(redis::RedisError::from(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "injected: the bus did not answer",
            ))));
        }
        let key = format!("{PREFIX}:doc:{}:seq", encode_key(room));
        // The TTL is refreshed on every edit, so an active document never loses its counter
        // and an abandoned one eventually stops costing memory.
        // Built once: the hottest bus call should not hash its script on every edit.
        static NEXT_SEQ: std::sync::LazyLock<redis::Script> = std::sync::LazyLock::new(|| redis::Script::new(SCRIPT));
        match NEXT_SEQ.key(&key).arg(floor).arg(SEQ_TTL.as_secs() as i64).invoke_async(&mut self.conn).await {
            Ok(next) => Ok(next),
            Err(e) if is_readonly(&e) => Err(BusError::ReadOnlyReplica),
            Err(e) => Err(e.into()),
        }
    }

    /// Raise a document's sequence counter to at least `floor`, without handing out a number.
    ///
    /// For a rejoin that has applied more than the counter holds ([`MAX_SEQ_FLOOR`] bounds
    /// what is honoured): the counter was lost or went backwards, and the next op must still be
    /// numbered past what that replica applied. A counter already at or above `floor` is left
    /// alone. Returns the counter after the raise.
    pub async fn raise_seq(&mut self, room: &str, floor: u64) -> Result<u64, BusError> {
        const SCRIPT: &str = r#"
            local floor = tonumber(ARGV[1])
            local raw = redis.call('GET', KEYS[1])
            local current = 0
            if raw then current = tonumber(raw) end
            if not current or current < floor then
                redis.call('SET', KEYS[1], ARGV[1])
                redis.call('EXPIRE', KEYS[1], ARGV[2])
                return floor
            end
            return current
        "#;
        let key = format!("{PREFIX}:doc:{}:seq", encode_key(room));
        let floor = floor.min(MAX_SEQ_FLOOR);
        match redis::Script::new(SCRIPT).key(&key).arg(floor).arg(SEQ_TTL.as_secs() as i64).invoke_async::<Option<u64>>(&mut self.conn).await {
            Ok(v) => Ok(v.unwrap_or(0)),
            Err(e) if is_readonly(&e) => Err(BusError::ReadOnlyReplica),
            Err(e) => Err(e.into()),
        }
    }

    /// The retained sequence high-water mark for a document, zero when absent.
    ///
    /// Read-only, so a peer joining an empty in-memory room can learn where the
    /// document already was without advancing the counter. Used to seed the
    /// room's sequence on join.
    ///
    /// A counter that is not a number reads as absent (and is logged): refusing the read would
    /// refuse every join for good, and [`Bus::next_seq`] and [`Bus::raise_seq`] reset such a
    /// counter to their floor.
    pub async fn current_seq(&mut self, room: &str) -> Result<u64, BusError> {
        let key = format!("{PREFIX}:doc:{}:seq", encode_key(room));
        let v: Option<String> = match self.conn.get(&key).await {
            Ok(v) => v,
            Err(e) if is_readonly(&e) => return Err(BusError::ReadOnlyReplica),
            Err(e) => return Err(e.into()),
        };
        Ok(match v.map(|raw| raw.parse::<u64>().map_err(|_| raw)) {
            None => 0,
            Some(Ok(seq)) => seq,
            Some(Err(raw)) => {
                tracing::error!(room, bytes = raw.len(), "the sequence counter is not a number; treating it as lost");
                0
            }
        })
    }

    /// The next CRDT id space for a document, above `kept`.
    ///
    /// One `INCR` per connection, from the same instance that hands out sequences, so two
    /// relays cannot grant the same space and a restart cannot hand out one twice. A client
    /// deriving its own space from its peer id could collide with another peer's, and the two
    /// would then mint the same character ids and drop each other's text.
    ///
    /// The counter can go backwards: a flush, a crash inside the `everysec` AOF window, a
    /// failover to a replica that lagged. The last two lose exactly the latest grants, whose
    /// holders are still connected and keep their space across rejoins. So in the same script
    /// as the `INCR`, the counter is first raised to a floor, the higher of
    ///
    /// * `kept`, the space a rejoining replica already mints in (0 for none), and
    /// * the highest space this relay process granted in the room, or saw kept there.
    ///
    /// While this process runs, no grant can then equal a space a live replica holds. **Not
    /// covered:** the relay process restarts *and* Valkey loses recent grants. A newcomer may
    /// then be granted a space whose holder has not rejoined yet; the holder's rejoin raises the
    /// floor from then on, but the two already share the space. That case depends on how Valkey
    /// persists (an owner decision) and is not fixed in M1.
    ///
    /// A `kept` past [`MAX_SPACE_FLOOR`] is not honoured, so a client cannot run the counter
    /// towards its wrap. A counter that is not a number is left alone, and the `INCR` refuses it
    /// as before.
    ///
    /// Space 0 is never granted. That space belongs to the text already in the file, which is
    /// how two peers loading the same string reach the same identities.
    pub async fn next_space(&mut self, room: &str, kept: u32) -> Result<u32, BusError> {
        const SCRIPT: &str = r#"
            local floor = tonumber(ARGV[1])
            local raw = redis.call('GET', KEYS[1])
            local current = 0
            if raw then current = tonumber(raw) end
            if current and current < floor then
                redis.call('SET', KEYS[1], floor)
            end
            local granted = redis.call('INCR', KEYS[1])
            redis.call('EXPIRE', KEYS[1], ARGV[2])
            return granted
        "#;
        let key = format!("{PREFIX}:doc:{}:space", encode_key(room));
        let kept = if kept > MAX_SPACE_FLOOR { 0 } else { kept };
        let floor = kept.max(self.highest_space(room));
        let next: u64 = match redis::Script::new(SCRIPT).key(&key).arg(floor).arg(SEQ_TTL.as_secs() as i64).invoke_async(&mut self.conn).await {
            Ok(v) => v,
            Err(e) if is_readonly(&e) => return Err(BusError::ReadOnlyReplica),
            Err(e) => return Err(e.into()),
        };
        // Wrapped rather than refused. The counter would have to pass four billion
        // connections to one document, and a wrapped space collides only with a peer from
        // that many connections ago.
        let granted = ((next - 1) % u64::from(u32::MAX)) as u32 + 1;
        self.note_space(room, granted.max(kept));
        Ok(granted)
    }

    /// The highest space this process granted in `room`, or saw kept there (0 for none).
    fn highest_space(&self, room: &str) -> u32 {
        self.spaces.lock().map_or(0, |spaces| spaces.get(room).map_or(0, |(space, _)| *space))
    }

    /// Remember `space` as granted or kept in `room`. Rooms untouched for longer than the
    /// counter lives ([`SEQ_TTL`]) are forgotten, as the counter is, so the map holds about one
    /// entry per room used within that time.
    fn note_space(&self, room: &str, space: u32) {
        let Ok(mut spaces) = self.spaces.lock() else { return };
        let now = Instant::now();
        if spaces.len() >= 4096 && !spaces.contains_key(room) {
            spaces.retain(|_, (_, at)| now.duration_since(*at) < SEQ_TTL);
        }
        let entry = spaces.entry(room.to_owned()).or_insert((0, now));
        *entry = (entry.0.max(space), now);
    }

    /// Set the room's space counter back to `to` (0 deletes it), as a lost write or a flush
    /// does, for a test of the floor.
    #[cfg(test)]
    pub async fn rewind_spaces(&mut self, room: &str, to: u32) {
        let key = format!("{PREFIX}:doc:{}:space", encode_key(room));
        if to == 0 {
            let _: Result<i64, _> = self.conn.del(&key).await;
        } else {
            let _: Result<(), _> = self.conn.set(&key, to).await;
        }
    }

    /// Overwrite the room's sequence counter with something that is not a number, as a stray
    /// write would, for a test that such a counter does not wedge the document.
    #[cfg(test)]
    pub async fn corrupt_seq(&mut self, room: &str) {
        let key = format!("{PREFIX}:doc:{}:seq", encode_key(room));
        let _: Result<(), _> = self.conn.set(&key, "not-a-number").await;
    }

    /// Set the room's sequence counter back to `to` (0 deletes it), as a lost write, a flush or
    /// a wiped volume does, for a test of the sequence floor.
    #[cfg(test)]
    pub async fn rewind_seq(&mut self, room: &str, to: u64) {
        let key = format!("{PREFIX}:doc:{}:seq", encode_key(room));
        if to == 0 {
            let _: Result<i64, _> = self.conn.del(&key).await;
        } else {
            let _: Result<(), _> = self.conn.set(&key, to).await;
        }
    }

    /// Forget what this process granted, as a relay restart does, for a test of the floor.
    #[cfg(test)]
    pub fn forget_granted_spaces(&mut self) {
        if let Ok(mut spaces) = self.spaces.lock() {
            spaces.clear();
        }
    }

    /// How long a lease lasts without a refresh.
    pub fn lock_ttl(&self) -> Duration {
        self.lock_ttl
    }

    /// Shorten (or lengthen) the lease, for a test that needs one to lapse.
    #[cfg(test)]
    pub fn set_lock_ttl(&mut self, ttl: Duration) {
        self.lock_ttl = ttl;
    }

    fn ttl_ms(&self) -> i64 {
        i64::try_from(self.lock_ttl.as_millis()).unwrap_or(i64::MAX)
    }

    /// Claim a scope, or report who holds it.
    ///
    /// One script, so the claim and the fencing token are one step: `SET NX PX` on the lease,
    /// or a refresh when this peer already holds it, then `INCR` of the scope's token counter.
    /// Atomic, which is what makes this safe with more than one relay. Re-granting to the
    /// current holder keeps it idempotent for a client retrying after a reconnect, but hands
    /// out a new token: a run under the old one must not pass for a run under the new one.
    ///
    /// The token counter outlives the lease ([`SEQ_TTL`]), so tokens only grow while a document
    /// is in use; a stale holder's token is never handed out again.
    pub async fn try_lock(&mut self, room: &str, scope: &str, peer: &str) -> Result<LockOutcome, BusError> {
        const SCRIPT: &str = r#"
            local claimed = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
            if not claimed then
                local holder = redis.call('GET', KEYS[1])
                if holder and holder ~= ARGV[1] then
                    return {0, holder}
                end
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            end
            local token = redis.call('INCR', KEYS[2])
            redis.call('EXPIRE', KEYS[2], ARGV[3])
            return {1, tostring(token)}
        "#;
        let (granted, value): (i64, String) = match redis::Script::new(SCRIPT)
            .key(self.lock_key(room, scope))
            .key(self.fence_key(room, scope))
            .arg(peer)
            .arg(self.ttl_ms())
            .arg(SEQ_TTL.as_secs() as i64)
            .invoke_async(&mut self.conn)
            .await
        {
            Ok(v) => v,
            Err(e) if is_readonly(&e) => return Err(BusError::ReadOnlyReplica),
            Err(e) => return Err(e.into()),
        };
        if granted == 1 {
            let token = value
                .parse()
                .map_err(|_| redis::RedisError::from(std::io::Error::new(std::io::ErrorKind::InvalidData, "the fencing token is not a number")))?;
            Ok(LockOutcome::Granted { token })
        } else {
            Ok(LockOutcome::Held { holder: value })
        }
    }

    /// Whether `peer` holds `scope` under fencing token `token` now, and if so extend the lease.
    ///
    /// One script: the holder and the token are compared and the lease extended in one step, so
    /// nobody can be granted the scope between the check and the edit it lets through (the
    /// lease is a full TTL from here). A lease that lapsed is not revived: `false`.
    pub async fn check_fence(&mut self, room: &str, scope: &str, peer: &str, token: u64) -> Result<bool, BusError> {
        const SCRIPT: &str = r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] and redis.call('GET', KEYS[2]) == ARGV[2] then
                redis.call('PEXPIRE', KEYS[1], ARGV[3])
                return 1
            end
            return 0
        "#;
        let held: i64 = match redis::Script::new(SCRIPT)
            .key(self.lock_key(room, scope))
            .key(self.fence_key(room, scope))
            .arg(peer)
            .arg(token.to_string())
            .arg(self.ttl_ms())
            .invoke_async(&mut self.conn)
            .await
        {
            Ok(v) => v,
            Err(e) if is_readonly(&e) => return Err(BusError::ReadOnlyReplica),
            Err(e) => return Err(e.into()),
        };
        Ok(held == 1)
    }

    /// Who currently holds a scope, read directly rather than through [`Bus::holders`]' `SCAN`.
    ///
    /// For confirming a belief about one scope (the submit preflight's local mirror) against
    /// the authority before refusing an edit on it. A `SCAN` across every held scope would be
    /// wasteful just to check one, and `holders` exists for the join payload, not a per-submit
    /// recheck.
    pub async fn current_holder(&mut self, room: &str, scope: &str) -> Result<Option<String>, BusError> {
        let key = self.lock_key(room, scope);
        match self.conn.get(&key).await {
            Ok(v) => Ok(v),
            Err(e) if is_readonly(&e) => Err(BusError::ReadOnlyReplica),
            Err(e) => Err(e.into()),
        }
    }

    /// Extend a lock this peer holds. Returns whether it still held it.
    pub async fn refresh_lock(&mut self, room: &str, scope: &str, peer: &str) -> Result<bool, BusError> {
        let key = self.lock_key(room, scope);
        let holder: Option<String> = self.conn.get(&key).await?;
        if holder.as_deref() != Some(peer) {
            return Ok(false);
        }
        let ttl = self.ttl_ms();
        let _: Result<bool, _> = self.conn.pexpire(&key, ttl).await;
        Ok(true)
    }

    /// Release a scope, but only when this peer holds it.
    ///
    /// Compare and delete in one script. A plain `GET` then `DEL` could delete
    /// a lock another peer acquired in the gap, which is the classic way to
    /// hand the same region to two editors at once.
    pub async fn release(&mut self, room: &str, scope: &str, peer: &str) -> Result<bool, BusError> {
        const SCRIPT: &str = r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        "#;
        let key = self.lock_key(room, scope);
        let deleted: i64 = match redis::Script::new(SCRIPT).key(&key).arg(peer).invoke_async(&mut self.conn).await {
            Ok(v) => v,
            Err(e) if is_readonly(&e) => return Err(BusError::ReadOnlyReplica),
            Err(e) => return Err(e.into()),
        };
        Ok(deleted > 0)
    }

    /// Release a scope only when this peer holds it under fencing token `token`.
    ///
    /// A stale unlock (from a run whose lease lapsed and was granted again, to somebody else or
    /// to this peer under a newer token) must not release the newer holder's lock, which a
    /// compare on the peer alone would.
    pub async fn release_fenced(&mut self, room: &str, scope: &str, peer: &str, token: u64) -> Result<ReleaseOutcome, BusError> {
        const SCRIPT: &str = r#"
            local holder = redis.call('GET', KEYS[1])
            if not holder then
                return -1
            end
            if holder == ARGV[1] and redis.call('GET', KEYS[2]) == ARGV[2] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
        "#;
        let outcome: i64 = match redis::Script::new(SCRIPT)
            .key(self.lock_key(room, scope))
            .key(self.fence_key(room, scope))
            .arg(peer)
            .arg(token.to_string())
            .invoke_async(&mut self.conn)
            .await
        {
            Ok(v) => v,
            Err(e) if is_readonly(&e) => return Err(BusError::ReadOnlyReplica),
            Err(e) => return Err(e.into()),
        };
        Ok(match outcome {
            -1 => ReleaseOutcome::Absent,
            0 => ReleaseOutcome::NotYours,
            _ => ReleaseOutcome::Released,
        })
    }

    /// Every held scope for a document, for a join payload.
    ///
    /// `SCAN` rather than `KEYS`, because this is a shared instance and `KEYS`
    /// blocks the server for every other app on it.
    pub async fn holders(&mut self, room: &str) -> Result<Vec<(String, String)>, BusError> {
        let prefix = format!("{PREFIX}:doc:{}:lock:", encode_key(room));
        let pattern = format!("{prefix}*");
        let mut out = Vec::new();
        let mut cursor: u64 = 0;

        loop {
            let (next, keys): (u64, Vec<String>) =
                redis::cmd("SCAN").arg(cursor).arg("MATCH").arg(&pattern).arg("COUNT").arg(200i64).query_async(&mut self.conn).await?;

            for key in keys {
                // Strip the prefix rather than splitting on the separator. An encoded scope
                // can no longer contain one, and trusting a split cost a mis-reported holder
                // when it could.
                if let Some(encoded) = key.strip_prefix(&prefix)
                    && let Ok(Some(holder)) = self.conn.get::<_, Option<String>>(&key).await
                {
                    out.push((decode_key(encoded), holder));
                }
            }

            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        out.sort();
        Ok(out)
    }

    fn lock_key(&self, room: &str, scope: &str) -> String {
        format!("{PREFIX}:doc:{}:lock:{}", encode_key(room), encode_key(scope))
    }

    /// The scope's fencing token counter. Outside the `lock:` family, so [`Bus::holders`]'
    /// scan never reads it as a held scope.
    fn fence_key(&self, room: &str, scope: &str) -> String {
        format!("{PREFIX}:doc:{}:fence:{}", encode_key(room), encode_key(scope))
    }
}

/// Make a room or a scope safe to put in a key.
///
/// A scope is whatever the owning app calls a region, and a room is whatever the provider calls
/// a document. Neither is this module's to constrain. Dropped into a key raw, a scope containing
/// the separator came back out of [`Bus::holders`] as a different scope, and the join payload
/// then marked the wrong region busy. A room containing one could reach into another document's
/// keys altogether.
///
/// Everything outside the unreserved set is escaped, so the result holds no `:` and no glob
/// character and the round trip is exact.
fn encode_key(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Read back what [`encode_key`] wrote.
fn decode_key(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether an error is Valkey refusing a write on a replica.
fn is_readonly(err: &redis::RedisError) -> bool {
    err.code() == Some("READONLY") || err.to_string().contains("READONLY")
}
