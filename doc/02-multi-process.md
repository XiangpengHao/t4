# 02 — Multiple processes

Status: design, not built (2026-10-19).

A store is used in one of two modes:

- **In-process**: the process that mounts the store owns it, as today. No
  other process can use it.
- **Shared**: a server process owns the store, and any number of client
  processes use it over a Unix socket. Each store has its own server. A
  client starts it if none is running; it exits when idle.

Either way, exactly one process owns the store file, and only that process
runs the engine: the index, the allocator and the WAL. No engine state is
shared between processes.

```text
 in-process                  shared

 +------------------+        +----------+   socket    +-------------------+
 | app              |        | client A |------------>| t4-server         |
 |  Store -> engine |        +----------+   memfd     |  engine           |
 +--------+---------+        +----------+------------>|  (T4Store, as     |
          |                  | client B |             |   in-process)     |
       data.t4               +----------+             +---------+---------+
                                                                |
                                                             data.t4
```

## Why a server

Two other designs were considered:

- **Every process mounts the file**, keeping its own index caught up from
  the WAL, as SQLite's readers do. Each process then holds a full index and
  pays a full replay at mount, appends take a cross-process WAL lock, and
  freed space can be reused only once every process has moved past it
  (LSN pins in shared memory).
- **One bbtree in shared memory.** A process killed mid-split leaves a
  broken tree that the others go on reading, and a latch held by a dead
  process blocks everyone: a latch word can't tell a dead process from a
  slow one. bbtree handles a failure mid-split by aborting the process,
  which is sound only when every reader dies with it.

A server keeps every engine invariant inside one process, where the shuttle
tests check them. Only a lock, a socket and value bytes cross processes.

## The mount lock

Every mount takes `flock(LOCK_EX | LOCK_NB)` on the store file before
replay and holds it until the store closes. In-process mounts and the
server take the same lock.

- **Lost: `Error::Busy`.** An in-process mount fails while a server or
  another mount owns the store; a server fails while an in-process mount
  does.
- **No stale locks.** The kernel drops the lock when its holder exits,
  killed or not.
- **`flock`, not `fcntl` record locks.** Record locks don't conflict within
  one process, so two mounts in one process would both get one, and closing
  any descriptor of the file drops them.
- **Advisory.** A tool that writes the file without t4 isn't stopped.

The lock ships first, on its own. Today a second process that mounts the
same file corrupts it silently: both rewrite the WAL tail page from their
own copy (losing acknowledged puts), fork the WAL chain, and allocate the
same space twice (a value can land on a WAL page, and the store no longer
mounts).

## API

```rust
// In-process: this process owns the store.
let store = t4::mount("data.t4").await?;

// Shared: use the store's server, starting one if none is running.
let options = MountOptions { mode: MountMode::Shared, ..MountOptions::default() };
let store = t4::mount_with_options("data.t4", options).await?;
```

- **One `Store` type**, with the same methods in both modes. Inside, it
  holds either the engine or a server connection; clones share it.
- **`len` and `is_empty` return `Result`** in both modes: in shared mode
  they make a round trip, which can fail.
- **New errors**: `Busy` (the store has another owner), `ServerGone` (the
  server died with the call in flight), `Server(String)` (no server could
  be started or reached: the message says why).
- **Server options are set by the client that starts the server.** A later
  client whose `direct_io` or `dsync` differ gets `InvalidArgument`: they
  decide durability, which a client must not lose silently. `queue_depth`
  and `idle_timeout` are the server's alone.
- **Linux and macOS.** macOS uses fallbacks where Linux's mechanisms don't
  exist (below): it has to build and work, not perform (00-meta).

What a caller can rely on is the same in both modes:

- A put or remove is acknowledged once its WAL write completes.
- A request sent after an acknowledgment sees its effect, whichever process
  sent either: there is one index, in the owner.
- Calls in flight at once may complete in any order.

## Finding the server

The server listens on an abstract Unix socket named from the store file's
`stat`: `\0t4/<dev>/<ino>`, in hex.

- **One name per file**, whatever path reaches it: relative, through a
  symlink, or a hard link. The client opens the store first, creating it if
  needed, and takes the name from its descriptor.
- **No socket file**, so nothing to clean up, and no 108-byte path limit.
- **The name is a lock among servers.** A second bind fails with
  `EADDRINUSE`, and the name frees itself when its socket closes, however
  the server exits.
- **No file permissions.** Abstract names have none; the handshake checks
  access instead (Connection, below).
- **Per network namespace.** A client in another container doesn't see the
  server. The server it starts then fails on the mount lock: `Busy`, not
  corruption.

**macOS has no abstract sockets.** Its server listens on a socket file,
`$TMPDIR/t4-<dev>-<ino>.sock` (`$TMPDIR` is per-user there, and short
enough for the 104-byte path limit). Servers exclude each other with a
`flock` on `$TMPDIR/t4-<dev>-<ino>.lock` instead of the name. A socket file
outlives a killed server, so a server unlinks a stale one before it binds,
which is safe once it holds that lock.

A client mounts in shared mode like this:

```text
until the deadline:
  connect(name)
    ok      → handshake → done
              reset during the handshake (a server shutting down) → loop
    refused → spawn t4-server, read its status line:
                ready, already-running → loop
                busy                   → Error::Busy
                error: <message>       → Error::Server
```

## Starting and stopping the server

The server starts in these steps:

1. **Bind the name** (macOS: take the server lock). Taken: print
   `already-running`, exit.
2. **Open the store and take the mount lock**, waiting up to a few seconds:
   a server that is shutting down holds the lock until its last sync. Still
   held: an in-process mount owns the store; print `busy`, exit.
3. **Listen** (macOS: unlink any stale socket file, bind, then listen).
4. **Fork once**, before any thread exists. The parent exits, and the
   spawning client reaps it at once, so the server never lingers as the
   client's zombie. The child calls `setsid`, so the client's terminal,
   Ctrl-C and exit don't reach it. The socket and the lock pass to the child:
   a `flock` belongs to the open file, which the child still holds.
5. **Print `ready`** on stdout, the spawning client's pipe, then point
   stdout at `/dev/null`.
6. **Mount** (replay), then answer handshakes. Clients that connect during
   replay wait in the handshake rather than time out and start more
   servers.

Spawning:

- **A binary of its own**, `t4-server`, built from this crate, so it speaks
  the library's protocol. Found at `$T4_SERVER`, else on `PATH`.
- **Never `fork()` without `exec` in the client.** The client has other
  threads (the I/O worker, the caller's runtime). A child gets only the
  calling thread, and any lock another thread held stays locked in it
  forever.
- **Never re-execute the app's own binary.** An app whose `main` doesn't
  hand that run to t4 would start a second copy of itself.

`idle_timeout` counts from the last client's disconnect: a connected client
keeps the server up however quiet it is. It defaults to 1 s, io_uring's
default `sq_thread_idle`, after which an `SQPOLL` thread stops polling and
sleeps. A sleeping `SQPOLL` thread costs a wake-up to restart; an exited
server costs a start and a full replay. So a process that mounts the store,
exits, and comes back more than a second later pays both again. Per-store
servers stay cheap on the other side: a test suite with 500 temporary
stores has each server gone a second after its test.

After `idle_timeout` with no connection, the server stops:

1. **Close the listener** (macOS: unlink the socket file and release the
   server lock). The name frees. A new client starts a new server, which
   waits on the mount lock (step 2 above).
2. **Sync and close the store.** The lock frees. Exit.

A client that connected just before step 1 sees its handshake reset, and
loops.

## Connection

The handshake, client to server:

- **The protocol version.**
- **The client's own read-write descriptor of the store**, by `SCM_RIGHTS`.
  The server checks that it names the same file (`fstat`: dev and ino) and
  is open read-write (`F_GETFL`). The kernel checked the client's
  permissions when it opened the file, so whoever can open the store
  read-write may use its server. This stands in for the file permissions
  abstract names lack.
- **The client's `direct_io` and `dsync`.**

The server replies ok, or with what didn't match.

After the handshake, frames go over `SOCK_STREAM`: length, request id,
operation, payload. The client may send many requests without waiting;
replies carry the request id and come back in any order.

| request     | payload                         | reply          |
|-------------|---------------------------------|----------------|
| `put`       | key, value                      | ok             |
| `get`       | key                             | value          |
| `get_range` | key, start, length              | bytes          |
| `remove`    | key                             | removed: bool  |
| `sync`      | —                               | ok             |
| `len`       | —                               | entry count    |
| `snapshot`  | target file, by `SCM_RIGHTS`    | ok             |

Any reply can be an error instead.

**Snapshot targets are descriptors, not paths.** The client creates the
target (`O_CREAT | O_EXCL`) and sends it. Given a path, the server would
resolve it against its own working directory, and create it with the
permissions of whichever client started the server.

**The client mirrors `IoWorker`.** One thread per connection owns the
socket. Callers hand it requests, and it completes their futures by request
id as replies arrive, so the API stays runtime-agnostic.

## Value bytes

**Stage 1: in the frames.** Values are copied through the socket, into the
kernel and out again, on top of the engine's own copy into an `AlignedBuf`.

**Stage 2: shared memory**, on Linux. No copies:

- **One region per connection.** The client creates a memfd
  (`MFD_ALLOW_SEALING`), sizes it, seals it with `F_SEAL_SHRINK`, and sends
  it in the handshake; both sides map it. The server refuses an unsealed
  memfd: a client that shrank it would crash the server with `SIGBUS` on
  its next access.
- **Frames carry `{offset, len}`** into the region instead of bytes.
  Offsets are page-aligned, so the server's O_DIRECT I/O goes straight
  between disk and the region: a put writes from it, a get reads into it.
- **The client allocates for both directions.** A get doesn't know the
  value's length ahead: it names a buffer, and a value that doesn't fit
  comes back as `too-small(len)`, unread. The client asks again with room:
  the lookup repeats, the disk read happens once.
- **A value larger than the region** travels in a memfd of its own, sent
  with its request.
- **The server zeroes a put's padding** in the region before the write, so
  the file holds zeros there whatever the client left.
- **The server reads a request once.** It copies the fields out of shared
  memory, then checks the copy, and never reads them again (a client that
  changes them between the check and the use is the "double fetch" bug). A
  client that changes a value while the server writes it gets that value
  stored garbled: its own data, nothing else.
- **The server keeps its mapping** until every I/O on it completes, even
  after the client has gone.

The engine gains a put from, and a get into, a caller's page-aligned
buffer, in place of its copy into an `AlignedBuf`. In-process mode gets the
same calls.

**macOS stays on stage 1.** It has neither memfd nor seals, and values in
frames work there unchanged.

## Failures

| what dies                 | what happens                                       |
|---------------------------|----------------------------------------------------|
| a client                  | The server sees EOF and drops the connection. Requests in flight finish (a put still commits); their replies go nowhere. |
| the server                | Calls in flight fail with `ServerGone`. The next call reconnects, starting a server, which replays the WAL. |
| the server, starting up   | Its status pipe closes without `ready`: `Error::Server`. |

- **Acknowledged writes are durable**: they were acknowledged after their
  WAL write completed.
- **Unacknowledged writes may or may not have happened.** Retrying is safe:
  a put writes the same value again, a remove still removes (its `bool` may
  differ).
- **Nothing depends on a clean exit.** The store file is as crash-safe as
  in-process, and everything else lives in the one server.

## Cost

A handoff is one thread making another run. To a sleeping thread, it costs
a syscall and the scheduler's wake-up: a few µs, more on busy CPUs.

| request                                   | in-process                                | shared                    |
|-------------------------------------------|-------------------------------------------|---------------------------|
| index only: missing key, `len`, empty value | no handoff, ~100–300 ns                 | + a round trip            |
| disk: get, put, remove                    | caller → I/O worker → caller              | the same, + a round trip  |

- **One disk I/O per request**, in both modes.
- **Later, only if measured:** the server's I/O loop reads client sockets
  itself (io_uring `recv`), taking out its handoff to the I/O worker; and
  the server polls a while before it sleeps, as io_uring's `SQPOLL` thread
  does.

## Testing

- **Engine tests stay as they are**: the server runs the same engine.
- **Protocol tests in one process.** Server and client run over a
  `socketpair`, without spawning; the client's request table runs under
  shuttle, as `IoWorker`'s does.
- **Process tests, real processes**, in CI on Linux and macOS:
  - two clients racing to start one server;
  - `kill -9` of the server mid-put: acknowledged puts survive, and the
    next call reconnects;
  - an in-process mount against a running server, and the reverse: `Busy`;
  - an idle shutdown racing a new client.

## Build order

1. **The mount lock** and `Error::Busy`, in-process mode only.
2. **`t4-server`** and the socket protocol, on Linux and macOS, values in
   the frames. `len` and `is_empty` return `Result`.
3. **Values in shared memory**, on Linux, with the engine's put-from and
   get-into calls.
4. **Polling, and sockets on the server's I/O loop**, only if measurements
   call for them.
