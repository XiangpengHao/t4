# 02 — Multiple processes

Status: build steps 1 to 3 built (2026-10-19): the mount lock, `t4-server`,
and values in shared memory on Linux (`src/shared/`, `src/bin/t4-server.rs`,
`tests/shared_mode.rs`). Step 4 not built.

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
replay (`File::try_lock`, `T4Store::lock_file`) and holds it until the store
closes. In-process mounts and the server take the same lock.

- **Lost: `Error::Busy`.** An in-process mount fails while a server or
  another mount owns the store; a server fails while an in-process mount
  does.
- **No stale locks.** The kernel drops the lock when its holder exits,
  killed or not.
- **`flock`, not `fcntl` record locks.** Record locks don't conflict within
  one process, so two mounts in one process would both get one, and closing
  any descriptor of the file drops them.
- **Advisory.** A tool that writes the file without t4 isn't stopped.
- **Free once the drop returns.** The lock lives on the open file the I/O
  worker owns, and dropping a store joins the worker thread, which closes
  it. A remount right after a drop doesn't see `Busy`.

Without the lock, a second process that mounted the same file corrupted it
silently: both rewrote the WAL tail page from their own copy (losing
acknowledged puts), forked the WAL chain, and allocated the same space
twice (a value could land on a WAL page, and the store no longer mounted).

## API

```rust
// In-process: this process owns the store.
let store = t4::mount("data.t4").await?;

// Shared: use the store's server, starting one if none is running.
let options = MountOptions {
    mode: MountMode::Shared(ServerOptions::default()),
    ..MountOptions::default()
};
let store = t4::mount_with_options("data.t4", options).await?;
```

- **One `Store` type**, with the same methods in both modes. Inside, it
  holds either the engine or a server connection; clones share it.
- **`len` and `is_empty` return `Result`** in both modes: in shared mode
  they make a round trip, which can fail.
- **`ServerOptions`**: `idle_timeout` (1 s by default, below) and
  `program`, the `t4-server` binary (`None`: `$T4_SERVER`, else `t4-server`
  on `PATH`). `MountOptions` holds them, so it is `Clone`, not `Copy`.
- **New errors**: `Busy` (the store has another owner), `ServerGone` (the
  server died with the call in flight), `Server(String)` (no server could
  be started or reached, or it failed in a way no other variant carries:
  the message says why).
- **Server options are set by the client that starts the server.** A later
  client whose `direct_io` differs from the running server's gets
  `InvalidArgument`, rather than silently the server's. `queue_depth` and
  `idle_timeout` are the server's alone.
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

The deadline, 10 s, covers servers starting and stopping. A long replay
doesn't count against it: the client that started the server waits on its
status line, and the others in their handshake.

## Starting and stopping the server

The server starts in these steps (`Server::main`):

1. **Fork once**, first thing, while the process has one thread. The
   parent exits at once, and the spawning client reaps it, so the server
   never lingers as the client's zombie. The child calls `setsid`, so the
   client's terminal, Ctrl-C and exit don't reach it.
2. **Open the store.** Its identity names the address.
3. **Claim the address**: bind and listen (macOS: take the server lock,
   unlink any stale socket file, then bind and listen). Taken: print
   `already-running`, exit.
4. **Take the mount lock**, waiting up to 1 s: a server that is shutting
   down holds it until its store closes, a moment after it frees the
   address. Still held: an in-process mount owns the store; print `busy`,
   exit.
5. **Mount** (replay). A failure prints `error: <message>`, so the client
   learns why rather than timing out.
6. **Print `ready`** on stdout, the spawning client's pipe, then point
   stdout and stderr at `/dev/null`. Until then stderr is the client's, so a
   server that crashes starting up shows there.
7. **Serve.** Clients that connected during replay waited in the listen
   backlog, and get their handshakes answered now.

Spawning:

- **A binary of its own**, `t4-server`, built from this crate, so it speaks
  the library's protocol. Found at `ServerOptions::program`, else
  `$T4_SERVER`, else on `PATH`.
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
   waits on the mount lock (step 4 above).
2. **Close the store.** Dropping it joins the I/O worker, which closes the
   file: the lock frees. Exit.

A client that connected just before step 1 sees its handshake reset, and
loops.

## Connection

Every frame is a little-endian `u64` body length, then the body
(`src/shared/wire.rs`). The handshake, client to server:

- **A magic and the protocol version.**
- **The client's own read-write descriptor of the store**, by `SCM_RIGHTS`.
  The server checks that it names the same file (`fstat`: dev and ino) and
  is open read-write (`F_GETFL`). The kernel checked the client's
  permissions when it opened the file, so whoever can open the store
  read-write may use its server. This stands in for the file permissions
  abstract names lack.
- **The connection's shared region**, a memfd, by `SCM_RIGHTS` after the
  store, where the client has one (Value bytes, below).

The server accepts, telling the client its process id and its `direct_io`,
which the client checks against its own; or refuses, saying why.

After the handshake, frames go over `SOCK_STREAM`: request id, operation,
payload. The client may send many requests without waiting; replies carry
the request id and come back in any order.

| request     | payload                         | reply          |
|-------------|---------------------------------|----------------|
| `put`       | key, value or its buffer        | ok             |
| `get`       | key, a buffer to read into      | value, or where in the buffer, or too small |
| `get_range` | key, start, length, a buffer    | bytes, or where in the buffer |
| `remove`    | key                             | removed: bool  |
| `len`       | —                               | entry count    |
| `snapshot`  | target file, by `SCM_RIGHTS`    | ok             |

Any reply can be an error instead. `NotFound`, `RangeOutOfBounds`, `Busy`,
`LockPoisoned`, `Format` and input errors come through as they are; I/O
errors keep their OS error code; anything else arrives as `Error::Server`,
with its message.

**Snapshot targets are descriptors, not paths.** The client creates the
target (`O_CREAT | O_EXCL`) and sends it. Given a path, the server would
resolve it against its own working directory, and create it with the
permissions of whichever client started the server.

**The client mirrors `IoWorker`.** Each connection has a writer thread,
which sends the requests callers hand it, and a reader thread, which
completes their futures by request id as replies arrive (`TaskCompletion`),
so the API stays runtime-agnostic. Connecting, which blocks, runs on the
reader thread before it starts reading.

**The server runs requests on a thread pool.** Each connection has a
reader thread, which decodes requests and runs each as a task on the pool,
and a writer thread, which sends replies as they finish. A snapshot runs on
a thread of its own: it holds the store's write lock across its awaits,
which no pool task may.

## Value bytes

**On Linux, in shared memory** (`src/shared/memory.rs`). Direct I/O goes
straight between the disk and memory the client shares, with no copy on
the server:

- **One region per connection.** The client creates a 64 MiB memfd
  (`MFD_ALLOW_SEALING`), seals it at that size (`F_SEAL_SHRINK`,
  `F_SEAL_GROW`, `F_SEAL_SEAL`), and sends it in the handshake after the
  store's descriptor; both sides map it. Pages take memory only once
  touched. The server refuses a memfd not sealed against shrinking: a
  client that shrank it would crash the server with `SIGBUS` on its next
  access.
- **A buffer per request.** The client carves page-aligned buffers out of
  the region, first fit, and frames name them by `{offset, capacity}`
  instead of carrying bytes. A put writes from its buffer, a get reads into
  one.
- **A memfd of its own** for a buffer over 16 MiB, or one the region has no
  room for, sealed the same way and sent with its request; the server maps
  it for that request alone.
- **A get doesn't know the value's length ahead.** It lends a buffer the
  size the last get needed (64 KiB at first). A value that doesn't fit comes
  back as `too-small(needed)`, unread, and the client asks again with a
  buffer that size: the lookup repeats, the disk read happens once. A
  `get_range` knows its window, and lends exactly that.
- **The client holds a buffer until its reply.** The pending call keeps it,
  even if the caller stopped waiting, so no other request reuses memory the
  server may still be reading or writing.
- **The server zeroes a put's padding** before the write, so the file holds
  zeros past every value whatever the client left there.
- **The server reads a request's fields from its frame**, never from shared
  memory, so a client changing that memory can't change what the server
  checked; it can only garble its own value. Two requests whose buffers
  overlap garble only that client's values too: the server touches shared
  memory only through raw pointers, by direct I/O and zeroing.
- **The server keeps a mapping until every I/O on it completes**, even
  after the client has gone: the buffer it lends the I/O holds the mapping.

The engine reads and writes through the same calls in both modes. A put
takes a `PaddedValue`, a value already padded to whole pages in a buffer
direct I/O can write from: a copy into a heap buffer in-process, the
client's buffer on the server. A read covers a `ValueWindow`, the whole
pages around the bytes asked for, into a buffer of exactly that size: a
heap buffer in-process, the client's on the server. An `AlignedBuf`'s
memory is either its own or lent by a shared mapping it keeps alive.

**Elsewhere, in the frames.** Values are copied through the socket, into
the kernel and out again, on top of the engine's own copy into an
`AlignedBuf`. macOS has neither memfd nor seals.

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

- **Engine tests stay as they are**: the server runs the same engine. A
  second mount is `Busy` until the first drops.
- **Protocol tests in one process.** Frames round-trip through a socket
  pair, descriptors included; one claim on an address at a time. Shared
  buffers round-trip between two mappings of one memfd; large ones get a
  memfd of their own; an unsealed memfd is refused; the region's free list
  takes first fit and merges what comes back. Value windows and padding
  are checked against their arithmetic.
- **Process tests, real processes** (`tests/shared_mode.rs`), in CI on
  Linux and macOS:
  - two clients sharing one store;
  - an in-process mount against a running server, and the reverse: `Busy`;
  - eight clients racing to start one server;
  - an idle server exits and frees the store;
  - a mount cancelled while connecting doesn't keep the server up;
  - `kill -9` of the server: acknowledged puts survive, and the next call
    reconnects;
  - a client whose `direct_io` differs from the server's;
  - a snapshot through the server;
  - a missing `t4-server` binary;
  - values from empty to 20 MiB, put, read whole and by range: through the
    region, after a too-small first try, and in a memfd of their own;
  - eight threads' calls sharing one connection's region.
- **Not covered yet:** the client's request table under shuttle, and an
  idle shutdown racing a new client.

## Build order

1. **The mount lock** and `Error::Busy`, in-process mode only. Built.
2. **`t4-server`** and the socket protocol, on Linux and macOS, values in
   the frames. `len` and `is_empty` return `Result`. Built.
3. **Values in shared memory**, on Linux, with the engine's put-from and
   get-into calls. Built.
4. **Polling, and sockets on the server's I/O loop**, only if measurements
   call for them.
