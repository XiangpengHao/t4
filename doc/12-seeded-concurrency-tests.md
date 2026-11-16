# 12 — Concurrent tests on fixed schedules

Run both suites, without a test-name filter:

```sh
cargo test --locked
cargo test --locked --features shuttle
```

The ordinary suite keeps pure state/format tests and the OS adapter checks
listed below. The shuttle build runs the concurrent cache, network, policy,
I/O-worker and shared-mode tests. Some pure state tests run in both builds.
The `shuttle` feature substitutes primitives only in the unit-test binary;
normal library consumers still get the real implementations.

## Schedules and replay

`src/io/test_schedule/mod.rs` owns the corpus. Shuttle is pinned to exactly
0.9.1, with the lockfile checked in. Every scheduler is PCT, depth 3,
constructed with `PctScheduler::new_from_seed`, with seeds `0x57bf0283` and
`0x0dc82a31`: PCT finds a bug of a few ordering constraints with a known
probability per run, where random scheduling gives no bound. Each seed runs 16
iterations for the integration and state checks (`TestSchedule::check`), 500
for the focused queue, cache-store, memory-pool and CLOCK races, as their PCT
budget was before, and 16 for the two owner-shutdown regressions. A test that
starts no thread of its own runs once, inside shuttle's runtime
(`TestSchedule::once`): PCT refuses to repeat a run with no concurrency, and
repeating it would explore nothing. Longer stress loops now repeat small cases
under these schedules instead of relying on OS thread timing.

A failure prints the seed, iteration budget, and shuttle's encoded schedule.
Rerun just that test to repeat the corpus. To replay exactly the printed
schedule, join its lines and run on the failing revision:

```sh
T4_SHUTTLE_REPLAY='<encoded schedule>' cargo test --locked --features shuttle \
  --lib 'full::test::path' -- --exact --nocapture
```

Replay uses the same 512 KiB task stacks as the corpus. No entropy-seeded
scheduler, random scheduler or convenience checker is used. Shared call/read tables use ordered
maps so destruction and wake order do not depend on hash-map entropy.

## Shared code and controlled time

The frame parser, ordered sender, handshake parser/admission checks, owner
listener and session loop, client connection/read/fetch code, region span
allocator and ownership rules all run under shuttle. Only the boundaries differ:

- `frame_socket/simulated.rs` provides bounded byte streams (64 KiB), closure,
  backpressure, descriptor handover and an explicitly owned listener registry.
  The test's builders receive that registry; there is no global registry.
- `owner/simulated_waker.rs` models the socket bell with shuttle park tokens.
  The real bell's tests also run against this implementation.
- Region backing is one anonymous private allocation held by both simulated
  peers, rather than two kernel mappings. Spans, bounds checks, staged fetches,
  output buffers, and lending until `Ended` use the production code. The OS
  tests retain seals, descriptor and mapping checks.
- The existing simulated I/O driver schedules submission and completion. Its
  optional file backing performs synchronous file operations at completion;
  retaining the file also retains its lock. This preserves snapshots, restart
  bytes and allocation checks without starting a kernel I/O worker. The
  focused storage races still use entirely in-memory `SimDisk`s. Nonblocking
  file-lock and metadata syscalls are serial side effects under shuttle, not
  modeled kernel operations; process tests cover their OS semantics.

`io::time::Clock` owns an epoch. Production clones share the same monotonic
clock; shuttle clones share manual time. The builder passes it to the network,
its tiers, and shared-mode hand-off. I/O reports measure this clock, including
GET rate/late-race inputs. The existing asynchronous `with_sleep` hook still
controls when a late racer fires; its tests release it at explicit poll/state
boundaries. A dedicated network test advances time at I/O events and checks
exact reported durations.

The modeled transport shares its clock with the builders. Its read deadlines,
including `Hello::WAIT`, expire only when the test advances that clock. The
builder's retry wait advances manual time and yields; `OWNER_WAIT` and hand-off
`RETRY` therefore use the same owned time source. Tests cover the exact owner
wait budget and a hello held until the final nanosecond of its deadline.
Readiness checks observe registered claim waiters, calls, spans, or channel
acknowledgements. A protocol round trip replaces the old “quiet for 100 ms”
check. There are no test sleeps, receive timeouts or elapsed-time assertions.

## Real-OS coverage retained

These are the concurrent exceptions to the shuttle-only rule. They assert
protocol/data results and synchronize on operations or acknowledgements,
without elapsed-time assertions or delays:

| Tests | Why the real OS is required |
|---|---|
| `t4_caching::tests::processes::{an_owner_killed_mid_read_leaves_its_client_the_cache, an_owner_shut_down_then_killed_leaves_its_disk_tier, a_client_finds_an_owner_of_another_tmpdir, a_client_killed_holding_a_claim_hands_it_to_its_waiter, eight_processes_starting_at_once_fetch_each_block_once}` and their `a_child_process_plays_its_role` entry point | `exec`, `SIGKILL`, descriptor inheritance, kernel lock release, cross-process discovery and simultaneous process startup cannot be modeled by shuttle. Marker acknowledgements and registered claim waiters replace timing assumptions. Children wait on stdin EOF so a parent that exits leaves no waiting child. |
| `shared::tests::{a_client_connects_only_to_an_owner_of_its_user, an_owner_whose_address_is_held_still_owns_its_cache}` | Real peer credentials, abstract socket ownership on Linux and socket-path replacement on macOS. The cache still uses its actual owner/session threads in these adapter checks. |
| `shared::tests::{a_hellos_descriptors_close_on_exec, the_owner_maps_a_clients_region_that_cannot_shrink}` | Real SCM_RIGHTS, CLOEXEC, memfd seals or POSIX shared-memory resizing rules; the latter uses a real owner session. |
| `shared::tests::{frames_cross_a_socket_whole, a_batch_longer_than_a_write_takes_goes_whole}` | Unix stream EOF/partial frames and batches beyond `IOV_MAX` exercise real sendmsg/read behavior. A sending thread and receiver exchange a prescribed sequence; joins and bytes acknowledge completion. Both cases also run under shuttle. |
| `shared::owner::tests::{a_session_waits_for_its_socket_or_a_wake, a_frame_queued_wakes_the_session_from_another_thread_only}` | The real socket bell/poll adapter. Explicit wakes and writes replace sleeps. Both also run against the modeled bell under shuttle. |
| `io::io_worker::test::{basic_io_worker_test, one_worker_reads_and_writes_several_files}` | Small real-driver smoke checks: io_uring on Linux, the blocking driver elsewhere. The caller waits for each acknowledged I/O; the concurrent pool and drain cases run only under shuttle. Both smoke checks also run under shuttle. |

`shared/protocol/region/tests.rs` remains outside shuttle too: its four tests
(five on Linux) are single-threaded checks of actual mappings, span allocation,
and refusal of shrinkable/unsealed memory. Pure frame-format tests and the
synchronous snapshot/file-allocation checks also stay ordinary unit tests.
No original concurrent scenario was removed: the remaining shared/client,
restart, network, claim and policy scenarios moved to the modeled suite.

The process tests retain real OS scheduling; shuttle cannot make a SIGKILL or
multi-process startup schedule portable. They remain as OS integration coverage
rather than pretending those operations were explored by a model scheduler.
Shuttle also treats atomics as sequentially consistent: the corpus does not
prove weak-memory behavior or exhaust all possible schedules.

## The owner-shutdown regression

`a_clients_read_waiting_on_another_when_the_owner_goes_reads_from_its_store`
now uses modeled transport and waits for an actual registered claim waiter.
The focused
`a_stopping_owner_never_hands_a_departing_sessions_claim_to_another_client`
uses the production `ClientSession::serve_reads` loop. It pauses shutdown
between sessions: first register the waiter, set `stopping`, end the claiming
session, and let the waiting session run while its socket is still open.
This ordering is legal for the real listener's successive shutdowns.

Removing only the stopping check from `serve_reads` failed with seed
`0x57bf0283`: the waiting session sent `Retry { next: Plan(Remote) }` instead of
closing. Replaying the emitted schedule failed the same assertion. Restoring
the check passed the full suite. The focused ordering is needed: the broader
integration case did not expose the reverted fix, under random scheduling nor
under PCT at up to 256 runs a seed. The race needs a preemption in a narrow
window between two sessions' shutdowns, out of thousands of steps.

## CI and local validation

An independent `shuttle` matrix job runs the entire feature build on Linux and
macOS, regardless of the lint/proof jobs' result, with a five-minute step limit.
Both platforms are useful: their production socket/region adapters differ,
while the modeled protocol is required to work on each host. Ordinary test
jobs retain real-OS coverage on both platforms, also with five-minute test-step
limits so a missing OS acknowledgement cannot hang CI indefinitely.

Locally on macOS arm64, the complete shuttle run passed 213 unit tests plus
the compile-only doctest in 55 seconds, excluding compilation, on a machine
busy with other builds. Formatting,
Clippy with all targets/features and warnings denied, and the ordinary suite
(91 unit tests plus the doctest) passed. No `verus!` body changed, so no proof
check was required. Miri and benchmarks were not run.
