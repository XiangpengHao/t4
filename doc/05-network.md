# 05 — The network

Status: measured, and t4's remote reads made cheaper (2026-10-23). A remote
block's bytes come over HTTPS from the object store; this is what they cost,
through object_store's own HTTP transport and through one whose TLS the
kernel decrypts (kTLS), straight from S3 and through t4 ("Measured"). t4
then stopped faulting its memory tier in 4 KiB at a time, and takes a
fetch's bytes as they come ("t4's remote reads"): through it, remote blocks
come at 9.6 to 9.9 GiB/s, within 7% of straight from S3.

`00-meta.md`'s first goal: saturate the NIC when blocks come from the
remote.

## How a fetched byte travels

A disk hit costs the CPU nothing a byte: the drive writes the output
(`04-multi-device.md`). A fetched byte costs it every time, however it
comes. The NIC writes packets into buffers the kernel posted in advance,
before anyone knows whose they are, so a received byte lands in the
kernel's memory first, never in the reader's; TCP links the packets on
the socket; the bytes are encrypted, so something reads and writes each
once to decrypt it.

Through object_store's default transport, reqwest over hyper and rustls
(ring), and t4, a byte of a fetched block is touched seven times:

```text
NIC --DMA--> kernel buffers                        (no CPU)
 1. recv(): the kernel copies the ciphertext into rustls's buffer
 2. rustls decrypts it in place
 3. rustls copies the plaintext into its queue
 4. rustls copies it from there into hyper's buffer
 5. object_store's bytes() joins hyper's pieces into one buffer
 6. t4 copies the read's part into the caller's output (fill)
 7. t4 copies the block into the cache (a memory chunk, or a buffer the
    disk write takes)
```

`KtlsConnector` (`bench/src/ktls_transport.rs`, over
`bench/src/http1_connection.rs`) does the handshake in rustls, hands the
session's keys to the kernel, and reads a body into one buffer sized from
`Content-Length`, which the kernel decrypts into: steps 1 to 5 become the
kernel's decryption, and t4's two copies remain. It plugs into a store's
builder (`with_http_connector`) under object_store's signing and retries.

What the cloud gives (`i7i.48xlarge`, us-east-1): a 100 Gbps ENA NIC, 16
receive queues, MTU 9001; no header split, so no zero-copy receive
(io_uring `zcrx`); no TLS offload. S3 speaks HTTP(S) over TCP only, so no
kernel bypass reaches it; a TCP flow is held to 5 Gbps outside a cluster
placement group.

## Measured

`fetch_ranges` (`bench/src/bin/fetch_ranges.rs`), 23 Oct 2026: 8 MiB
ranges of eight copies of ClickBench's `hits.parquet` in S3 (118 GB), in
a fixed shuffled order, 64 GiB a run, never one twice, from an
`i7i.48xlarge` in us-east-1, Spot. CPU is every core's busy time over the
run (`/proc/stat`), the kernel's work on packets wherever it ran
included; the box ran nothing else.

| Transport | In flight | GiB/s | Fetch p50 / p99 / max | CPU-s a GiB |
|---|---|---|---|---|
| object_store's, direct | 256 | 10.31 | 175 / 407 / 861 ms | 1.02 |
| object_store's, direct | 512 | 10.14 | 222 / 1,581 / 4,124 ms | 1.27 |
| object_store's, direct | 1,024 | 7.66 | 451 / 3,686 / 7,281 ms | 1.41 |
| kTLS, direct | 256 | 9.78 | 161 / 546 / 1,159 ms | 0.84 |
| kTLS, direct | 512 | 9.24 | 209 / 2,085 / 6,903 ms | 0.94 |
| kTLS, direct | 1,024 | 6.12 | 330 / 6,172 / 10,392 ms | 1.14 |
| object_store's, through t4 | 256 | 5.77 | 206 / 1,107 / 1,496 ms | 1.70 |
| kTLS, through t4 | 256 | 5.59 | 197 / 1,120 / 1,819 ms | 1.47 |

(Through t4: `--cache-policy memory`, a 32 GiB memory tier, every block
admitted to it, every block read from the remote. The rows through t4 are
from the profiled runs below, whose direct runs land within 5% of the
table's rates and 10% of its CPU.)

- **Requests in flight set the rate.** An 8 MiB GET takes about 170 ms
  whatever else is in flight: S3 gives a connection about 47 MiB/s. 64 in
  flight read 2.65 GiB/s; 256 read 10.3, 89% of the NIC's 11.6 GiB/s.
  More than 256 slows both transports, their tails reaching seconds;
  why (S3 throttling, retransmits) isn't measured yet.
- **The transport doesn't change the rate, only its cost.** kTLS takes
  20% less CPU straight from S3 (0.77 against 0.97 CPU-s a GiB in the
  profiled runs), its user-space time nearly gone (2.6 s against 30.2 s),
  its kernel time up (35.9 s against 19.3 s): the kernel decrypts.
- **t4 halved the rate and added 0.7 CPU-s a GiB** at 256 in flight,
  whatever the transport ("t4's remote reads" says why, and what fixed it).

Where the CPU goes, straight from S3 at 256 in flight (every CPU sampled
by `perf`; CPU-s a GiB):

| | object_store's | kTLS |
|---|---|---|
| TLS decryption | 0.07 (ring, user space) | 0.11 (kernel AES-GCM, VAES; record handling) |
| copies in user space | 0.15 | 0.00 |
| the kernel's copy to user space | 0.09 | 0.10 |
| TCP/IP, the ENA driver | 0.27 | 0.23 |
| page allocation and zeroing | 0.06 | 0.07 |
| scheduling, locks, wakeups | 0.07 | 0.09 |
| other kernel | 0.16 | 0.14 |
| other user space (HTTP, tokio, object_store) | 0.10 | 0.02 |
| **all** | **0.97** | **0.77** |

The copies were the cheap part: about 0.2 CPU-s a GiB went with them. Two
thirds of what is left is the kernel taking packets in, the same under
either transport. And kTLS on this kernel decrypts into its own buffer and
then copies, not straight into ours. The kernel zeroes every page it
allocates (`init_on_alloc`, Ubuntu's default: `mem auto-init: heap
alloc:on`), the NIC's receive buffers included.

## t4's remote reads

The rows through t4 above mixed three costs: the remote reads themselves,
eviction (memory's victims go to disk, so once the 32 GiB tier filled each
remote block also wrote a block to a 1 GiB disk tier, itself evicting), and
the pool's first touch. Apart, at 256 in flight, on the stock transport and
kTLS:

| Through t4 | GiB/s | page faults | CPU-s a GiB |
|---|---|---|---|
| no eviction (a 96 GiB memory tier, every chunk fresh) | 6.99 / 7.47 | 17.1 M / 16.9 M | 1.99 / 1.77 |
| demoting (8 GiB of memory over twelve SSDs of 16 GiB, warm) | 9.05 / 9.59 | 2.4 M / 2.2 M | 1.69 / 1.28 |

**The pool's first touch cost most.** 17 million faults is 64 GiB of 4 KiB
pages: every page of the memory tier faulted, and the kernel zeroed it, as
the cache first filled, when remote reads matter most. The pool now asks for
transparent huge pages (`MADV_HUGEPAGE`, its chunks from a 2 MiB boundary):
a first write faults in two chunks at once. Where the kernel gives none,
chunks take 4 KiB pages as before.

**Then the copies.** A quarter of the busy time through t4 was `memmove`:
`bytes()` joining the body, then t4 copying it into the output and again
into the cache, each pass reading 8 MiB back from memory. A fetch's bytes
now land as they come (`FetchedBlocks`): each piece of the answer, while
the CPU's cache holds it, into the output's pages the fetch covers
(lent, as a disk read's are) and into its block's chunk (lent, as a
promotion's is), taken as the block's first byte comes; the blocks are
cached once the last byte is in, and nothing is if the fetch ends short.
Two copies of a byte, from cache, where there were three from memory.

| Through t4 | before | huge pages | and streaming |
|---|---|---|---|
| stock, no eviction | 6.99 GiB/s, 1.99 CPU-s a GiB | 8.33, 1.76 | 9.57, 1.29 |
| stock, demoting | 9.05, 1.69 | 9.77, 1.63 | 9.91, 1.14 |
| kTLS, no eviction | 7.47, 1.77 | 9.87, 1.44 | 9.55, 1.54 |
| kTLS, demoting | 9.59, 1.28 | 9.40, 1.16 | 9.62, 1.25 |

The stock transport gains most, as hyper hands its body over in pieces
of 16 to 400 KiB, each copied while cached. `KtlsConnector` hands over
the whole 8 MiB body, read before t4 sees a byte, so t4's copies still
read it from memory: its runs moved within their noise (8% from run to
run). Through t4 it now costs more than the stock transport.

## Later

- **A transport that lands a fetched block in its chunk:** the kernel
  decrypting straight into the cache's memory, t4 copying only the bytes
  asked into the output. One copy of a byte, not two; an interface
  between t4 and its transport (the request's extensions carry the
  chunks).
- **`KtlsConnector` handing its body over in pieces**, for t4 to copy
  each while cached.
- **kTLS decrypting straight into the reader's buffer:** TLS 1.3 needs
  `TLS_RX_EXPECT_NO_PAD`, which the `ktls` crate doesn't set.
- **The kernel's receive path:** `init_on_alloc=0` (a hardening setting,
  the operator's call), receive ring sizes, busy polling.
- **Past 256 in flight:** what slows both transports.
- **Plain HTTP to S3 inside a VPC** removes decryption, the operator's call.

## Code map

| Path | What it is |
|---|---|
| `bench/src/bin/fetch_ranges.rs` | ranges fetched many at once, direct or through t4: rate, latency, CPU a GiB |
| `bench/src/ktls_transport.rs` | `KtlsConnector`: object_store's HTTP over kTLS connections, kept per host |
| `bench/src/http1_connection.rs` | one HTTP/1.1 connection's exchanges over any stream, a body read whole into one buffer |
| `bench/src/host_counters.rs` | the process's and the machine's CPU time, with the bytes moved |
| `bench/src/table_store.rs` | `s3://` tables; `--http reqwest\|ktls` |
| `src/cache/object_cache.rs` | `FetchedBlocks`: a fetch's bytes into the output and the cache as they come |
| `src/cache/chunk_pool/memory_pool.rs` | the memory tier on transparent huge pages |
