- [x] we need to cap the disk size, and memory size, and never exceed it.
  (`03-cache.md`, decision 2)

- [x] when memory is full, we need to evict to disk, when disk is full, we need to remove it to make space for new data.
  (`03-cache.md`, decisions 7 and 8)

- [ ] we should register the memory pool to io uring once.

- [ ] Each read lands in a freshly allocated, zeroed buffer and is then copied into the caller’s out. Reading straight into the caller’s buffer, or into a memory chunk when promoting a block, would skip that.

- [ ] We need to have a benchmark, e.g., datafusion run clickbench, or datafusion run tpc-h
We show that 1. cold run saturates network, 2. cold run saturates disk, 3. varies memory cache size, 4. varies disk cache size.

We need to cover both parquet and vortex.

- [ ] datafusion clickbench workload analysis: https://claude.ai/artifact/NsCkwMvhFJnVvT4QxyyxM1?sk=mrUcYUF7vGDUiLBcmQSiOA

- [ ] on a disk miss, current we allow it to fetch from object store, use it, and drop it.
On a memory miss, we fetch it from disk, use it, and drop.
This means every miss, we need to at runtime allocate memory, use it once and remove. 
This also means io_uring need to setup the memory mapping each time, which is stupid.

I think ideally, we should never at runtime allocate memory for buffering, we just use exactly what are we pre-allocated.
This means we either: 
1. Reserve a few buffers for one time use, we make sure those memory are always avaliable, those are the scratch pad memory, already mapped. but this may limit the amount of in flight io request, which may not be great.
2. every io/network read means we need to make some space in memory chunk. so there's no pre-reservation, full memory space is used, no limit on inflight io. But, the problem is we might need to first evict data to make room. 

- [ ]
when serving a request, we basically have 3 modes: 1. cost optimized, 2. latency optimized, 3. throughput optimized.

1. cost optimized: object store charges per request, and the traffic is free within the provider network.
So downloading 1GB of data costs the same as downloading 1MB of data.
This is useful when we pre-warm the cache.

2. throughput optimized: most of the time, scanning is throughput optimized. 
For reading from object store: breaking down the large requests into smaller requests helps.
For reading from disk cache: distribute the cache chunks across multiple devices helps.
For reading from memory cache: struggler decides the throughput, a get_ranges must wait until all chunks are read.

3. latency optimized: some parts of the files are latency critical, like metadata.
From object store: racing reads, multi requests for metadata, taking the fastest one.
From disk cache: replicate the metadata to multiple devices, sending to multiple devices, taking the fastest one.
From memory cache: it should by default stays in memory.

How do we know which part are latency critical? 1. parquet's foot, 2. similar reagion of vortex, 3. mp4, jpeg?, 4. HEAD, LIST requests

- [ ] We need to show a cost-latency Pareto frontier.
Basically, how much does it cost to run a query, maybe?

- [ ] We need to measure the p-max, because it's very important to demonstrate the effect of racing reads.


- [ ] Next, we need to make sure we saturate network bandwidth with minimal threads, which is very hard.

- [x] split long runs into GETs of 4 MiB or more while the NIC has room, merge once it’s full. Measure on the S3 replay’s cold pass (time, p50/p99/max, GET count).

- [ ] Automatic racing if a request has reached p90 latency. 1. model how much extra request we'd send 2. since we know the max request from t4 is 4mb (not very large), we just take the p90 over all requests. 3. we can make it a policy: never, or max of 10, 100, 1000.

- [ ] We should limit the burst network request to object store. this should cut the tail latency, esp the ones that takes longer than 1s.
