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

