- [ ] we need to cap the disk size, and memory size, and never exceed it.

- [ ] when memory is full, we need to evict to disk, when disk is full, we need to remove it to make space for new data.

- [ ] we should register the memory pool to io uring once.

- [ ] Each read lands in a freshly allocated, zeroed buffer and is then copied into the caller’s out. Reading straight into the caller’s buffer, or into a memory chunk when promoting a block, would skip that.

- [ ]
