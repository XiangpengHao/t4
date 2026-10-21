- [ ] we need to cap the disk size, and memory size, and never exceed it.

- [ ] when memory is full, we need to evict to disk, when disk is full, we need to remove it to make space for new data.

- [ ] 
T4 consists of two parts: the in-memory bbtree index, and the on-disk file.
On a crash, we just scan the t4 file's wal, and rebuild the bbtree index.

bbtree-store has two parts: the leaf and inner nodes; and the heap pages.
leaf and inner nodes are always in memory, but the leaf node may point to a heap page (in memory cached), or a file range on the t4 file.
The key of index is the path of the file, and the value is a list of [(range of file, bytes), (range of file, bytes), ...].
Bytes is either points to a heap page, or a file range on the t4 file.

This means that caching in t4 is at leaf page level, a leaf page's heap page is either in memory, or evicted to disk (in which case leaf page will point to a file range on the t4 file).

Each leaf page points to a chain of heap pages, each of which is 4KB.
When memory runs low, i.e., insertion caused allocation error of the bbtree-store, we need to evict the heap pages.

Eviction works as follows:
1. there's a external cache replacement policy, which is a trait, allows user to configure and supply their own policy.
2. when the heap pages running low, we need to consult the policy to decide which heap page to evict.
3. evicting a heap page means moves all the keys/values in that page to t4.
We might hit a page who is in the middle of a value, then the entire value is being evicted. We never evict a partial value.
When a value is evicted, its leaf node must be updated.
After a heap page is reclaimed, its prev and next page are repaired to form a proper linked list.


When to persist data?
invariant: a crash or power failure should leave t4 in a consistent state: we can lose value, but never a incorrect/inconsistent value.

Persisting value happens at eviction time. This means that when memory is not full, no write happens, not even WAL.
Evictions happens in batches, we often evict more than 1 heap page.
When a value is evicted, we write to t4.
A value can be updated because of range merge: bytes 0..128; byte 512..1024; might be merged to byte 0..1024.
This is indistuiguisingble from first remove the key, then insert the key with new value.
Note that object store is immutable, so values never change. This means that it's considered as consistent if the in memory index says 0..1024 and disk says 0..128 & 512..1024.
This means update a index value doesn't require disk write.
Delete doesn't need to flush index as well. A value in disk that's already being deleted will be evicted out automatically.

bbtree-store's allocator exposes alloc_inner/leaf/heap apis. we can use that to distinguish memory pressure.
inner and leaf never fail, we ask from global alloc.
heap memory is acquired once on initialization. and never give back to system.

======

So far we have disucssed how memory eviction works. Next is how t4 evict data out.

On initialization, t4 allocate the file size of the specified cache size.

Similar to memory, no replacement happens when budget is not full.

When the bugdet is full, a replacement policy trait (similar to memory) will govern which page to evict,
when evicted, those pages are added to free list. 
