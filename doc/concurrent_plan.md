# Concurrent ART Index Plan

## 1. Overview

Make the ART index concurrent using Optimistic Lock Coupling (OLC) with epoch-based
reclamation. The existing verified (Verus) code remains completely untouched; all
concurrency machinery lives in new, unverified modules that call into the verified node
operations.

**Core principles:**
- Hand-over-hand locking: at most two version locks held simultaneously.
- Readers never block; they retry on version mismatch.
- Writers hold at most two exclusive locks (parent + current node).
- Deferred reclamation via `crossbeam-epoch` prevents use-after-free.
- Zero-overhead abstractions make protocol violations unrepresentable.

---

## 2. Version Lock

A single `AtomicU64` per node encodes three things:

```
Bit 0:    obsolete  (node has been replaced, must restart)
Bit 1:    exclusive (a writer holds the lock)
Bits 2-63: version counter
```

### 2.1 Operations

```rust
struct VersionLock(AtomicU64);

// Bits
const OBSOLETE: u64 = 0b01;
const LOCKED:   u64 = 0b10;
```

| Operation | Semantics |
|-----------|-----------|
| `VersionedNode::read_optimistic(f: &N -> T) -> Result<T, Restart>` | Load version (Acquire). If `OBSOLETE \| LOCKED`, return `Err(Restart)`. Call `f(&node)`. Load version again (Acquire). If changed, return `Err(Restart)`. Return `Ok(f_result)`. |
| `VersionLock::read_optimistic(f: () -> T) -> Result<T, Restart>` | Same as above but closure takes no args. Used for root pointer slot. |
| `VersionedNode::write_lock() -> Result<WriteGuard<N>, Restart>` | Spin trying CAS(v, v \| LOCKED, AcqRel). On timeout, `Err(Restart)`. Returns RAII guard that derefs to `&mut N`. |
| `VersionLock::write_lock() -> Result<LockGuard, Restart>` | Same as above but no node type. Used for root pointer slot. |
| `WriteGuard::drop()` | Store(v + 2, Release). Clears exclusive bit and increments version. |
| `WriteGuard::unlock_obsolete()` | Store(v + 3, Release). Same carry trick but also sets the obsolete bit. |

Why `v + 2` for unlock: if `v = ...VV10` (version V, locked, not obsolete), then
`v + 2 = ...(V+1)00` (version V+1, unlocked, not obsolete). The carry propagation
clears the lock bit and increments the version in a single addition.

Why `v + 3` for unlock-obsolete: `v + 3 = ...(V+1)01` (version V+1, unlocked, obsolete).

### 2.2 Memory Ordering

- **Readers**: Acquire load before reading node data, Acquire load after. The two
  acquire loads bracket the data reads, ensuring proper ordering.
- **Writers**: CAS (AcqRel) to acquire, Release store to publish. The AcqRel CAS
  prevents writes from leaking before the lock. The Release store ensures data
  writes are visible before the version increment.
- **Data fields**: Non-atomic reads/writes are fine. Readers detect torn reads via
  version mismatch. Writers have exclusive access.

---

## 3. Memory Layout

### 3.1 Alignment Analysis

The verified code uses `TAG_MASK = 0x7` (3 low bits) in `TaggedPointer`, with
`valid_tag` allowing `tag < 5` (fits in 3 bits). Node types are
`#[repr(C, align(8))]`, providing exactly 3 free low bits.

This means `VersionedNode<N>` has **zero padding overhead**: the 8-byte
`VersionLock` sits at offset 0, and the node (8-byte aligned) starts
immediately at offset 8.

### 3.2 `VersionedNode<N>`

Wrap each node allocation with a version lock header. The verified node types
(`Node4`, `Node16`, `Node48`, `Node256`) are embedded unchanged:

```rust
#[repr(C)]
struct VersionedNode<N> {
    lock: VersionLock,       // 8 bytes at offset 0
    node: UnsafeCell<N>,     // starts at offset 8, no padding (8-byte aligned)
}
```

`UnsafeCell` is required because readers access `&N` optimistically (through a
shared `&VersionedNode<N>`) while a writer may hold `&mut N` via an exclusive
lock. This is the same pattern as `Mutex<T>` / `RwLock<T>`.

Overhead: 8 bytes per inner node — just the version lock, no padding. Node
sizes become Node4: 72B, Node16: 168B, Node48: ~658B, Node256: ~2068B.

Leaf values (`KVData`) are immutable after creation and don't need a version lock.
They are protected by epoch only.

### 3.3 Tagged Pointer Adaptation

The existing verified `TaggedPointer` (3-bit tag in low bits of `usize`) remains
unchanged. For the concurrent index, a new unverified dispatch function maps tags
to `VersionedNode<N>` pointers:

```rust
enum ConcurrentNextNode {
    Node4(*const VersionedNode<Node4>),
    Node16(*const VersionedNode<Node16>),
    Node48(*const VersionedNode<Node48>),
    Node256(*const VersionedNode<Node256>),
    Value(*const KVData),
}
```

The constructors (`from_node4`, etc.) are paralleled by new constructors that wrap
in `VersionedNode`. The single-threaded index and its constructors are completely
untouched.

### 3.4 Root Pointer

```rust
struct ConcurrentArtIndex {
    root: VersionLock,        // version lock for the root slot
    root_ptr: AtomicUsize,    // 0 = empty, otherwise tagged pointer
}
```

The root is treated as a single-slot "node" with its own version lock. This keeps
the protocol uniform: reading the root follows the same `read_optimistic` pattern
as any inner node.

---

## 4. Zero-Overhead Abstractions

The goal: make it **structurally impossible** to violate the OLC protocol at
the type level, with zero runtime cost beyond the protocol itself.

The key insight: the `#[must_use]` / `ReadGuard` approach from standard OLC
implementations has a critical flaw — it prevents *forgetting* the version
check, but it does NOT prevent *reading node data and using the result without
checking the version first*. A programmer can read fields from the node, stash
the results in local variables, then check the guard — but nothing forces the
stashed results to be invalidated if the check fails. Worse, `#[must_use]`
only triggers a warning, not a compile error.

We use a stronger pattern: **closure-based optimistic reads** and **RAII
exclusive writes**, which make the protocol structurally correct.

### 4.1 `Result<T, Restart>` for Control Flow

```rust
struct Restart;  // zero-size signal type
```

All fallible OLC steps return `Result<T, Restart>`. The `?` operator propagates
restarts automatically. This means:
- You cannot forget to handle a restart (it's in the return type).
- The code reads linearly despite the restart logic.
- It's zero-cost: `Result<T, Restart>` has the same layout as `Option<T>` for
  pointer-sized `T`, and the compiler optimizes the happy path.

Tree-level methods wrap the inner traversal in a retry loop:

```rust
fn get(&self, key: &[u8]) -> Option<ValueRef> {
    let guard = crossbeam_epoch::pin();
    loop {
        match self.get_optimistic(key, &guard) {
            Ok(result) => return result,
            Err(Restart) => continue,
        }
    }
}
```

### 4.2 Closure-Based Optimistic Read: `read_optimistic`

`read_optimistic` lives on `VersionedNode<N>`, not `VersionLock`. The closure
receives `&N` — the node reference is scoped to the closure and cannot outlive
the version check.

```rust
impl<N> VersionedNode<N> {
    /// Execute `f` under optimistic read protection.
    ///
    /// 1. Loads the version (Acquire). If locked/obsolete, returns Err(Restart).
    /// 2. Calls `f(&node)` to read data from the node.
    /// 3. Re-loads the version (Acquire). If changed, returns Err(Restart).
    /// 4. Returns Ok(result).
    ///
    /// The node reference only exists inside the closure. The caller
    /// never sees unvalidated data — if the version changed, the
    /// closure's return value is discarded.
    #[inline(always)]
    fn read_optimistic<T>(&self, f: impl FnOnce(&N) -> T) -> Result<T, Restart> {
        let v = self.lock.0.load(Acquire);
        if v & (OBSOLETE | LOCKED) != 0 {
            return Err(Restart);
        }
        let result = f(unsafe { &*self.node.get() });
        let v2 = self.lock.0.load(Acquire);
        if v2 != v {
            return Err(Restart);
        }
        Ok(result)
    }
}
```

**Why the closure takes `&N`:**

- The node reference is **scoped** — it cannot escape the closure. The caller
  only gets the return value of type `T`, which is a copy of whatever was read
  (a child pointer, a prefix length, etc.), not a reference into the node.
- This makes it structurally impossible to hold a stale `&N` after the version
  check. The reference is created inside the version-checked window and dies
  when the closure returns.
- It composes naturally: `let child = vn.read_optimistic(|node| node.get_child(edge))?;`

For the **root pointer** (which has no node type), `VersionLock` retains a
lower-level `read_optimistic` that takes a zero-argument closure:

```rust
impl VersionLock {
    #[inline(always)]
    fn read_optimistic<T>(&self, f: impl FnOnce() -> T) -> Result<T, Restart> {
        let v = self.0.load(Acquire);
        if v & (OBSOLETE | LOCKED) != 0 {
            return Err(Restart);
        }
        let result = f();
        let v2 = self.0.load(Acquire);
        if v2 != v {
            return Err(Restart);
        }
        Ok(result)
    }
}
```

This is used only for the root slot: `root.lock.read_optimistic(|| root_ptr.load(Acquire))`.

### 4.3 `WriteGuard` (RAII Exclusive Lock)

`write_lock` lives on `VersionedNode<N>`. The returned `WriteGuard<'a, N>`
implements `Deref<Target = N>` and `DerefMut`, so the caller can access node
methods directly through the guard (like `MutexGuard<T>`):

```rust
impl<N> VersionedNode<N> {
    /// Acquire exclusive access. Spins briefly, then returns Err(Restart).
    fn write_lock(&self) -> Result<WriteGuard<'_, N>, Restart> {
        for _ in 0..MAX_SPIN {
            let v = self.lock.0.load(Relaxed);
            if v & (OBSOLETE | LOCKED) != 0 {
                core::hint::spin_loop();
                continue;
            }
            if self.lock.0.compare_exchange_weak(
                v, v | LOCKED, AcqRel, Relaxed
            ).is_ok() {
                return Ok(WriteGuard { vn: self, version: v | LOCKED });
            }
        }
        Err(Restart)
    }
}

/// RAII exclusive lock. Derefs to &N / &mut N.
struct WriteGuard<'a, N> {
    vn: &'a VersionedNode<N>,
    version: u64,  // the locked version word (with LOCKED bit set)
}

impl<N> Deref for WriteGuard<'_, N> {
    type Target = N;
    fn deref(&self) -> &N { unsafe { &*self.vn.node.get() } }
}

impl<N> DerefMut for WriteGuard<'_, N> {
    fn deref_mut(&mut self) -> &mut N { unsafe { &mut *self.vn.node.get() } }
}

impl<N> WriteGuard<'_, N> {
    /// Release and mark the node as obsolete (it's been replaced).
    fn unlock_obsolete(self) {
        // version + 3: clears LOCKED, sets OBSOLETE, increments version
        // ...VV10 + 3 = ...VV10 + 11 = ...(V+1)01 (obsolete, unlocked, version+1)
        self.vn.lock.0.store(self.version + 3, Release);
        core::mem::forget(self);  // skip Drop
    }
}

impl<N> Drop for WriteGuard<'_, N> {
    fn drop(&mut self) {
        // version + 2: clears LOCKED, increments version
        // ...VV10 + 2 = ...(V+1)00
        self.vn.lock.0.store(self.version + 2, Release);
    }
}
```

Usage — the guard derefs to the node, so writes look natural:
```rust
let mut wg = vn.write_lock()?;
wg.insert(edge, new_leaf(key, value));  // calls N::insert via DerefMut
drop(wg);  // version bumped, lock released
```

For the **root pointer** (no node type), `VersionLock` retains a lower-level
`write_lock`:

```rust
impl VersionLock {
    fn write_lock(&self) -> Result<LockGuard<'_>, Restart> { ... }
}

/// Bare lock guard for slots without a node (e.g., root pointer).
struct LockGuard<'a> {
    lock: &'a VersionLock,
    version: u64,
}
// Same Drop / unlock_obsolete as WriteGuard, but no Deref to a node type.
```

**No upgrade path.** The first draft had `ReadGuard::upgrade()` to go from
optimistic read to exclusive. This is removed. Instead:

1. Read data optimistically via `vn.read_optimistic(|node| ...)`.
2. If a write is needed, acquire the lock via `vn.write_lock()`.
3. Re-check any conditions under exclusive access (the data may have changed
   between the optimistic read and the exclusive acquire).

The re-check costs a few extra comparisons under the exclusive lock, which is
negligible relative to memory allocation cost. And it eliminates the entire
`ReadGuard` type and the possibility of misusing an upgrade.

### 4.4 Epoch Requirement

The public API of `ConcurrentArtIndex` pins the epoch internally, so callers
can never forget it. Internal methods take `&Guard` as a parameter, making the
epoch dependency explicit:

```rust
impl ConcurrentArtIndex {
    // Public: epoch handled internally
    pub fn get(&self, key: &[u8]) -> Option<ValueRef>;

    // Internal: requires explicit epoch guard
    fn get_optimistic(&self, key: &[u8], guard: &Guard) -> Result<Option<ValueRef>, Restart>;
}
```

### 4.5 Why Not ReadGuard / OptimisticGuard

For comparison, here is the approach we **rejected** and why:

```rust
// REJECTED approach:
let guard = vn.lock.read_lock()?;    // returns ReadGuard
let child = vn.node.get_child(edge); // read data — BUT nothing ties
                                      // this read to the guard!
guard.check()?;                       // version check
// Problem: if programmer swaps lines 2 and 3, or forgets line 3,
// they use unvalidated data. #[must_use] only warns, doesn't prevent.
```

With the closure approach:
```rust
// ACCEPTED approach:
let child = vn.read_optimistic(|node| {
    node.get_child(edge)
})?;
// child is guaranteed version-checked. There is no way to get the
// value out without the check passing. The &N reference died when
// the closure returned.
```

---

## 5. Algorithms

### 5.1 GET (Read-Only)

```
get_optimistic(key, guard) -> Result<Option<ValueRef>, Restart>:
    ptr = root.lock.read_optimistic(|| root_ptr.load(Acquire))?
    if ptr == 0: return Ok(None)

    current = TaggedPointer::from_raw(ptr)
    depth = 0

    loop:
        match current.concurrent_next_node():
            Value(leaf_ptr):
                // Leaves are immutable; no version lock needed.
                // Epoch guarantees leaf is alive.
                if leaf.key == key: return Ok(Some(leaf.value_ref))
                else: return Ok(None)

            InnerNode(vn_ptr):
                // Everything inside the closure is version-protected.
                // The &node reference is scoped to the closure.
                let result = vn.read_optimistic(|node| {
                    let prefix_len = node.prefix_len()
                    let matched = common_prefix_len(node.prefix(), key[depth..])
                    if matched != prefix_len:
                        return ReadResult::NotFound
                    let child = node.get_child(key[depth + prefix_len])
                    match child:
                        None => ReadResult::NotFound
                        Some(c) => ReadResult::Descend(c, prefix_len)
                })?

                match result:
                    NotFound => return Ok(None)
                    Descend(child, prefix_len):
                        current = child
                        depth += prefix_len + 1
```

Cost per node: two atomic loads (before + after closure). No writes, no CAS, no
cache line invalidation. Readers scale linearly.

### 5.2 INSERT

Insert has three possible outcomes at each inner node, requiring different locking
strategies:

| Outcome | What changes | Locks needed |
|---------|-------------|--------------|
| **Descend** | Nothing | None (optimistic read) |
| **Insert into non-full node** | Current node only | Current (exclusive) |
| **Grow / Split** | Parent pointer + new node | Parent (exclusive) + Current (read-verify) |

```
insert_optimistic(key, value, guard) -> Result<Option<ValueRef>, Restart>:
    // Phase 1: Read root optimistically
    ptr = root.lock.read_optimistic(|| root_ptr.load(Acquire))?

    if ptr == 0:
        // Empty tree: lock root, re-check, insert
        let wg = root.lock.write_lock()?
        if root_ptr.load(Relaxed) != 0:
            drop(wg)  // someone else inserted, restart
            return Err(Restart)
        root_ptr.store(new_leaf(key, value))
        drop(wg)  // write_unlock via Drop
        return Ok(None)

    current = TaggedPointer::from_raw(ptr)
    depth = 0
    parent_info = Root  // tracks which parent slot to modify

    loop:
        match current.concurrent_next_node():
            Value(existing_leaf):
                if existing_leaf.key == key:
                    // Replace value: lock parent, re-check, swap
                    let wg = parent.lock.write_lock()?
                    // Re-check that parent still points to this leaf
                    if parent.child_at(edge) != current:
                        drop(wg); return Err(Restart)
                    old = swap_child_in_parent(parent, edge, new_leaf(key, value))
                    drop(wg)
                    guard.defer_destroy(old)
                    return Ok(Some(old_value))
                else:
                    // Split: lock parent, re-check, create new branch
                    let wg = parent.lock.write_lock()?
                    if parent.child_at(edge) != current:
                        drop(wg); return Err(Restart)
                    branch = new_branching_node(existing_leaf, new_leaf(key, value))
                    replace_child_in_parent(parent, edge, branch)
                    drop(wg)
                    return Ok(None)

            InnerNode(vn_ptr):
                // Read node state optimistically — closure gets &node
                let step = vn.read_optimistic(|node| {
                    compute_insert_step(node, key, depth)
                })?

                match step:
                    Descend(child, edge, next_depth):
                        // Move parent tracking to current node
                        parent_info = NodeParent(vn_ptr, edge)
                        current = child
                        depth = next_depth
                        continue

                    NeedInsert(edge):
                        // Non-full node: lock current, re-check, insert
                        let mut wg = vn.write_lock()?
                        // Re-check: node might have changed since optimistic read
                        if wg.is_full():
                            drop(wg); return Err(Restart)
                        if wg.has_child(edge):
                            drop(wg); return Err(Restart)
                        wg.insert(edge, new_leaf(key, value))
                        drop(wg)
                        return Ok(None)

                    NeedGrow(edge):
                        // Full node: lock parent, re-check, grow
                        let pwg = parent.lock.write_lock()?
                        if parent.child_at(edge_to_current) != current:
                            drop(pwg); return Err(Restart)
                        let mut cwg = vn.write_lock()?
                        grown = grow_node(&*cwg)  // read current node via Deref
                        grown.insert(edge, new_leaf(key, value))
                        replace_child_in_parent(parent, edge_to_current, alloc(grown))
                        cwg.unlock_obsolete()
                        drop(pwg)
                        guard.defer_destroy(vn_ptr)
                        return Ok(None)

                    NeedSplit(matched):
                        // Prefix mismatch: lock parent, re-check, split
                        let pwg = parent.lock.write_lock()?
                        if parent.child_at(edge_to_current) != current:
                            drop(pwg); return Err(Restart)
                        let mut cwg = vn.write_lock()?
                        branch = split_prefix(&mut *cwg, key, value, matched)
                        replace_child_in_parent(parent, edge_to_current, branch)
                        drop(cwg)   // node modified (prefix trimmed) but not obsolete
                        drop(pwg)
                        return Ok(None)
```

**Key difference from the ReadGuard/upgrade approach:** Instead of upgrading an
optimistic read to an exclusive lock (which requires the version to be unchanged
since the read), we:

1. Read optimistically with `vn.read_optimistic(|node| ...)`.
2. If a write is needed, acquire the lock independently with `vn.write_lock()`.
3. Re-check conditions under exclusive access (via `Deref`/`DerefMut` on the guard).

This means the version may have changed between the optimistic read and the
write lock acquisition. The re-check handles this: if the state has changed
(node became full, child appeared, parent changed), we restart. The cost is a
few extra comparisons under the exclusive lock, which is negligible.

### 5.3 DELETE

Simplified compared to the single-threaded version: **no node shrinking or pruning**.
Empty inner nodes are left in place. This keeps the protocol to at most two locks
and avoids the need for grandparent locking.

```
delete_optimistic(key, guard) -> Result<Option<ValueRef>, Restart>:
    // Read root optimistically
    ptr = root.lock.read_optimistic(|| root_ptr.load(Acquire))?
    if ptr == 0: return Ok(None)

    // Special case: root is a leaf
    if ptr.is_leaf():
        if leaf.key == key:
            let wg = root.lock.write_lock()?
            // Re-check: root might have changed
            if root_ptr.load(Relaxed) != ptr:
                drop(wg); return Err(Restart)
            root_ptr.store(0)
            drop(wg)
            guard.defer_destroy(ptr)
            return Ok(Some(old_value))
        else:
            return Ok(None)

    current = ptr
    depth = 0
    parent_info = Root

    loop:
        match current.concurrent_next_node():
            InnerNode(vn_ptr):
                let result = vn.read_optimistic(|node| {
                    let prefix_len = node.prefix_len()
                    let matched = common_prefix_len(...)
                    if matched != prefix_len:
                        return ReadResult::NotFound
                    let edge = key[depth + prefix_len]
                    let child = node.get_child(edge)
                    match child:
                        None => ReadResult::NotFound
                        Some(child_ptr) =>
                            if child_ptr.is_leaf():
                                ReadResult::FoundLeaf(child_ptr, edge, prefix_len)
                            else:
                                ReadResult::Descend(child_ptr, edge, prefix_len)
                })?

                match result:
                    NotFound => return Ok(None)
                    FoundLeaf(leaf_ptr, edge, prefix_len):
                        if leaf.key != key: return Ok(None)
                        // Lock current node, re-check, remove child
                        let mut wg = vn.write_lock()?
                        // Re-check: child at this edge might have changed
                        if wg.get_child(edge) != Some(leaf_ptr):
                            drop(wg); return Err(Restart)
                        wg.remove_child(edge)
                        drop(wg)
                        guard.defer_destroy(leaf_ptr)
                        return Ok(Some(old_value))
                    Descend(child_ptr, edge, prefix_len):
                        parent_info = NodeParent(vn_ptr, edge)
                        current = child_ptr
                        depth += prefix_len + 1
                        continue

            Value(_):
                // Should not happen (leaves are children, not current)
                unreachable
```

### 5.4 Why No Node Shrinking/Pruning

The single-threaded delete prunes empty inner nodes (removes them from the
parent, returning `replacement: None`). This requires recursive modification of
the grandparent, which would need three-level locking in OLC.

Trade-off:
- Without pruning: empty inner nodes waste memory but are harmless to correctness.
  Under typical workloads, this is negligible.
- A background compaction pass can prune empty nodes by walking the tree under
  a full exclusive lock, or by doing OLC-style bottom-up pruning.
- If pruning is critical, a two-pass approach works: first find the deepest
  node to prune (optimistic read), then lock from that node's grandparent
  downward (at most two exclusive locks). But this adds complexity and is
  deferred from the initial implementation.

---

## 6. Epoch-Based Reclamation

### 6.1 Integration with `crossbeam-epoch`

Add `crossbeam-epoch` as a dependency:

```toml
[dependencies]
crossbeam-epoch = "0.9"
```

Usage pattern:
- **Enter epoch**: `let guard = crossbeam_epoch::pin();` at the start of each
  public method (`get`, `insert`, `delete`).
- **Defer destruction**: When a node or leaf is removed/replaced:
  ```rust
  unsafe { guard.defer_unchecked(move || drop(Box::from_raw(old_ptr))); }
  ```
- **Leave epoch**: The `guard` is dropped at the end of the public method,
  automatically unpinning.

### 6.2 What Gets Deferred

| Operation | Deferred Object | When |
|-----------|----------------|------|
| Node growth (4->16, etc.) | Old node (`VersionedNode<NodeN>`) | After parent pointer updated |
| Prefix split | Nothing (old node is modified in-place, not replaced) | N/A |
| Value replace | Old `KVData` | After parent pointer updated |
| Delete | Removed `KVData` (leaf) | After removal from parent |

Note: prefix splitting modifies the current node in-place (trimming its prefix)
rather than replacing it. The node is locked exclusively during this, so no
concurrent readers can see a partially modified prefix.

### 6.3 Shuttle Compatibility

The project already abstracts threading primitives behind `src/sync.rs` for
shuttle testing. We need to extend this:

```rust
// In src/sync.rs:
#[cfg(feature = "shuttle")]
pub(crate) use shuttle::sync::atomic::{AtomicU64, AtomicUsize};
#[cfg(not(feature = "shuttle"))]
pub(crate) use std::sync::atomic::{AtomicU64, AtomicUsize};
```

For epoch: shuttle does not provide a crossbeam-epoch equivalent. Options:
1. Gate epoch behind a feature flag: use crossbeam-epoch in production,
   use a no-op stub in shuttle tests. Shuttle tests would detect
   concurrency bugs in the locking protocol but not reclamation bugs.
2. Implement a minimal epoch stub for shuttle that defers deallocation to
   the end of the test. This is simple and sufficient for shuttle's
   exhaustive interleaving exploration.

Option 2 is recommended. The stub:
```rust
#[cfg(feature = "shuttle")]
mod epoch {
    pub struct Guard;
    pub fn pin() -> Guard { Guard }
    impl Guard {
        pub unsafe fn defer_unchecked(&self, f: impl FnOnce()) {
            // In shuttle tests, just leak (or collect and free at end)
        }
    }
}
```

---

## 7. Interaction with Verus Verification

### 7.1 What Doesn't Change

All of the following remain **byte-for-byte identical**:

- `NodeMeta` (`meta.rs`)
- `TaggedPointer` (`ptr.rs`)
- `DenseNode<CAP>` (`dense.rs`)
- `Node4`, `Node16`, `Node48`, `Node256` (`n4.rs`, `n16.rs`, `n48.rs`, `n256.rs`)
- `ArtNode` trait (`mod.rs`)
- `common_prefix_len`, `get_from_node`, `delete_from_node` (`mod.rs`, `index.rs`)
- `KVData`, `KVPair` (`index.rs`)
- Single-threaded `ArtIndex` (`index.rs`)
- All Verus proofs and specifications

### 7.2 How Concurrent Code Calls Verified Code

The concurrent index accesses nodes through the `VersionedNode<N>` wrappers,
which provide `&N` (via `read_optimistic` closure) and `&mut N` (via
`WriteGuard` deref) to call the existing verified methods:

```rust
// Reading: closure receives &Node4 from read_optimistic
let child = vn.read_optimistic(|node| {
    node.get_child(edge)  // same verified method
})?;

// Writing: WriteGuard derefs to &mut Node4
let mut wg = vn.write_lock()?;
wg.insert(edge, child_ptr);  // same verified method via DerefMut
drop(wg);
```

The verified pre/postconditions (`wf()`, `has_key()`, `maps_to()`) still apply
to each individual call. The concurrency layer's job is to ensure these
preconditions are met by the locking protocol:
- `wf()`: Maintained because only exclusive writers mutate nodes, and verified
  methods preserve `wf()`.
- `has_key(edge)` precondition for `replace_child`: Verified by the optimistic
  read phase (we saw the child existed), and re-checked under exclusive lock
  before mutation.

### 7.3 New Unverified Code

All new code lives outside the `verus! {}` blocks:

```
src/art/
  version_lock.rs   // NEW: VersionLock, WriteGuard
  concurrent.rs     // NEW: ConcurrentArtIndex, VersionedNode
```

These files do not contain `verus! {}` blocks and are invisible to the verifier.

---

## 8. File Structure

```
src/art/
  mod.rs             UNCHANGED  (trait, get_from_node, delete_from_node)
  meta.rs            UNCHANGED  (NodeMeta)
  ptr.rs             UNCHANGED  (TaggedPointer)
  dense.rs           UNCHANGED  (DenseNode<CAP>)
  n4.rs              UNCHANGED  (Node4)
  n16.rs             UNCHANGED  (Node16)
  n48.rs             UNCHANGED  (Node48)
  n256.rs            UNCHANGED  (Node256)
  index.rs           UNCHANGED  (single-threaded ArtIndex, KVData, KVPair)

  version_lock.rs    NEW  VersionLock, LockGuard, WriteGuard, Restart
  concurrent.rs      NEW  ConcurrentArtIndex, VersionedNode<N>,
                          ConcurrentNextNode, allocation helpers
```

The `mod.rs` file gets two new `mod` declarations:
```rust
mod version_lock;
mod concurrent;
pub use concurrent::ConcurrentArtIndex;
```

---

## 9. Alternatives Considered

### 9.1 RwLock Per Node

Standard `pthread_rwlock` or Rust `RwLock` at each node.

| | OLC (version lock) | RwLock per node |
|-|---------------------|-----------------|
| Size | 8 bytes | 40-56 bytes (pthread) |
| Reader cost | 2 atomic loads | lock + unlock (2 atomic RMW) |
| Reader scaling | Linear (no writes to shared cache line) | Bottlenecked (writers invalidate lock cache line) |
| Writer cost | 1 CAS + 1 store | lock + unlock (2 atomic RMW) |
| Complexity | Higher (restart logic) | Lower |

**Verdict**: OLC is strictly better for read-heavy workloads (which index lookups
are). The restart logic adds code complexity but the `Result<T, Restart>` + `?`
pattern keeps it manageable.

### 9.2 ROWEX (Read-Optimized Write EXclusion)

From "The ART of Practical Synchronization" (Leis et al., 2016). Readers are truly
wait-free: they never restart. Writers use careful node modification ordering so
that readers always see a consistent state.

| | OLC | ROWEX |
|-|-----|-------|
| Reader restarts | Yes (rare) | Never |
| Writer complexity | Moderate | High (must carefully order all writes) |
| Structural changes | Lock + replace + obsolete | Copy-on-write + atomic pointer swap |
| Memory overhead | Lower (modify in place) | Higher (copy on write for every structural change) |

**Verdict**: ROWEX has theoretical advantages for readers, but the writer
complexity is significantly higher. OLC restarts are rare in practice (only under
contention), and the simpler protocol is much easier to get right. OLC is the
better starting point; ROWEX can be explored later if profiling shows restart
overhead matters.

### 9.3 Lock-Free (CAS-Based)

Use compare-and-swap on all pointers for fully lock-free operations.

**Rejected**: ART structural modifications (prefix splitting, node growth) require
multi-word atomic updates, which CAS cannot provide without heavy-weight
multi-CAS protocols. The complexity is orders of magnitude higher with no
practical performance benefit over OLC for this workload.

### 9.4 Single-Writer / Flat Combining

Funnel all writes through a single thread; readers are lock-free.

**Rejected**: Doesn't scale write throughput. The store may need concurrent inserts
from multiple IO completions or client threads.

### 9.5 Seqlock Without Obsolete Bit

A standard seqlock (even version = unlocked, odd = locked) without the obsolete bit.

**Rejected**: Without the obsolete bit, readers who begin reading a node just
before it's replaced might succeed their version check (the node wasn't modified,
just replaced in the parent). They'd then traverse into a node that's no longer
part of the tree. The obsolete bit prevents this: the writer marks the old node
obsolete before unlocking, causing any concurrent reader to restart.

Wait -- there is a subtlety: a reader who loaded the child pointer from the parent
*before* the parent was modified will descend into the old child. The old child
hasn't been marked obsolete at that point. But:
- If the old child's data hasn't changed, the reader gets a stale-but-consistent
  result. This is linearizable (the read is linearized before the write).
- If the old child's data WAS changed (e.g., prefix trimmed during a split), the
  writer held the child's exclusive lock, which bumped its version. The reader's
  version check will fail.

So the obsolete bit is primarily useful as a fast-fail mechanism for stragglers,
not strictly required for correctness. But it reduces wasted work and is cheap
(one bit), so we include it.

---

## 10. Implementation Order

### Phase 1: Version Lock

1. Implement `VersionLock` with all operations (`read_optimistic`, `write_lock`).
2. Implement `WriteGuard` type (RAII exclusive lock).
3. Unit tests for lock state transitions, write lock, obsolete.
4. Shuttle tests for concurrent lock operations.

### Phase 2: VersionedNode + Allocation

1. Implement `VersionedNode<N>` wrapper.
2. Implement allocation/deallocation helpers that go through `crossbeam-epoch`.
3. Implement `ConcurrentNextNode` dispatch.
4. Wire up the shuttle-compatible epoch stub.

### Phase 3: Concurrent GET

1. Implement `ConcurrentArtIndex::get` with the optimistic read protocol.
2. Test single-threaded correctness (should match `ArtIndex::get`).
3. Multi-threaded read-only stress tests.
4. Shuttle tests for concurrent reads.

### Phase 4: Concurrent INSERT

1. Implement the descent + insert-at-leaf path (non-full node, no structural change).
2. Implement node growth path (lock parent + current).
3. Implement prefix split path.
4. Implement value replacement.
5. Test correctness against single-threaded reference.
6. Shuttle tests for concurrent inserts.
7. Mixed read/write shuttle tests.

### Phase 5: Concurrent DELETE

1. Implement optimistic delete (no pruning).
2. Test correctness against single-threaded reference.
3. Shuttle tests for concurrent deletes.
4. Full mixed workload tests (read + insert + delete).

### Phase 6: Integration

1. Replace `RwLock<HashMap<T4Key, ValueRef>>` in `store.rs` with
   `ConcurrentArtIndex` (pending the decision on what the ART maps to).
2. End-to-end tests.
3. Benchmarks comparing `HashMap` vs concurrent ART.

---

## 11. Open Questions

1. **What does the concurrent ART store as values?** Currently `ArtIndex` stores
   `KVPair` (key + value bytes). But `T4Store` only needs key -> `ValueRef` (offset +
   length). The concurrent ART should probably store just a `ValueRef` (which is
   `Copy`, 12 bytes) to avoid dynamic allocation for values. This simplifies epoch
   reclamation (no need to defer-free values, just overwrite the `ValueRef` in the
   leaf). We'd change KVPair to KVRef or make the leaf type generic.

2. **Should we add node shrinking later?** Node256->48->16->4 shrinking during
   delete keeps memory usage tight but requires more complex locking (or a
   background compaction thread). Punted for now.

3. **Range scans?** Not yet implemented even in the single-threaded version. For
   OLC, range scans use the same optimistic read protocol but must handle restarts
   mid-scan (typically by remembering the last successful key and resuming from
   there). This is a separate feature.

4. **Should version lock spin or yield on contention?** For very short critical
   sections (a few memory writes), spinning is appropriate. We should add a bounded
   spin loop (e.g., 16 iterations) before falling back to `Restart`. This prevents
   wasting CPU on long stalls while keeping the fast path fast.
