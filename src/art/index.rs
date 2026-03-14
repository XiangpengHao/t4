use std::{alloc::Layout, ptr::copy_nonoverlapping};

use vstd::prelude::*;
use vstd::raw_ptr;
use vstd::slice::slice_subrange;
use vstd::simple_pptr::PPtr;

use crate::art::{
    ArtNode, InsertStep, delete_from_node, get_from_node,
    n4::Node4,
    n16::Node16,
    n48::Node48,
    n256::Node256,
    ptr::{NextNodeMut, NextNodeRef, TaggedPointer},
};

const KV_HEADER_SIZE: usize = 16;
const KV_HEADER_ALIGN: usize = 16;

verus! {

pub const KV_HEADER_SIZE_VERUS: usize = 16;
pub const KV_HEADER_ALIGN_VERUS: usize = 16;
#[allow(dead_code)]
pub const MAX_LEAF_ALLOC_VERUS: usize =
    isize::MAX as usize - (isize::MAX as usize % KV_HEADER_ALIGN_VERUS);

#[repr(C, align(16))]
pub struct KVData {
    key_len: u8,
    _pad: [u8; 3],
    value_len: u32,
    data: [u8; 0],
}

/// Single-allocation key-value pair handle.
pub struct KVPairOwned {
    ptr: PPtr<KVData>,
    perm: Tracked<KVLeafPerm>,
}

#[allow(dead_code)]
pub tracked struct KVLeafPerm {
    header: raw_ptr::PointsTo<KVData>,
    payload: raw_ptr::PointsToRaw,
    dealloc: raw_ptr::Dealloc,
    exposed: raw_ptr::IsExposed,
}

impl KVLeafPerm {
    pub closed spec fn wf(self, ptr: PPtr<KVData>) -> bool {
        &&& self.header.ptr().addr() == ptr.addr()
        &&& self.header.is_init()
        &&& self.payload.is_range(
            ptr.addr() as int + vstd::layout::size_of::<KVData>() as int,
            self.header.value().key_len as int + self.header.value().value_len as int,
        )
        &&& self.dealloc.addr() == ptr.addr()
        &&& self.dealloc.size()
            == vstd::layout::size_of::<KVData>() + self.header.value().key_len as nat
                + self.header.value().value_len as nat
        &&& self.dealloc.size() <= usize::MAX
        &&& self.dealloc.align() == 16
        &&& self.header.ptr()@.provenance == self.dealloc.provenance()
        &&& self.payload.provenance() == self.dealloc.provenance()
        &&& self.exposed.provenance() == self.dealloc.provenance()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TerminatedKeyRef<'a> {
    key: &'a [u8],
    start: usize,
    needs_terminator: bool,
}

impl<'a> TerminatedKeyRef<'a> {
    pub closed spec fn wf(&self) -> bool {
        &&& self.key@.len() < usize::MAX
        &&& self.start as nat
            <= self.key@.len() + if self.needs_terminator { 1nat } else { 0nat }
    }

    pub closed spec fn spec_len(self) -> int
        recommends
            self.wf(),
    {
        self.key@.len() as int + if self.needs_terminator { 1int } else { 0int }
            - self.start as int
    }

    pub closed spec fn spec_index(self, i: int) -> u8
        recommends
            self.wf(),
            0 <= i < self.spec_len(),
    {
        if self.start as int + i < self.key@.len() as int {
            self.key[self.start as int + i]
        } else {
            0
        }
    }

    pub fn new(key: &'a [u8]) -> (result: Self)
        requires
            key.len() < usize::MAX,
        ensures
            result.wf(),
            result.spec_len()
                == key@.len() as int
                    + if key@.len() > 0 && key[key@.len() - 1] == 0 { 0int } else { 1int },
    {
        let needs_terminator = key.last() != Some(&0);
        Self { key, start: 0, needs_terminator }
    }

    pub fn len(&self) -> (result: usize)
        requires
            self.wf(),
        ensures
            result as int == self.spec_len(),
    {
        let full_len = if self.needs_terminator {
            self.key.len() + 1
        } else {
            self.key.len()
        };
        full_len - self.start
    }

    pub fn byte(self, idx: usize) -> (result: u8)
        requires
            self.wf(),
            (idx as int) < self.spec_len(),
        ensures
            result == self.spec_index(idx as int),
    {
        let abs = self.start + idx;
        if abs < self.key.len() {
            self.key[abs]
        } else {
            0
        }
    }

    pub fn suffix(self, n: usize) -> (result: Self)
        requires
            self.wf(),
            n as int <= self.spec_len(),
        ensures
            result.wf(),
            result.spec_len() == self.spec_len() - n as int,
    {
        Self { key: self.key, start: self.start + n, needs_terminator: self.needs_terminator }
    }

    pub fn materialized_subrange(self, start: usize, end: usize) -> (result: &'a [u8])
        requires
            self.wf(),
            start <= end,
            end as int <= self.spec_len(),
        ensures
            result@.len() <= end - start,
    {
        let abs_start = self.start + start;
        let clamped_start = if abs_start < self.key.len() {
            abs_start
        } else {
            self.key.len()
        };
        let abs_end_unclamped = self.start + end;
        let abs_end = if abs_end_unclamped < self.key.len() {
            abs_end_unclamped
        } else {
            self.key.len()
        };
        slice_subrange(self.key, clamped_start, abs_end)
    }

    pub fn eq(self, other: Self) -> (result: bool)
        requires
            self.wf(),
            other.wf(),
    {
        if self.len() != other.len() {
            return false;
        }

        let mut i = 0usize;
        while i < self.len()
            invariant
                self.wf(),
                other.wf(),
                self.spec_len() == other.spec_len(),
                i as int <= self.spec_len(),
                forall|j: int| 0 <= j < i ==> self.spec_index(j) == other.spec_index(j),
            decreases self.spec_len() - i as int,
        {
            if self.byte(i) != other.byte(i) {
                return false;
            }
            i = i + 1;
        }
        true
    }
}

pub struct ArtIndex {
    root: Option<TaggedPointer>,
    #[allow(dead_code)]
    leaf_perms: Tracked<Map<usize, KVLeafPerm>>,
}

impl ArtIndex {
    pub closed spec fn wf(&self) -> bool {
        match self.root {
            Some(root) => root.wf(),
            None => true,
        }
    }

    pub fn new() -> (result: Self)
        ensures
            result.wf(),
    {
        Self { root: None, leaf_perms: Tracked(Map::tracked_empty()) }
    }

    #[verifier::external_body]
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> (result: Option<KVPairOwned>)
        requires
            old(self).wf(),
            key.len() <= u8::MAX as usize,
            value.len() <= u32::MAX as usize,
            KV_HEADER_SIZE_VERUS + key.len() + value.len() <= MAX_LEAF_ALLOC_VERUS,
        ensures
            self.wf(),
    {
        let terminated_key = TerminatedKeyRef::new(key);
        let value = KVPairOwned::new(key, value);
        let (value_leaf_ptr, Tracked(value_perm)) = value.into_parts();
        let value_ptr = TaggedPointer::from_value(value_leaf_ptr);
        proof {
            self.leaf_perms.borrow_mut().tracked_insert(value_leaf_ptr.addr(), value_perm);
        }
        let mut current = self.root;
        let mut parent = Parent::Root(&mut self.root);
        let mut depth = 0;

        loop {
            let Some(current_ptr) = current else {
                parent.update(value_ptr);
                return None;
            };
            
            match unsafe { current_ptr.next_node_mut() } {
                NextNodeMut::Value(existing) => {
                    let terminated_existing = TerminatedKeyRef::new(existing.key());
                    if terminated_existing.eq(terminated_key) {
                        let value_leaf_ptr = current_ptr.value_ptr();
                        let tracked removed_perm = self.leaf_perms.borrow_mut().tracked_remove(
                            value_leaf_ptr.addr(),
                        );
                        parent.update(value_ptr);
                        return Some(KVPairOwned::from_parts(value_leaf_ptr, Tracked(removed_perm)));
                    }
                    let shared = common_prefix_len_terminated(
                        terminated_existing.suffix(depth),
                        terminated_key.suffix(depth),
                    );
                    let split = new_branching_path(
                        terminated_key.materialized_subrange(depth, depth + shared),
                        terminated_existing.byte(depth + shared),
                        current_ptr,
                        terminated_key.byte(depth + shared),
                        value_ptr,
                    );
                    parent.update(split);
                    return None;
                },
                NextNodeMut::Node4(node) => {
                    let step = node.insert_step(terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current_ptr,
                                terminated_key,
                                value_ptr,
                                depth,
                                matched,
                            );
                            parent.update(replacement);
                            return None;
                        },
                        InsertStep::Descend { edge, child, next_depth } => {
                            parent = Parent::Node4(node, edge);
                            current = Some(child);
                            depth = next_depth;
                        },
                        InsertStep::Grow { prefix_depth, prefix_len } => {
                            let replacement = node.grow(
                                terminated_key.materialized_subrange(
                                    prefix_depth,
                                    prefix_depth + prefix_len,
                                ),
                            );
                            parent.update(replacement);
                            unsafe { current_ptr.drop_node() };
                            current = Some(replacement);
                            depth = prefix_depth;
                        },
                        InsertStep::Done => return None,
                    }
                },
                NextNodeMut::Node16(node) => {
                    let step = node.insert_step(terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current_ptr,
                                terminated_key,
                                value_ptr,
                                depth,
                                matched,
                            );
                            parent.update(replacement);
                            return None;
                        },
                        InsertStep::Descend { edge, child, next_depth } => {
                            parent = Parent::Node16(node, edge);
                            current = Some(child);
                            depth = next_depth;
                        },
                        InsertStep::Grow { prefix_depth, prefix_len } => {
                            let replacement = node.grow(
                                terminated_key.materialized_subrange(
                                    prefix_depth,
                                    prefix_depth + prefix_len,
                                ),
                            );
                            parent.update(replacement);
                            unsafe { current_ptr.drop_node() };
                            current = Some(replacement);
                            depth = prefix_depth;
                        },
                        InsertStep::Done => return None,
                    }
                },
                NextNodeMut::Node48(node) => {
                    let step = node.insert_step(terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current_ptr,
                                terminated_key,
                                value_ptr,
                                depth,
                                matched,
                            );
                            parent.update(replacement);
                            return None;
                        },
                        InsertStep::Descend { edge, child, next_depth } => {
                            parent = Parent::Node48(node, edge);
                            current = Some(child);
                            depth = next_depth;
                        },
                        InsertStep::Grow { prefix_depth, prefix_len } => {
                            let replacement = node.grow(
                                terminated_key.materialized_subrange(
                                    prefix_depth,
                                    prefix_depth + prefix_len,
                                ),
                            );
                            parent.update(replacement);
                            unsafe { current_ptr.drop_node() };
                            current = Some(replacement);
                            depth = prefix_depth;
                        },
                        InsertStep::Done => return None,
                    }
                },
                NextNodeMut::Node256(node) => {
                    let step = node.insert_step(terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current_ptr,
                                terminated_key,
                                value_ptr,
                                depth,
                                matched,
                            );
                            parent.update(replacement);
                            return None;
                        },
                        InsertStep::Descend { edge, child, next_depth } => {
                            parent = Parent::Node256(node, edge);
                            current = Some(child);
                            depth = next_depth;
                        },
                        InsertStep::Grow { .. } => { unreachable!() },
                        InsertStep::Done => return None,
                    }
                },
            }
        }
    }

    #[verifier::external_body]
    pub fn get(&self, key: &[u8]) -> (result: Option<(&[u8], &[u8])>)
        requires
            self.wf(),
            key.len() <= u8::MAX as usize,
        ensures
            self.wf(),
    {
        let terminated_key = TerminatedKeyRef::new(key);
        let mut ptr = self.root;
        let mut depth = 0;

        loop {
            let ptr_value = ptr?;
            match unsafe { ptr_value.next_node_ref() } {
                NextNodeRef::Value(leaf) => {
                    return if TerminatedKeyRef::new(leaf.key()).eq(terminated_key) {
                        Some((leaf.key(), leaf.value()))
                    } else {
                        None
                    };
                },
                NextNodeRef::Node4(node) => {
                    let (next_ptr, next_depth) = get_from_node(node, terminated_key, depth)?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                },
                NextNodeRef::Node16(node) => {
                    let (next_ptr, next_depth) = get_from_node(node, terminated_key, depth)?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                },
                NextNodeRef::Node48(node) => {
                    let (next_ptr, next_depth) = get_from_node(node, terminated_key, depth)?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                },
                NextNodeRef::Node256(node) => {
                    let (next_ptr, next_depth) = get_from_node(node, terminated_key, depth)?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                },
            }
        }
    }

    #[verifier::external_body]
    pub fn delete(&mut self, key: &[u8]) -> (result: Option<KVPairOwned>)
        requires
            old(self).wf(),
            key.len() <= u8::MAX as usize,
        ensures
            self.wf(),
    {
        let terminated_key = TerminatedKeyRef::new(key);
        let result = delete_at(self.root, terminated_key, 0);
        match result {
            DeleteResult::NotFound { current } => {
                self.root = current;
                None
            },
            DeleteResult::Deleted { removed, replacement } => {
                let removed_ptr = removed.value_ptr();
                let tracked removed_perm = self.leaf_perms.borrow_mut().tracked_remove(
                    removed_ptr.addr(),
                );
                self.root = replacement;
                Some(KVPairOwned::from_parts(removed_ptr, Tracked(removed_perm)))
            },
        }
    }
}

} // verus!

const _: [(); KV_HEADER_SIZE] = [(); std::mem::size_of::<KVData>()];
const _: [(); KV_HEADER_ALIGN] = [(); std::mem::align_of::<KVData>()];

impl Default for ArtIndex {
    fn default() -> Self {
        Self::new()
    }
}

unsafe fn free_subtree(ptr: TaggedPointer) {
    unsafe {
        let raw = ptr.untagged_ptr();
        match ptr.tag() {
            4 => {
                let header = raw as *mut KVData;
                let key_len = (*header).key_len as usize;
                let value_len = (*header).value_len as usize;
                let data_offset = KV_HEADER_SIZE;
                let total_size = data_offset + key_len + value_len;
                let layout = Layout::from_size_align(total_size.max(1), KV_HEADER_ALIGN).unwrap();
                std::alloc::dealloc(header as *mut u8, layout);
            }
            0 => {
                let node = Box::from_raw(raw as *mut Node4);
                node.for_each_child(|_, child| free_subtree(child));
            }
            1 => {
                let node = Box::from_raw(raw as *mut Node16);
                node.for_each_child(|_, child| free_subtree(child));
            }
            2 => {
                let node = Box::from_raw(raw as *mut Node48);
                node.for_each_child(|child| free_subtree(child));
            }
            3 => {
                let node = Box::from_raw(raw as *mut Node256);
                node.for_each_child(|child| free_subtree(child));
            }
            _ => unreachable!("TaggedPointer type invariant guarantees a valid tag"),
        }
    }
}

impl Drop for ArtIndex {
    fn drop(&mut self) {
        if let Some(root) = self.root.take() {
            unsafe { free_subtree(root) };
        }
    }
}

enum Parent<'a> {
    Root(&'a mut Option<TaggedPointer>),
    Node4(&'a mut Node4, u8),
    Node16(&'a mut Node16, u8),
    Node48(&'a mut Node48, u8),
    Node256(&'a mut Node256, u8),
}

impl Parent<'_> {
    fn update(&mut self, value: TaggedPointer) {
        match self {
            Parent::Root(slot) => **slot = Some(value),
            Parent::Node4(node, edge) => (**node).replace_child(*edge, value),
            Parent::Node16(node, edge) => (**node).replace_child(*edge, value),
            Parent::Node48(node, edge) => (**node).replace_child(*edge, value),
            Parent::Node256(node, edge) => (**node).replace_child(*edge, value),
        }
    }
}

pub(crate) fn split_node(
    node: &mut impl ArtNode,
    old_ptr: TaggedPointer,
    terminated_key: TerminatedKeyRef<'_>,
    value_ptr: TaggedPointer,
    depth: usize,
    matched: usize,
) -> TaggedPointer {
    let old_prefix_len = node.prefix_len();
    let old_prefix = node.prefix();

    let mut parent = Node4::new(&old_prefix[..matched]);

    node.set_prefix(&old_prefix[matched + 1..old_prefix_len]);
    let _ = parent.insert(old_prefix[matched], old_ptr);
    let _ = parent.insert(terminated_key.byte(depth + matched), value_ptr);

    TaggedPointer::from_node4(Box::new(parent))
}

verus! {

pub(crate) enum DeleteResult {
    NotFound { current: Option<TaggedPointer> },
    Deleted { removed: TaggedPointer, replacement: Option<TaggedPointer> },
}

#[verifier::external_body]
pub(crate) fn delete_at(
    current: Option<TaggedPointer>,
    terminated_key: TerminatedKeyRef<'_>,
    depth: usize,
) -> (result: DeleteResult) {
    let Some(current) = current else {
        return DeleteResult::NotFound { current: None };
    };

    match unsafe { current.next_node_mut() } {
        NextNodeMut::Value(value) => {
            if !TerminatedKeyRef::new(value.key()).eq(terminated_key) {
                return DeleteResult::NotFound { current: Some(current) };
            }
            DeleteResult::Deleted { removed: current, replacement: None }
        },
        NextNodeMut::Node4(node) => delete_from_node(node, current, terminated_key, depth),
        NextNodeMut::Node16(node) => delete_from_node(node, current, terminated_key, depth),
        NextNodeMut::Node48(node) => delete_from_node(node, current, terminated_key, depth),
        NextNodeMut::Node256(node) => delete_from_node(node, current, terminated_key, depth),
    }
}

pub(crate) fn common_prefix_len_slice_terminated(a: &[u8], b: TerminatedKeyRef<'_>) -> (result: usize)
    requires
        b.wf(),
    ensures
        result <= a.len(),
        result as int <= b.spec_len(),
        forall|i: int| 0 <= i < result ==> a[i] == #[trigger] b.spec_index(i),
{
    let limit = if a.len() < b.len() {
        a.len()
    } else {
        b.len()
    };
    let mut idx = 0usize;
    while idx < limit
        invariant
            b.wf(),
            idx <= limit,
            limit <= a.len(),
            limit as int <= b.spec_len(),
            forall|i: int| 0 <= i < idx ==> a[i] == #[trigger] b.spec_index(i),
        decreases limit - idx,
    {
        proof {
            assert((idx as int) < b.spec_len());
        }
        if a[idx] == b.byte(idx) {
            idx = idx + 1;
        } else {
            return idx;
        }
    }

    idx
}

pub(crate) fn common_prefix_len_terminated(
    a: TerminatedKeyRef<'_>,
    b: TerminatedKeyRef<'_>,
) -> (result: usize)
    requires
        a.wf(),
        b.wf(),
    ensures
        result as int <= a.spec_len(),
        result as int <= b.spec_len(),
        forall|i: int| 0 <= i < result ==> a.spec_index(i) == b.spec_index(i),
{
    let limit = if a.len() < b.len() {
        a.len()
    } else {
        b.len()
    };
    let mut idx = 0usize;
    while idx < limit
        invariant
            a.wf(),
            b.wf(),
            idx <= limit,
            limit as int <= a.spec_len(),
            limit as int <= b.spec_len(),
            forall|i: int| 0 <= i < idx ==> a.spec_index(i) == b.spec_index(i),
        decreases limit - idx,
    {
        proof {
            assert((idx as int) < a.spec_len());
            assert((idx as int) < b.spec_len());
        }
        if a.byte(idx) == b.byte(idx) {
            idx = idx + 1;
        } else {
            return idx;
        }
    }

    idx
}

pub assume_specification[crate::art::ptr::TaggedPointer::from_node4](node: Box<Node4>) -> (result:
    TaggedPointer)
    ensures
        result.wf(),
;

pub(crate) fn new_branching_path(
    prefix: &[u8],
    left_edge: u8,
    left_child: TaggedPointer,
    right_edge: u8,
    right_child: TaggedPointer,
) -> (result: TaggedPointer)
    decreases prefix.len(),
{
    if prefix.len() <= 8 {
        proof {
            assert(crate::art::meta::NodeMeta::prefix_capacity() == 8) by (compute);
            assert(prefix.len() <= crate::art::meta::NodeMeta::prefix_capacity());
        }
        let mut node = Node4::new(prefix);
        let _ = node.insert(left_edge, left_child);
        let _ = node.insert(right_edge, right_child);
        return TaggedPointer::from_node4(Box::new(node));
    }

    let prefix8 = slice_subrange(prefix, 0, 8);
    proof {
        assert(crate::art::meta::NodeMeta::prefix_capacity() == 8) by (compute);
        assert(prefix8@.len() == 8) by {
            assert(prefix8@ == prefix@.subrange(0, 8));
        }
        assert(prefix8.len() == 8);
        assert(prefix8.len() <= crate::art::meta::NodeMeta::prefix_capacity());
    }
    let mut node = Node4::new(prefix8);
    let child = new_branching_path(
        slice_subrange(prefix, 9, prefix.len()),
        left_edge,
        left_child,
        right_edge,
        right_child,
    );
    let _ = node.insert(prefix[8], child);
    TaggedPointer::from_node4(Box::new(node))
}

} // verus!

// Header for a leaf allocation. The actual key and value bytes follow immediately
// after this header in memory (`data` is a zero-length flexible array marker).
//
// Layout (16-byte aligned): `[key_len: u8][_pad: 3][value_len: u32][key bytes...][value bytes...]`
verus! {

impl KVData {
    pub closed spec fn key_len_spec(&self) -> nat {
        self.key_len as nat
    }

    pub closed spec fn value_len_spec(&self) -> nat {
        self.value_len as nat
    }

    pub fn key(&self) -> (result: &[u8])
        ensures
            result@.len() == self.key_len_spec() as int,
    {
        let key_len = self.key_len as usize;
        kv_bytes(self, 0, key_len)
    }

    pub fn value(&self) -> (result: &[u8])
        ensures
            result@.len() == self.value_len_spec() as int,
    {
        let key_len = self.key_len as usize;
        let value_len = self.value_len as usize;
        kv_bytes(self, key_len, value_len)
    }
}

#[verifier::external_body]
fn kv_bytes<'a>(header: &'a KVData, offset: usize, len: usize) -> (result: &'a [u8])
    ensures
        result@.len() == len as int,
    opens_invariants none
    no_unwind
{
    unsafe { std::slice::from_raw_parts(header.data.as_ptr().add(offset), len) }
}

#[verifier::external_body]
fn kv_owned_header(this: &KVPairOwned) -> (result: &KVData)
    requires
        this.wf(),
    ensures
        result.key_len_spec() == this.perm@.header.value().key_len as nat,
        result.value_len_spec() == this.perm@.header.value().value_len as nat,
    opens_invariants none
    no_unwind
{
    unsafe { &*(this.ptr.addr() as *const KVData) }
}

#[verifier::external_body]
fn write_kv_payload(ptr: *mut KVData, key: &[u8], value: &[u8])
    opens_invariants none
    no_unwind
{
    unsafe {
        let data = (*ptr).data.as_mut_ptr();
        copy_nonoverlapping(key.as_ptr(), data, key.len());
        copy_nonoverlapping(value.as_ptr(), data.add(key.len()), value.len());
    }
}

#[verifier::external_body]
proof fn kv_data_layout()
    ensures
        vstd::layout::size_of::<KVData>() == KV_HEADER_SIZE_VERUS,
        vstd::layout::align_of::<KVData>() == KV_HEADER_ALIGN_VERUS,
        vstd::layout::valid_layout(MAX_LEAF_ALLOC_VERUS, KV_HEADER_ALIGN_VERUS),
{
}

impl KVPairOwned {
    pub closed spec fn wf(&self) -> bool {
        self.perm@.wf(self.ptr)
    }

    pub closed spec fn key_len_spec(&self) -> nat
        recommends
            self.wf(),
    {
        self.perm@.header.value().key_len as nat
    }

    pub closed spec fn value_len_spec(&self) -> nat
        recommends
            self.wf(),
    {
        self.perm@.header.value().value_len as nat
    }

    pub fn new(key: &[u8], value: &[u8]) -> (result: Self)
        requires
            key.len() <= u8::MAX as usize,
            value.len() <= u32::MAX as usize,
            KV_HEADER_SIZE_VERUS + key.len() + value.len() <= MAX_LEAF_ALLOC_VERUS,
        ensures
            result.wf(),
    {
        let data_offset = KV_HEADER_SIZE_VERUS;
        let total_size = data_offset
            .checked_add(key.len())
            .unwrap()
            .checked_add(value.len())
            .unwrap();
        proof {
            kv_data_layout();
            assert(total_size == data_offset + key.len() + value.len());
            assert(vstd::layout::valid_layout(total_size, KV_HEADER_ALIGN_VERUS)) by {
                assert(vstd::layout::valid_layout(MAX_LEAF_ALLOC_VERUS, KV_HEADER_ALIGN_VERUS));
                assert(total_size <= MAX_LEAF_ALLOC_VERUS);
            }
            assert(total_size != 0);
        }
        let (ptr_u8, Tracked(raw), Tracked(dealloc)) =
            raw_ptr::allocate(total_size, KV_HEADER_ALIGN_VERUS);
        let Tracked(exposed) = raw_ptr::expose_provenance(ptr_u8);
        let ptr = ptr_u8 as *mut KVData;
        let tracked header;
        let tracked payload;
        proof {
            let tracked (header_raw, payload_raw) = raw.split(vstd::set_lib::set_int_range(
                ptr_u8.addr() as int,
                ptr_u8.addr() as int + data_offset as int,
            ));
            header = header_raw.into_typed::<KVData>(ptr_u8.addr());
            payload = payload_raw;
        }
        let tracked mut header_perm = header;
        raw_ptr::ptr_mut_write(
            ptr,
            Tracked(&mut header_perm),
            KVData {
                key_len: key.len() as u8,
                _pad: [0; 3],
                value_len: value.len() as u32,
                data: [],
            },
        );
        let tracked header = header_perm;
        write_kv_payload(ptr, key, value);

        Self {
            ptr: PPtr::from_usize(ptr as usize),
            perm: Tracked(KVLeafPerm { header, payload, dealloc, exposed }),
        }
    }

    pub fn key(&self) -> (result: &[u8])
        requires
            self.wf(),
        ensures
            result@.len() == self.key_len_spec() as int,
    {
        let header = kv_owned_header(self);
        header.key()
    }

    pub fn value(&self) -> (result: &[u8])
        requires
            self.wf(),
        ensures
            result@.len() == self.value_len_spec() as int,
    {
        let header = kv_owned_header(self);
        header.value()
    }

    #[verifier::external_body]
    pub fn into_parts(self) -> (result: (PPtr<KVData>, Tracked<KVLeafPerm>))
        requires
            self.wf(),
        ensures
            result.1@.wf(result.0),
        opens_invariants none
    {
        let mut this = std::mem::ManuallyDrop::new(self);
        let ptr = this.ptr;
        let perm = std::mem::replace(&mut this.perm, Tracked::assume_new());
        let Tracked(perm) = perm;
        (ptr, Tracked(perm))
    }

    pub fn from_parts(
        ptr: PPtr<KVData>,
        Tracked(perm): Tracked<KVLeafPerm>,
    ) -> (result: Self)
        requires
            perm.wf(ptr),
        ensures
            result.wf(),
    {
        Self { ptr, perm: Tracked(perm) }
    }

    pub fn free(self)
        requires
            self.wf(),
    {
        let (ptr, Tracked(perm)) = self.into_parts();
        let tracked KVLeafPerm { header, payload, dealloc, exposed } = perm;
        let addr = ptr.0;
        let header_ptr: *mut KVData = raw_ptr::with_exposed_provenance(addr, Tracked(exposed));
        let tracked mut header_perm = header;
        let ghost stored_header = header_perm.value();
        let header_value = raw_ptr::ptr_mut_read(header_ptr, Tracked(&mut header_perm));
        let key_len = header_value.key_len as usize;
        let value_len = header_value.value_len as usize;
        proof {
            assert(header_value == stored_header);
            assert(key_len == stored_header.key_len as usize);
            assert(value_len == stored_header.value_len as usize);
            assert(vstd::layout::size_of::<KVData>() + key_len as nat <= usize::MAX) by {
                assert(vstd::layout::size_of::<KVData>() + key_len as nat + value_len as nat
                    <= usize::MAX);
            }
            assert(vstd::layout::size_of::<KVData>() + key_len as nat + value_len as nat
                <= usize::MAX) by {
                assert(vstd::layout::size_of::<KVData>() + stored_header.key_len as nat
                    + stored_header.value_len as nat == dealloc.size());
            }
        }
        let total_size = std::mem::size_of::<KVData>() + key_len + value_len;
        let tracked header_raw = header_perm.into_raw();
        let tracked full_raw = header_raw.join(payload);
        let dealloc_ptr: *mut u8 = raw_ptr::with_exposed_provenance(addr, Tracked(exposed));
        proof {
            assert(full_raw.is_range(addr as int, total_size as int)) by {
                assert(
                    vstd::set_lib::set_int_range(
                        addr as int,
                        addr as int + total_size as int,
                    ) =~= vstd::set_lib::set_int_range(
                        addr as int,
                        addr as int + vstd::layout::size_of::<KVData>() as int,
                    ) + vstd::set_lib::set_int_range(
                        addr as int + vstd::layout::size_of::<KVData>() as int,
                        addr as int + total_size as int,
                    )
                ) by {
                    assert(full_raw.dom() =~=
                        vstd::set_lib::set_int_range(
                            addr as int,
                            addr as int + vstd::layout::size_of::<KVData>() as int,
                        ) + vstd::set_lib::set_int_range(
                            addr as int + vstd::layout::size_of::<KVData>() as int,
                            addr as int + total_size as int,
                        )
                    );
                };
                assert forall|i: int|
                    #[trigger] vstd::set_lib::set_int_range(
                        addr as int,
                        addr as int + total_size as int,
                    ).contains(i) <==> #[trigger] (
                    vstd::set_lib::set_int_range(
                        addr as int,
                        addr as int + vstd::layout::size_of::<KVData>() as int,
                    ) + vstd::set_lib::set_int_range(
                        addr as int + vstd::layout::size_of::<KVData>() as int,
                        addr as int + total_size as int,
                    )
                ).contains(i) by {};
            };
        }
        raw_ptr::deallocate(
            dealloc_ptr,
            total_size,
            16,
            Tracked(full_raw),
            Tracked(dealloc),
        );
    }
}

impl Drop for KVPairOwned {
    #[verifier::external_body]
    fn drop(&mut self)
        opens_invariants none
        no_unwind
    {
        unsafe { std::ptr::read(self).free() }
    }
}

} // verus!

#[cfg(test)]
mod tests {
    use super::ArtIndex;

    #[test]
    fn insert_and_get_single_key() {
        let mut index = ArtIndex::new();

        index.insert(b"hello", b"world");

        let (k, v) = index.get(b"hello").expect("value");
        assert_eq!(k, b"hello");
        assert_eq!(v, b"world");
    }

    #[test]
    fn insert_distinguishes_prefix_keys() {
        let mut index = ArtIndex::new();

        index.insert(b"a", b"1");
        index.insert(b"ab", b"2");

        assert_eq!(index.get(b"a").expect("a").1, b"1");
        assert_eq!(index.get(b"ab").expect("ab").1, b"2");
        assert!(index.get(b"abc").is_none());
    }

    #[test]
    fn insert_handles_shared_long_prefix() {
        let mut index = ArtIndex::new();

        index.insert(b"prefix-path-alpha", b"alpha");
        index.insert(b"prefix-path-beta", b"beta");

        assert_eq!(index.get(b"prefix-path-alpha").expect("alpha").1, b"alpha");
        assert_eq!(index.get(b"prefix-path-beta").expect("beta").1, b"beta");
    }

    #[test]
    fn insert_grows_past_node4_and_node16() {
        let mut index = ArtIndex::new();

        for byte in 0u8..20 {
            let key = [b'x', byte];
            let value = [byte];
            index.insert(&key, &value);
        }

        for byte in 0u8..20 {
            let key = [b'x', byte];
            let result = index.get(&key);
            assert!(result.is_some(), "missing key {:?}", key);
            assert_eq!(result.expect("value").1, [byte]);
        }
    }

    #[test]
    fn insert_grows_past_node48() {
        let mut index = ArtIndex::new();

        for byte in 0u8..60 {
            let key = [b'y', byte];
            let value = [byte];
            index.insert(&key, &value);
        }

        for byte in 0u8..60 {
            let key = [b'y', byte];
            let result = index.get(&key);
            assert!(result.is_some(), "missing key {:?}", key);
            assert_eq!(result.expect("value").1, [byte]);
        }
    }

    #[test]
    fn insert_accepts_explicit_terminator() {
        let mut index = ArtIndex::new();

        index.insert(b"name\0", b"value");

        assert_eq!(index.get(b"name\0").expect("value").1, b"value");
        assert_eq!(index.get(b"name").expect("value").1, b"value");
    }

    #[test]
    fn long_prefix_mismatch_returns_none() {
        let mut index = ArtIndex::new();

        index.insert(b"prefix-path-alpha", b"alpha");
        index.insert(b"prefix-path-beta", b"beta");

        assert!(index.get(b"prefix-path-gamma").is_none());
    }

    #[test]
    fn insert_handles_shared_prefix_longer_than_eight_bytes() {
        let mut index = ArtIndex::new();

        index.insert(b"123456789abcdef-left", b"left");
        index.insert(b"123456789abcdef-right", b"right");

        assert_eq!(index.get(b"123456789abcdef-left").expect("left").1, b"left");
        assert_eq!(
            index.get(b"123456789abcdef-right").expect("right").1,
            b"right"
        );
        assert!(index.get(b"123456789abcdef-middle").is_none());
    }

    #[test]
    fn insert_replace_returns_old_value() {
        let mut index = ArtIndex::new();

        assert!(index.insert(b"key", b"v1").is_none());
        let old = index.insert(b"key", b"v2").expect("old");
        assert_eq!(old.key(), b"key");
        assert_eq!(old.value(), b"v1");
        assert_eq!(index.get(b"key").expect("value").1, b"v2");
    }

    #[test]
    fn delete_removes_single_key() {
        let mut index = ArtIndex::new();

        index.insert(b"hello", b"world");

        let deleted = index.delete(b"hello").expect("deleted");
        assert_eq!(deleted.key(), b"hello");
        assert_eq!(deleted.value(), b"world");
        assert!(index.get(b"hello").is_none());
    }

    #[test]
    fn delete_missing_key_keeps_existing_values() {
        let mut index = ArtIndex::new();

        index.insert(b"hello", b"world");

        assert!(index.delete(b"missing").is_none());
        assert_eq!(index.get(b"hello").expect("value").1, b"world");
    }

    #[test]
    fn delete_distinguishes_prefix_keys() {
        let mut index = ArtIndex::new();

        index.insert(b"a", b"1");
        index.insert(b"ab", b"2");

        assert_eq!(index.delete(b"a").expect("deleted").value(), b"1");
        assert!(index.get(b"a").is_none());
        assert_eq!(index.get(b"ab").expect("ab").1, b"2");
    }

    #[test]
    fn delete_handles_shared_long_prefix() {
        let mut index = ArtIndex::new();

        index.insert(b"prefix-path-alpha", b"alpha");
        index.insert(b"prefix-path-beta", b"beta");

        assert_eq!(
            index.delete(b"prefix-path-beta").expect("deleted").value(),
            b"beta"
        );
        assert!(index.get(b"prefix-path-beta").is_none());
        assert_eq!(index.get(b"prefix-path-alpha").expect("alpha").1, b"alpha");
    }

    #[test]
    fn delete_allows_reinsert_after_pruning_empty_nodes() {
        let mut index = ArtIndex::new();

        index.insert(b"ab", b"old");
        index.insert(b"ac", b"stay");

        assert!(index.delete(b"ab").is_some());
        assert!(index.delete(b"ac").is_some());
        assert!(index.get(b"ab").is_none());
        assert!(index.get(b"ac").is_none());

        index.insert(b"xyz", b"new");
        assert_eq!(index.get(b"xyz").expect("xyz").1, b"new");
    }

    #[test]
    fn delete_works_after_node_growth() {
        let mut index = ArtIndex::new();

        for byte in 0u8..60 {
            let key = [b'y', byte];
            let value = [byte];
            index.insert(&key, &value);
        }

        for byte in 10u8..50 {
            let key = [b'y', byte];
            let deleted = index.delete(&key);
            assert!(deleted.is_some(), "missing delete for {:?}", key);
        }

        for byte in 0u8..10 {
            let key = [b'y', byte];
            assert_eq!(index.get(&key).expect("present").1, [byte]);
        }
        for byte in 10u8..50 {
            let key = [b'y', byte];
            assert!(
                index.get(&key).is_none(),
                "deleted key still present {:?}",
                key
            );
        }
        for byte in 50u8..60 {
            let key = [b'y', byte];
            assert_eq!(index.get(&key).expect("present").1, [byte]);
        }
    }
}
