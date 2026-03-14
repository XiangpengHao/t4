use std::{alloc::Layout, ptr::copy_nonoverlapping};

use vstd::prelude::*;

use crate::art::{
    ArtNode, InsertStep, delete_from_node, get_from_node,
    n4::Node4,
    n16::Node16,
    n48::Node48,
    n256::Node256,
    ptr::{NextNodeMut, NextNodeRef, TaggedPointer},
};

verus! {

pub struct ArtIndex {
    root: Option<TaggedPointer>,
}

impl ArtIndex {
    pub closed spec fn wf(&self) -> bool {
        match self.root {
            Some(root) => root.wf(),
            None => true,
        }
    }

    #[verifier::external_body]
    pub fn new() -> (result: Self)
        ensures
            result.wf(),
    {
        Self { root: None }
    }

    #[verifier::external_body]
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> (result: Option<KVPairOwned>)
        requires
            old(self).wf(),
            key.len() <= u8::MAX as usize,
            value.len() <= u32::MAX as usize,
        ensures
            self.wf(),
    {
        let terminated_key = terminated_key_owned(key);
        let value_ptr = TaggedPointer::from_value(KVPairOwned::new(key, value));
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
                    let terminated_existing = terminated_key_owned(existing.key());
                    if terminated_existing == terminated_key {
                        parent.update(value_ptr);
                        return Some(unsafe { current_ptr.into_value() });
                    }
                    let shared = common_prefix_len(
                        &terminated_existing[depth..],
                        &terminated_key[depth..],
                    );
                    let split = new_branching_path(
                        &terminated_key[depth..depth + shared],
                        terminated_existing[depth + shared],
                        current_ptr,
                        terminated_key[depth + shared],
                        value_ptr,
                    );
                    parent.update(split);
                    return None;
                },
                NextNodeMut::Node4(node) => {
                    let step = node.insert_step(&terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current_ptr,
                                &terminated_key,
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
                                &terminated_key[prefix_depth..prefix_depth + prefix_len],
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
                    let step = node.insert_step(&terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current_ptr,
                                &terminated_key,
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
                                &terminated_key[prefix_depth..prefix_depth + prefix_len],
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
                    let step = node.insert_step(&terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current_ptr,
                                &terminated_key,
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
                                &terminated_key[prefix_depth..prefix_depth + prefix_len],
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
                    let step = node.insert_step(&terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current_ptr,
                                &terminated_key,
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
        let terminated_key = terminated_key_owned(key);
        let mut ptr = self.root;
        let mut depth = 0;

        loop {
            let ptr_value = ptr?;
            match unsafe { ptr_value.next_node_ref() } {
                NextNodeRef::Value(leaf) => {
                    return if terminated_key_owned(leaf.key()) == terminated_key {
                        Some((leaf.key(), leaf.value()))
                    } else {
                        None
                    };
                },
                NextNodeRef::Node4(node) => {
                    let (next_ptr, next_depth) = get_from_node(node, &terminated_key, depth)?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                },
                NextNodeRef::Node16(node) => {
                    let (next_ptr, next_depth) = get_from_node(node, &terminated_key, depth)?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                },
                NextNodeRef::Node48(node) => {
                    let (next_ptr, next_depth) = get_from_node(node, &terminated_key, depth)?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                },
                NextNodeRef::Node256(node) => {
                    let (next_ptr, next_depth) = get_from_node(node, &terminated_key, depth)?;
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
        let terminated_key = terminated_key_owned(key);
        let result = delete_at(self.root, &terminated_key, 0);
        match result {
            DeleteResult::NotFound { current } => {
                self.root = current;
                None
            },
            DeleteResult::Deleted { removed, replacement } => {
                self.root = replacement;
                Some(unsafe { removed.into_value() })
            },
        }
    }
}

} // verus!
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
                drop(ptr.into_value());
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
    terminated_key: &[u8],
    value_ptr: TaggedPointer,
    depth: usize,
    matched: usize,
) -> TaggedPointer {
    let old_prefix_len = node.prefix_len();
    let old_prefix = node.prefix();

    let mut parent = Node4::new(&old_prefix[..matched]);

    node.set_prefix(&old_prefix[matched + 1..old_prefix_len]);
    let _ = parent.insert(old_prefix[matched], old_ptr);
    let _ = parent.insert(terminated_key[depth + matched], value_ptr);

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
    terminated_key: &[u8],
    depth: usize,
) -> (result: DeleteResult) {
    let Some(current) = current else {
        return DeleteResult::NotFound { current: None };
    };

    match unsafe { current.next_node_mut() } {
        NextNodeMut::Value(value) => {
            if terminated_key_owned(value.key()) != terminated_key {
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

pub(crate) fn common_prefix_len(a: &[u8], b: &[u8]) -> (result: usize)
    ensures
        result <= a.len(),
        result <= b.len(),
        forall|i: int| 0 <= i < result ==> a[i] == b[i],
{
    let limit = if a.len() < b.len() {
        a.len()
    } else {
        b.len()
    };
    let mut idx = 0usize;
    while idx < limit
        invariant
            idx <= limit,
            limit <= a.len(),
            limit <= b.len(),
            forall|i: int| 0 <= i < idx ==> a[i] == b[i],
        decreases limit - idx,
    {
        if a[idx] == b[idx] {
            idx = idx + 1;
        } else {
            return idx;
        }
    }

    idx
}

} // verus!
fn new_branching_path(
    prefix: &[u8],
    left_edge: u8,
    left_child: TaggedPointer,
    right_edge: u8,
    right_child: TaggedPointer,
) -> TaggedPointer {
    if prefix.len() <= 8 {
        let mut node = Node4::new(prefix);
        let _ = node.insert(left_edge, left_child);
        let _ = node.insert(right_edge, right_child);
        return TaggedPointer::from_node4(Box::new(node));
    }

    let mut node = Node4::new(&prefix[..8]);
    let child = new_branching_path(&prefix[9..], left_edge, left_child, right_edge, right_child);
    let _ = node.insert(prefix[8], child);
    TaggedPointer::from_node4(Box::new(node))
}

fn terminated_key_owned(key: &[u8]) -> Vec<u8> {
    if key.last() == Some(&0) {
        return key.to_vec();
    }

    let mut terminated = Vec::with_capacity(key.len() + 1);
    terminated.extend_from_slice(key);
    terminated.push(0);
    terminated
}

// Header for a leaf allocation. The actual key and value bytes follow immediately
// after this header in memory (`data` is a zero-length flexible array marker).
//
// Layout (16-byte aligned): `[key_len: u8][_pad: 3][value_len: u32][key bytes...][value bytes...]`
verus! {

#[repr(C, align(16))]
pub struct KVData {
    key_len: u8,
    _pad: [u8; 3],
    value_len: u32,
    data: [u8; 0],
}

/// Single-allocation key-value pair handle.
pub struct KVPairOwned(*mut KVData);

impl KVData {
    #[verifier::external_body]
    pub fn key(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.key_len as usize) }
    }

    #[verifier::external_body]
    pub fn value(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr().add(self.key_len as usize),
                self.value_len as usize,
            )
        }
    }
}

impl KVPairOwned {
    #[verifier::external_body]
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        let data_offset = std::mem::size_of::<KVData>();
        let total_size = data_offset + key.len() + value.len();
        let layout = Layout::from_size_align(total_size.max(1), 16).unwrap();
        let ptr = unsafe { std::alloc::alloc(layout) } as *mut KVData;
        let ptr = std::ptr::NonNull::new(ptr).unwrap().as_ptr();

        unsafe {
            let header = ptr;
            (*header).key_len = key.len() as u8;
            (*header)._pad = [0; 3];
            (*header).value_len = value.len() as u32;
            let data = (*header).data.as_mut_ptr();
            copy_nonoverlapping(key.as_ptr(), data, key.len());
            copy_nonoverlapping(value.as_ptr(), data.add(key.len()), value.len());
        }

        Self(ptr)
    }

    #[verifier::external_body]
    pub fn key(&self) -> &[u8] {
        unsafe { &*self.0 }.key()
    }

    #[verifier::external_body]
    pub fn value(&self) -> &[u8] {
        unsafe { &*self.0 }.value()
    }

    #[verifier::external_body]
    pub fn into_raw(self) -> *mut KVData {
        let ptr = self.0;
        std::mem::forget(self);
        ptr
    }

    #[verifier::external_body]
    pub unsafe fn from_raw(ptr: *mut KVData) -> Self {
        Self(ptr)
    }
}

impl Drop for KVPairOwned {
    #[verifier::external_body]
    fn drop(&mut self)
        opens_invariants none
        no_unwind
    {
        unsafe {
            let header = self.0;
            let key_len = (*header).key_len as usize;
            let value_len = (*header).value_len as usize;
            let data_offset = std::mem::size_of::<KVData>();
            let total_size = data_offset + key_len + value_len;
            let layout = Layout::from_size_align(total_size.max(1), 16).unwrap();
            std::alloc::dealloc(header as *mut u8, layout);
        }
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
