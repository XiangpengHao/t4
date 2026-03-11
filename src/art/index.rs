use std::{
    alloc::Layout,
    ptr::{NonNull, copy_nonoverlapping},
};

use vstd::prelude::*;

use crate::art::{
    ArtNode, InsertStep, delete_from_node, get_from_node,
    n4::Node4,
    n16::Node16,
    n48::Node48,
    n256::Node256,
    ptr::{NextNode, TaggedPointer},
};

pub struct ArtIndex {
    root: Option<TaggedPointer>,
}

impl ArtIndex {
    pub fn new() -> Self {
        Self { root: None }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<KVPair> {
        let terminated_key = terminated_key_owned(key);
        let value_ptr = TaggedPointer::from_value(KVPair::new(key, value));
        let mut parent = Parent::Root(std::ptr::addr_of_mut!(self.root));
        let mut current = self.root;
        let mut depth = 0;

        loop {
            let Some(current_ptr) = current else {
                update_parent(parent, value_ptr);
                return None;
            };

            match current_ptr.next_node() {
                NextNode::Value(existing_ptr) => {
                    let terminated_existing = terminated_key_owned(unsafe { &*existing_ptr }.key());
                    if terminated_existing == terminated_key {
                        update_parent(parent, value_ptr);
                        return Some(unsafe { KVPair::from_raw(existing_ptr) });
                    }

                    let shared =
                        common_prefix_len(&terminated_existing[depth..], &terminated_key[depth..]);
                    let split = new_branching_path(
                        &terminated_key[depth..depth + shared],
                        terminated_existing[depth + shared],
                        current_ptr,
                        terminated_key[depth + shared],
                        value_ptr,
                    );
                    update_parent(parent, split);
                    return None;
                }
                NextNode::Node4(node_ptr) => {
                    let node = unsafe { &mut *node_ptr };
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
                            update_parent(parent, replacement);
                            return None;
                        }
                        InsertStep::Descend {
                            edge,
                            child,
                            next_depth,
                        } => {
                            parent = Parent::Node4(node_ptr, edge);
                            current = Some(child);
                            depth = next_depth;
                        }
                        InsertStep::Grow {
                            prefix_depth,
                            prefix_len,
                        } => {
                            let replacement = unsafe {
                                (&*node_ptr)
                                    .grow(&terminated_key[prefix_depth..prefix_depth + prefix_len])
                            };
                            update_parent(parent, replacement);
                            drop(unsafe { Box::from_raw(node_ptr) });
                            current = Some(replacement);
                            depth = prefix_depth;
                        }
                        InsertStep::Done => return None,
                    }
                }
                NextNode::Node16(node_ptr) => {
                    let node = unsafe { &mut *node_ptr };
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
                            update_parent(parent, replacement);
                            return None;
                        }
                        InsertStep::Descend {
                            edge,
                            child,
                            next_depth,
                        } => {
                            parent = Parent::Node16(node_ptr, edge);
                            current = Some(child);
                            depth = next_depth;
                        }
                        InsertStep::Grow {
                            prefix_depth,
                            prefix_len,
                        } => {
                            let replacement = unsafe {
                                (&*node_ptr)
                                    .grow(&terminated_key[prefix_depth..prefix_depth + prefix_len])
                            };
                            update_parent(parent, replacement);
                            drop(unsafe { Box::from_raw(node_ptr) });
                            current = Some(replacement);
                            depth = prefix_depth;
                        }
                        InsertStep::Done => return None,
                    }
                }
                NextNode::Node48(node_ptr) => {
                    let node = unsafe { &mut *node_ptr };
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
                            update_parent(parent, replacement);
                            return None;
                        }
                        InsertStep::Descend {
                            edge,
                            child,
                            next_depth,
                        } => {
                            parent = Parent::Node48(node_ptr, edge);
                            current = Some(child);
                            depth = next_depth;
                        }
                        InsertStep::Grow {
                            prefix_depth,
                            prefix_len,
                        } => {
                            let replacement = unsafe {
                                (&*node_ptr)
                                    .grow(&terminated_key[prefix_depth..prefix_depth + prefix_len])
                            };
                            update_parent(parent, replacement);
                            drop(unsafe { Box::from_raw(node_ptr) });
                            current = Some(replacement);
                            depth = prefix_depth;
                        }
                        InsertStep::Done => return None,
                    }
                }
                NextNode::Node256(node_ptr) => {
                    let node = unsafe { &mut *node_ptr };
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
                            update_parent(parent, replacement);
                            return None;
                        }
                        InsertStep::Descend {
                            edge,
                            child,
                            next_depth,
                        } => {
                            parent = Parent::Node256(node_ptr, edge);
                            current = Some(child);
                            depth = next_depth;
                        }
                        InsertStep::Grow { .. } => {
                            unreachable!()
                        }
                        InsertStep::Done => return None,
                    }
                }
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<(&[u8], &[u8])> {
        let terminated_key = terminated_key_owned(key);
        let mut ptr = self.root;
        let mut depth = 0;

        loop {
            let ptr_value = ptr?;
            match ptr_value.next_node() {
                NextNode::Value(value_ptr) => {
                    let leaf = unsafe { &*value_ptr };
                    return if terminated_key_owned(leaf.key()) == terminated_key {
                        Some((leaf.key(), leaf.value()))
                    } else {
                        None
                    };
                }
                NextNode::Node4(node_ptr) => {
                    let (next_ptr, next_depth) =
                        unsafe { get_from_node(&*node_ptr, &terminated_key, depth) }?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                }
                NextNode::Node16(node_ptr) => {
                    let (next_ptr, next_depth) =
                        unsafe { get_from_node(&*node_ptr, &terminated_key, depth) }?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                }
                NextNode::Node48(node_ptr) => {
                    let (next_ptr, next_depth) =
                        unsafe { get_from_node(&*node_ptr, &terminated_key, depth) }?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                }
                NextNode::Node256(node_ptr) => {
                    let (next_ptr, next_depth) =
                        unsafe { get_from_node(&*node_ptr, &terminated_key, depth) }?;
                    ptr = Some(next_ptr);
                    depth = next_depth;
                }
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Option<KVPair> {
        let terminated_key = terminated_key_owned(key);
        let result = delete_at(self.root, &terminated_key, 0);
        match result {
            DeleteResult::NotFound { current } => {
                self.root = current;
                None
            }
            DeleteResult::Deleted {
                removed,
                replacement,
            } => {
                self.root = replacement;
                Some(match removed.next_node() {
                    NextNode::Value(value_ptr) => unsafe { KVPair::from_raw(value_ptr) },
                    _ => unreachable!(),
                })
            }
        }
    }
}

impl Default for ArtIndex {
    fn default() -> Self {
        Self::new()
    }
}

unsafe fn free_subtree(ptr: TaggedPointer) {
    unsafe {
        match ptr.next_node() {
            NextNode::Value(leaf_ptr) => {
                drop(KVPair::from_raw(leaf_ptr));
            }
            NextNode::Node4(node_ptr) => {
                let node = Box::from_raw(node_ptr);
                node.for_each_child(|_, child| free_subtree(child));
            }
            NextNode::Node16(node_ptr) => {
                let node = Box::from_raw(node_ptr);
                node.for_each_child(|_, child| free_subtree(child));
            }
            NextNode::Node48(node_ptr) => {
                let node = Box::from_raw(node_ptr);
                node.for_each_child(|child| free_subtree(child));
            }
            NextNode::Node256(node_ptr) => {
                let node = Box::from_raw(node_ptr);
                node.for_each_child(|child| free_subtree(child));
            }
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

#[derive(Clone, Copy)]
enum Parent {
    Root(*mut Option<TaggedPointer>),
    Node4(*mut Node4, u8),
    Node16(*mut Node16, u8),
    Node48(*mut Node48, u8),
    Node256(*mut Node256, u8),
}

fn update_parent(parent: Parent, value: TaggedPointer) {
    match parent {
        Parent::Root(slot) => unsafe { *slot = Some(value) },
        Parent::Node4(node_ptr, edge) => unsafe { (&mut *node_ptr).replace_child(edge, value) },
        Parent::Node16(node_ptr, edge) => unsafe { (&mut *node_ptr).replace_child(edge, value) },
        Parent::Node48(node_ptr, edge) => unsafe { (&mut *node_ptr).replace_child(edge, value) },
        Parent::Node256(node_ptr, edge) => unsafe { (&mut *node_ptr).replace_child(edge, value) },
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

pub assume_specification[ delete_at ](
    current: Option<TaggedPointer>,
    terminated_key: &[u8],
    depth: usize,
) -> (result: DeleteResult);

pub(crate) fn common_prefix_len(a: &[u8], b: &[u8]) -> (result: usize)
    ensures
        result <= a.len(),
        result <= b.len(),
        forall|i: int| 0 <= i < result ==> a[i] == b[i],
{
    let limit = if a.len() < b.len() { a.len() } else { b.len() };
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

pub(crate) fn delete_at(
    current: Option<TaggedPointer>,
    terminated_key: &[u8],
    depth: usize,
) -> DeleteResult {
    let Some(current) = current else {
        return DeleteResult::NotFound { current: None };
    };

    match current.next_node() {
        NextNode::Value(value_ptr) => {
            if terminated_key_owned(unsafe { &*value_ptr }.key()) != terminated_key {
                return DeleteResult::NotFound {
                    current: Some(current),
                };
            }

            DeleteResult::Deleted {
                removed: current,
                replacement: None,
            }
        }
        NextNode::Node4(node_ptr) => unsafe {
            delete_from_node(&mut *node_ptr, current, terminated_key, depth)
        },
        NextNode::Node16(node_ptr) => unsafe {
            delete_from_node(&mut *node_ptr, current, terminated_key, depth)
        },
        NextNode::Node48(node_ptr) => unsafe {
            delete_from_node(&mut *node_ptr, current, terminated_key, depth)
        },
        NextNode::Node256(node_ptr) => unsafe {
            delete_from_node(&mut *node_ptr, current, terminated_key, depth)
        },
    }
}

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

/// Header for a leaf allocation. The actual key and value bytes follow immediately
/// after this header in memory (`data` is a zero-length flexible array marker).
///
/// Layout (16-byte aligned): `[key_len: u8][_pad: 3][value_len: u32][key bytes...][value bytes...]`
#[repr(C, align(16))]
pub struct KVData {
    key_len: u8,
    _pad: [u8; 3],
    value_len: u32,
    data: [u8; 0],
}

impl KVData {
    pub fn key(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.key_len as usize) }
    }

    pub fn value(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr().add(self.key_len as usize),
                self.value_len as usize,
            )
        }
    }
}

/// Single-allocation key-value pair handle.
pub struct KVPair(NonNull<KVData>);

impl KVPair {
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        let data_offset = std::mem::size_of::<KVData>();
        let total_size = data_offset + key.len() + value.len();
        let layout = Layout::from_size_align(total_size.max(1), 16).unwrap();
        let ptr = unsafe { std::alloc::alloc(layout) } as *mut KVData;
        let ptr = NonNull::new(ptr).unwrap();

        unsafe {
            let header = ptr.as_ptr();
            (*header).key_len = key.len() as u8;
            (*header)._pad = [0; 3];
            (*header).value_len = value.len() as u32;
            let data = (*header).data.as_mut_ptr();
            copy_nonoverlapping(key.as_ptr(), data, key.len());
            copy_nonoverlapping(value.as_ptr(), data.add(key.len()), value.len());
        }

        Self(ptr)
    }

    pub fn key(&self) -> &[u8] {
        unsafe { &*self.0.as_ptr() }.key()
    }

    pub fn value(&self) -> &[u8] {
        unsafe { &*self.0.as_ptr() }.value()
    }

    pub fn into_raw(self) -> *mut KVData {
        let ptr = self.0.as_ptr();
        std::mem::forget(self);
        ptr
    }

    pub unsafe fn from_raw(ptr: *mut KVData) -> Self {
        unsafe { Self(NonNull::new_unchecked(ptr)) }
    }
}

impl Drop for KVPair {
    fn drop(&mut self) {
        unsafe {
            let header = self.0.as_ptr();
            let key_len = (*header).key_len as usize;
            let value_len = (*header).value_len as usize;
            let data_offset = std::mem::size_of::<KVData>();
            let total_size = data_offset + key_len + value_len;
            let layout = Layout::from_size_align(total_size.max(1), 16).unwrap();
            std::alloc::dealloc(header as *mut u8, layout);
        }
    }
}

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
