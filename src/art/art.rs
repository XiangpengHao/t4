use std::{
    alloc::Layout,
    ptr::{NonNull, copy_nonoverlapping},
};

use crate::art::{
    ArtNode, InsertStep,
    n4::Node4,
    n16::Node16,
    n48::Node48,
    n256::Node256,
    ptr::{NextNode, TaggedPointer},
};

pub struct ArtIndex {
    root: TaggedPointer,
}

impl ArtIndex {
    pub(crate) fn new() -> Self {
        Self {
            root: TaggedPointer::default(),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        let terminated_key = terminated_key_owned(key);
        let value_ptr = TaggedPointer::from_value(Box::new(KVPair::new(key, value)));
        let mut parent = Parent::Root(std::ptr::addr_of_mut!(self.root));
        let mut current = self.root;
        let mut depth = 0;

        loop {
            if current.is_null() {
                update_parent(parent, value_ptr);
                return;
            }

            match current.next_node() {
                NextNode::Value(existing_ptr) => {
                    let existing = unsafe { &*existing_ptr };
                    let terminated_existing = terminated_key_owned(existing.key());
                    if terminated_existing == terminated_key {
                        update_parent(parent, value_ptr);
                        return;
                    }

                    let shared =
                        common_prefix_len(&terminated_existing[depth..], &terminated_key[depth..]);
                    let mut split = Node4::new(&terminated_key[depth..depth + shared]);
                    let _ = split.insert(terminated_existing[depth + shared], current);
                    let _ = split.insert(terminated_key[depth + shared], value_ptr);
                    update_parent(parent, TaggedPointer::from_node4(Box::new(split)));
                    return;
                }
                NextNode::Node4(node_ptr) => {
                    let step =
                        unsafe { (&mut *node_ptr).insert_step(&terminated_key, value_ptr, depth) };
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement =
                                split_node(current, &terminated_key, value_ptr, depth, matched);
                            update_parent(parent, replacement);
                            return;
                        }
                        InsertStep::Descend {
                            edge,
                            child,
                            next_depth,
                        } => {
                            parent = Parent::Node4(node_ptr, edge);
                            current = child;
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
                            current = replacement;
                            depth = prefix_depth;
                        }
                        InsertStep::Done => return,
                    }
                }
                NextNode::Node16(node_ptr) => {
                    let step =
                        unsafe { (&mut *node_ptr).insert_step(&terminated_key, value_ptr, depth) };
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement =
                                split_node(current, &terminated_key, value_ptr, depth, matched);
                            update_parent(parent, replacement);
                            return;
                        }
                        InsertStep::Descend {
                            edge,
                            child,
                            next_depth,
                        } => {
                            parent = Parent::Node16(node_ptr, edge);
                            current = child;
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
                            current = replacement;
                            depth = prefix_depth;
                        }
                        InsertStep::Done => return,
                    }
                }
                NextNode::Node48(node_ptr) => {
                    let step =
                        unsafe { (&mut *node_ptr).insert_step(&terminated_key, value_ptr, depth) };
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement =
                                split_node(current, &terminated_key, value_ptr, depth, matched);
                            update_parent(parent, replacement);
                            return;
                        }
                        InsertStep::Descend {
                            edge,
                            child,
                            next_depth,
                        } => {
                            parent = Parent::Node48(node_ptr, edge);
                            current = child;
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
                            current = replacement;
                            depth = prefix_depth;
                        }
                        InsertStep::Done => return,
                    }
                }
                NextNode::Node256(node_ptr) => {
                    let step =
                        unsafe { (&mut *node_ptr).insert_step(&terminated_key, value_ptr, depth) };
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement =
                                split_node(current, &terminated_key, value_ptr, depth, matched);
                            update_parent(parent, replacement);
                            return;
                        }
                        InsertStep::Descend {
                            edge,
                            child,
                            next_depth,
                        } => {
                            parent = Parent::Node256(node_ptr, edge);
                            current = child;
                            depth = next_depth;
                        }
                        InsertStep::Grow { .. } => {
                            unreachable!()
                        }
                        InsertStep::Done => return,
                    }
                }
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<KVPair> {
        let terminated_key = terminated_key_owned(key);
        let mut ptr = self.root;
        let mut depth = 0;

        loop {
            if ptr.is_null() {
                return None;
            }

            match ptr.next_node() {
                NextNode::Value(value_ptr) => {
                    let value = unsafe { &*value_ptr };
                    return if terminated_key_owned(value.key()) == terminated_key {
                        Some(*value)
                    } else {
                        None
                    };
                }
                NextNode::Node4(node_ptr) => {
                    (ptr, depth) = unsafe { (&*node_ptr).get_from_node(&terminated_key, depth) }?
                }
                NextNode::Node16(node_ptr) => {
                    (ptr, depth) = unsafe { (&*node_ptr).get_from_node(&terminated_key, depth) }?
                }
                NextNode::Node48(node_ptr) => {
                    (ptr, depth) = unsafe { (&*node_ptr).get_from_node(&terminated_key, depth) }?
                }
                NextNode::Node256(node_ptr) => {
                    (ptr, depth) = unsafe { (&*node_ptr).get_from_node(&terminated_key, depth) }?
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Parent {
    Root(*mut TaggedPointer),
    Node4(*mut Node4, u8),
    Node16(*mut Node16, u8),
    Node48(*mut Node48, u8),
    Node256(*mut Node256, u8),
}

fn update_parent(parent: Parent, value: TaggedPointer) {
    match parent {
        Parent::Root(slot) => unsafe { *slot = value },
        Parent::Node4(node_ptr, edge) => unsafe { (&mut *node_ptr).replace_child(edge, value) },
        Parent::Node16(node_ptr, edge) => unsafe { (&mut *node_ptr).replace_child(edge, value) },
        Parent::Node48(node_ptr, edge) => unsafe { (&mut *node_ptr).replace_child(edge, value) },
        Parent::Node256(node_ptr, edge) => unsafe { (&mut *node_ptr).replace_child(edge, value) },
    }
}

pub(crate) fn split_node(
    old_ptr: TaggedPointer,
    terminated_key: &[u8],
    value_ptr: TaggedPointer,
    depth: usize,
    matched: usize,
) -> TaggedPointer {
    let reference_key = representative_terminated_key(old_ptr);
    let old_prefix_len = node_prefix_len(old_ptr);

    let mut parent = Node4::new(&reference_key[depth..depth + matched]);

    rewrite_node_prefix(
        old_ptr,
        &reference_key[depth + matched + 1..depth + old_prefix_len],
    );
    let _ = parent.insert(reference_key[depth + matched], old_ptr);
    let _ = parent.insert(terminated_key[depth + matched], value_ptr);

    TaggedPointer::from_node4(Box::new(parent))
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .zip(b.iter())
        .take_while(|(lhs, rhs)| lhs == rhs)
        .count()
}

pub(crate) fn match_prefix(
    prefix_len: usize,
    inline_prefix: [u8; 8],
    first_child: Option<TaggedPointer>,
    terminated_key: &[u8],
    depth: usize,
) -> usize {
    let available = terminated_key.len().saturating_sub(depth);
    let compare_len = prefix_len.min(available);
    let inline_len = compare_len.min(inline_prefix.len());

    for idx in 0..inline_len {
        if terminated_key[depth + idx] != inline_prefix[idx] {
            return idx;
        }
    }

    if compare_len <= inline_prefix.len() {
        return compare_len;
    }

    let reference = representative_terminated_key(
        first_child.expect("node with long prefix must have a child"),
    );
    for idx in inline_prefix.len()..compare_len {
        if terminated_key[depth + idx] != reference[depth + idx] {
            return idx;
        }
    }

    compare_len
}

fn representative_terminated_key(ptr: TaggedPointer) -> Vec<u8> {
    match ptr.next_node() {
        NextNode::Value(value_ptr) => {
            let value = unsafe { &*value_ptr };
            terminated_key_owned(value.key())
        }
        NextNode::Node4(node_ptr) => unsafe {
            let node = &*node_ptr;
            representative_terminated_key(node.first_child().expect("node has no children"))
        },
        NextNode::Node16(node_ptr) => unsafe {
            let node = &*node_ptr;
            representative_terminated_key(node.first_child().expect("node has no children"))
        },
        NextNode::Node48(node_ptr) => unsafe {
            let node = &*node_ptr;
            representative_terminated_key(node.first_child().expect("node has no children"))
        },
        NextNode::Node256(node_ptr) => unsafe {
            let node = &*node_ptr;
            representative_terminated_key(node.first_child().expect("node has no children"))
        },
    }
}

fn node_prefix_len(ptr: TaggedPointer) -> usize {
    match ptr.next_node() {
        NextNode::Node4(node_ptr) => unsafe { (*node_ptr).meta().prefix_len() },
        NextNode::Node16(node_ptr) => unsafe { (*node_ptr).meta().prefix_len() },
        NextNode::Node48(node_ptr) => unsafe { (*node_ptr).meta().prefix_len() },
        NextNode::Node256(node_ptr) => unsafe { (*node_ptr).meta().prefix_len() },
        NextNode::Value(_) => 0,
    }
}

fn rewrite_node_prefix(ptr: TaggedPointer, prefix: &[u8]) {
    match ptr.next_node() {
        NextNode::Node4(node_ptr) => unsafe { (*node_ptr).meta_mut().set_prefix(prefix) },
        NextNode::Node16(node_ptr) => unsafe { (*node_ptr).meta_mut().set_prefix(prefix) },
        NextNode::Node48(node_ptr) => unsafe { (*node_ptr).meta_mut().set_prefix(prefix) },
        NextNode::Node256(node_ptr) => unsafe { (*node_ptr).meta_mut().set_prefix(prefix) },
        NextNode::Value(_) => panic!("value has no prefix"),
    }
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

#[repr(C, align(16))]
#[derive(Clone, Copy)]
pub struct KVPair {
    key_len: u8,
    value_len: u8,
    data: NonNull<u8>,
}

impl KVPair {
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        let total_size = key.len() + value.len();
        let layout = Layout::from_size_align(total_size.max(1), 16).unwrap();
        let ptr = unsafe { std::alloc::alloc(layout) };
        let ptr = NonNull::new(ptr).unwrap();

        let key_ptr = ptr.as_ptr();
        unsafe { copy_nonoverlapping(key.as_ptr(), key_ptr, key.len()) };
        let value_ptr = unsafe { key_ptr.add(key.len()) };
        unsafe { copy_nonoverlapping(value.as_ptr(), value_ptr, value.len()) };

        Self {
            data: ptr,
            key_len: key.len() as u8,
            value_len: value.len() as u8,
        }
    }

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

#[cfg(test)]
mod tests {
    use super::ArtIndex;

    #[test]
    fn insert_and_get_single_key() {
        let mut index = ArtIndex::new();

        index.insert(b"hello", b"world");

        let kv = index.get(b"hello").expect("value");
        assert_eq!(kv.key(), b"hello");
        assert_eq!(kv.value(), b"world");
    }

    #[test]
    fn insert_distinguishes_prefix_keys() {
        let mut index = ArtIndex::new();

        index.insert(b"a", b"1");
        index.insert(b"ab", b"2");

        assert_eq!(index.get(b"a").expect("a").value(), b"1");
        assert_eq!(index.get(b"ab").expect("ab").value(), b"2");
        assert!(index.get(b"abc").is_none());
    }

    #[test]
    fn insert_handles_shared_long_prefix() {
        let mut index = ArtIndex::new();

        index.insert(b"prefix-path-alpha", b"alpha");
        index.insert(b"prefix-path-beta", b"beta");

        assert_eq!(
            index.get(b"prefix-path-alpha").expect("alpha").value(),
            b"alpha"
        );
        assert_eq!(
            index.get(b"prefix-path-beta").expect("beta").value(),
            b"beta"
        );
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
            let value = index.get(&key);
            assert!(value.is_some(), "missing key {:?}", key);
            assert_eq!(value.expect("value").value(), [byte]);
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
            let value = index.get(&key);
            assert!(value.is_some(), "missing key {:?}", key);
            assert_eq!(value.expect("value").value(), [byte]);
        }
    }

    #[test]
    fn insert_accepts_explicit_terminator() {
        let mut index = ArtIndex::new();

        index.insert(b"name\0", b"value");

        assert_eq!(index.get(b"name\0").expect("value").value(), b"value");
        assert_eq!(index.get(b"name").expect("value").value(), b"value");
    }
}
