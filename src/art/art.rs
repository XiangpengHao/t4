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
    pub fn new() -> Self {
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
                    let split = new_branching_path(
                        &terminated_key[depth..depth + shared],
                        terminated_existing[depth + shared],
                        current,
                        terminated_key[depth + shared],
                        value_ptr,
                    );
                    update_parent(parent, split);
                    return;
                }
                NextNode::Node4(node_ptr) => {
                    let node = unsafe { &mut *node_ptr };
                    let step = node.insert_step(&terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current,
                                &terminated_key,
                                value_ptr,
                                depth,
                                matched,
                            );
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
                    let node = unsafe { &mut *node_ptr };
                    let step = node.insert_step(&terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current,
                                &terminated_key,
                                value_ptr,
                                depth,
                                matched,
                            );
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
                    let node = unsafe { &mut *node_ptr };
                    let step = node.insert_step(&terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current,
                                &terminated_key,
                                value_ptr,
                                depth,
                                matched,
                            );
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
                    let node = unsafe { &mut *node_ptr };
                    let step = node.insert_step(&terminated_key, value_ptr, depth);
                    match step {
                        InsertStep::Split { matched } => {
                            let replacement = split_node(
                                node,
                                current,
                                &terminated_key,
                                value_ptr,
                                depth,
                                matched,
                            );
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

pub(crate) fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .zip(b.iter())
        .take_while(|(lhs, rhs)| lhs == rhs)
        .count()
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

        assert_eq!(
            index.get(b"123456789abcdef-left").expect("left").value(),
            b"left"
        );
        assert_eq!(
            index.get(b"123456789abcdef-right").expect("right").value(),
            b"right"
        );
        assert!(index.get(b"123456789abcdef-middle").is_none());
    }
}
