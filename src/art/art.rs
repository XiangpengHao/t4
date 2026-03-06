use std::{
    alloc::Layout,
    ptr::{NonNull, copy_nonoverlapping},
};

use crate::art::{
    n16::Node16,
    n256::Node256,
    n4::Node4,
    n48::Node48,
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
        let value_ptr = TaggedPointer::from_value(Box::new(KVPair::new(key, value)));
        insert_at(&mut self.root, key, value_ptr, 0);
    }

    pub fn get(&self, key: &[u8]) -> Option<KVPair> {
        get_at(self.root, key, 0)
    }
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

        let key_ptr = ptr.as_ptr() as *mut u8;
        unsafe { copy_nonoverlapping(key.as_ptr(), key_ptr, key.len()) };
        let value_ptr = unsafe { key_ptr.add(key.len() as usize) as *mut u8 };
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

fn insert_at(slot: &mut TaggedPointer, key: &[u8], value_ptr: TaggedPointer, depth: usize) {
    if slot.is_null() {
        *slot = value_ptr;
        return;
    }

    match slot.next_node() {
        NextNode::Value(existing_ptr) => {
            let existing = unsafe { &*existing_ptr };
            if existing.key() == key {
                *slot = value_ptr;
                return;
            }

            let shared = common_prefix_len(&existing.key()[depth..], &key[depth..]);
            let mut parent = Node4::new();
            parent.meta_mut().set_prefix(&key[depth..depth + shared]);

            if depth + shared == existing.key().len() {
                parent.set_terminal(*slot);
            } else {
                parent.insert(existing.key()[depth + shared], *slot);
            }

            if depth + shared == key.len() {
                parent.set_terminal(value_ptr);
            } else {
                parent.insert(key[depth + shared], value_ptr);
            }

            *slot = TaggedPointer::from_node4(Box::new(parent));
        }
        NextNode::Node4(node_ptr) => unsafe {
            insert_into_node4(&mut *node_ptr, slot, key, value_ptr, depth);
        },
        NextNode::Node16(node_ptr) => unsafe {
            insert_into_node16(&mut *node_ptr, slot, key, value_ptr, depth);
        },
        NextNode::Node48(node_ptr) => unsafe {
            insert_into_node48(&mut *node_ptr, slot, key, value_ptr, depth);
        },
        NextNode::Node256(node_ptr) => unsafe {
            insert_into_node256(&mut *node_ptr, slot, key, value_ptr, depth);
        },
    }
}

fn get_at(ptr: TaggedPointer, key: &[u8], depth: usize) -> Option<KVPair> {
    if ptr.is_null() {
        return None;
    }

    match ptr.next_node() {
        NextNode::Value(value_ptr) => {
            let value = unsafe { &*value_ptr };
            if value.key() == key {
                Some(*value)
            } else {
                None
            }
        }
        NextNode::Node4(node_ptr) => unsafe { get_from_node4(&*node_ptr, key, depth) },
        NextNode::Node16(node_ptr) => unsafe { get_from_node16(&*node_ptr, key, depth) },
        NextNode::Node48(node_ptr) => unsafe { get_from_node48(&*node_ptr, key, depth) },
        NextNode::Node256(node_ptr) => unsafe { get_from_node256(&*node_ptr, key, depth) },
    }
}

fn get_from_node4(node: &Node4, key: &[u8], depth: usize) -> Option<KVPair> {
    get_from_node_common(
        node.meta().prefix_len(),
        node.meta().prefix(),
        node.first_child(),
        node.terminal(),
        |edge| node.get(edge),
        key,
        depth,
    )
}

fn get_from_node16(node: &Node16, key: &[u8], depth: usize) -> Option<KVPair> {
    get_from_node_common(
        node.meta().prefix_len(),
        node.meta().prefix(),
        node.first_child(),
        node.terminal(),
        |edge| node.get(edge),
        key,
        depth,
    )
}

fn get_from_node48(node: &Node48, key: &[u8], depth: usize) -> Option<KVPair> {
    get_from_node_common(
        node.meta().prefix_len(),
        node.meta().prefix(),
        node.first_child(),
        node.terminal(),
        |edge| node.get(edge),
        key,
        depth,
    )
}

fn get_from_node256(node: &Node256, key: &[u8], depth: usize) -> Option<KVPair> {
    get_from_node_common(
        node.meta().prefix_len(),
        node.meta().prefix(),
        node.first_child(),
        node.terminal(),
        |edge| node.get(edge),
        key,
        depth,
    )
}

fn get_from_node_common(
    prefix_len: usize,
    inline_prefix: [u8; 8],
    first_child: Option<TaggedPointer>,
    terminal: TaggedPointer,
    get_child: impl Fn(u8) -> Option<TaggedPointer>,
    key: &[u8],
    depth: usize,
) -> Option<KVPair> {
    let matched = match_prefix(prefix_len, inline_prefix, first_child, key, depth);
    if matched != prefix_len {
        return None;
    }

    let depth = depth + prefix_len;
    if depth == key.len() {
        if terminal.is_null() {
            return None;
        }
        return get_at(terminal, key, key.len());
    }

    let child = get_child(key[depth])?;
    get_at(child, key, depth + 1)
}

fn insert_into_node4(
    node: &mut Node4,
    slot: &mut TaggedPointer,
    key: &[u8],
    value_ptr: TaggedPointer,
    depth: usize,
) {
    let prefix_depth = depth;
    let prefix_len = node.meta().prefix_len();
    let matched = match_prefix(prefix_len, node.meta().prefix(), node.first_child(), key, depth);
    if matched != prefix_len {
        split_node(slot, key, value_ptr, depth, matched);
        return;
    }

    let depth = depth + prefix_len;
    if depth == key.len() {
        node.set_terminal(value_ptr);
        return;
    }

    let edge = key[depth];
    if let Some(mut child) = node.get(edge) {
        insert_at(&mut child, key, value_ptr, depth + 1);
        let _ = node.insert(edge, child);
        return;
    }

    if node.is_full() {
        grow_node(slot, &key[prefix_depth..prefix_depth + prefix_len]);
        insert_at(slot, key, value_ptr, prefix_depth);
        return;
    }

    let _ = node.insert(edge, value_ptr);
}

fn insert_into_node16(
    node: &mut Node16,
    slot: &mut TaggedPointer,
    key: &[u8],
    value_ptr: TaggedPointer,
    depth: usize,
) {
    let prefix_depth = depth;
    let prefix_len = node.meta().prefix_len();
    let matched = match_prefix(prefix_len, node.meta().prefix(), node.first_child(), key, depth);
    if matched != prefix_len {
        split_node(slot, key, value_ptr, depth, matched);
        return;
    }

    let depth = depth + prefix_len;
    if depth == key.len() {
        node.set_terminal(value_ptr);
        return;
    }

    let edge = key[depth];
    if let Some(mut child) = node.get(edge) {
        insert_at(&mut child, key, value_ptr, depth + 1);
        let _ = node.insert(edge, child);
        return;
    }

    if node.is_full() {
        grow_node(slot, &key[prefix_depth..prefix_depth + prefix_len]);
        insert_at(slot, key, value_ptr, prefix_depth);
        return;
    }

    let _ = node.insert(edge, value_ptr);
}

fn insert_into_node48(
    node: &mut Node48,
    slot: &mut TaggedPointer,
    key: &[u8],
    value_ptr: TaggedPointer,
    depth: usize,
) {
    let prefix_depth = depth;
    let prefix_len = node.meta().prefix_len();
    let matched = match_prefix(prefix_len, node.meta().prefix(), node.first_child(), key, depth);
    if matched != prefix_len {
        split_node(slot, key, value_ptr, depth, matched);
        return;
    }

    let depth = depth + prefix_len;
    if depth == key.len() {
        node.set_terminal(value_ptr);
        return;
    }

    let edge = key[depth];
    if let Some(mut child) = node.get(edge) {
        insert_at(&mut child, key, value_ptr, depth + 1);
        let _ = node.insert(edge, child);
        return;
    }

    if node.is_full() {
        grow_node(slot, &key[prefix_depth..prefix_depth + prefix_len]);
        insert_at(slot, key, value_ptr, prefix_depth);
        return;
    }

    let _ = node.insert(edge, value_ptr);
}

fn insert_into_node256(
    node: &mut Node256,
    slot: &mut TaggedPointer,
    key: &[u8],
    value_ptr: TaggedPointer,
    depth: usize,
) {
    let matched = match_prefix(
        node.meta().prefix_len(),
        node.meta().prefix(),
        node.first_child(),
        key,
        depth,
    );
    if matched != node.meta().prefix_len() {
        split_node(slot, key, value_ptr, depth, matched);
        return;
    }

    let depth = depth + node.meta().prefix_len();
    if depth == key.len() {
        node.set_terminal(value_ptr);
        return;
    }

    let edge = key[depth];
    let mut child = node.get(edge).unwrap_or_default();
    insert_at(&mut child, key, value_ptr, depth + 1);
    let _ = node.insert(edge, child);
}

fn split_node(slot: &mut TaggedPointer, key: &[u8], value_ptr: TaggedPointer, depth: usize, matched: usize) {
    let old_ptr = *slot;
    let reference_key = representative_key(old_ptr);
    let old_prefix_len = node_prefix_len(old_ptr);

    let mut parent = Node4::new();
    parent
        .meta_mut()
        .set_prefix(&reference_key[depth..depth + matched]);

    let old_branch = reference_key[depth + matched];
    rewrite_node_prefix(old_ptr, &reference_key[depth + matched + 1..depth + old_prefix_len]);
    parent.insert(old_branch, old_ptr);

    if depth + matched == key.len() {
        parent.set_terminal(value_ptr);
    } else {
        parent.insert(key[depth + matched], value_ptr);
    }

    *slot = TaggedPointer::from_node4(Box::new(parent));
}

fn grow_node(slot: &mut TaggedPointer, prefix: &[u8]) {
    match slot.next_node() {
        NextNode::Node4(node_ptr) => unsafe {
            let node = &*node_ptr;
            let mut grown = Node16::new();
            grown.meta_mut().set_prefix(prefix);
            grown.set_terminal(node.terminal());
            node.for_each_child(|key, child| {
                let _ = grown.insert(key, child);
            });
            *slot = TaggedPointer::from_node16(Box::new(grown));
        },
        NextNode::Node16(node_ptr) => unsafe {
            let node = &*node_ptr;
            let mut grown = Node48::new();
            grown.meta_mut().set_prefix(prefix);
            grown.set_terminal(node.terminal());
            node.for_each_child(|key, child| {
                let _ = grown.insert(key, child);
            });
            *slot = TaggedPointer::from_node48(Box::new(grown));
        },
        NextNode::Node48(node_ptr) => unsafe {
            let node = &*node_ptr;
            let mut grown = Node256::new();
            grown.meta_mut().set_prefix(prefix);
            grown.set_terminal(node.terminal());
            node.for_each_child(|key, child| {
                let _ = grown.insert(key, child);
            });
            *slot = TaggedPointer::from_node256(Box::new(grown));
        },
        NextNode::Node256(_) => panic!("Node256 is full"),
        NextNode::Value(_) => panic!("Cannot grow a value"),
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .zip(b.iter())
        .take_while(|(lhs, rhs)| lhs == rhs)
        .count()
}

fn match_prefix(
    prefix_len: usize,
    inline_prefix: [u8; 8],
    first_child: Option<TaggedPointer>,
    key: &[u8],
    depth: usize,
) -> usize {
    let available = key.len().saturating_sub(depth);
    let compare_len = prefix_len.min(available);
    let inline_len = compare_len.min(inline_prefix.len());

    for idx in 0..inline_len {
        if key[depth + idx] != inline_prefix[idx] {
            return idx;
        }
    }

    if compare_len <= inline_prefix.len() {
        return compare_len;
    }

    let reference = representative_key(first_child.expect("node with long prefix must have a value"));
    for idx in inline_prefix.len()..compare_len {
        if key[depth + idx] != reference[depth + idx] {
            return idx;
        }
    }

    compare_len
}

fn representative_key(ptr: TaggedPointer) -> &'static [u8] {
    match ptr.next_node() {
        NextNode::Value(value_ptr) => unsafe { (&*value_ptr).key() },
        NextNode::Node4(node_ptr) => unsafe {
            let node = &*node_ptr;
            if !node.terminal().is_null() {
                representative_key(node.terminal())
            } else {
                representative_key(node.first_child().expect("node has no children"))
            }
        },
        NextNode::Node16(node_ptr) => unsafe {
            let node = &*node_ptr;
            if !node.terminal().is_null() {
                representative_key(node.terminal())
            } else {
                representative_key(node.first_child().expect("node has no children"))
            }
        },
        NextNode::Node48(node_ptr) => unsafe {
            let node = &*node_ptr;
            if !node.terminal().is_null() {
                representative_key(node.terminal())
            } else {
                representative_key(node.first_child().expect("node has no children"))
            }
        },
        NextNode::Node256(node_ptr) => unsafe {
            let node = &*node_ptr;
            if !node.terminal().is_null() {
                representative_key(node.terminal())
            } else {
                representative_key(node.first_child().expect("node has no children"))
            }
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
}
