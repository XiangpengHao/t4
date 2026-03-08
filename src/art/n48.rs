use crate::art::{
    ArtNode, InsertStep,
    art::common_prefix_len,
    meta::{NodeMeta, NodeType},
    n256::Node256,
    ptr::TaggedPointer,
};
use std::mem::MaybeUninit;

#[repr(C, align(16))]
pub(crate) struct Node48 {
    meta: NodeMeta,
    child_idx: [u8; 256],
    children: [MaybeUninit<TaggedPointer>; 48],
}

impl Node48 {
    pub(crate) fn new(prefix: &[u8]) -> Self {
        let meta = NodeMeta::new(NodeType::Node48, prefix);
        Self {
            meta,
            child_idx: [0; 256],
            children: [const { MaybeUninit::uninit() }; 48],
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> Option<TaggedPointer> {
        let key_idx = key as usize;
        let child_idx = self.child_idx[key_idx];

        if child_idx != 0 {
            let slot = (child_idx - 1) as usize;
            let old = self.children[slot];
            self.children[slot] = MaybeUninit::new(value);
            return Some(unsafe { old.assume_init() });
        }

        let len = self.meta.len();
        assert!(len < self.children.len(), "Node48 is full");

        self.child_idx[key_idx] = (len + 1) as u8;
        self.children[len] = MaybeUninit::new(value);
        self.meta.increment_len();
        None
    }

    pub(crate) fn get(&self, key: u8) -> Option<TaggedPointer> {
        let child_idx = self.child_idx[key as usize];
        if child_idx == 0 {
            return None;
        }

        Some(unsafe { self.children[(child_idx - 1) as usize].assume_init() })
    }

    pub(crate) fn remove(&mut self, key: u8) -> Option<TaggedPointer> {
        let key_idx = key as usize;
        let child_idx = self.child_idx[key_idx];
        if child_idx == 0 {
            return None;
        }

        let slot = (child_idx - 1) as usize;
        let len = self.meta.len();
        let removed = self.children[slot];
        let last_slot = len - 1;

        self.child_idx[key_idx] = 0;
        if slot != last_slot {
            let moved = self.children[last_slot];
            self.children[slot] = moved;
            for idx in 0..=u8::MAX {
                if self.child_idx[idx as usize] == len as u8 {
                    self.child_idx[idx as usize] = child_idx;
                    break;
                }
            }
        }

        self.meta.decrement_len();
        Some(unsafe { removed.assume_init() })
    }

    pub(crate) fn meta_mut(&mut self) -> &mut NodeMeta {
        &mut self.meta
    }

    pub(crate) fn is_full(&self) -> bool {
        self.meta.len() == self.children.len()
    }

    pub(crate) fn for_each_child(&self, mut f: impl FnMut(u8, TaggedPointer)) {
        for key in 0..=u8::MAX {
            if let Some(child) = self.get(key) {
                f(key, child);
            }
        }
    }

    pub(crate) fn grow(&self, prefix: &[u8]) -> TaggedPointer {
        let mut grown = Node256::new(prefix);
        self.for_each_child(|key, child| {
            let _ = grown.insert(key, child);
        });
        TaggedPointer::from_node256(Box::new(grown))
    }
}

impl ArtNode for Node48 {
    fn insert_step(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> InsertStep {
        let prefix_depth = depth;
        let prefix_len = self.meta.prefix_len();
        let matched =
            common_prefix_len(&self.meta.prefix()[..prefix_len], &terminated_key[depth..]);
        if matched != prefix_len {
            return InsertStep::Split { matched };
        }

        let depth = depth + prefix_len;
        let edge = terminated_key[depth];
        if let Some(child) = self.get(edge) {
            return InsertStep::Descend {
                edge,
                child,
                next_depth: depth + 1,
            };
        }

        if self.is_full() {
            return InsertStep::Grow {
                prefix_depth,
                prefix_len,
            };
        }

        let _ = self.insert(edge, value_ptr);
        InsertStep::Done
    }

    fn replace_child(&mut self, edge: u8, child: TaggedPointer) {
        let _ = self.insert(edge, child);
    }

    fn remove_child(&mut self, edge: u8) -> Option<TaggedPointer> {
        self.remove(edge)
    }

    fn child_count(&self) -> usize {
        self.meta.len()
    }

    fn prefix_len(&self) -> usize {
        self.meta.prefix_len()
    }

    fn prefix(&self) -> [u8; 8] {
        self.meta.prefix()
    }

    fn set_prefix(&mut self, prefix: &[u8]) {
        self.meta_mut().set_prefix(prefix);
    }

    fn get_child(&self, edge: u8) -> Option<TaggedPointer> {
        self.get(edge)
    }
}

#[cfg(test)]
mod tests {
    use super::Node48;
    use crate::art::ptr::TaggedPointer;

    #[test]
    fn insert_and_get_sparse_keys() {
        let mut node = Node48::new(b"");

        node.insert(200, TaggedPointer::from_raw(200));
        node.insert(3, TaggedPointer::from_raw(3));
        node.insert(128, TaggedPointer::from_raw(128));

        assert_eq!(node.meta.len(), 3);
        assert_eq!(node.get(3), Some(TaggedPointer::from_raw(3)));
        assert_eq!(node.get(128), Some(TaggedPointer::from_raw(128)));
        assert_eq!(node.get(200), Some(TaggedPointer::from_raw(200)));
        assert_eq!(node.get(42), None);
    }

    #[test]
    fn insert_replaces_existing_child() {
        let mut node = Node48::new(b"");

        assert_eq!(node.insert(7, TaggedPointer::from_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_raw(2)),
            Some(TaggedPointer::from_raw(1))
        );
        assert_eq!(node.meta.len(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_raw(2)));
    }

    #[test]
    fn remove_deletes_sparse_child() {
        let mut node = Node48::new(b"");

        node.insert(200, TaggedPointer::from_raw(200));
        node.insert(3, TaggedPointer::from_raw(3));
        node.insert(128, TaggedPointer::from_raw(128));

        assert_eq!(node.remove(3), Some(TaggedPointer::from_raw(3)));
        assert_eq!(node.get(3), None);
        assert_eq!(node.get(128), Some(TaggedPointer::from_raw(128)));
        assert_eq!(node.get(200), Some(TaggedPointer::from_raw(200)));
    }
}
