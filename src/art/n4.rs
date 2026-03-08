use crate::art::{
    ArtNode, InsertStep,
    art::common_prefix_len,
    meta::{NodeMeta, NodeType},
    n16::Node16,
    ptr::TaggedPointer,
};
use std::mem::MaybeUninit;

#[repr(C, align(16))]
pub(crate) struct Node4 {
    meta: NodeMeta,
    keys: [u8; 4],
    children: [MaybeUninit<TaggedPointer>; 4],
}

impl Node4 {
    pub(crate) fn new(prefix: &[u8]) -> Self {
        let meta = NodeMeta::new(NodeType::Node4, prefix);
        Self {
            meta,
            keys: [0; 4],
            children: [const { MaybeUninit::uninit() }; 4],
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> Option<TaggedPointer> {
        let len = self.meta.len();

        for idx in 0..len {
            if self.keys[idx] == key {
                let old = self.children[idx];
                self.children[idx] = MaybeUninit::new(value);
                return Some(unsafe { old.assume_init() });
            }
        }

        assert!(len < self.keys.len(), "Node4 is full");

        let insert_at = self.keys[..len].partition_point(|existing| *existing < key);
        for idx in (insert_at..len).rev() {
            self.keys[idx + 1] = self.keys[idx];
            self.children[idx + 1] = self.children[idx];
        }

        self.keys[insert_at] = key;
        self.children[insert_at] = MaybeUninit::new(value);
        self.meta.increment_len();
        None
    }

    pub(crate) fn get(&self, key: u8) -> Option<TaggedPointer> {
        let len = self.meta.len();
        let idx = self.keys[..len]
            .iter()
            .position(|existing| *existing == key)?;
        Some(unsafe { self.children[idx].assume_init() })
    }

    pub(crate) fn remove(&mut self, key: u8) -> Option<TaggedPointer> {
        let len = self.meta.len();
        let idx = self.keys[..len]
            .iter()
            .position(|existing| *existing == key)?;
        let removed = self.children[idx];
        for shift in idx + 1..len {
            self.keys[shift - 1] = self.keys[shift];
            self.children[shift - 1] = self.children[shift];
        }
        self.keys[len - 1] = 0;
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
        let len = self.meta.len();
        for idx in 0..len {
            f(self.keys[idx], unsafe { self.children[idx].assume_init() });
        }
    }

    pub(crate) fn grow(&self, prefix: &[u8]) -> TaggedPointer {
        let mut grown = Node16::new(prefix);
        self.for_each_child(|key, child| {
            let _ = grown.insert(key, child);
        });
        TaggedPointer::from_node16(Box::new(grown))
    }
}

impl ArtNode for Node4 {
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
    use super::Node4;
    use crate::art::ptr::TaggedPointer;

    #[test]
    fn insert_keeps_keys_sorted() {
        let mut node = Node4::new(b"");

        node.insert(20, TaggedPointer::from_raw(20));
        node.insert(10, TaggedPointer::from_raw(10));
        node.insert(30, TaggedPointer::from_raw(30));

        assert_eq!(node.keys[..node.meta.len()], [10, 20, 30]);
        assert_eq!(node.get(10), Some(TaggedPointer::from_raw(10)));
        assert_eq!(node.get(20), Some(TaggedPointer::from_raw(20)));
        assert_eq!(node.get(30), Some(TaggedPointer::from_raw(30)));
    }

    #[test]
    fn insert_replaces_existing_child() {
        let mut node = Node4::new(b"");

        assert_eq!(node.insert(7, TaggedPointer::from_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_raw(2)),
            Some(TaggedPointer::from_raw(1))
        );
        assert_eq!(node.meta.len(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_raw(2)));
    }

    #[test]
    fn remove_deletes_child_and_keeps_keys_sorted() {
        let mut node = Node4::new(b"");

        node.insert(20, TaggedPointer::from_raw(20));
        node.insert(10, TaggedPointer::from_raw(10));
        node.insert(30, TaggedPointer::from_raw(30));

        assert_eq!(node.remove(20), Some(TaggedPointer::from_raw(20)));
        assert_eq!(node.keys[..node.meta.len()], [10, 30]);
        assert_eq!(node.get(20), None);
    }
}
