use crate::art::{
    art::match_prefix,
    meta::{NodeMeta, NodeType},
    ptr::TaggedPointer,
    ArtNode, InsertStep,
};

#[repr(C, align(16))]
pub(crate) struct Node16 {
    meta: NodeMeta,
    keys: [u8; 16],
    children: [TaggedPointer; 16],
}

impl Node16 {
    pub(crate) fn new() -> Self {
        let meta = NodeMeta::new(NodeType::Node16);
        Self {
            meta,
            keys: [0; 16],
            children: [TaggedPointer::default(); 16],
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> Option<TaggedPointer> {
        let len = self.meta.len();

        for idx in 0..len {
            if self.keys[idx] == key {
                let old = self.children[idx];
                self.children[idx] = value;
                return Some(old);
            }
        }

        assert!(len < self.keys.len(), "Node16 is full");

        let insert_at = self.keys[..len].partition_point(|existing| *existing < key);
        for idx in (insert_at..len).rev() {
            self.keys[idx + 1] = self.keys[idx];
            self.children[idx + 1] = self.children[idx];
        }

        self.keys[insert_at] = key;
        self.children[insert_at] = value;
        self.meta.increment_len();
        None
    }

  

    pub(crate) fn get(&self, key: u8) -> Option<TaggedPointer> {
        let len = self.meta.len();
        let idx = self.keys[..len]
            .iter()
            .position(|existing| *existing == key)?;
        Some(self.children[idx])
    }

    pub(crate) fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    pub(crate) fn meta_mut(&mut self) -> &mut NodeMeta {
        &mut self.meta
    }

    pub(crate) fn is_full(&self) -> bool {
        self.meta.len() == self.children.len()
    }

    pub(crate) fn first_child(&self) -> Option<TaggedPointer> {
        if self.meta.len() == 0 {
            return None;
        }

        Some(self.children[0])
    }

    pub(crate) fn for_each_child(&self, mut f: impl FnMut(u8, TaggedPointer)) {
        let len = self.meta.len();
        for idx in 0..len {
            f(self.keys[idx], self.children[idx]);
        }
    }
}

impl ArtNode for Node16 {
    fn insert_step(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> InsertStep {
        let prefix_depth = depth;
        let prefix_len = self.meta.prefix_len();
        let matched = match_prefix(
            prefix_len,
            self.meta.prefix(),
            self.first_child(),
            terminated_key,
            depth,
        );
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

    fn prefix_len(&self) -> usize {
        self.meta.prefix_len()
    }

    fn prefix(&self) -> [u8; 8] {
        self.meta.prefix()
    }

    fn first_child(&self) -> Option<TaggedPointer> {
        self.first_child()
    }

    fn get_child(&self, edge: u8) -> Option<TaggedPointer> {
        self.get(edge)
    }
}

#[cfg(test)]
mod tests {
    use super::Node16;
    use crate::art::ptr::TaggedPointer;

    #[test]
    fn insert_keeps_keys_sorted() {
        let mut node = Node16::new();

        node.insert(40, TaggedPointer::from_raw(40));
        node.insert(10, TaggedPointer::from_raw(10));
        node.insert(30, TaggedPointer::from_raw(30));
        node.insert(20, TaggedPointer::from_raw(20));

        assert_eq!(node.keys[..node.meta.len()], [10, 20, 30, 40]);
        assert_eq!(node.get(10), Some(TaggedPointer::from_raw(10)));
        assert_eq!(node.get(20), Some(TaggedPointer::from_raw(20)));
        assert_eq!(node.get(30), Some(TaggedPointer::from_raw(30)));
        assert_eq!(node.get(40), Some(TaggedPointer::from_raw(40)));
    }

    #[test]
    fn insert_replaces_existing_child() {
        let mut node = Node16::new();

        assert_eq!(node.insert(7, TaggedPointer::from_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_raw(2)),
            Some(TaggedPointer::from_raw(1))
        );
        assert_eq!(node.meta.len(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_raw(2)));
    }
}
