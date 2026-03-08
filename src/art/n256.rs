use crate::art::{
    ArtNode, InsertStep,
    art::common_prefix_len,
    meta::{NodeMeta, NodeType},
    ptr::TaggedPointer,
};

#[repr(C, align(16))]
pub(crate) struct Node256 {
    meta: NodeMeta,
    key_mask: [u8; 32],
    children: [TaggedPointer; 256],
}

impl Node256 {
    pub(crate) fn new(prefix: &[u8]) -> Self {
        let meta = NodeMeta::new(NodeType::Node256, prefix);
        Self {
            meta,
            key_mask: [0; 32],
            children: [TaggedPointer::default(); 256],
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> Option<TaggedPointer> {
        let key_idx = key as usize;
        let mask_idx = key_idx / 8;
        let bit = 1u8 << (key_idx % 8);

        if self.key_mask[mask_idx] & bit != 0 {
            let old = self.children[key_idx];
            self.children[key_idx] = value;
            return Some(old);
        }

        self.key_mask[mask_idx] |= bit;
        self.children[key_idx] = value;
        self.meta.increment_len();
        None
    }

    pub(crate) fn get(&self, key: u8) -> Option<TaggedPointer> {
        let key_idx = key as usize;
        let mask_idx = key_idx / 8;
        let bit = 1u8 << (key_idx % 8);

        if self.key_mask[mask_idx] & bit == 0 {
            return None;
        }

        Some(self.children[key_idx])
    }

    pub(crate) fn meta_mut(&mut self) -> &mut NodeMeta {
        &mut self.meta
    }
}

impl ArtNode for Node256 {
    fn insert_step(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> InsertStep {
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

    fn set_prefix(&mut self, prefix: &[u8]) {
        self.meta_mut().set_prefix(prefix);
    }

    fn get_child(&self, edge: u8) -> Option<TaggedPointer> {
        self.get(edge)
    }
}

#[cfg(test)]
mod tests {
    use super::Node256;
    use crate::art::ptr::TaggedPointer;

    #[test]
    fn insert_and_get_direct_slots() {
        let mut node = Node256::new(b"");

        node.insert(0, TaggedPointer::from_raw(10));
        node.insert(127, TaggedPointer::from_raw(20));
        node.insert(255, TaggedPointer::from_raw(30));

        assert_eq!(node.meta.len(), 3);
        assert_eq!(node.get(0), Some(TaggedPointer::from_raw(10)));
        assert_eq!(node.get(127), Some(TaggedPointer::from_raw(20)));
        assert_eq!(node.get(255), Some(TaggedPointer::from_raw(30)));
        assert_eq!(node.get(42), None);
    }

    #[test]
    fn insert_replaces_existing_child() {
        let mut node = Node256::new(b"");

        assert_eq!(node.insert(7, TaggedPointer::from_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_raw(2)),
            Some(TaggedPointer::from_raw(1))
        );
        assert_eq!(node.meta.len(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_raw(2)));
    }
}
