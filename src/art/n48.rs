use crate::art::{
    meta::{NodeMeta, NodeType}, n256::Node256, ptr::TaggedPointer
};

#[repr(C)]
pub(crate) struct Node48 {
    meta: NodeMeta,
    child_idx: [u8; 256],
    children: [TaggedPointer; 48],
}

impl Node48 {
    pub(crate) fn new() -> Self {
        let meta = NodeMeta::new(NodeType::Node48);
        Self {
            meta,
            child_idx: [0; 256],
            children: [TaggedPointer::default(); 48],
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> Option<TaggedPointer> {
        let key_idx = key as usize;
        let child_idx = self.child_idx[key_idx];

        if child_idx != 0 {
            let slot = (child_idx - 1) as usize;
            let old = self.children[slot];
            self.children[slot] = value;
            return Some(old);
        }

        let len = self.meta.len();
        assert!(len < self.children.len(), "Node48 is full");

        self.child_idx[key_idx] = (len + 1) as u8;
        self.children[len] = value;
        self.meta.increment_len();
        None
    }

  

    pub(crate) fn get(&self, key: u8) -> Option<TaggedPointer> {
        let child_idx = self.child_idx[key as usize];
        if child_idx == 0 {
            return None;
        }

        Some(self.children[(child_idx - 1) as usize])
    }
}

#[cfg(test)]
mod tests {
    use super::Node48;
    use crate::art::{meta::NodeMeta, ptr::TaggedPointer};

    #[test]
    fn insert_and_get_sparse_keys() {
        let mut node = Node48::new();

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
        let mut node = Node48::new();

        assert_eq!(node.insert(7, TaggedPointer::from_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_raw(2)),
            Some(TaggedPointer::from_raw(1))
        );
        assert_eq!(node.meta.len(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_raw(2)));
    }
}
