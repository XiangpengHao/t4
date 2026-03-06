use crate::art::{
    meta::{NodeMeta, NodeType}, n48::Node48, ptr::TaggedPointer
};

#[repr(C)]
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
}

#[cfg(test)]
mod tests {
    use super::Node16;
    use crate::art::{meta::NodeMeta, ptr::TaggedPointer};

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
