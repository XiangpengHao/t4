use crate::art::{
    meta::{NodeMeta, NodeType},
    n16::Node16,
    ptr::TaggedPointer,
};

pub(crate) struct Node4 {
    meta: NodeMeta,
    keys: [u8; 4],
    children: [TaggedPointer; 4],
}

impl Node4 {
    pub(crate) fn new() -> Self {
        let meta = NodeMeta::new(NodeType::Node4);
        Self {
            meta,
            keys: [0; 4],
            children: [TaggedPointer::default(); 4],
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

        assert!(len < self.keys.len(), "Node4 is full");

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
    use super::Node4;
    use crate::art::{meta::NodeMeta, ptr::TaggedPointer};

    #[test]
    fn insert_keeps_keys_sorted() {
        let mut node = Node4::new();

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
        let mut node = Node4::new();

        assert_eq!(node.insert(7, TaggedPointer::from_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_raw(2)),
            Some(TaggedPointer::from_raw(1))
        );
        assert_eq!(node.meta.len(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_raw(2)));
    }

}
