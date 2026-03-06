use crate::art::{meta::{NodeMeta, NodeType}, ptr::TaggedPointer};

#[repr(C, align(16))]
pub(crate) struct Node4 {
    meta: NodeMeta,
    terminal: TaggedPointer,
    keys: [u8; 4],
    children: [TaggedPointer; 4],
}

impl Node4 {
    pub(crate) fn new() -> Self {
        let meta = NodeMeta::new(NodeType::Node4);
        Self {
            meta,
            terminal: TaggedPointer::default(),
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

    pub(crate) fn meta(&self) -> &NodeMeta {
        &self.meta
    }

    pub(crate) fn meta_mut(&mut self) -> &mut NodeMeta {
        &mut self.meta
    }

    pub(crate) fn terminal(&self) -> TaggedPointer {
        self.terminal
    }

    pub(crate) fn set_terminal(&mut self, value: TaggedPointer) {
        self.terminal = value;
    }

    pub(crate) fn is_full(&self) -> bool {
        self.meta.len() == self.children.len()
    }

    pub(crate) fn first_child(&self) -> Option<TaggedPointer> {
        if !self.terminal.is_null() {
            return Some(self.terminal);
        }

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

#[cfg(test)]
mod tests {
    use super::Node4;
    use crate::art::ptr::TaggedPointer;

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
