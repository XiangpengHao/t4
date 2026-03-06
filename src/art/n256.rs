use crate::art::{meta::{NodeMeta, NodeType}, ptr::TaggedPointer};

#[repr(C, align(16))]
pub(crate) struct Node256 {
    meta: NodeMeta,
    terminal: TaggedPointer,
    key_mask: [u8; 32],
    children: [TaggedPointer; 256],
}

impl Node256 {
    pub(crate) fn new() -> Self {
        let meta = NodeMeta::new(NodeType::Node256);
        Self {
            meta,
            terminal: TaggedPointer::default(),
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

        for key in 0..=u8::MAX {
            if let Some(child) = self.get(key) {
                return Some(child);
            }
        }

        None
    }

    pub(crate) fn for_each_child(&self, mut f: impl FnMut(u8, TaggedPointer)) {
        for key in 0..=u8::MAX {
            if let Some(child) = self.get(key) {
                f(key, child);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Node256;
    use crate::art::ptr::TaggedPointer;

    #[test]
    fn insert_and_get_direct_slots() {
        let mut node = Node256::new();

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
        let mut node = Node256::new();

        assert_eq!(node.insert(7, TaggedPointer::from_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_raw(2)),
            Some(TaggedPointer::from_raw(1))
        );
        assert_eq!(node.meta.len(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_raw(2)));
    }
}
