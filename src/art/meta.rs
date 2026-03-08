#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum NodeType {
    Node4 = 0,
    Node16 = 1,
    Node48 = 2,
    Node256 = 3,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct NodeMeta {
    len: u8,
    prefix_len: u8,
    node_type: NodeType,
    prefix: [u8; 8],
}

impl NodeMeta {
    pub(crate) fn new(node_type: NodeType, prefix: &[u8]) -> Self {
        let mut meta = Self {
            len: 0,
            prefix_len: 0,
            node_type,
            prefix: [0; 8],
        };
        meta.set_prefix(prefix);
        meta
    }

    pub(crate) const fn len(self) -> usize {
        self.len as usize
    }

    pub(crate) fn increment_len(&mut self) {
        self.len += 1;
    }

    pub(crate) fn decrement_len(&mut self) {
        self.len -= 1;
    }

    pub(crate) const fn prefix_len(self) -> usize {
        self.prefix_len as usize
    }

    pub(crate) const fn prefix(self) -> [u8; 8] {
        self.prefix
    }

    pub(crate) fn set_prefix(&mut self, prefix: &[u8]) {
        assert!(
            prefix.len() <= self.prefix.len(),
            "node prefixes longer than 8 bytes must be represented by subnodes"
        );
        self.prefix_len = prefix.len() as u8;
        self.prefix = [0; 8];
        self.prefix[..prefix.len()].copy_from_slice(prefix);
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeMeta, NodeType};

    #[test]
    fn new_meta_starts_with_empty_prefix() {
        let meta = NodeMeta::new(NodeType::Node4, b"");

        assert_eq!(meta.prefix_len(), 0);
        assert_eq!(meta.prefix(), [0; 8]);
    }

    #[test]
    fn set_prefix_stores_full_prefix_when_it_fits() {
        let meta = NodeMeta::new(NodeType::Node16, b"prefix-p");

        assert_eq!(meta.prefix_len(), 8);
        assert_eq!(meta.prefix(), *b"prefix-p");
    }

    #[test]
    #[should_panic(expected = "node prefixes longer than 8 bytes must be represented by subnodes")]
    fn set_prefix_rejects_prefixes_longer_than_eight_bytes() {
        let _ = NodeMeta::new(NodeType::Node16, b"prefix-path");
    }
}
