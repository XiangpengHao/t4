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
    pub(crate) const fn new(node_type: NodeType) -> Self {
        Self {
            len: 0,
            prefix_len: 0,
            node_type,
            prefix: [0; 8],
        }
    }

    pub(crate) const fn len(self) -> usize {
        self.len as usize
    }

    pub(crate) fn increment_len(&mut self) {
        self.len += 1;
    }

    pub(crate) const fn prefix_len(self) -> usize {
        self.prefix_len as usize
    }

    pub(crate) const fn prefix(self) -> [u8; 8] {
        self.prefix
    }

    pub(crate) const fn node_type(self) -> NodeType {
        self.node_type
    }

    pub(crate) fn set_prefix(&mut self, prefix: &[u8]) {
        self.prefix_len = prefix.len() as u8;
        self.prefix = [0; 8];
        let stored_len = prefix.len().min(self.prefix.len());
        self.prefix[..stored_len].copy_from_slice(&prefix[..stored_len]);
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeMeta, NodeType};

    #[test]
    fn new_meta_starts_with_empty_prefix() {
        let meta = NodeMeta::new(NodeType::Node4);

        assert_eq!(meta.prefix_len(), 0);
        assert_eq!(meta.prefix(), [0; 8]);
    }

    #[test]
    fn set_prefix_tracks_logical_and_inline_lengths() {
        let mut meta = NodeMeta::new(NodeType::Node16);

        meta.set_prefix(b"prefix-path");

        assert_eq!(meta.prefix_len(), 11);
        assert_eq!(meta.prefix(), *b"prefix-p");
    }
}
