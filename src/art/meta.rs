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
    node_type: NodeType,
}

impl NodeMeta {
    pub(crate) const fn new(node_type: NodeType) -> Self {
        Self { len: 0, node_type }
    }

    pub(crate) const fn len(self) -> usize {
        self.len as usize
    }

    pub(crate) fn increment_len(&mut self) {
        self.len += 1;
    }
}
