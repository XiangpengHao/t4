use crate::art::{art::KVPair, n4::Node4, n16::Node16, n48::Node48, n256::Node256};

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TaggedPointer {
    /// Lower 4 bits are the tag. It can point to a node meta or a value pair.
    /// Pointers must be aligned to 64 bit boundaries.
    ptr: usize,
}

impl TaggedPointer {
    pub(crate) const fn from_raw(ptr: usize) -> Self {
        Self { ptr }
    }

    fn tag(&self) -> u8 {
        (self.ptr & 0x0000_0000_0000_000f) as u8
    }

    pub(crate) fn next_node(&self) -> NextNode {
        match self.tag() {
            0 => NextNode::Node4(self.ptr as *const Node4),
            1 => NextNode::Node16(self.ptr as *const Node16),
            2 => NextNode::Node48(self.ptr as *const Node48),
            3 => NextNode::Node256(self.ptr as *const Node256),
            4 => NextNode::Value(self.ptr as *const KVPair),
            _ => panic!("Invalid tag"),
        }
    }
}

pub(crate) enum NextNode {
    Node4(*const Node4),
    Node16(*const Node16),
    Node48(*const Node48),
    Node256(*const Node256),
    Value(*const KVPair),
}
