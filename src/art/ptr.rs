use std::num::NonZeroUsize;

use crate::art::{art::KVPair, n4::Node4, n16::Node16, n48::Node48, n256::Node256};

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TaggedPointer {
    /// Lower 4 bits are the tag. It can point to a node meta or a value pair.
    /// Pointers must be aligned to 64 bit boundaries.
    ptr: NonZeroUsize,
}

impl TaggedPointer {
    const TAG_MASK: usize = 0x0000_0000_0000_000f;

    #[cfg(test)]
    pub(crate) const fn from_raw(ptr: usize) -> Self {
        match NonZeroUsize::new(ptr) {
            Some(ptr) => Self { ptr },
            None => panic!("TaggedPointer must be non-null"),
        }
    }

    fn tag(&self) -> u8 {
        (self.ptr.get() & Self::TAG_MASK) as u8
    }

    fn untagged_ptr(&self) -> usize {
        self.ptr.get() & !Self::TAG_MASK
    }

    pub(crate) fn next_node(&self) -> NextNode {
        let ptr = self.untagged_ptr();
        match self.tag() {
            0 => NextNode::Node4(ptr as *mut Node4),
            1 => NextNode::Node16(ptr as *mut Node16),
            2 => NextNode::Node48(ptr as *mut Node48),
            3 => NextNode::Node256(ptr as *mut Node256),
            4 => NextNode::Value(ptr as *mut KVPair),
            _ => panic!("Invalid tag"),
        }
    }

    pub(crate) fn from_node4(node: Box<Node4>) -> Self {
        Self::from_tagged_ptr(Box::into_raw(node) as usize, 0)
    }

    pub(crate) fn from_node16(node: Box<Node16>) -> Self {
        Self::from_tagged_ptr(Box::into_raw(node) as usize, 1)
    }

    pub(crate) fn from_node48(node: Box<Node48>) -> Self {
        Self::from_tagged_ptr(Box::into_raw(node) as usize, 2)
    }

    pub(crate) fn from_node256(node: Box<Node256>) -> Self {
        Self::from_tagged_ptr(Box::into_raw(node) as usize, 3)
    }

    pub(crate) fn from_value(value: Box<KVPair>) -> Self {
        Self::from_tagged_ptr(Box::into_raw(value) as usize, 4)
    }

    fn from_tagged_ptr(ptr: usize, tag: usize) -> Self {
        assert_ne!(ptr, 0, "TaggedPointer must be non-null");
        debug_assert_eq!(ptr & Self::TAG_MASK, 0);
        let ptr = NonZeroUsize::new(ptr | tag).expect("TaggedPointer must be non-null");
        Self { ptr }
    }
}

pub(crate) enum NextNode {
    Node4(*mut Node4),
    Node16(*mut Node16),
    Node48(*mut Node48),
    Node256(*mut Node256),
    Value(*mut KVPair),
}
