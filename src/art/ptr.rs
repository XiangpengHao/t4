use vstd::prelude::*;

use crate::art::{index::KVData, n4::Node4, n16::Node16, n48::Node48, n256::Node256};

verus! {

const TAG_MASK: usize = 0x0000_0000_0000_000f;

spec fn valid_tag(tag: usize) -> bool {
    tag < 5
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TaggedPointer {
    /// Lower 4 bits are the tag. It can point to a node meta or a value pair.
    /// Pointers must be aligned so the low 4 bits are available for tagging.
    ptr: usize,
}

impl TaggedPointer {
    pub closed spec fn tag_mask() -> usize {
        TAG_MASK
    }

    pub closed spec fn wf_raw(raw: usize) -> bool {
        &&& raw & !TAG_MASK != 0
        &&& raw & TAG_MASK < 5
    }

    pub closed spec fn raw(self) -> usize {
        self.ptr
    }

    pub(crate) const fn to_raw(self) -> (result: usize)
        ensures
            result == self.raw(),
            Self::wf_raw(result),
    {
        proof {
            use_type_invariant(&self);
        }
        self.ptr
    }

    #[verifier::type_invariant]
    pub closed spec fn wf(&self) -> bool {
        Self::wf_raw(self.ptr)
    }

    pub(crate) fn tag(&self) -> u8 {
        proof {
            use_type_invariant(self);
        }
        (self.ptr & TAG_MASK) as u8
    }

    pub(crate) fn untagged_ptr(&self) -> (ptr: usize)
        ensures
            ptr == self.raw() & !Self::tag_mask(),
            ptr != 0,
            ptr & Self::tag_mask() == 0,
    {
        proof {
            use_type_invariant(self);
        }

        let raw = self.ptr;
        let ptr = raw & !TAG_MASK;

        proof {
            assert(ptr != 0usize) by (bit_vector)
                requires
                    ptr == raw & !TAG_MASK,
                    raw & !TAG_MASK != 0,
            ;
            assert(ptr & TAG_MASK == 0usize) by (bit_vector)
                requires
                    ptr == raw & !TAG_MASK,
            ;
        }

        ptr
    }

    pub(crate) fn from_raw(raw: usize) -> (result: Self)
        requires
            Self::wf_raw(raw),
        ensures
            result.wf(),
            result.raw() == raw,
    {
        Self { ptr: raw }
    }

    pub proof fn lemma_wf_raw_nonzero(raw: usize)
        requires
            Self::wf_raw(raw),
        ensures
            raw != 0,
    {
        assert(raw & !TAG_MASK != 0);
        assert(raw != 0usize) by (bit_vector)
            requires
                raw & !TAG_MASK != 0,
        ;
    }

    fn from_tagged_ptr(ptr: usize, tag: usize) -> (result: Self)
        requires
            ptr != 0,
            ptr & TAG_MASK == 0,
            valid_tag(tag),
        ensures
            result.wf(),
            result.raw() == ptr | tag,
    {
        let raw = ptr | tag;
        proof {
            assert(Self::wf_raw(raw)) by {
                assert(raw & !TAG_MASK != 0usize) by (bit_vector)
                    requires
                        raw == ptr | tag,
                        ptr != 0,
                        ptr & TAG_MASK == 0,
                ;
                assert(raw & TAG_MASK < 5usize) by (bit_vector)
                    requires
                        raw == ptr | tag,
                        ptr & TAG_MASK == 0,
                        tag < 5,
                ;
            }
        }
        Self::from_raw(raw)
    }
}

} // verus!
impl TaggedPointer {
    #[cfg(test)]
    pub(crate) const fn from_test_raw(raw: usize) -> Self {
        Self {
            ptr: raw.wrapping_add(1) << 4,
        }
    }

    pub(crate) fn next_node(&self) -> NextNode {
        let ptr = self.untagged_ptr();
        match self.tag() {
            0 => NextNode::Node4(ptr as *mut Node4),
            1 => NextNode::Node16(ptr as *mut Node16),
            2 => NextNode::Node48(ptr as *mut Node48),
            3 => NextNode::Node256(ptr as *mut Node256),
            4 => NextNode::Value(ptr as *mut KVData),
            _ => unreachable!("TaggedPointer type invariant guarantees a valid tag"),
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

    pub(crate) fn from_value(kv: crate::art::index::KVPair) -> Self {
        Self::from_tagged_ptr(kv.into_raw() as usize, 4)
    }
}

pub(crate) enum NextNode {
    Node4(*mut Node4),
    Node16(*mut Node16),
    Node48(*mut Node48),
    Node256(*mut Node256),
    Value(*mut KVData),
}
