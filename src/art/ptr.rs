use vstd::prelude::*;

use crate::art::{
    index::{KVData, KVPairOwned},
    n4::Node4,
    n16::Node16,
    n48::Node48,
    n256::Node256,
};

verus! {

const TAG_MASK: usize = 0x7;

spec fn valid_tag(tag: usize) -> bool {
    tag < 5
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TaggedPointer {
    /// Lower 3 bits are the tag. It can point to a node meta or a value pair.
    /// Pointers must be aligned so the low 3 bits are available for tagging.
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

    pub closed spec fn is_value(self) -> bool {
        self.raw() & TAG_MASK == 4
    }

    pub closed spec fn is_node(self) -> bool {
        self.raw() & TAG_MASK < 4
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

    pub(crate) fn tag(&self) -> (result: u8)
        ensures
            result as usize == self.raw() & Self::tag_mask(),
            result < 5,
    {
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
pub(crate) enum NextNodeRef<'a> {
    Node4(&'a Node4),
    Node16(&'a Node16),
    Node48(&'a Node48),
    Node256(&'a Node256),
    Value(&'a KVData),
}

pub(crate) enum NextNodeMut<'a> {
    Node4(&'a mut Node4),
    Node16(&'a mut Node16),
    Node48(&'a mut Node48),
    Node256(&'a mut Node256),
    Value(&'a mut KVData),
}

/// Methods that perform raw pointer operations (Box::into_raw, Box::from_raw, ptr deref).
/// These live outside verus! because Verus lacks specs for these Rust primitives.
/// The TaggedPointer type invariant (wf) is maintained by construction:
/// from_tagged_ptr proves the bit-level invariant, and all constructors go through it.
impl TaggedPointer {
    /// Safety: `self` must point to a live allocation whose concrete type matches the tag.
    pub(crate) unsafe fn next_node_ref<'a>(&self) -> NextNodeRef<'a> {
        let ptr = self.untagged_ptr();
        match self.tag() {
            0 => unsafe { NextNodeRef::Node4(&*(ptr as *const Node4)) },
            1 => unsafe { NextNodeRef::Node16(&*(ptr as *const Node16)) },
            2 => unsafe { NextNodeRef::Node48(&*(ptr as *const Node48)) },
            3 => unsafe { NextNodeRef::Node256(&*(ptr as *const Node256)) },
            4 => unsafe { NextNodeRef::Value(&*(ptr as *const KVData)) },
            _ => unreachable!("TaggedPointer type invariant guarantees a valid tag"),
        }
    }

    /// Safety: `self` must point to a live allocation whose concrete type matches the tag, and
    /// the caller must have exclusive access to that allocation for the duration of the borrow.
    pub(crate) unsafe fn next_node_mut<'a>(&self) -> NextNodeMut<'a> {
        let ptr = self.untagged_ptr();
        match self.tag() {
            0 => unsafe { NextNodeMut::Node4(&mut *(ptr as *mut Node4)) },
            1 => unsafe { NextNodeMut::Node16(&mut *(ptr as *mut Node16)) },
            2 => unsafe { NextNodeMut::Node48(&mut *(ptr as *mut Node48)) },
            3 => unsafe { NextNodeMut::Node256(&mut *(ptr as *mut Node256)) },
            4 => unsafe { NextNodeMut::Value(&mut *(ptr as *mut KVData)) },
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

    pub(crate) fn from_value(kv: KVPairOwned) -> Self {
        Self::from_tagged_ptr(kv.into_raw() as usize, 4)
    }

    /// Safety: `self` must point to a live leaf allocation owned by this tagged pointer.
    pub(crate) unsafe fn into_value(self) -> KVPairOwned {
        let ptr = self.untagged_ptr() as *mut KVData;
        unsafe { KVPairOwned::from_raw(ptr) }
    }

    /// Safety: `self` must point to a live node allocation owned by this tagged pointer.
    pub(crate) unsafe fn drop_node(self) {
        let ptr = self.untagged_ptr();
        unsafe {
            match self.tag() {
                0 => drop(Box::from_raw(ptr as *mut Node4)),
                1 => drop(Box::from_raw(ptr as *mut Node16)),
                2 => drop(Box::from_raw(ptr as *mut Node48)),
                3 => drop(Box::from_raw(ptr as *mut Node256)),
                4 => unreachable!("node-tag precondition rules out value pointers"),
                _ => unreachable!("TaggedPointer type invariant guarantees a valid tag"),
            }
        }
    }

    #[cfg(test)]
    pub(crate) const fn from_test_raw(raw: usize) -> Self {
        Self {
            ptr: raw.wrapping_add(1) << 3,
        }
    }
}
