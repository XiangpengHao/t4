use vstd::prelude::*;

verus! {

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
    len: u16,
    prefix_len: u8,
    node_type: NodeType,
    prefix: [u8; 8],
}

impl NodeMeta {
    pub closed spec fn prefix_capacity() -> usize {
        8
    }

    pub closed spec fn spec_len(self) -> u16 {
        self.len
    }

    pub closed spec fn raw_prefix_len(self) -> u8 {
        self.prefix_len
    }

    #[verifier::type_invariant]
    pub closed spec fn wf(&self) -> bool {
        self.prefix_len as usize <= Self::prefix_capacity()
    }

    pub(crate) fn new(node_type: NodeType, prefix: &[u8]) -> (result: Self)
        requires
            prefix.len() <= Self::prefix_capacity(),
        ensures
            result.spec_len() == 0,
            result.raw_prefix_len() as usize == prefix.len(),
    {
        let mut meta = Self { len: 0, prefix_len: 0, node_type, prefix: [0;8] };
        meta.set_prefix(prefix);
        meta
    }

    pub(crate) const fn len(self) -> (result: usize)
        ensures
            result == self.spec_len() as usize,
    {
        self.len as usize
    }

    pub(crate) fn increment_len(&mut self)
        requires
            old(self).spec_len() < u16::MAX,
        ensures
            self.spec_len() == old(self).spec_len() + 1,
            self.raw_prefix_len() == old(self).raw_prefix_len(),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.len = self.len + 1;
    }

    pub(crate) fn decrement_len(&mut self)
        requires
            old(self).spec_len() > 0,
        ensures
            self.spec_len() + 1 == old(self).spec_len(),
            self.raw_prefix_len() == old(self).raw_prefix_len(),
    {
        proof {
            use_type_invariant(&*self);
        }
        self.len = self.len - 1;
    }

    pub(crate) const fn prefix_len(self) -> (result: usize)
        ensures
            result == self.raw_prefix_len() as usize,
            result <= Self::prefix_capacity(),
    {
        proof {
            use_type_invariant(&self);
        }
        self.prefix_len as usize
    }

    pub(crate) const fn prefix(self) -> [u8; 8] {
        self.prefix
    }

    pub(crate) fn prefix_slice(&self) -> (result: &[u8])
        ensures
            result@.len() == Self::prefix_capacity(),
    {
        &self.prefix
    }

    pub(crate) fn set_prefix(&mut self, prefix: &[u8])
        requires
            prefix.len() <= Self::prefix_capacity(),
        ensures
            self.spec_len() == old(self).spec_len(),
            self.raw_prefix_len() as usize == prefix.len(),
            self.wf(),
    {
        self.prefix_len = prefix.len() as u8;
        self.prefix = [0;8];
        let mut idx = 0usize;
        while idx < prefix.len()
            invariant
                self.wf(),
                self.spec_len() == old(self).spec_len(),
                self.raw_prefix_len() as usize == prefix.len(),
                idx <= prefix.len(),
            decreases prefix.len() - idx,
        {
            self.prefix[idx] = prefix[idx];
            idx = idx + 1;
        }
    }
}

} // verus!
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
    #[should_panic]
    fn set_prefix_rejects_prefixes_longer_than_eight_bytes() {
        let _ = NodeMeta::new(NodeType::Node16, b"prefix-path");
    }
}
