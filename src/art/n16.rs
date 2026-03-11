use vstd::prelude::*;

use crate::art::{
    ArtNode, InsertStep, dense::DenseNode, meta::NodeType, n48::Node48, ptr::TaggedPointer,
};

verus! {

#[repr(transparent)]
pub(crate) struct Node16(DenseNode<16>);

impl Node16 {
    pub closed spec fn live_len(self) -> usize {
        self.0.live_len()
    }

    pub closed spec fn has_key(self, key: u8) -> bool {
        self.0.has_key(key)
    }

    pub closed spec fn maps_to(self, key: u8, raw: usize) -> bool {
        self.0.maps_to(key, raw)
    }

    pub closed spec fn wf(&self) -> bool {
        self.0.wf()
    }

    pub closed spec fn raw_prefix_len(self) -> usize {
        self.0.raw_prefix_len()
    }

    pub(crate) fn new(prefix: &[u8]) -> (result: Self)
        requires
            prefix.len() <= crate::art::meta::NodeMeta::prefix_capacity(),
        ensures
            result.wf(),
            result.live_len() == 0,
    {
        Self(DenseNode::new(NodeType::Node16, prefix))
    }

    pub(crate) fn get(&self, key: u8) -> (result: Option<TaggedPointer>)
        requires
            self.wf(),
        ensures
            result.is_some() <==> self.has_key(key),
            result.is_some() ==> self.maps_to(key, result.unwrap().raw()),
    {
        self.0.get(key)
    }

    #[allow(dead_code)]
    pub(crate) fn is_full(&self) -> (result: bool)
        ensures
            result <==> self.live_len() == 16,
    {
        self.0.is_full()
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> (result: Option<
        TaggedPointer,
    >)
        requires
            old(self).wf(),
            old(self).has_key(key) || old(self).live_len() < 16,
        ensures
            self.wf(),
            self.has_key(key),
            self.maps_to(key, value.raw()),
            old(self).has_key(key) ==> result.is_some(),
            !old(self).has_key(key) ==> result.is_none(),
            result.is_some() ==> old(self).maps_to(key, result.unwrap().raw()),
            old(self).has_key(key) ==> self.live_len() == old(self).live_len(),
            !old(self).has_key(key) ==> self.live_len() == old(self).live_len() + 1,
    {
        self.0.insert(key, value)
    }

    pub(crate) fn remove(&mut self, key: u8) -> (result: Option<TaggedPointer>)
        requires
            old(self).wf(),
        ensures
            self.wf(),
            !self.has_key(key),
            old(self).has_key(key) <==> result.is_some(),
            result.is_some() ==> old(self).maps_to(key, result.unwrap().raw()),
            result.is_some() ==> self.live_len() + 1 == old(self).live_len(),
            result.is_none() ==> self.live_len() == old(self).live_len(),
            forall|other_key: u8, raw: usize|
                self.maps_to(other_key, raw) ==> old(self).maps_to(other_key, raw) && other_key
                    != key,
            forall|other_key: u8, raw: usize|
                other_key != key && old(self).maps_to(other_key, raw) ==> self.maps_to(
                    other_key,
                    raw,
                ),
    {
        self.0.remove(key)
    }

    pub(crate) fn grow_node(&self, prefix: &[u8]) -> (result: Node48)
        requires
            self.wf(),
            prefix.len() <= crate::art::meta::NodeMeta::prefix_capacity(),
        ensures
            result.wf(),
    {
        let mut grown = Node48::new(prefix);
        let len = self.0.child_count();
        proof {
            self.0.lemma_live_len_bound();
            assert(len <= 16);
        }
        let mut idx = 0usize;
        while idx < len
            invariant
                self.wf(),
                prefix.len() <= crate::art::meta::NodeMeta::prefix_capacity(),
                len == self.live_len(),
                len <= 16,
                idx <= len,
                grown.wf(),
                grown.live_len() <= idx,
            decreases len - idx,
        {
            let (key, child) = self.0.entry_at(idx);
            proof {
                assert(grown.live_len() < 48) by {
                    assert(grown.live_len() <= idx);
                    assert(idx < len);
                    assert(len <= 16);
                }
            }
            let _ = grown.insert(key, child);
            idx = idx + 1;
        }
        grown
    }
}

impl ArtNode for Node16 {
    closed spec fn live_len(self) -> usize {
        Node16::live_len(self)
    }

    closed spec fn has_key(self, key: u8) -> bool {
        Node16::has_key(self, key)
    }

    closed spec fn maps_to(self, key: u8, raw: usize) -> bool {
        Node16::maps_to(self, key, raw)
    }

    closed spec fn wf(&self) -> bool {
        Node16::wf(self)
    }

    closed spec fn raw_prefix_len(self) -> usize {
        Node16::raw_prefix_len(self)
    }

    fn insert_step(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> (result: InsertStep) {
        self.0.insert_step_impl(terminated_key, value_ptr, depth)
    }

    fn replace_child(&mut self, edge: u8, child: TaggedPointer) {
        let _ = self.insert(edge, child);
    }

    fn remove_child(&mut self, edge: u8) -> (result: Option<TaggedPointer>) {
        self.remove(edge)
    }

    fn child_count(&self) -> (result: usize) {
        self.0.child_count()
    }

    fn prefix_len(&self) -> (result: usize) {
        self.0.prefix_len()
    }

    fn prefix(&self) -> (result: [u8; 8]) {
        self.0.prefix()
    }

    fn prefix_bytes(&self) -> (result: &[u8]) {
        self.0.prefix_slice()
    }

    fn set_prefix(&mut self, prefix: &[u8]) {
        self.0.set_prefix(prefix);
    }

    fn get_child(&self, edge: u8) -> (result: Option<TaggedPointer>) {
        self.get(edge)
    }
}

} // verus!

impl Node16 {
    pub(crate) fn for_each_child(&self, f: impl FnMut(u8, TaggedPointer)) {
        self.0.for_each_child(f);
    }

    pub(crate) fn grow(&self, prefix: &[u8]) -> TaggedPointer {
        TaggedPointer::from_node48(Box::new(self.grow_node(prefix)))
    }
}

#[cfg(test)]
mod tests {
    use super::Node16;
    use crate::art::ptr::TaggedPointer;

    #[test]
    fn insert_keeps_keys_sorted() {
        let mut node = Node16::new(b"");

        node.insert(40, TaggedPointer::from_test_raw(40));
        node.insert(10, TaggedPointer::from_test_raw(10));
        node.insert(30, TaggedPointer::from_test_raw(30));
        node.insert(20, TaggedPointer::from_test_raw(20));

        let mut keys = Vec::new();
        node.for_each_child(|key, _| keys.push(key));
        assert_eq!(keys, [10, 20, 30, 40]);
        assert_eq!(node.get(10), Some(TaggedPointer::from_test_raw(10)));
        assert_eq!(node.get(20), Some(TaggedPointer::from_test_raw(20)));
        assert_eq!(node.get(30), Some(TaggedPointer::from_test_raw(30)));
        assert_eq!(node.get(40), Some(TaggedPointer::from_test_raw(40)));
    }

    #[test]
    fn insert_replaces_existing_child() {
        let mut node = Node16::new(b"");

        assert_eq!(node.insert(7, TaggedPointer::from_test_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_test_raw(2)),
            Some(TaggedPointer::from_test_raw(1))
        );
        let mut count = 0;
        node.for_each_child(|_, _| count = count + 1);
        assert_eq!(count, 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_test_raw(2)));
    }

    #[test]
    fn remove_deletes_child_and_keeps_keys_sorted() {
        let mut node = Node16::new(b"");

        node.insert(40, TaggedPointer::from_test_raw(40));
        node.insert(10, TaggedPointer::from_test_raw(10));
        node.insert(30, TaggedPointer::from_test_raw(30));

        assert_eq!(node.remove(30), Some(TaggedPointer::from_test_raw(30)));
        let mut keys = Vec::new();
        node.for_each_child(|key, _| keys.push(key));
        assert_eq!(keys, [10, 40]);
        assert_eq!(node.get(30), None);
    }
}
