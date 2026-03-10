use vstd::prelude::*;

use crate::art::{
    ArtNode, InsertStep, dense::DenseNode, meta::NodeType, n16::Node16, ptr::TaggedPointer,
};

verus! {

#[repr(transparent)]
pub(crate) struct Node4(DenseNode<4>);

impl Node4 {
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
        Self(DenseNode::new(NodeType::Node4, prefix))
    }

    pub(crate) fn get(&self, key: u8) -> (result: Option<TaggedPointer>)
        requires
            self.wf(),
        ensures
            result.is_some() <==> self.has_key(key),
    {
        self.0.get(key)
    }

    #[allow(dead_code)]
    pub(crate) fn is_full(&self) -> (result: bool)
        ensures
            result <==> self.live_len() == 4,
    {
        self.0.is_full()
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> (result: Option<
        TaggedPointer,
    >)
        requires
            old(self).wf(),
            old(self).has_key(key) || old(self).live_len() < 4,
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

    pub(crate) fn insert_step_impl(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> (result: InsertStep)
        requires
            old(self).wf(),
            depth + old(self).raw_prefix_len() < terminated_key.len(),
        ensures
            self.wf(),
            match result {
                InsertStep::Split { .. } => self.live_len() == old(self).live_len(),
                InsertStep::Descend { edge, child, next_depth } => {
                    &&& self.live_len() == old(self).live_len()
                    &&& edge == terminated_key[depth + old(self).raw_prefix_len()]
                    &&& next_depth == depth + old(self).raw_prefix_len() + 1
                    &&& old(self).maps_to(edge, child.raw())
                },
                InsertStep::Grow { prefix_depth, prefix_len } => {
                    &&& self.live_len() == old(self).live_len()
                    &&& prefix_depth == depth
                    &&& prefix_len == old(self).raw_prefix_len()
                },
                InsertStep::Done => {
                    &&& self.live_len() == old(self).live_len() + 1
                    &&& self.maps_to(
                        terminated_key[depth + old(self).raw_prefix_len()],
                        value_ptr.raw(),
                    )
                },
            },
    {
        self.0.insert_step_impl(terminated_key, value_ptr, depth)
    }

    pub(crate) fn grow_node(&self, prefix: &[u8]) -> (result: Node16)
        requires
            self.wf(),
            prefix.len() <= crate::art::meta::NodeMeta::prefix_capacity(),
        ensures
            result.wf(),
    {
        let mut grown = Node16::new(prefix);
        let len = self.0.child_count();
        proof {
            self.0.lemma_live_len_bound();
            assert(len <= 4);
        }
        let mut idx = 0usize;
        while idx < len
            invariant
                self.wf(),
                prefix.len() <= crate::art::meta::NodeMeta::prefix_capacity(),
                len == self.live_len(),
                len <= 4,
                idx <= len,
                grown.wf(),
                grown.live_len() <= idx,
            decreases len - idx,
        {
            let (key, child) = self.0.entry_at(idx);
            proof {
                assert(grown.live_len() < 16) by {
                    assert(grown.live_len() <= idx);
                    assert(idx < len);
                    assert(len <= 4);
                }
            }
            let _ = grown.insert(key, child);
            idx += 1;
        }
        grown
    }
}

} // verus!

impl Node4 {
    pub(crate) fn for_each_child(&self, f: impl FnMut(u8, TaggedPointer)) {
        self.0.for_each_child(f);
    }

    pub(crate) fn grow(&self, prefix: &[u8]) -> TaggedPointer {
        TaggedPointer::from_node16(Box::new(self.grow_node(prefix)))
    }
}

impl ArtNode for Node4 {
    fn insert_step(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> InsertStep {
        self.insert_step_impl(terminated_key, value_ptr, depth)
    }

    fn replace_child(&mut self, edge: u8, child: TaggedPointer) {
        let _ = self.insert(edge, child);
    }

    fn remove_child(&mut self, edge: u8) -> Option<TaggedPointer> {
        self.remove(edge)
    }

    fn child_count(&self) -> usize {
        self.0.child_count()
    }

    fn prefix_len(&self) -> usize {
        self.0.prefix_len()
    }

    fn prefix(&self) -> [u8; 8] {
        self.0.prefix()
    }

    fn set_prefix(&mut self, prefix: &[u8]) {
        self.0.set_prefix(prefix);
    }

    fn get_child(&self, edge: u8) -> Option<TaggedPointer> {
        self.get(edge)
    }
}

#[cfg(test)]
mod tests {
    use super::Node4;
    use crate::art::ptr::TaggedPointer;

    #[test]
    fn insert_keeps_keys_sorted() {
        let mut node = Node4::new(b"");

        node.insert(20, TaggedPointer::from_test_raw(20));
        node.insert(10, TaggedPointer::from_test_raw(10));
        node.insert(30, TaggedPointer::from_test_raw(30));

        let mut keys = Vec::new();
        node.for_each_child(|key, _| keys.push(key));
        assert_eq!(keys, [10, 20, 30]);
        assert_eq!(node.get(10), Some(TaggedPointer::from_test_raw(10)));
        assert_eq!(node.get(20), Some(TaggedPointer::from_test_raw(20)));
        assert_eq!(node.get(30), Some(TaggedPointer::from_test_raw(30)));
    }

    #[test]
    fn insert_replaces_existing_child() {
        let mut node = Node4::new(b"");

        assert_eq!(node.insert(7, TaggedPointer::from_test_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_test_raw(2)),
            Some(TaggedPointer::from_test_raw(1))
        );
        let mut count = 0;
        node.for_each_child(|_, _| count += 1);
        assert_eq!(count, 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_test_raw(2)));
    }

    #[test]
    fn remove_deletes_child_and_keeps_keys_sorted() {
        let mut node = Node4::new(b"");

        node.insert(20, TaggedPointer::from_test_raw(20));
        node.insert(10, TaggedPointer::from_test_raw(10));
        node.insert(30, TaggedPointer::from_test_raw(30));

        assert_eq!(node.remove(20), Some(TaggedPointer::from_test_raw(20)));
        let mut keys = Vec::new();
        node.for_each_child(|key, _| keys.push(key));
        assert_eq!(keys, [10, 30]);
        assert_eq!(node.get(20), None);
    }
}
