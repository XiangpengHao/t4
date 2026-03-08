use vstd::prelude::*;

use crate::art::{
    ArtNode, InsertStep,
    art::common_prefix_len,
    meta::{NodeMeta, NodeType},
    n16::Node16,
    ptr::TaggedPointer,
};

verus! {

#[repr(C, align(16))]
pub(crate) struct Node4 {
    meta: NodeMeta,
    keys: [u8; 4],
    children: [usize; 4],
}

impl Node4 {
    pub closed spec fn live_len(self) -> usize {
        self.meta.raw_len() as usize
    }

    pub closed spec fn has_key(self, key: u8) -> bool {
        exists|i: int| 0 <= i < self.live_len() && self.keys[i] == key
    }

    #[verifier::type_invariant]
    pub closed spec fn wf(&self) -> bool {
        &&& self.live_len() <= 4
        &&& forall|i: int, j: int|
            0 <= i < j < self.live_len() ==> self.keys[i] < self.keys[j]
        &&& forall|i: int|
            0 <= i < self.live_len() ==> #[trigger] TaggedPointer::wf_raw(self.children[i])
    }

    pub(crate) fn new(prefix: &[u8]) -> (result: Self)
        requires
            prefix.len() <= NodeMeta::prefix_capacity(),
        ensures
            result.wf(),
            result.live_len() == 0,
    {
        let meta = NodeMeta::new(NodeType::Node4, prefix);
        Self {
            meta,
            keys: [0; 4],
            children: [0; 4],
        }
    }

    fn key_index(&self, key: u8) -> (result: Option<usize>)
        ensures
            result.is_some() ==> result.unwrap() < self.live_len(),
            result.is_some() ==> self.keys[result.unwrap() as int] == key,
            result.is_none() ==> !self.has_key(key),
    {
        proof {
            use_type_invariant(self);
        }

        let len = self.meta.len();
        let mut idx = 0usize;
        while idx < len
            invariant
                self.wf(),
                idx <= len,
                len == self.live_len(),
                forall|j: int| 0 <= j < idx ==> self.keys[j] != key,
            decreases len - idx,
        {
            if self.keys[idx] == key {
                return Some(idx);
            }
            idx += 1;
        }
        None
    }

    pub(crate) fn get(&self, key: u8) -> (result: Option<TaggedPointer>)
        ensures
            result.is_some() <==> self.has_key(key),
    {
        let Some(idx) = self.key_index(key) else {
            return None;
        };

        proof {
            use_type_invariant(self);
        }

        Some(TaggedPointer::from_raw(self.children[idx]))
    }

    pub(crate) fn is_full(&self) -> (result: bool)
        ensures
            result <==> self.live_len() == 4,
    {
        self.meta.len() == 4
    }
}

} // verus!

impl Node4 {
    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> Option<TaggedPointer> {
        let len = self.meta.len();

        for idx in 0..len {
            if self.keys[idx] == key {
                let old = TaggedPointer::from_raw(self.children[idx]);
                self.children[idx] = value.to_raw();
                return Some(old);
            }
        }

        assert!(len < self.keys.len(), "Node4 is full");

        let insert_at = self.keys[..len].partition_point(|existing| *existing < key);
        for idx in (insert_at..len).rev() {
            self.keys[idx + 1] = self.keys[idx];
            self.children[idx + 1] = self.children[idx];
        }

        self.keys[insert_at] = key;
        self.children[insert_at] = value.to_raw();
        self.meta.increment_len();
        None
    }

    pub(crate) fn remove(&mut self, key: u8) -> Option<TaggedPointer> {
        let len = self.meta.len();
        let idx = self.keys[..len]
            .iter()
            .position(|existing| *existing == key)?;
        let removed = TaggedPointer::from_raw(self.children[idx]);
        for shift in idx + 1..len {
            self.keys[shift - 1] = self.keys[shift];
            self.children[shift - 1] = self.children[shift];
        }
        self.keys[len - 1] = 0;
        self.children[len - 1] = 0;
        self.meta.decrement_len();
        Some(removed)
    }

    pub(crate) fn meta_mut(&mut self) -> &mut NodeMeta {
        &mut self.meta
    }

    pub(crate) fn for_each_child(&self, mut f: impl FnMut(u8, TaggedPointer)) {
        let len = self.meta.len();
        for idx in 0..len {
            f(self.keys[idx], TaggedPointer::from_raw(self.children[idx]));
        }
    }

    pub(crate) fn grow(&self, prefix: &[u8]) -> TaggedPointer {
        let mut grown = Node16::new(prefix);
        self.for_each_child(|key, child| {
            let _ = grown.insert(key, child);
        });
        TaggedPointer::from_node16(Box::new(grown))
    }
}

impl ArtNode for Node4 {
    fn insert_step(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> InsertStep {
        let prefix_depth = depth;
        let prefix_len = self.meta.prefix_len();
        let matched =
            common_prefix_len(&self.meta.prefix()[..prefix_len], &terminated_key[depth..]);
        if matched != prefix_len {
            return InsertStep::Split { matched };
        }

        let depth = depth + prefix_len;
        let edge = terminated_key[depth];
        if let Some(child) = self.get(edge) {
            return InsertStep::Descend {
                edge,
                child,
                next_depth: depth + 1,
            };
        }

        if self.is_full() {
            return InsertStep::Grow {
                prefix_depth,
                prefix_len,
            };
        }

        let _ = self.insert(edge, value_ptr);
        InsertStep::Done
    }

    fn replace_child(&mut self, edge: u8, child: TaggedPointer) {
        let _ = self.insert(edge, child);
    }

    fn remove_child(&mut self, edge: u8) -> Option<TaggedPointer> {
        self.remove(edge)
    }

    fn child_count(&self) -> usize {
        self.meta.len()
    }

    fn prefix_len(&self) -> usize {
        self.meta.prefix_len()
    }

    fn prefix(&self) -> [u8; 8] {
        self.meta.prefix()
    }

    fn set_prefix(&mut self, prefix: &[u8]) {
        self.meta_mut().set_prefix(prefix);
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

        assert_eq!(node.keys[..node.meta.len()], [10, 20, 30]);
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
        assert_eq!(node.meta.len(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_test_raw(2)));
    }

    #[test]
    fn remove_deletes_child_and_keeps_keys_sorted() {
        let mut node = Node4::new(b"");

        node.insert(20, TaggedPointer::from_test_raw(20));
        node.insert(10, TaggedPointer::from_test_raw(10));
        node.insert(30, TaggedPointer::from_test_raw(30));

        assert_eq!(node.remove(20), Some(TaggedPointer::from_test_raw(20)));
        assert_eq!(node.keys[..node.meta.len()], [10, 30]);
        assert_eq!(node.get(20), None);
    }
}
