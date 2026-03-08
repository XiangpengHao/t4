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

enum SearchResult {
    Found(usize),
    Vacant(usize),
}

impl Node4 {
    pub closed spec fn live_len(self) -> usize {
        self.meta.spec_len() as usize
    }

    pub closed spec fn has_key(self, key: u8) -> bool {
        exists|i: int| 0 <= i < self.live_len() && self.keys[i] == key
    }

    pub closed spec fn maps_to(self, key: u8, raw: usize) -> bool {
        exists|i: int| 0 <= i < self.live_len() && self.keys[i] == key && self.children[i] == raw
    }

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

    fn search(&self, key: u8) -> (result: SearchResult)
        requires
            self.wf(),
        ensures
            match result {
                SearchResult::Found(idx) => {
                    &&& idx < self.live_len()
                    &&& self.keys[idx as int] == key
                }
                SearchResult::Vacant(idx) => {
                    &&& idx <= self.live_len()
                    &&& !self.has_key(key)
                    &&& forall|i: int| 0 <= i < idx ==> self.keys[i] < key
                    &&& forall|i: int| idx <= i < self.live_len() ==> key < self.keys[i]
                }
            },
    {
        let len = self.meta.len();
        let mut idx = 0usize;
        while idx < len
            invariant
                self.wf(),
                idx <= len,
                len == self.live_len(),
                forall|j: int| 0 <= j < idx ==> self.keys[j] < key,
            decreases len - idx,
        {
            if self.keys[idx] < key {
                idx += 1;
            } else if self.keys[idx] == key {
                return SearchResult::Found(idx);
            } else {
                return SearchResult::Vacant(idx);
            }
        }

        SearchResult::Vacant(idx)
    }

    pub(crate) fn get(&self, key: u8) -> (result: Option<TaggedPointer>)
        requires
            self.wf(),
        ensures
            result.is_some() <==> self.has_key(key),
    {
        match self.search(key) {
            SearchResult::Found(idx) => Some(TaggedPointer::from_raw(self.children[idx])),
            SearchResult::Vacant(_) => None,
        }
    }

    pub(crate) fn is_full(&self) -> (result: bool)
        ensures
            result <==> self.live_len() == 4,
    {
        self.meta.len() == 4
    }

    fn replace_at(&mut self, idx: usize, value: TaggedPointer) -> (result: TaggedPointer)
        requires
            old(self).wf(),
            idx < old(self).live_len(),
        ensures
            self.wf(),
            self.live_len() == old(self).live_len(),
            forall|i: int| 0 <= i < self.live_len() ==> self.keys[i] == old(self).keys[i],
            self.keys[idx as int] == old(self).keys[idx as int],
            self.children[idx as int] == value.raw(),
            result.raw() == old(self).children[idx as int],
    {
        let prev = TaggedPointer::from_raw(self.children[idx]);
        self.children[idx] = value.to_raw();

        prev
    }

    fn insert_at(&mut self, idx: usize, key: u8, value: TaggedPointer)
        requires
            old(self).wf(),
            old(self).live_len() < 4,
            !old(self).has_key(key),
            idx <= old(self).live_len(),
            forall|i: int| 0 <= i < idx ==> old(self).keys[i] < key,
            forall|i: int| idx <= i < old(self).live_len() ==> key < old(self).keys[i],
        ensures
            self.wf(),
            self.live_len() == old(self).live_len() + 1,
            self.keys[idx as int] == key,
            self.children[idx as int] == value.raw(),
            forall|i: int| 0 <= i < idx ==> self.keys[i] == old(self).keys[i],
            forall|i: int| idx < i < self.live_len() ==> self.keys[i] == old(self).keys[i - 1],
    {
        let ghost old_len = self.live_len();
        let ghost old_keys = self.keys@;
        let ghost old_children = self.children@;
        let value_raw = value.to_raw();
        let len = self.meta.len();
        let mut shift = len;
        while shift > idx
            invariant
                idx <= shift <= len,
                self.meta.spec_len() == len < 4,
                forall|i: int| 0 <= i < idx ==> self.keys[i] == #[trigger] old_keys[i],
                forall|i: int| 0 <= i < idx ==> self.children[i] == #[trigger] old_children[i],
                forall|i: int| idx <= i < shift ==> self.keys[i] == #[trigger] old_keys[i],
                forall|i: int| idx <= i < shift ==> self.children[i] == #[trigger] old_children[i],
                forall|i: int| shift <= i < len ==> self.keys[i + 1] == #[trigger] old_keys[i],
                forall|i: int| shift <= i < len ==> self.children[i + 1] == #[trigger] old_children[i],
            decreases shift - idx,
        {
            self.keys[shift] = self.keys[shift - 1];
            self.children[shift] = self.children[shift - 1];
            shift -= 1;
        }

        self.keys[idx] = key;
        self.children[idx] = value_raw;
        self.meta.increment_len();
        proof {
            assert(self.wf()) by {
                assert forall|i: int, j: int| 0 <= i < j < self.live_len() implies self.keys[i] < self.keys[j] by {
                    if j < idx as int {
                        assert(self.keys[i] == old_keys[i]);
                        assert(self.keys[j] == old_keys[j]);
                    } else if j == idx as int {
                        assert(self.keys[j] == key);
                        if i < idx as int {
                            assert(self.keys[i] == old_keys[i]);
                            assert(old_keys[i] < key);
                        }
                    } else if i < idx as int {
                        assert(self.keys[i] == old_keys[i]);
                        assert(self.keys[j] == old_keys[j - 1]);
                        assert(old_keys[i] < key);
                        assert(key < old_keys[j - 1]);
                    } else if i == idx as int {
                        assert(self.keys[i] == key);
                        assert(self.keys[j] == old_keys[j - 1]);
                        assert(key < old_keys[j - 1]);
                    } else {
                        assert(self.keys[i] == old_keys[i - 1]);
                        assert(self.keys[j] == old_keys[j - 1]);
                    }
                }
                assert forall|i: int| 0 <= i < self.live_len() implies #[trigger] TaggedPointer::wf_raw(self.children[i]) by {
                    if i == idx as int {
                        assert(self.children[i] == value_raw);
                    } else if i < idx as int {
                        assert(self.children[i] == old_children[i]);
                    } else {
                        assert(self.children[i] == old_children[i - 1]);
                    }
                }
            }
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> (result: Option<TaggedPointer>)
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
        match self.search(key) {
            SearchResult::Found(idx) => Some(self.replace_at(idx, value)),
            SearchResult::Vacant(idx) => {
                self.insert_at(idx, key, value);
                None
            }
        }
    }

    fn remove_at(&mut self, idx: usize) -> (result: TaggedPointer)
        requires
            old(self).wf(),
            idx < old(self).live_len(),
        ensures
            self.wf(),
            self.live_len() + 1 == old(self).live_len(),
            result.raw() == old(self).children[idx as int],
            forall|i: int| 0 <= i < idx ==> self.keys[i] == old(self).keys[i],
            forall|i: int| idx <= i < self.live_len() ==> self.keys[i] == old(self).keys[i + 1],
            forall|i: int| 0 <= i < idx ==> self.children[i] == old(self).children[i],
            forall|i: int| idx <= i < self.live_len() ==> self.children[i] == old(self).children[i + 1],
    {
        let ghost old_len = self.live_len();
        let ghost old_keys = self.keys@;
        let ghost old_children = self.children@;
        let len = self.meta.len();
        let removed_raw = self.children[idx];
        let mut shift = idx + 1;
        while shift < len
            invariant
                idx + 1 <= shift <= len,
                self.meta.spec_len() == len,
                len == old_len,
                len <= 4,
                forall|i: int| 0 <= i < idx ==> self.keys[i] == #[trigger] old_keys[i],
                forall|i: int| 0 <= i < idx ==> self.children[i] == #[trigger] old_children[i],
                forall|i: int| idx <= i < shift - 1 ==> self.keys[i] == #[trigger] old_keys[i + 1],
                forall|i: int| idx <= i < shift - 1 ==> self.children[i] == #[trigger] old_children[i + 1],
                forall|i: int| shift <= i < len ==> self.keys[i] == #[trigger] old_keys[i],
                forall|i: int| shift <= i < len ==> self.children[i] == #[trigger] old_children[i],
            decreases len - shift,
        {
            self.keys[shift - 1] = self.keys[shift];
            self.children[shift - 1] = self.children[shift];
            shift += 1;
        }

        self.meta.decrement_len();

        proof {
            assert(self.wf()) by {
                assert forall|i: int, j: int| 0 <= i < j < self.live_len() implies self.keys[i] < self.keys[j] by {
                    if j < idx as int {
                        assert(self.keys[i] == old_keys[i]);
                        assert(self.keys[j] == old_keys[j]);
                    } else if i < idx as int {
                        assert(self.keys[i] == old_keys[i]);
                        assert(self.keys[j] == old_keys[j + 1]);
                    } else {
                        assert(self.keys[i] == old_keys[i + 1]);
                        assert(self.keys[j] == old_keys[j + 1]);
                    }
                }
                assert forall|i: int| 0 <= i < self.live_len() implies #[trigger] TaggedPointer::wf_raw(self.children[i]) by {
                    if i < idx as int {
                        assert(self.children[i] == old_children[i]);
                    } else {
                        assert(self.children[i] == old_children[i + 1]);
                    }
                }
            }
        }

        TaggedPointer::from_raw(removed_raw)
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
                self.maps_to(other_key, raw) ==> old(self).maps_to(other_key, raw) && other_key != key,
            forall|other_key: u8, raw: usize|
                other_key != key && old(self).maps_to(other_key, raw) ==> self.maps_to(other_key, raw),
    {
        match self.search(key) {
            SearchResult::Found(idx) => {
                let removed = self.remove_at(idx);
                proof {
                    assert forall|other_key: u8, raw: usize|
                        other_key != key && old(self).maps_to(other_key, raw) implies self.maps_to(other_key, raw) by {
                        let i = choose|i: int| 0 <= i < old(self).live_len() && old(self).keys[i] == other_key && old(self).children[i] == raw;
                        if i < idx as int {
                            assert(self.keys[i] == old(self).keys[i]);
                            assert(self.children[i] == old(self).children[i]);
                            assert(self.maps_to(other_key, raw));
                        } else {
                            assert(self.keys[i - 1] == old(self).keys[i]);
                            assert(self.children[i - 1] == old(self).children[i]);
                            assert(self.maps_to(other_key, raw));
                        }
                    }
                }
                Some(removed)
            }
            SearchResult::Vacant(_) => None,
        }
    }

}

} // verus!

impl Node4 {
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
        self.meta.set_prefix(prefix);
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
