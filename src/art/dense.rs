use vstd::{prelude::*, slice::slice_subrange};

use crate::art::{
    InsertStep,
    index::common_prefix_len,
    meta::{NodeMeta, NodeType},
    ptr::TaggedPointer,
};

verus! {

enum SearchResult {
    Found(usize),
    Vacant(usize),
}

#[repr(C, align(16))]
pub(crate) struct DenseNode<const CAP: usize> {
    meta: NodeMeta,
    keys: [u8; CAP],
    children: [usize; CAP],
}

impl<const CAP: usize> DenseNode<CAP> {
    pub closed spec fn live_len(self) -> usize {
        self.meta.spec_len() as usize
    }

    pub closed spec fn raw_prefix_len(self) -> usize {
        self.meta.raw_prefix_len() as usize
    }

    pub closed spec fn has_key(self, key: u8) -> bool {
        exists|i: int| 0 <= i < self.live_len() && self.keys[i] == key
    }

    pub closed spec fn maps_to(self, key: u8, raw: usize) -> bool {
        exists|i: int| 0 <= i < self.live_len() && self.keys[i] == key && self.children[i] == raw
    }

    pub closed spec fn wf(&self) -> bool {
        &&& CAP <= u16::MAX as usize
        &&& self.live_len() <= CAP
        &&& forall|i: int, j: int| 0 <= i < j < self.live_len() ==> self.keys[i] < self.keys[j]
        &&& forall|i: int|
            0 <= i < self.live_len() ==> #[trigger] TaggedPointer::wf_raw(self.children[i])
    }

    pub(crate) fn new(node_type: NodeType, prefix: &[u8]) -> (result: Self)
        requires
            CAP <= u16::MAX as usize,
            prefix.len() <= NodeMeta::prefix_capacity(),
        ensures
            result.wf(),
            result.live_len() == 0,
    {
        let meta = NodeMeta::new(node_type, prefix);
        Self { meta, keys: [0;CAP], children: [0;CAP] }
    }

    fn search(&self, key: u8) -> (result: SearchResult)
        requires
            self.wf(),
        ensures
            match result {
                SearchResult::Found(idx) => {
                    &&& idx < self.live_len()
                    &&& self.keys[idx as int] == key
                },
                SearchResult::Vacant(idx) => {
                    &&& idx <= self.live_len()
                    &&& !self.has_key(key)
                    &&& forall|i: int| 0 <= i < idx ==> self.keys[i] < key
                    &&& forall|i: int| idx <= i < self.live_len() ==> key < self.keys[i]
                },
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
                idx = idx + 1;
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
            result.is_some() ==> self.maps_to(key, result.unwrap().raw()),
    {
        match self.search(key) {
            SearchResult::Found(idx) => {
                let result = TaggedPointer::from_raw(self.children[idx]);
                Some(result)
            },
            SearchResult::Vacant(_) => None,
        }
    }

    pub(crate) fn is_full(&self) -> (result: bool)
        ensures
            result <==> self.live_len() == CAP,
    {
        self.meta.len() == CAP
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
            old(self).live_len() < CAP,
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
        let ghost old_keys = self.keys@;
        let ghost old_children = self.children@;
        let value_raw = value.to_raw();
        let len = self.meta.len();
        let mut shift = len;
        while shift > idx
            invariant
                idx <= shift <= len,
                self.meta.spec_len() == len < CAP,
                forall|i: int| 0 <= i < idx ==> self.keys[i] == #[trigger] old_keys[i],
                forall|i: int| 0 <= i < idx ==> self.children[i] == #[trigger] old_children[i],
                forall|i: int| idx <= i < shift ==> self.keys[i] == #[trigger] old_keys[i],
                forall|i: int| idx <= i < shift ==> self.children[i] == #[trigger] old_children[i],
                forall|i: int| shift <= i < len ==> self.keys[i + 1] == #[trigger] old_keys[i],
                forall|i: int|
                    shift <= i < len ==> self.children[i + 1] == #[trigger] old_children[i],
            decreases shift - idx,
        {
            self.keys[shift] = self.keys[shift - 1];
            self.children[shift] = self.children[shift - 1];
            shift = shift - 1;
        }

        self.keys[idx] = key;
        self.children[idx] = value_raw;
        self.meta.increment_len();
        proof {
            assert forall|i: int| 0 <= i < self.live_len() implies self.keys[i] == (if i
                < idx as int {
                old_keys[i]
            } else if i == idx as int {
                key
            } else {
                old_keys[i - 1]
            }) by {};
            assert forall|i: int| 0 <= i < self.live_len() implies self.children[i] == (if i
                < idx as int {
                old_children[i]
            } else if i == idx as int {
                value_raw
            } else {
                old_children[i - 1]
            }) by {};
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> (result: Option<
        TaggedPointer,
    >)
        requires
            old(self).wf(),
            old(self).has_key(key) || old(self).live_len() < CAP,
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
            },
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
            forall|i: int|
                idx <= i < self.live_len() ==> self.children[i] == old(self).children[i + 1],
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
                len <= CAP,
                forall|i: int| 0 <= i < idx ==> self.keys[i] == #[trigger] old_keys[i],
                forall|i: int| 0 <= i < idx ==> self.children[i] == #[trigger] old_children[i],
                forall|i: int| idx <= i < shift - 1 ==> self.keys[i] == #[trigger] old_keys[i + 1],
                forall|i: int|
                    idx <= i < shift - 1 ==> self.children[i] == #[trigger] old_children[i + 1],
                forall|i: int| shift <= i < len ==> self.keys[i] == #[trigger] old_keys[i],
                forall|i: int| shift <= i < len ==> self.children[i] == #[trigger] old_children[i],
            decreases len - shift,
        {
            self.keys[shift - 1] = self.keys[shift];
            self.children[shift - 1] = self.children[shift];
            shift = shift + 1;
        }

        self.meta.decrement_len();

        proof {
            assert forall|i: int| 0 <= i < self.live_len() implies self.keys[i] == (if i
                < idx as int {
                old_keys[i]
            } else {
                old_keys[i + 1]
            }) by {};
            assert forall|i: int| 0 <= i < self.live_len() implies self.children[i] == (if i
                < idx as int {
                old_children[i]
            } else {
                old_children[i + 1]
            }) by {};
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
                self.maps_to(other_key, raw) ==> old(self).maps_to(other_key, raw) && other_key
                    != key,
            forall|other_key: u8, raw: usize|
                other_key != key && old(self).maps_to(other_key, raw) ==> self.maps_to(
                    other_key,
                    raw,
                ),
    {
        match self.search(key) {
            SearchResult::Found(idx) => {
                let removed = self.remove_at(idx);
                proof {
                    assert forall|other_key: u8, raw: usize|
                        other_key != key && old(self).maps_to(other_key, raw) implies self.maps_to(
                        other_key,
                        raw,
                    ) by {
                        let i = choose|i: int|
                            0 <= i < old(self).live_len() && old(self).keys[i] == other_key && old(
                                self,
                            ).children[i] == raw;
                        if i < idx as int {
                            assert(self.keys[i] == old(self).keys[i]);
                            assert(self.children[i] == old(self).children[i]);
                        } else {
                            assert(self.keys[i - 1] == old(self).keys[i]);
                            assert(self.children[i - 1] == old(self).children[i]);
                        }
                    }
                }
                Some(removed)
            },
            SearchResult::Vacant(_) => None,
        }
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
        let prefix_depth = depth;
        let prefix_len = self.meta.prefix_len();
        let prefix = self.meta.prefix_slice();
        let matched = common_prefix_len(
            slice_subrange(prefix, 0, prefix_len),
            slice_subrange(terminated_key, depth, terminated_key.len()),
        );
        if matched != prefix_len {
            return InsertStep::Split { matched };
        }
        let depth = depth + prefix_len;
        let edge = terminated_key[depth];
        match self.search(edge) {
            SearchResult::Found(idx) => {
                let child = TaggedPointer::from_raw(self.children[idx]);
                InsertStep::Descend { edge, child, next_depth: depth + 1 }
            },
            SearchResult::Vacant(_) => {
                if self.is_full() {
                    InsertStep::Grow { prefix_depth, prefix_len }
                } else {
                    let _ = self.insert(edge, value_ptr);
                    InsertStep::Done
                }
            },
        }
    }

    pub(crate) fn child_count(&self) -> (result: usize)
        ensures
            result == self.live_len(),
    {
        self.meta.len()
    }

    pub(crate) fn prefix_len(&self) -> (result: usize)
        ensures
            result == self.raw_prefix_len(),
            result <= NodeMeta::prefix_capacity(),
    {
        self.meta.prefix_len()
    }

    pub(crate) fn prefix(&self) -> [u8; 8] {
        self.meta.prefix()
    }

    pub(crate) fn prefix_slice(&self) -> (result: &[u8])
        ensures
            result@.len() == NodeMeta::prefix_capacity(),
    {
        self.meta.prefix_slice()
    }

    pub(crate) fn set_prefix(&mut self, prefix: &[u8])
        requires
            old(self).wf(),
            prefix.len() <= NodeMeta::prefix_capacity(),
        ensures
            self.wf(),
            self.live_len() == old(self).live_len(),
            self.raw_prefix_len() == prefix.len(),
    {
        self.meta.set_prefix(prefix);
    }

    pub(crate) fn entry_at(&self, idx: usize) -> (result: (u8, TaggedPointer))
        requires
            self.wf(),
            idx < self.live_len(),
        ensures
            self.maps_to(result.0, result.1.raw()),
    {
        (self.keys[idx], TaggedPointer::from_raw(self.children[idx]))
    }

    pub proof fn lemma_live_len_bound(&self)
        requires
            self.wf(),
        ensures
            self.live_len() <= CAP,
    {
    }
}

} // verus!
impl<const CAP: usize> DenseNode<CAP> {
    pub(crate) fn for_each_child(&self, mut f: impl FnMut(u8, TaggedPointer)) {
        let len = self.meta.len();
        for idx in 0..len {
            f(self.keys[idx], TaggedPointer::from_raw(self.children[idx]));
        }
    }
}
