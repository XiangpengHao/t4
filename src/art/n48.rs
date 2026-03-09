use vstd::{prelude::*, slice::slice_subrange};

use crate::art::{
    ArtNode, InsertStep,
    art::common_prefix_len,
    meta::{NodeMeta, NodeType},
    n256::Node256,
    ptr::TaggedPointer,
};

verus! {

const EMPTY_CHILD: u8 = 255;

#[repr(C, align(16))]
pub(crate) struct Node48 {
    meta: NodeMeta,
    child_idx: [u8; 256],
    children: [usize; 48],
}

impl Node48 {
    pub closed spec fn live_len(self) -> usize {
        self.meta.spec_len() as usize
    }

    pub closed spec fn raw_prefix_len(self) -> usize {
        self.meta.raw_prefix_len() as usize
    }

    pub closed spec fn has_key(self, key: u8) -> bool {
        self.child_idx[key as int] != EMPTY_CHILD
    }

    pub closed spec fn maps_to(self, key: u8, raw: usize) -> bool {
        exists|slot: int|
            0 <= slot < self.live_len() && self.child_idx[key as int] as int == slot
                && self.children[slot] == raw
    }

    pub closed spec fn slot_has_key(self, slot: int) -> bool {
        exists|key: int| 0 <= key < 256 && self.child_idx[key] as int == slot
    }

    pub closed spec fn wf(&self) -> bool {
        &&& self.live_len() <= 48
        &&& forall|key: int|
            0 <= key < 256 ==> self.child_idx[key] == EMPTY_CHILD || (self.child_idx[key] as int)
                < self.live_len()
        &&& forall|key: int|
            0 <= key < 256 && self.child_idx[key] != EMPTY_CHILD ==> #[trigger] TaggedPointer::wf_raw(
                self.children[self.child_idx[key] as int],
            )
        &&& forall|left: int, right: int|
            0 <= left < 256 && 0 <= right < 256 && self.child_idx[left] != EMPTY_CHILD
                && self.child_idx[left] == self.child_idx[right] ==> left == right
        &&& forall|slot: int| 0 <= slot < self.live_len() ==> #[trigger] self.slot_has_key(slot)
    }

    pub(crate) fn new(prefix: &[u8]) -> (result: Self)
        requires
            prefix.len() <= NodeMeta::prefix_capacity(),
        ensures
            result.wf(),
            result.live_len() == 0,
    {
        let meta = NodeMeta::new(NodeType::Node48, prefix);
        Self {
            meta,
            child_idx: [EMPTY_CHILD; 256],
            children: [0; 48],
        }
    }

    fn key_for_slot(&self, target: u8) -> (result: u8)
        requires
            self.wf(),
            (target as int) < self.live_len(),
            target < 48u8,
        ensures
            self.child_idx[result as int] == target,
    {
        let mut idx = 0usize;
        while idx < 256
            invariant
                (target as int) < self.live_len(),
                forall|j: int| 0 <= j < idx ==> self.child_idx[j] != target,
            decreases 256 - idx,
        {
            if self.child_idx[idx] == target {
                return idx as u8;
            }
            idx += 1;
        }

        proof {
            assert(self.slot_has_key(target as int));
            let w = choose|k: int| 0 <= k < 256 && self.child_idx[k] as int == target as int;
            assert(self.child_idx[w] != target);
        }
        0
    }

    pub(crate) fn get(&self, key: u8) -> (result: Option<TaggedPointer>)
        requires
            self.wf(),
        ensures
            result.is_some() <==> self.has_key(key),
            result.is_some() ==> self.maps_to(key, result.unwrap().raw()),
    {
        let child_idx = self.child_idx[key as usize];
        if child_idx == EMPTY_CHILD {
            return None;
        }

        Some(TaggedPointer::from_raw(self.children[child_idx as usize]))
    }

    fn insert_existing(&mut self, key: u8, value: TaggedPointer) -> (result: TaggedPointer)
        requires
            old(self).wf(),
            old(self).has_key(key),
        ensures
            self.wf(),
            self.live_len() == old(self).live_len(),
            self.maps_to(key, value.raw()),
            result.raw() == old(self).children[old(self).child_idx[key as int] as int],
    {
        let slot = self.child_idx[key as usize] as usize;
        let prev = TaggedPointer::from_raw(self.children[slot]);
        let value_raw = value.to_raw();
        self.children[slot] = value_raw;

        proof {
            assert forall|idx: int|
                0 <= idx < 256 && self.child_idx[idx] != EMPTY_CHILD
                    implies #[trigger] TaggedPointer::wf_raw(
                    self.children[self.child_idx[idx] as int],
                ) by {
                if idx == key as int {
                    assert(self.children[self.child_idx[idx] as int] == value_raw);
                }
            };
            assert forall|s: int|
                0 <= s < self.live_len() implies self.slot_has_key(s) by {
                assert(old(self).slot_has_key(s));
                let w = choose|w: int|
                    0 <= w < 256 && old(self).child_idx[w] as int == s;
                assert(self.child_idx[w] as int == s);
            };
            assert(self.children[slot as int] == value_raw);
            assert(self.child_idx[key as int] as int == slot as int);
        }

        prev
    }

    fn insert_fresh(&mut self, key: u8, value: TaggedPointer)
        requires
            old(self).wf(),
            !old(self).has_key(key),
            old(self).live_len() < 48,
        ensures
            self.wf(),
            self.live_len() == old(self).live_len() + 1,
            self.maps_to(key, value.raw()),
    {
        let len = self.meta.len();
        let value_raw = value.to_raw();
        self.child_idx[key as usize] = len as u8;
        self.children[len] = value_raw;
        self.meta.increment_len();

        proof {
            assert forall|idx: int| #![auto] 0 <= idx < 256 implies self.child_idx[idx] == (
                if idx == key as int { len as u8 } else { old(self).child_idx[idx] }
            ) by {};
            assert forall|idx: int|
                0 <= idx < 256 && self.child_idx[idx] != EMPTY_CHILD
                    implies #[trigger] TaggedPointer::wf_raw(
                    self.children[self.child_idx[idx] as int],
                ) by {
                if idx == key as int {
                    assert(self.children[len as int] == value_raw);
                }
            };
            assert forall|s: int|
                0 <= s < self.live_len() implies self.slot_has_key(s) by {
                if s == len as int {
                    assert(self.child_idx[key as int] as int == s);
                } else {
                    assert(old(self).slot_has_key(s));
                    let w = choose|w: int|
                        0 <= w < 256 && old(self).child_idx[w] as int == s;
                    assert(self.child_idx[w] as int == s);
                }
            };
            assert(self.children[len as int] == value_raw);
            assert(self.child_idx[key as int] as int == len as int);
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> (result: Option<
        TaggedPointer,
    >)
        requires
            old(self).wf(),
            old(self).has_key(key) || old(self).live_len() < 48,
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
        if self.child_idx[key as usize] != EMPTY_CHILD {
            Some(self.insert_existing(key, value))
        } else {
            self.insert_fresh(key, value);
            None
        }
    }

    fn remove_existing(&mut self, key: u8) -> (result: TaggedPointer)
        requires
            old(self).wf(),
            old(self).has_key(key),
        ensures
            self.wf(),
            !self.has_key(key),
            self.live_len() + 1 == old(self).live_len(),
            result.raw() == old(self).children[old(self).child_idx[key as int] as int],
            forall|other_key: u8, raw: usize|
                self.maps_to(other_key, raw) ==> old(self).maps_to(other_key, raw) && other_key
                    != key,
            forall|other_key: u8, raw: usize|
                other_key != key && old(self).maps_to(other_key, raw) ==> self.maps_to(
                    other_key,
                    raw,
                ),
    {
        let child_idx = self.child_idx[key as usize];
        let slot = child_idx as usize;
        let len = self.meta.len();
        let last_slot = len - 1;
        let removed_raw = self.children[slot];
        let moved_key = if slot != last_slot {
            proof {
                assert(last_slot < 48);
                assert((last_slot as u8) < 48u8);
            }
            Some(self.key_for_slot(last_slot as u8))
        } else {
            None
        };

        if let Some(moved_key) = moved_key {
            self.children[slot] = self.children[last_slot];
            self.child_idx[moved_key as usize] = child_idx;
        }
        self.child_idx[key as usize] = EMPTY_CHILD;
        self.meta.decrement_len();

        proof {
            // slot_has_key: provide existential witnesses
            assert forall|s: int|
                0 <= s < self.live_len() implies self.slot_has_key(s) by {
                if moved_key.is_some() && s == slot as int {
                    assert(self.child_idx[moved_key.unwrap() as int] as int == s);
                } else {
                    assert(old(self).slot_has_key(s));
                    let w = choose|w: int|
                        0 <= w < 256 && old(self).child_idx[w] as int == s;
                    assert(self.child_idx[w] == old(self).child_idx[w]);
                }
            };
            // maps_to: new → old
            assert forall|other_key: u8, raw: usize|
                self.maps_to(other_key, raw) implies old(self).maps_to(other_key, raw)
                    && other_key != key by {
                let w = choose|s: int|
                    0 <= s < 48 && self.child_idx[other_key as int] as int == s
                        && self.children[s] == raw;
                if moved_key.is_some() && other_key as int == moved_key.unwrap() as int {
                    assert(raw == old(self).children[last_slot as int]);
                }
            };
            // maps_to: old → new
            assert forall|other_key: u8, raw: usize|
                other_key != key && old(self).maps_to(other_key, raw) implies self.maps_to(
                    other_key,
                    raw,
                ) by {
                let w = choose|s: int|
                    0 <= s < 48 && old(self).child_idx[other_key as int] as int == s
                        && old(self).children[s] == raw;
                if moved_key.is_some() && other_key as int == moved_key.unwrap() as int {
                    assert(self.child_idx[other_key as int] == child_idx);
                    assert(self.children[child_idx as int] == raw);
                } else {
                    assert(self.children[w] == old(self).children[w]);
                }
            };
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
        if self.child_idx[key as usize] == EMPTY_CHILD {
            None
        } else {
            Some(self.remove_existing(key))
        }
    }

    pub(crate) fn is_full(&self) -> (result: bool)
        ensures
            result <==> self.live_len() == 48,
    {
        self.meta.len() == 48
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

    pub(crate) fn child_count(&self) -> usize {
        self.meta.len()
    }

    pub(crate) fn prefix_len(&self) -> usize {
        self.meta.prefix_len()
    }

    pub(crate) fn prefix(&self) -> [u8; 8] {
        self.meta.prefix()
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
        proof {
            assert forall|slot: int|
                0 <= slot < self.live_len() implies self.slot_has_key(slot) by {
                assert(old(self).slot_has_key(slot));
                let w = choose|w: int|
                    0 <= w < 256 && old(self).child_idx[w] as int == slot;
                assert(self.child_idx[w] as int == slot);
            };
        }
    }
}

} // verus!

impl Node48 {
    pub(crate) fn for_each_child(&self, mut f: impl FnMut(u8, TaggedPointer)) {
        for key in 0..=u8::MAX {
            if let Some(child) = self.get(key) {
                f(key, child);
            }
        }
    }

    pub(crate) fn grow(&self, prefix: &[u8]) -> TaggedPointer {
        let mut grown = Node256::new(prefix);
        self.for_each_child(|key, child| {
            let _ = grown.insert(key, child);
        });
        TaggedPointer::from_node256(Box::new(grown))
    }
}

impl ArtNode for Node48 {
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
        self.child_count()
    }

    fn prefix_len(&self) -> usize {
        self.prefix_len()
    }

    fn prefix(&self) -> [u8; 8] {
        self.prefix()
    }

    fn set_prefix(&mut self, prefix: &[u8]) {
        self.set_prefix(prefix);
    }

    fn get_child(&self, edge: u8) -> Option<TaggedPointer> {
        self.get(edge)
    }
}

#[cfg(test)]
mod tests {
    use super::Node48;
    use crate::art::ptr::TaggedPointer;

    #[test]
    fn insert_and_get_sparse_keys() {
        let mut node = Node48::new(b"");

        node.insert(200, TaggedPointer::from_test_raw(200));
        node.insert(3, TaggedPointer::from_test_raw(3));
        node.insert(128, TaggedPointer::from_test_raw(128));

        assert_eq!(node.child_count(), 3);
        assert_eq!(node.get(3), Some(TaggedPointer::from_test_raw(3)));
        assert_eq!(node.get(128), Some(TaggedPointer::from_test_raw(128)));
        assert_eq!(node.get(200), Some(TaggedPointer::from_test_raw(200)));
        assert_eq!(node.get(42), None);
    }

    #[test]
    fn insert_replaces_existing_child() {
        let mut node = Node48::new(b"");

        assert_eq!(node.insert(7, TaggedPointer::from_test_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_test_raw(2)),
            Some(TaggedPointer::from_test_raw(1))
        );
        assert_eq!(node.child_count(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_test_raw(2)));
    }

    #[test]
    fn remove_deletes_sparse_child() {
        let mut node = Node48::new(b"");

        node.insert(200, TaggedPointer::from_test_raw(200));
        node.insert(3, TaggedPointer::from_test_raw(3));
        node.insert(128, TaggedPointer::from_test_raw(128));

        assert_eq!(node.remove(3), Some(TaggedPointer::from_test_raw(3)));
        assert_eq!(node.get(3), None);
        assert_eq!(node.get(128), Some(TaggedPointer::from_test_raw(128)));
        assert_eq!(node.get(200), Some(TaggedPointer::from_test_raw(200)));
    }
}
