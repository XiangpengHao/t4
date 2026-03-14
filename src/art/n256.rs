use vstd::{prelude::*, slice::slice_subrange};

use crate::art::{
    ArtNode, InsertStep,
    meta::{NodeMeta, NodeType},
    ptr::TaggedPointer,
};

verus! {

spec fn live_child_count(children: [usize; 256], upto: int) -> int
    decreases upto,
{
    if upto <= 0 {
        0int
    } else {
        live_child_count(children, upto - 1) + if children[upto - 1] == 0 {
            0int
        } else {
            1int
        }
    }
}

proof fn lemma_live_child_count_replace(
    old_children: [usize; 256],
    new_children: [usize; 256],
    key: int,
    upto: int,
)
    requires
        0 <= key < 256,
        0 <= upto <= 256,
        old_children[key] != 0,
        new_children[key] != 0,
        forall|idx: int|
            0 <= idx < 256 ==> #[trigger] new_children[idx] == (if idx == key {
                new_children[key]
            } else {
                old_children[idx]
            }),
    ensures
        live_child_count(new_children, upto) == live_child_count(old_children, upto),
    decreases upto,
{
    if upto > 0 {
        lemma_live_child_count_replace(old_children, new_children, key, upto - 1);
        if upto - 1 == key {
        } else {
            assert(new_children[upto - 1] == old_children[upto - 1]);
        }
    }
}

proof fn lemma_live_child_count_insert(
    old_children: [usize; 256],
    new_children: [usize; 256],
    key: int,
    upto: int,
)
    requires
        0 <= key < 256,
        0 <= upto <= 256,
        old_children[key] == 0,
        new_children[key] != 0,
        forall|idx: int|
            0 <= idx < 256 ==> #[trigger] new_children[idx] == (if idx == key {
                new_children[key]
            } else {
                old_children[idx]
            }),
    ensures
        live_child_count(new_children, upto) == live_child_count(old_children, upto) + if key
            < upto {
            1int
        } else {
            0int
        },
    decreases upto,
{
    if upto > 0 {
        lemma_live_child_count_insert(old_children, new_children, key, upto - 1);
        if upto - 1 == key {
        } else {
            assert(new_children[upto - 1] == old_children[upto - 1]);
        }
    }
}

proof fn lemma_live_child_count_remove(
    old_children: [usize; 256],
    new_children: [usize; 256],
    key: int,
    upto: int,
)
    requires
        0 <= key < 256,
        0 <= upto <= 256,
        old_children[key] != 0,
        new_children[key] == 0,
        forall|idx: int|
            0 <= idx < 256 ==> #[trigger] new_children[idx] == (if idx == key {
                0usize
            } else {
                old_children[idx]
            }),
    ensures
        live_child_count(old_children, upto) == live_child_count(new_children, upto) + if key
            < upto {
            1int
        } else {
            0int
        },
    decreases upto,
{
    if upto > 0 {
        lemma_live_child_count_remove(old_children, new_children, key, upto - 1);
        if upto - 1 == key {
        } else {
            assert(new_children[upto - 1] == old_children[upto - 1]);
        }
    }
}

proof fn lemma_live_child_count_all_zero(children: [usize; 256], upto: int)
    requires
        0 <= upto <= 256,
        forall|idx: int| 0 <= idx < upto ==> children[idx] == 0,
    ensures
        live_child_count(children, upto) == 0int,
    decreases upto,
{
    if upto > 0 {
        lemma_live_child_count_all_zero(children, upto - 1);
    }
}

proof fn lemma_live_child_count_upper(children: [usize; 256], upto: int)
    requires
        0 <= upto <= 256,
    ensures
        live_child_count(children, upto) <= upto,
    decreases upto,
{
    if upto > 0 {
        lemma_live_child_count_upper(children, upto - 1);
    }
}

proof fn lemma_live_child_count_nonnegative(children: [usize; 256], upto: int)
    requires
        0 <= upto <= 256,
    ensures
        0 <= live_child_count(children, upto),
    decreases upto,
{
    if upto > 0 {
        lemma_live_child_count_nonnegative(children, upto - 1);
    }
}

proof fn lemma_live_child_count_positive(children: [usize; 256], key: int, upto: int)
    requires
        0 <= key < upto <= 256,
        children[key] != 0,
    ensures
        live_child_count(children, upto) > 0,
    decreases upto,
{
    if upto - 1 == key {
        lemma_live_child_count_nonnegative(children, upto - 1);
    } else {
        lemma_live_child_count_positive(children, key, upto - 1);
    }
}

proof fn lemma_live_child_count_missing_bound(children: [usize; 256], key: int, upto: int)
    requires
        0 <= key < upto <= 256,
        children[key] == 0,
    ensures
        live_child_count(children, upto) < upto,
    decreases upto,
{
    if upto - 1 == key {
        lemma_live_child_count_upper(children, upto - 1);
    } else {
        lemma_live_child_count_missing_bound(children, key, upto - 1);
    }
}

#[repr(C, align(16))]
pub(crate) struct Node256 {
    meta: NodeMeta,
    children: [usize; 256],
}

impl Node256 {
    pub closed spec fn live_len(self) -> usize {
        self.meta.spec_len() as usize
    }

    pub closed spec fn raw_prefix_len(self) -> usize {
        self.meta.raw_prefix_len() as usize
    }

    pub closed spec fn has_key(self, key: u8) -> bool {
        self.children[key as int] != 0
    }

    pub closed spec fn maps_to(self, key: u8, raw: usize) -> bool {
        self.has_key(key) && self.children[key as int] == raw
    }

    pub closed spec fn wf(&self) -> bool {
        &&& self.live_len() <= 256
        &&& self.live_len() as int == live_child_count(self.children, 256)
        &&& forall|key: int|
            0 <= key < 256 && self.children[key] != 0 ==> #[trigger] TaggedPointer::wf_raw(
                self.children[key],
            )
    }

    pub(crate) fn new(prefix: &[u8]) -> (result: Self)
        requires
            prefix.len() <= NodeMeta::prefix_capacity(),
        ensures
            result.wf(),
            result.live_len() == 0,
    {
        let meta = NodeMeta::new(NodeType::Node256, prefix);
        let result = Self { meta, children: [0;256] };
        proof {
            lemma_live_child_count_all_zero(result.children, 256);
        }
        result
    }

    pub(crate) fn get(&self, key: u8) -> (result: Option<TaggedPointer>)
        requires
            self.wf(),
        ensures
            result.is_some() <==> self.has_key(key),
            result.is_some() ==> self.maps_to(key, result.unwrap().raw()),
    {
        let raw = self.children[key as usize];
        if raw == 0 {
            None
        } else {
            Some(TaggedPointer::from_raw(raw))
        }
    }

    fn insert_existing(&mut self, key: u8, value: TaggedPointer) -> (result: TaggedPointer)
        requires
            old(self).wf(),
            old(self).has_key(key),
        ensures
            self.wf(),
            self.live_len() == old(self).live_len(),
            self.maps_to(key, value.raw()),
            result.raw() == old(self).children[key as int],
    {
        let ghost old_children = self.children;
        let prev = TaggedPointer::from_raw(self.children[key as usize]);
        let value_raw = value.to_raw();
        self.children[key as usize] = value_raw;

        proof {
            TaggedPointer::lemma_wf_raw_nonzero(value_raw);
            assert forall|idx: int| 0 <= idx < 256 implies #[trigger] self.children[idx] == (if idx
                == key as int {
                value_raw
            } else {
                old_children[idx]
            }) by {};
            lemma_live_child_count_replace(old_children, self.children, key as int, 256);
        }

        prev
    }

    fn insert_fresh(&mut self, key: u8, value: TaggedPointer)
        requires
            old(self).wf(),
            !old(self).has_key(key),
            old(self).live_len() < 256,
        ensures
            self.wf(),
            self.live_len() == old(self).live_len() + 1,
            self.maps_to(key, value.raw()),
            self.has_key(key),
    {
        let ghost old_children = self.children;
        let value_raw = value.to_raw();
        self.children[key as usize] = value_raw;
        self.meta.increment_len();

        proof {
            TaggedPointer::lemma_wf_raw_nonzero(value_raw);
            assert forall|idx: int| 0 <= idx < 256 implies #[trigger] self.children[idx] == (if idx
                == key as int {
                value_raw
            } else {
                old_children[idx]
            }) by {};
            lemma_live_child_count_insert(old_children, self.children, key as int, 256);
        }
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> (result: Option<
        TaggedPointer,
    >)
        requires
            old(self).wf(),
            old(self).has_key(key) || old(self).live_len() < 256,
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
        if self.children[key as usize] != 0 {
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
            result.raw() == old(self).children[key as int],
            forall|other_key: u8, raw: usize|
                self.maps_to(other_key, raw) ==> old(self).maps_to(other_key, raw) && other_key
                    != key,
            forall|other_key: u8, raw: usize|
                other_key != key && old(self).maps_to(other_key, raw) ==> self.maps_to(
                    other_key,
                    raw,
                ),
    {
        let ghost old_children = self.children;
        let removed_raw = self.children[key as usize];
        proof {
            lemma_live_child_count_positive(old_children, key as int, 256);
            assert(old(self).live_len() > 0);
        }
        self.children[key as usize] = 0;
        self.meta.decrement_len();

        proof {
            assert forall|idx: int| 0 <= idx < 256 implies #[trigger] self.children[idx] == (if idx
                == key as int {
                0usize
            } else {
                old_children[idx]
            }) by {};
            lemma_live_child_count_remove(old_children, self.children, key as int, 256);
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
        if self.children[key as usize] == 0 {
            None
        } else {
            Some(self.remove_existing(key))
        }
    }
}

impl ArtNode for Node256 {
    closed spec fn live_len(self) -> usize {
        Node256::live_len(self)
    }

    closed spec fn has_key(self, key: u8) -> bool {
        Node256::has_key(self, key)
    }

    closed spec fn maps_to(self, key: u8, raw: usize) -> bool {
        Node256::maps_to(self, key, raw)
    }

    closed spec fn wf(&self) -> bool {
        Node256::wf(self)
    }

    closed spec fn raw_prefix_len(self) -> usize {
        Node256::raw_prefix_len(self)
    }

    fn insert_step(
        &mut self,
        terminated_key: crate::art::index::TerminatedKeyRef<'_>,
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> (result: InsertStep) {
        let _key_len = terminated_key.len();
        let prefix_len = self.meta.prefix_len();
        let prefix = self.meta.prefix_slice();
        let matched = crate::art::index::common_prefix_len_slice_terminated(
            slice_subrange(prefix, 0, prefix_len),
            terminated_key.suffix(depth),
        );
        if matched != prefix_len {
            return InsertStep::Split { matched };
        }
        let depth = depth + prefix_len;
        let edge = terminated_key.byte(depth);
        if let Some(child) = self.get(edge) {
            return InsertStep::Descend { edge, child, next_depth: depth + 1 };
        }
        proof {
            lemma_live_child_count_missing_bound(old(self).children, edge as int, 256);
            assert(old(self).live_len() < 256);
        }
        let _ = self.insert(edge, value_ptr);
        InsertStep::Done
    }

    fn replace_child(&mut self, edge: u8, child: TaggedPointer) {
        let _ = self.insert(edge, child);
    }

    fn remove_child(&mut self, edge: u8) -> (result: Option<TaggedPointer>) {
        self.remove(edge)
    }

    fn child_count(&self) -> (result: usize) {
        self.meta.len()
    }

    fn prefix_len(&self) -> (result: usize) {
        self.meta.prefix_len()
    }

    fn prefix(&self) -> (result: [u8; 8]) {
        self.meta.prefix()
    }

    fn prefix_bytes(&self) -> (result: &[u8]) {
        self.meta.prefix_slice()
    }

    fn set_prefix(&mut self, prefix: &[u8]) {
        self.meta.set_prefix(prefix);
    }

    fn get_child(&self, edge: u8) -> (result: Option<TaggedPointer>) {
        self.get(edge)
    }
}

} // verus!
impl Node256 {
    pub(crate) fn for_each_child(&self, mut f: impl FnMut(TaggedPointer)) {
        for key in 0..256usize {
            let raw = self.children[key];
            if raw != 0 {
                f(TaggedPointer::from_raw(raw));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Node256;
    use crate::art::{ArtNode, ptr::TaggedPointer};

    #[test]
    fn insert_and_get_direct_slots() {
        let mut node = Node256::new(b"");

        node.insert(0, TaggedPointer::from_test_raw(10));
        node.insert(127, TaggedPointer::from_test_raw(20));
        node.insert(255, TaggedPointer::from_test_raw(30));

        assert_eq!(node.child_count(), 3);
        assert_eq!(node.get(0), Some(TaggedPointer::from_test_raw(10)));
        assert_eq!(node.get(127), Some(TaggedPointer::from_test_raw(20)));
        assert_eq!(node.get(255), Some(TaggedPointer::from_test_raw(30)));
        assert_eq!(node.get(42), None);
    }

    #[test]
    fn insert_replaces_existing_child() {
        let mut node = Node256::new(b"");

        assert_eq!(node.insert(7, TaggedPointer::from_test_raw(1)), None);
        assert_eq!(
            node.insert(7, TaggedPointer::from_test_raw(2)),
            Some(TaggedPointer::from_test_raw(1))
        );
        assert_eq!(node.child_count(), 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_test_raw(2)));
    }

    #[test]
    fn remove_deletes_direct_slot() {
        let mut node = Node256::new(b"");

        node.insert(0, TaggedPointer::from_test_raw(10));
        node.insert(127, TaggedPointer::from_test_raw(20));

        assert_eq!(node.remove(127), Some(TaggedPointer::from_test_raw(20)));
        assert_eq!(node.get(127), None);
        assert_eq!(node.get(0), Some(TaggedPointer::from_test_raw(10)));
    }
}
