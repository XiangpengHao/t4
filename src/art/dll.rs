use vstd::{
    prelude::*,
    simple_pptr::{PPtr, PointsTo},
};

verus! {

use vstd::raw_ptr::MemContents;
use vstd::assert_by_contradiction;

struct Node<V> {
    prev: Option<PPtr<Node<V>>>,
    next: Option<PPtr<Node<V>>>,
    value: V,
}

pub struct DoubleLinkedList<V> {
    head: Option<PPtr<Node<V>>>,
    tail: Option<PPtr<Node<V>>>,
    ghost_state: Tracked<GhostState<V>>,
}

pub tracked struct GhostState<V> {
    ghost ptrs: Seq<PPtr<Node<V>>>,
    tracked points_to_map: Map<nat, PointsTo<Node<V>>>,
}

impl<V> DoubleLinkedList<V> {
    spec fn prev_of(&self, i: nat) -> Option<PPtr<Node<V>>> {
        if i == 0 {
            None
        } else {
            Some(self.ghost_state@.ptrs[i as nat - 1])
        }
    }

    spec fn next_of(&self, i: nat) -> Option<PPtr<Node<V>>> {
        if i + 1 == self.ghost_state@.ptrs.len() {
            None
        } else {
            Some(self.ghost_state@.ptrs[i + 1 as int])
        }
    }

    spec fn well_formed_node(&self, i: nat) -> bool {
        &&& self.ghost_state@.points_to_map.dom().contains(i)
        &&& self.ghost_state@.points_to_map[i].pptr() == self.ghost_state@.ptrs[i as int]
        &&& self.ghost_state@.points_to_map[i].mem_contents() matches MemContents::Init(node)
            && node.prev == self.prev_of(i) && node.next == self.next_of(i)
    }

    pub closed spec fn well_formed(&self) -> bool {
        &&& forall|i: nat| 0 <= i < self.ghost_state@.ptrs.len() ==> self.well_formed_node(i)
        &&& if self.ghost_state@.ptrs.len() == 0 {
            self.head.is_none() && self.tail.is_none()
        } else {
            &&& self.head == Some(self.ghost_state@.ptrs[0])
            &&& self.tail == Some(self.ghost_state@.ptrs[self.ghost_state@.ptrs.len() - 1])
        }
    }

    pub closed spec fn view(&self) -> Seq<V> {
        Seq::<V>::new(
            self.ghost_state@.ptrs.len(),
            |i: int| { self.ghost_state@.points_to_map[i as nat].value().value },
        )
    }

    pub fn new() -> (s: Self)
        ensures
            s.well_formed(),
            s@.len() == 0,
    {
        DoubleLinkedList {
            head: None,
            tail: None,
            ghost_state: Tracked(
                GhostState { ptrs: Seq::empty(), points_to_map: Map::tracked_empty() },
            ),
        }
    }

    fn push_empty_case(&mut self, v: V)
        requires
            old(self).well_formed(),
            old(self).ghost_state@.ptrs.len() == 0,
        ensures
            self.well_formed(),
            self@ =~= old(self)@.push(v),
    {
        let (ptr, Tracked(points_to)) = PPtr::<Node<V>>::new(
            Node::<V> { prev: None, next: None, value: v },
        );

        self.tail = Some(ptr);
        self.head = Some(ptr);

        proof {
            self.ghost_state.borrow_mut().ptrs = self.ghost_state@.ptrs.push(ptr);
            self.ghost_state.borrow_mut().points_to_map.tracked_insert(
                (self.ghost_state@.ptrs.len() - 1) as nat,
                points_to,
            );
        }
    }

    pub fn push_back(&mut self, v: V)
        requires
            old(self).well_formed(),
        ensures
            self.well_formed(),
            self@ == old(self)@.push(v),
    {
        match self.tail {
            None => {
                self.push_empty_case(v);
            },
            Some(old_tail_ptr) => {
                let (new_tail_ptr, Tracked(new_tail_pointsto)) = PPtr::<Node<V>>::new(
                    Node::<V> { prev: Some(old_tail_ptr), next: None, value: v },
                );

                proof {
                    assert(self.well_formed_node((self.ghost_state@.ptrs.len() - 1) as nat));
                }

                let tracked mut old_tail_pointsto =
                    self.ghost_state.borrow_mut().points_to_map.tracked_remove(
                    (self.ghost_state@.ptrs.len() - 1) as nat,
                );

                let mut old_tail_node = old_tail_ptr.take(Tracked(&mut old_tail_pointsto));
                old_tail_node.next = Some(new_tail_ptr);
                old_tail_ptr.put(Tracked(&mut old_tail_pointsto), old_tail_node);
                proof {
                    self.ghost_state.borrow_mut().points_to_map.tracked_insert(
                        (self.ghost_state@.ptrs.len() - 1) as nat,
                        old_tail_pointsto,
                    );
                }

                self.tail = Some(new_tail_ptr);

                proof {
                    self.ghost_state.borrow_mut().points_to_map.tracked_insert(
                        self.ghost_state@.ptrs.len(),
                        new_tail_pointsto,
                    );
                    self.ghost_state@.ptrs = self.ghost_state@.ptrs.push(new_tail_ptr);

                    assert(forall|i: nat|
                        0 <= i < self.ghost_state@.ptrs.len() && old(self).well_formed_node(i)
                            ==> self.well_formed_node(i));
                }
            },
        }
    }

    pub fn push_front(&mut self, v: V)
        requires
            old(self).well_formed(),
        ensures
            self.well_formed(),
            self@ == seq![v].add(old(self)@),
    {
        match self.head {
            None => {
                self.push_empty_case(v);
                assert(self@ =~= seq![v].add(old(self)@));
            },
            Some(old_head_ptr) => {
                let (new_head_ptr, Tracked(new_head_pointsto)) = PPtr::<Node<V>>::new(
                    Node::<V> { prev: None, next: Some(old_head_ptr), value: v },
                );

                assert(old(self).well_formed_node(0));

                let tracked mut old_head_pointsto =
                    self.ghost_state.borrow_mut().points_to_map.tracked_remove(0);
                let mut old_head_node = old_head_ptr.take(Tracked(&mut old_head_pointsto));
                old_head_node.prev = Some(new_head_ptr);
                old_head_ptr.put(Tracked(&mut old_head_pointsto), old_head_node);
                proof {
                    self.ghost_state.borrow_mut().points_to_map.tracked_insert(
                        0,
                        old_head_pointsto,
                    );
                }

                self.head = Some(new_head_ptr);
                proof {
                    assert forall|j: nat|
                        0 <= j < old(
                            self,
                        )@.len() implies self.ghost_state@.points_to_map.dom().contains(j) by {
                        assert(old(self).well_formed_node(j));
                    }

                    self.ghost_state.borrow_mut().points_to_map.tracked_map_keys_in_place(
                        Map::<nat, nat>::new(
                            |j: nat| 1 <= j <= old(self).view().len(),
                            |j: nat| (j - 1) as nat,
                        ),
                    );

                    self.ghost_state.borrow_mut().points_to_map.tracked_insert(
                        0,
                        new_head_pointsto,
                    );
                    self.ghost_state@.ptrs = seq![new_head_ptr].add(self.ghost_state@.ptrs);

                    assert(forall|i: nat|
                        1 <= i <= old(self).ghost_state@.ptrs.len() && old(self).well_formed_node(
                            (i - 1) as nat,
                        ) ==> #[trigger] self.well_formed_node(i));
                    assert forall|i: int|
                        1 <= i <= self.ghost_state@.ptrs.len() as int - 1 implies old(self)@[i - 1]
                        == self@[i] by {
                        assert(old(self).well_formed_node((i - 1) as nat));
                    }
                    assert(self.well_formed());
                }
            },
        }
    }

    pub fn pop_back(&mut self) -> (v: V)
        requires
            old(self).well_formed(),
            old(self)@.len() > 0,
        ensures
            self.well_formed(),
            self@ == old(self)@.drop_last(),
            v == old(self)@[old(self)@.len() as int - 1],
    {
        let last_ptr = self.tail.unwrap();
        assert(self.well_formed_node((self.ghost_state@.ptrs.len() - 1) as nat));

        let tracked last_pointsto = self.ghost_state.borrow_mut().points_to_map.tracked_remove(
            (self.ghost_state@.ptrs.len() - 1) as nat,
        );
        let last_node = last_ptr.into_inner(Tracked(last_pointsto));
        let v = last_node.value;

        match last_node.prev {
            None => {
                self.head = None;
                self.tail = None;

                proof {
                    assert_by_contradiction!(self.ghost_state@.ptrs.len() == 1,
                        {
                            assert(old(self).well_formed_node((self.ghost_state@.ptrs.len() -2) as nat));
                        }
                    )
                }
            },
            Some(penultimate_ptr) => {
                self.tail = Some(penultimate_ptr);

                assert(old(self)@.len() >= 2);
                assert(old(self).well_formed_node((self.ghost_state@.ptrs.len() - 2) as nat));

                let tracked mut penultimate_pointsto =
                    self.ghost_state.borrow_mut().points_to_map.tracked_remove(
                    (self.ghost_state@.ptrs.len() - 2) as nat,
                );
                let mut penultimate_node = penultimate_ptr.take(Tracked(&mut penultimate_pointsto));
                penultimate_node.next = None;
                penultimate_ptr.put(Tracked(&mut penultimate_pointsto), penultimate_node);
                proof {
                    self.ghost_state.borrow_mut().points_to_map.tracked_insert(
                        (self.ghost_state@.ptrs.len() - 2) as nat,
                        penultimate_pointsto,
                    );
                }
                ;
            },
        }

        proof {
            self.ghost_state@.ptrs = self.ghost_state@.ptrs.drop_last();
            if self.ghost_state@.ptrs.len() > 0 {
                assert(self.well_formed_node((self.ghost_state@.ptrs.len() - 1) as nat));
            }
            assert(forall|i: nat|
                i < self@.len() && old(self).well_formed_node(i) ==> self.well_formed_node(i));
            assert forall|i: int| 0 <= i < self@.len() implies #[trigger] self@[i] == old(
                self,
            )@.drop_last()[i] by {
                assert(old(self).well_formed_node(i as nat));
            }
            assert(self.well_formed());
        }

        return v;
    }

    pub fn pop_front(&mut self) -> (v: V)
        requires
            old(self).well_formed(),
            old(self).view().len() > 0,
        ensures
            self.well_formed(),
            self@ == old(self)@.subrange(1, old(self)@.len() as int),
            v == old(self)@[0],
    {
        let first_ptr = self.head.unwrap();

        assert(old(self).well_formed_node(0));
        let tracked first_pointsto = self.ghost_state.borrow_mut().points_to_map.tracked_remove(0);
        let first_node = first_ptr.into_inner(Tracked(first_pointsto));
        let v = first_node.value;

        match first_node.next {
            None => {
                self.tail = None;
                self.head = None;

                proof {
                    assert_by_contradiction!(self.ghost_state@.ptrs.len() == 1,
                        {
                            assert(old(self).well_formed_node(1 as nat));
                        }
                    )
                }
            },
            Some(second_ptr) => {
                self.head = Some(second_ptr);

                assert(old(self).well_formed_node(1));
                let tracked mut second_pointsto =
                    self.ghost_state.borrow_mut().points_to_map.tracked_remove(1);
                let mut second_node = second_ptr.take(Tracked(&mut second_pointsto));
                second_node.prev = None;
                second_ptr.put(Tracked(&mut second_pointsto), second_node);

                proof {
                    self.ghost_state.borrow_mut().points_to_map.tracked_insert(1, second_pointsto);
                    assert forall|j: nat|
                        1 <= j < old(
                            self,
                        )@.len() implies self.ghost_state@.points_to_map.dom().contains(j) by {
                        assert(old(self).well_formed_node(j));
                    };
                    self.ghost_state.borrow_mut().points_to_map.tracked_map_keys_in_place(
                        Map::<nat, nat>::new(
                            |j: nat| 0 <= j < old(self).view().len() - 1,
                            |j: nat| (j + 1) as nat,
                        ),
                    );
                }
            },
        }

        proof {
            self.ghost_state@.ptrs = self.ghost_state@.ptrs.subrange(
                1,
                self.ghost_state@.ptrs.len() as int,
            );
            if self.ghost_state@.ptrs.len() > 0 {
                assert(self.well_formed_node(0));
            }
            assert(forall|i: nat|
                i < self@.len() && old(self).well_formed_node(i + 1) ==> self.well_formed_node(i));
            assert forall|i: int| 0 <= i < self@.len() implies #[trigger] self@[i] == old(
                self,
            )@.subrange(1, old(self)@.len() as int)[i] by {
                assert(old(self).well_formed_node(i as nat + 1));
            }

            assert(self.well_formed());
        }

        return v;
    }

    pub fn get<'a>(&'a self, i: usize) -> (v: &'a V)
        requires
            self.well_formed(),
            0 <= i < self@.len(),
        ensures
            v == self@[i as int],
    {
        let mut j = 0;
        let mut ptr = self.head.unwrap();

        while j < i
            invariant
                self.well_formed(),
                0 <= j <= i < self@.len(),
                ptr == self.ghost_state@.ptrs[j as int],
            decreases i - j,
        {
            assert(self.well_formed_node(j as nat));
            let tracked pointsto_ref = self.ghost_state.borrow().points_to_map.tracked_borrow(
                j as nat,
            );
            let node_ref = ptr.borrow(Tracked(pointsto_ref));
            let next_ptr = node_ref.next.unwrap();
            j += 1;
            ptr = next_ptr;
        }

        assert(self.well_formed_node(j as nat));

        let tracked pointsto_ref = self.ghost_state.borrow().points_to_map.tracked_borrow(j as nat);
        let node_ref = ptr.borrow(Tracked(pointsto_ref));
        return &node_ref.value;
    }
}

pub struct Iterator<'a, V> {
    dll: &'a DoubleLinkedList<V>,
    cur: Option<PPtr<Node<V>>>,
    index: Ghost<nat>,
}

impl<'a, V> Iterator<'a, V> {
    pub closed spec fn list(&self) -> &'a DoubleLinkedList<V> {
        self.dll
    }

    pub closed spec fn index(&self) -> nat {
        self.index@
    }

    pub closed spec fn valid(&self) -> bool {
        &&& self.list().well_formed()
        &&& self.index@ < self.list()@.len()
        &&& self.cur.is_some() && self.cur.unwrap()
            =~= self.list().ghost_state@.ptrs[self.index@ as int]
    }

    pub fn new(l: &'a DoubleLinkedList<V>) -> (it: Self)
        requires
            l.well_formed(),
            l@.len() > 0,
        ensures
            it.valid(),
            it.index() == 0,
            it.list() == l,
    {
        Iterator { dll: l, cur: l.head, index: Ghost(0) }
    }

    pub fn value(&self) -> (v: &V)
        requires
            self.valid(),
        ensures
            v == self.list()@[self.index() as int],
    {
        let cur = self.cur.unwrap();
        assert(self.list().well_formed_node(self.index@ as nat));
        let tracked pointsto_ref = self.dll.ghost_state.borrow().points_to_map.tracked_borrow(
            self.index@ as nat,
        );
        let node_ref = cur.borrow(Tracked(pointsto_ref));
        return &node_ref.value;
    }

    pub fn move_next(&mut self) -> (good: bool)
        requires
            old(self).valid(),
        ensures
            old(self).list() == self.list(),
            good == (old(self).index() < old(self).list()@.len() - 1),
            good ==> (self.valid() && self.index() == old(self).index() + 1),
    {
        let cur = self.cur.unwrap();
        assert(self.list().well_formed_node(self.index@ as nat));
        let tracked pointsto_ref = self.dll.ghost_state.borrow().points_to_map.tracked_borrow(
            self.index@ as nat,
        );
        let node = cur.borrow(Tracked(pointsto_ref));
        proof {
            self.index@ = self.index@ + 1;
        }
        match node.next {
            None => {
                self.cur = None;
                false
            },
            Some(next_ptr) => {
                self.cur = Some(next_ptr);
                true
            },
        }
    }
}

} // verus!
