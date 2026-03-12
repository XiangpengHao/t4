#![allow(unused)]

/// This serves as a doc for LLM to understand how pointer-based verification works.
use vstd::{
    prelude::*,
    simple_pptr::{PPtr, PointsTo},
};

verus! {

use vstd::assert_by_contradiction;

#[verifier::external_body]
proof fn lemma_usize_u64(x: u64)
    ensures
        x as usize as u64 == x,
{
    unimplemented!();
}

struct Node<V> {
    xored: u64,
    v: V,
}

type MemPerms<V> = PointsTo<Node<V>>;

struct DListXor<V> {
    ptrs: Ghost<Seq<PPtr<Node<V>>>>,
    perms: Tracked<Map<nat, MemPerms<V>>>,
    head: u64,
    tail: u64,
}

impl<V> DListXor<V> {
    spec fn wf_perms(&self) -> bool {
        forall|i: nat| 0 <= i < self.ptrs@.len() ==> self.wf_perm(i)
    }

    spec fn prev_of(&self, i: nat) -> u64
        recommends
            i < self.ptrs@.len(),
    {
        if i == 0 {
            0
        } else {
            self.ptrs@[i - 1].addr() as u64
        }
    }

    spec fn next_of(&self, i: nat) -> u64
        recommends
            i < self.ptrs@.len(),
    {
        if i + 1 == self.ptrs@.len() {
            0
        } else {
            self.ptrs@[i as int + 1].addr() as u64
        }
    }

    spec fn wf_perm(&self, i: nat) -> bool
        recommends
            i < self.ptrs@.len(),
    {
        &&& self.perms@.dom().contains(i)
        &&& self.perms@[i].pptr() == self.ptrs@[i as int]
        &&& 0 < self.ptrs@[i as int].addr() < 0x1_0000_0000_0000_0000
        &&& self.perms@[i].is_init()
        &&& self.perms@[i].value().xored == (self.prev_of(i) ^ self.next_of(i))
    }

    spec fn wf_head(&self) -> bool {
        if self.ptrs@.len() == 0 {
            self.head == 0
        } else {
            self.head == self.ptrs@[0].addr()
        }
    }

    spec fn wf_tail(&self) -> bool {
        if self.ptrs@.len() == 0 {
            self.tail == 0
        } else {
            self.tail == self.ptrs@[self.ptrs@.len() - 1].addr()
        }
    }

    spec fn wf(&self) -> bool {
        self.wf_perms() && self.wf_head() && self.wf_tail()
    }

    spec fn view(&self) -> Seq<V>
        recommends
            self.wf(),
    {
        Seq::<V>::new(self.ptrs@.len(), |i: int| { self.perms@[i as nat].value().v })
    }

    fn new() -> (s: Self)
        ensures
            s.wf(),
            s@.len() == 0,
    {
        DListXor {
            ptrs: Ghost(Seq::empty()),
            perms: Tracked(Map::tracked_empty()),
            head: 0,
            tail: 0,
        }
    }

    fn push_empty_case(&mut self, v: V)
        requires
            old(self).wf(),
            old(self).ptrs@.len() == 0,
        ensures
            self.wf(),
            self@ == old(self)@.push(v),
    {
        let (ptr, Tracked(perm)) = PPtr::new(Node { xored: 0, v });
        proof {
            self.ptrs@ = self.ptrs@.push(ptr);
            (&perm).is_nonnull();
            self.perms.borrow_mut().tracked_insert((self.ptrs@.len() - 1) as nat, perm);
        }
        self.tail = ptr.addr() as u64;
        self.head = self.tail;

        assert(0u64 ^ 0u64 == 0u64) by (bit_vector);
        assert(self@ =~= old(self)@.push(v));
    }

    fn push_back(&mut self, v: V)
        requires
            old(self).wf(),
        ensures
            self.wf(),
            self@ == old(self)@.push(v),
    {
        if self.tail == 0 {
            proof {
                assert_by_contradiction!(self.ptrs@.len() == 0, {
                    assert(self.wf_perm((self.ptrs@.len() - 1) as nat));
                });
            }
            self.push_empty_case(v);
            return ;
        } else {
            let tail_ptr_u64 = self.tail;
            proof {
                lemma_usize_u64(tail_ptr_u64);
            }
            let tail_ptr = PPtr::<Node<V>>::from_usize(tail_ptr_u64 as usize);
            assert(self.wf_perm((self.ptrs@.len() - 1) as nat));
            let tracked mut tail_perm = self.perms.borrow_mut().tracked_remove(
                (self.ptrs@.len() - 1) as nat,
            );
            let mut tail_node = tail_ptr.take(Tracked(&mut tail_perm));
            let second_to_last_ptr = tail_node.xored;
            let (ptr, Tracked(perm)) = PPtr::new(Node { xored: tail_ptr_u64, v });
            proof {
                perm.is_nonnull();
            }
            let new_ptr_u64 = ptr.addr() as u64;
            tail_node.xored = second_to_last_ptr ^ new_ptr_u64;
            tail_ptr.put(Tracked(&mut tail_perm), tail_node);
            proof {
                self.perms.borrow_mut().tracked_insert((self.ptrs@.len() - 1) as nat, tail_perm);
                self.perms.borrow_mut().tracked_insert((self.ptrs@.len()) as nat, perm);
                self.ptrs@ = self.ptrs@.push(ptr);
            }
            self.tail = new_ptr_u64;

            proof {
                assert(tail_ptr_u64 ^ 0 == tail_ptr_u64) by (bit_vector);
                let i = (self.ptrs@.len() - 2) as nat;
                let prev_of_i = self.prev_of(i);
                assert(prev_of_i ^ 0 == prev_of_i) by (bit_vector);

                assert(forall|i: nat|
                    i < self.ptrs@.len() ==> old(self).wf_perm(i) ==> self.wf_perm(i));
                assert forall|i: int| 0 <= i < self.ptrs@.len() - 1 implies old(self)@[i]
                    == self@[i] by {
                    assert(old(self).wf_perm(i as nat));
                };
                assert(self@ =~= old(self)@.push(v));

            }
        }
    }

    fn pop_back(&mut self) -> (v: V)
        requires
            old(self).wf(),
            old(self)@.len() > 0,
        ensures
            self.wf(),
            self@ == old(self)@.drop_last(),
            v == old(self)@[old(self)@.len() - 1],
    {
        let last_u64 = self.tail;

        let last_ptr = PPtr::<Node<V>>::from_usize(last_u64 as usize);
        assert(self.wf_perm((self.ptrs@.len() - 1) as nat));
        let tracked last_perm = self.perms.borrow_mut().tracked_remove(
            (self.ptrs@.len() - 1) as nat,
        );

        let last_node = last_ptr.into_inner(Tracked(last_perm));
        let penul_u64 = last_node.xored;
        let v = last_node.v;

        proof {
            let self_head = self.head;
            assert(self_head ^ 0 == self_head) by (bit_vector);
            assert(0u64 ^ 0 == 0u64) by (bit_vector);
        }

        if penul_u64 == 0 {
            self.tail = 0;
            self.head = 0;

            proof {
                assert_by_contradiction!(self.ptrs@.len() == 1, {
                    assert(old(self).wf_perm((self.ptrs@.len() -2) as nat));
                    #[verifier::spec] let actual_penult_u64 = self.prev_of((self.ptrs@.len() - 1) as nat);
                    assert(actual_penult_u64 ^ 0 == actual_penult_u64) by(bit_vector);
                });
            }
        } else {
            self.tail = penul_u64;

            assert(old(self)@.len() >= 2);
            assert(old(self).wf_perm((self.ptrs@.len() - 2) as nat));
            proof {
                let actual_penult_u64 = self.prev_of((self.ptrs@.len() - 1) as nat);
                assert(actual_penult_u64 ^ 0 == actual_penult_u64) by (bit_vector);
                lemma_usize_u64(penul_u64);
            }

            let penult_ptr = PPtr::<Node<V>>::from_usize(penul_u64 as usize);
            let tracked mut penult_perm = self.perms.borrow_mut().tracked_remove(
                (self.ptrs@.len() - 2) as nat,
            );
            let mut penult_node = penult_ptr.take(Tracked(&mut penult_perm));
            let t: Ghost<u64> = Ghost(self.prev_of((self.ptrs@.len() - 2) as nat));
            assert((t@ ^ last_u64) ^ last_u64 == t@ ^ 0) by (bit_vector);
            penult_node.xored = penult_node.xored ^ last_u64;
            assert(penult_node.xored == t@ ^ 0);
            penult_ptr.put(Tracked(&mut penult_perm), penult_node);
            proof {
                self.perms.borrow_mut().tracked_insert((self.ptrs@.len() - 2) as nat, penult_perm);
            }
        }

        proof {
            self.ptrs@ = self.ptrs@.drop_last();
            assert(self.wf_head());
            assert(self.wf_tail());
            if self.ptrs@.len() > 0 {
                assert(self.wf_perm((self.ptrs@.len() - 1) as nat));
            }
            assert(forall|i: nat| i < self@.len() ==> old(self).wf_perm(i) ==> self.wf_perm(i));
            assert forall|i: int| 0 <= i < self@.len() implies #[trigger] self@[i] == old(
                self,
            )@.drop_last()[i] by {
                assert(old(self).wf_perm(i as nat));
            }
            assert(self@ =~= old(self)@.drop_last());
        }

        v
    }

    fn push_front(&mut self, v: V)
        requires
            old(self).wf(),
        ensures
            self.wf(),
            self@ == seq![v].add(old(self)@),
    {
        if self.tail == 0 {
            proof {
                assert_by_contradiction!(self.ptrs@.len() == 0, {
                    assert(self.wf_perm((self.ptrs@.len() -1) as nat));
                });
            }
            self.push_empty_case(v);
        } else {
            let head_ptr_u64 = self.head;
            proof {
                lemma_usize_u64(head_ptr_u64);
            }
            let head_ptr = PPtr::<Node<V>>::from_usize(head_ptr_u64 as usize);
            assert(self.wf_perm(0));
            let tracked mut head_perm = self.perms.borrow_mut().tracked_remove(0);
            let mut head_node = head_ptr.take(Tracked(&mut head_perm));
            let second_ptr = head_node.xored;

            let (ptr, Tracked(perm)) = PPtr::new(Node { xored: head_ptr_u64, v });

            let new_ptr_u64 = ptr.addr() as u64;
            head_node.xored = new_ptr_u64 ^ second_ptr;
            head_ptr.put(Tracked(&mut head_perm), head_node);

            proof {
                perm.is_nonnull();
                self.perms.borrow_mut().tracked_insert(0, head_perm);
                assert forall|j: nat| 0 <= j < old(self)@.len() implies self.perms@.dom().contains(
                    j,
                ) by {
                    assert(old(self).wf_perm(j));
                }
                self.perms.borrow_mut().tracked_map_keys_in_place(
                    Map::<nat, nat>::new(
                        |i: nat| 1 <= i <= old(self)@.len(),
                        |i: nat| (i - 1) as nat,
                    ),
                );
                self.perms.borrow_mut().tracked_insert(0, perm);
                self.ptrs@ = seq![ptr].add(self.ptrs@);
            }

            self.head = new_ptr_u64;

            proof {
                assert(0 ^ head_ptr_u64 == head_ptr_u64) by (bit_vector);
                let i = 1;
                let next_of_i = self.next_of(i);
                assert(0 ^ next_of_i == next_of_i) by (bit_vector);
                assert(self.perms@.index(1).value().xored == new_ptr_u64 ^ second_ptr);
                assert(self.perms@.index(0).value().xored == head_ptr_u64);
                assert(self.perms@.index(1).pptr().addr() == head_ptr_u64);
                assert(self.wf_perm(1));
                assert(self.wf_perm(0));
                assert(forall|i: nat|
                    1 <= i <= old(self).ptrs@.len() ==> old(self).wf_perm((i - 1) as nat)
                        ==> #[trigger] self.wf_perm(i));

                assert forall|i: int| 1 <= i <= self.ptrs@.len() - 1 implies old(self)@[i - 1]
                    == self@[i] by {
                    assert(old(self).wf_perm((i - 1) as nat));
                }
                assert(self@ =~= seq![v].add(old(self)@));
            }
        }

    }
}

} // verus!
