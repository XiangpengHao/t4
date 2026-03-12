use crate::art::index::{DeleteResult, common_prefix_len, delete_at};
use crate::art::ptr::TaggedPointer;
use vstd::prelude::*;
use vstd::slice::slice_subrange;
pub mod dll_xor;
mod version_lock;

pub use index::ArtIndex;

verus! {

pub(crate) enum InsertStep {
    Split { matched: usize },
    Descend { edge: u8, child: TaggedPointer, next_depth: usize },
    Grow { prefix_depth: usize, prefix_len: usize },
    Done,
}

pub(crate) trait ArtNode {
    spec fn live_len(self) -> usize;

    spec fn has_key(self, key: u8) -> bool;

    spec fn maps_to(self, key: u8, raw: usize) -> bool;

    spec fn wf(&self) -> bool;

    spec fn raw_prefix_len(self) -> usize;

    fn insert_step(
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
                InsertStep::Done => true,
            },
    ;

    fn replace_child(&mut self, edge: u8, child: TaggedPointer)
        requires
            old(self).wf(),
            old(self).has_key(edge),
        ensures
            self.wf(),
            self.live_len() == old(self).live_len(),
            self.maps_to(edge, child.raw()),
    ;

    fn remove_child(&mut self, edge: u8) -> (result: Option<TaggedPointer>)
        requires
            old(self).wf(),
        ensures
            self.wf(),
            !self.has_key(edge),
            old(self).has_key(edge) <==> result.is_some(),
            result.is_some() ==> old(self).maps_to(edge, result.unwrap().raw()),
    ;

    fn child_count(&self) -> (result: usize)
        requires
            self.wf(),
        ensures
            result == self.live_len(),
    ;

    fn prefix(&self) -> (result: [u8; 8])
        requires
            self.wf(),
    ;

    fn prefix_bytes(&self) -> (result: &[u8])
        requires
            self.wf(),
        ensures
            result@.len() == crate::art::meta::NodeMeta::prefix_capacity(),
    ;

    fn prefix_len(&self) -> (result: usize)
        requires
            self.wf(),
        ensures
            result == self.raw_prefix_len(),
            result <= crate::art::meta::NodeMeta::prefix_capacity(),
    ;

    fn set_prefix(&mut self, prefix: &[u8])
        requires
            old(self).wf(),
            prefix.len() <= crate::art::meta::NodeMeta::prefix_capacity(),
        ensures
            self.wf(),
            self.live_len() == old(self).live_len(),
            self.raw_prefix_len() == prefix.len(),
    ;

    fn get_child(&self, edge: u8) -> (result: Option<TaggedPointer>)
        requires
            self.wf(),
        ensures
            result.is_some() <==> self.has_key(edge),
            result.is_some() ==> self.maps_to(edge, result.unwrap().raw()),
    ;
}

pub(crate) fn get_from_node(node: &impl ArtNode, terminated_key: &[u8], depth: usize) -> (result:
    Option<(TaggedPointer, usize)>)
    requires
        node.wf(),
        depth + node.raw_prefix_len() < terminated_key.len(),
{
    let prefix_len = node.prefix_len();
    let inline_prefix = node.prefix_bytes();
    let matched = common_prefix_len(
        slice_subrange(inline_prefix, 0, prefix_len),
        slice_subrange(terminated_key, depth, terminated_key.len()),
    );
    if matched != prefix_len {
        return None;
    }
    let depth = depth + prefix_len;
    let child = node.get_child(terminated_key[depth])?;
    Some((child, depth + 1))
}

pub(crate) fn delete_from_node(
    node: &mut impl ArtNode,
    self_ptr: TaggedPointer,
    terminated_key: &[u8],
    depth: usize,
) -> (result: DeleteResult)
    requires
        old(node).wf(),
        depth + old(node).raw_prefix_len() < terminated_key.len(),
    ensures
        node.wf(),
{
    let prefix_len = node.prefix_len();
    let prefix = node.prefix_bytes();
    let matched = common_prefix_len(
        slice_subrange(prefix, 0, prefix_len),
        slice_subrange(terminated_key, depth, terminated_key.len()),
    );
    if matched != prefix_len {
        return DeleteResult::NotFound { current: Some(self_ptr) };
    }
    let depth = depth + prefix_len;
    let edge = terminated_key[depth];
    let Some(child) = node.get_child(edge) else {
        return DeleteResult::NotFound { current: Some(self_ptr) };
    };

    let child_result = delete_at(Some(child), terminated_key, depth + 1);
    let DeleteResult::Deleted { removed: removed_ptr, replacement: child_replacement } =
        child_result else {
        return DeleteResult::NotFound { current: Some(self_ptr) };
    };

    if let Some(replacement) = child_replacement {
        node.replace_child(edge, replacement);
    } else {
        let _ = node.remove_child(edge);
    }

    let replacement = if node.child_count() == 0 {
        None
    } else {
        Some(self_ptr)
    };

    DeleteResult::Deleted { removed: removed_ptr, replacement }
}

} // verus!
mod dense;
mod meta;
mod n16;
mod n256;
mod n4;
mod n48;
mod ptr;

mod index;
