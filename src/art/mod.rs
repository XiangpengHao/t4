use crate::art::art::{DeleteResult, common_prefix_len, delete_at};
use crate::art::ptr::TaggedPointer;
use vstd::prelude::*;

pub use art::ArtIndex;

verus! {

pub(crate) enum InsertStep {
    Split { matched: usize },
    Descend { edge: u8, child: TaggedPointer, next_depth: usize },
    Grow { prefix_depth: usize, prefix_len: usize },
    Done,
}

} // verus!
pub(crate) trait ArtNode {
    fn insert_step(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> InsertStep;

    fn replace_child(&mut self, edge: u8, child: TaggedPointer);

    fn remove_child(&mut self, edge: u8) -> Option<TaggedPointer>;

    fn child_count(&self) -> usize;

    fn prefix(&self) -> [u8; 8];

    fn prefix_len(&self) -> usize;

    fn set_prefix(&mut self, prefix: &[u8]);

    fn get_child(&self, edge: u8) -> Option<TaggedPointer>;

    fn get_from_node(&self, terminated_key: &[u8], depth: usize) -> Option<(TaggedPointer, usize)> {
        let prefix_len = self.prefix_len();
        let inline_prefix = self.prefix();
        let matched = common_prefix_len(&inline_prefix[..prefix_len], &terminated_key[depth..]);
        if matched != prefix_len {
            return None;
        }

        let depth = depth + prefix_len;
        let child = self.get_child(terminated_key[depth])?;
        Some((child, depth + 1))
    }

    fn delete_from_node(
        &mut self,
        self_ptr: TaggedPointer,
        terminated_key: &[u8],
        depth: usize,
    ) -> DeleteResult {
        let prefix_len = self.prefix_len();
        let prefix = self.prefix();
        let matched = common_prefix_len(&prefix[..prefix_len], &terminated_key[depth..]);
        if matched != prefix_len {
            return DeleteResult {
                removed: None,
                replacement: Some(self_ptr),
            };
        }

        let depth = depth + prefix_len;
        let edge = terminated_key[depth];
        let Some(child) = self.get_child(edge) else {
            return DeleteResult {
                removed: None,
                replacement: Some(self_ptr),
            };
        };

        let child_result = delete_at(Some(child), terminated_key, depth + 1);
        let Some(removed) = child_result.removed else {
            return DeleteResult {
                removed: None,
                replacement: Some(self_ptr),
            };
        };

        if let Some(replacement) = child_result.replacement {
            self.replace_child(edge, replacement);
        } else {
            let _ = self.remove_child(edge);
        }

        let replacement = if self.child_count() == 0 {
            None
        } else {
            Some(self_ptr)
        };

        DeleteResult {
            removed: Some(removed),
            replacement,
        }
    }
}

mod meta;
mod n16;
mod n256;
mod n4;
mod n48;
mod ptr;

mod art;
