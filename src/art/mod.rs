use crate::art::art::common_prefix_len;
use crate::art::ptr::TaggedPointer;

pub use art::ArtIndex;

pub(crate) enum InsertStep {
    Split {
        matched: usize,
    },
    Descend {
        edge: u8,
        child: TaggedPointer,
        next_depth: usize,
    },
    Grow {
        prefix_depth: usize,
        prefix_len: usize,
    },
    Done,
}

pub(crate) trait ArtNode {
    fn insert_step(
        &mut self,
        terminated_key: &[u8],
        value_ptr: TaggedPointer,
        depth: usize,
    ) -> InsertStep;

    fn replace_child(&mut self, edge: u8, child: TaggedPointer);

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
}

mod meta;
mod n16;
mod n256;
mod n4;
mod n48;
mod ptr;

mod art;
