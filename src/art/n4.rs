use crate::art::{
    ArtNode, InsertStep, dense::DenseNode, meta::NodeType, n16::Node16, ptr::TaggedPointer,
};

#[repr(transparent)]
pub(crate) struct Node4(DenseNode<4>);

impl Node4 {
    pub(crate) fn new(prefix: &[u8]) -> Self {
        Self(DenseNode::new(NodeType::Node4, prefix))
    }

    pub(crate) fn insert(&mut self, key: u8, value: TaggedPointer) -> Option<TaggedPointer> {
        self.0.insert(key, value)
    }

    pub(crate) fn get(&self, key: u8) -> Option<TaggedPointer> {
        self.0.get(key)
    }

    pub(crate) fn remove(&mut self, key: u8) -> Option<TaggedPointer> {
        self.0.remove(key)
    }

    pub(crate) fn for_each_child(&self, f: impl FnMut(u8, TaggedPointer)) {
        self.0.for_each_child(f);
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
        self.0.insert_step_impl(terminated_key, value_ptr, depth)
    }

    fn replace_child(&mut self, edge: u8, child: TaggedPointer) {
        let _ = self.insert(edge, child);
    }

    fn remove_child(&mut self, edge: u8) -> Option<TaggedPointer> {
        self.remove(edge)
    }

    fn child_count(&self) -> usize {
        self.0.child_count()
    }

    fn prefix_len(&self) -> usize {
        self.0.prefix_len()
    }

    fn prefix(&self) -> [u8; 8] {
        self.0.prefix()
    }

    fn set_prefix(&mut self, prefix: &[u8]) {
        self.0.set_prefix(prefix);
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

        let mut keys = Vec::new();
        node.for_each_child(|key, _| keys.push(key));
        assert_eq!(keys, [10, 20, 30]);
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
        let mut count = 0;
        node.for_each_child(|_, _| count += 1);
        assert_eq!(count, 1);
        assert_eq!(node.get(7), Some(TaggedPointer::from_test_raw(2)));
    }

    #[test]
    fn remove_deletes_child_and_keeps_keys_sorted() {
        let mut node = Node4::new(b"");

        node.insert(20, TaggedPointer::from_test_raw(20));
        node.insert(10, TaggedPointer::from_test_raw(10));
        node.insert(30, TaggedPointer::from_test_raw(30));

        assert_eq!(node.remove(20), Some(TaggedPointer::from_test_raw(20)));
        let mut keys = Vec::new();
        node.for_each_child(|key, _| keys.push(key));
        assert_eq!(keys, [10, 30]);
        assert_eq!(node.get(20), None);
    }
}
