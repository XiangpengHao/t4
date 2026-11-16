//! What the disk tier's files and log live on: the OS's file system, or in a simulated
//! build an in-memory disk the test owns. Every operation the cache makes on its disk goes
//! through a `FileSystem` or an `OpenFile` it opened.

cfg_if::cfg_if! {
    if #[cfg(all(feature = "shuttle", any(test, t4_simulated)))] {
        mod simulated;
        pub use simulated::OpenFile;
        pub(crate) use simulated::FileSystem;
        #[cfg(test)]
        pub(crate) use simulated::WritePause;
    } else {
        mod os;
        pub use os::OpenFile;
        pub(crate) use os::FileSystem;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    pub(crate) fn new(device: u64, inode: u64) -> Self {
        Self { device, inode }
    }

    pub(crate) fn device(self) -> u64 {
        self.device
    }

    pub(crate) fn inode(self) -> u64 {
        self.inode
    }
}
