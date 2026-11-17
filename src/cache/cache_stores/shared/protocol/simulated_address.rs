//! An explicitly owned listener registry: one per simulated group of processes.
use super::{FrameSocket, Listener, Transport};
use crate::io::error::Result;
use crate::io::file_system::FileIdentity;
use std::io;

pub(in crate::cache::cache_stores::shared) struct OwnerAddress {
    file: FileIdentity,
    transport: Transport,
}
impl OwnerAddress {
    pub(in crate::cache::cache_stores::shared) fn of(
        file: FileIdentity,
        transport: &Transport,
    ) -> io::Result<Self> {
        Ok(Self {
            file,
            transport: transport.clone(),
        })
    }
    pub(in crate::cache::cache_stores::shared) fn listen(&self) -> io::Result<Listener> {
        self.transport.listen(self.file)
    }
    pub(in crate::cache::cache_stores::shared) fn connect(
        &self,
        _: rustix::fs::Uid,
    ) -> Result<FrameSocket> {
        Ok(self.transport.connect(self.file)?)
    }
    pub(in crate::cache::cache_stores::shared) fn wake(&self) {
        let _ = self.transport.connect(self.file);
    }
    pub(in crate::cache::cache_stores::shared) fn remove(&self) {
        self.transport.remove(self.file);
    }
}
