//! The disk under the simulated driver: what reads see once each operation
//! completes.

/// An in-memory disk for shuttle tests, owned by the driver running on it.
pub(crate) struct SimDisk {
    /// Every completed write: what reads see.
    bytes: Vec<u8>,
    file: Option<std::fs::File>,
    /// Writes submitted and not yet completed, oldest first.
    in_flight: Vec<InFlightWrite>,
    next_write: u64,
}

struct InFlightWrite {
    id: WriteId,
    offset: u64,
    bytes: Vec<u8>,
}

/// A write submitted to a [`SimDisk`], until it completes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct WriteId(u64);

/// An operation completing on a [`SimDisk`].
pub(super) enum Completion<'a> {
    Read { offset: u64, buf: &'a mut [u8] },
    Write(WriteId),
}

impl SimDisk {
    /// An empty disk.
    pub(crate) fn new() -> Self {
        Self {
            bytes: Vec::new(),
            file: None,
            in_flight: Vec::new(),
            next_write: 0,
        }
    }

    pub(super) fn on_file(file: std::fs::File) -> Self {
        Self {
            file: Some(file),
            ..Self::new()
        }
    }

    pub(super) fn submit_write(&mut self, offset: u64, bytes: Vec<u8>) -> WriteId {
        let id = WriteId(self.next_write);
        self.next_write += 1;
        self.in_flight.push(InFlightWrite { id, offset, bytes });
        id
    }

    /// Completes an operation, as a completion queue entry reports it: the
    /// byte count.
    pub(super) fn complete(&mut self, completion: Completion<'_>) -> i32 {
        match completion {
            Completion::Read { offset, buf } => {
                if let Some(file) = &self.file {
                    use std::os::unix::fs::FileExt;
                    return file.read_at(buf, offset).map(|n| n as i32).unwrap_or(-5);
                }
                self.read(offset, buf) as i32
            }
            Completion::Write(id) => self.complete_write(id) as i32,
        }
    }

    fn read(&self, offset: u64, buf: &mut [u8]) -> usize {
        let start = (offset as usize).min(self.bytes.len());
        let len = buf.len().min(self.bytes.len() - start);
        buf[..len].copy_from_slice(&self.bytes[start..start + len]);
        len
    }

    fn complete_write(&mut self, id: WriteId) -> usize {
        let index = self
            .in_flight
            .iter()
            .position(|write| write.id == id)
            .expect("a write completes once");
        let write = self.in_flight.remove(index);
        if let Some(file) = &self.file {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&write.bytes, write.offset).unwrap();
            return write.bytes.len();
        }
        let start = write.offset as usize;
        let end = start + write.bytes.len();
        if self.bytes.len() < end {
            self.bytes.resize(end, 0);
        }
        self.bytes[start..end].copy_from_slice(&write.bytes);
        write.bytes.len()
    }
}
