//! The disk under the simulated driver: what reads see, what survives a
//! power cut, and when the power goes. The store writes with `O_DSYNC`, so
//! a write is durable once it completes; only writes in flight are at risk.

use shuttle::rand::{Rng, thread_rng};

use crate::io::sync::{Arc, Mutex, MutexGuard};

/// What an operation completes with once the power is out.
const EIO: i32 = 5;

/// An in-memory disk for shuttle tests, shared by the store running on it
/// and the test that cuts its power: clones are the same disk.
#[derive(Clone)]
pub(crate) struct SimDisk(Arc<Mutex<Media>>);

struct Media {
    /// What the drive persists atomically: a power cut leaves each sector
    /// of a write in flight old or new, independently of the others.
    sector_len: usize,
    /// Every completed write: what reads see, and what a power cut keeps.
    bytes: Vec<u8>,
    /// Writes submitted and not yet completed, oldest first.
    in_flight: Vec<InFlightWrite>,
    power: Power,
    next_write: u64,
}

struct InFlightWrite {
    id: WriteId,
    offset: u64,
    bytes: Vec<u8>,
}

enum Power {
    On,
    /// Goes out just before this many more operations complete.
    CutIn(u32),
    /// Out: the disk as a reboot finds it.
    Cut {
        image: Vec<u8>,
    },
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
    /// An empty disk, whose power stays on until the test cuts it, and
    /// which persists 512-byte sectors atomically, as many drives
    /// guarantee no more.
    pub(crate) fn new() -> Self {
        Self::with_sector_len(512)
    }

    /// An empty disk that persists `sector_len` bytes atomically.
    pub(crate) fn with_sector_len(sector_len: usize) -> Self {
        Self::holding(Vec::new(), sector_len)
    }

    fn holding(bytes: Vec<u8>, sector_len: usize) -> Self {
        Self(Arc::new(Mutex::new(Media {
            sector_len,
            bytes,
            in_flight: Vec::new(),
            power: Power::On,
            next_write: 0,
        })))
    }

    /// Cuts the power just before a random one of the next `completions`
    /// operations completes, drawn from shuttle's random source.
    pub(crate) fn cut_power_within(&self, completions: u32) {
        self.media().power = Power::CutIn(thread_rng().gen_range(0..completions));
    }

    /// Cuts the power now, unless it is out already, and returns the disk
    /// as a reboot finds it.
    pub(crate) fn power_cut(&self) -> SimDisk {
        let mut media = self.media();
        if !matches!(media.power, Power::Cut { .. }) {
            media.cut();
        }
        let Power::Cut { image } = &media.power else {
            unreachable!("the power was just cut");
        };
        Self::holding(image.clone(), media.sector_len)
    }

    /// The file's length, as a mount sees it.
    pub(crate) fn len(&self) -> u64 {
        self.media().bytes.len() as u64
    }

    pub(super) fn submit_write(&self, offset: u64, bytes: Vec<u8>) -> WriteId {
        let mut media = self.media();
        let id = WriteId(media.next_write);
        media.next_write += 1;
        media.in_flight.push(InFlightWrite { id, offset, bytes });
        id
    }

    /// Completes an operation, as a completion queue entry reports it: the
    /// byte count, or a negated errno once the power is out.
    pub(super) fn complete(&self, completion: Completion<'_>) -> i32 {
        let mut media = self.media();
        if !media.powered() {
            return -EIO;
        }
        match completion {
            Completion::Read { offset, buf } => media.read(offset, buf) as i32,
            Completion::Write(id) => media.complete_write(id) as i32,
        }
    }

    fn media(&self) -> MutexGuard<'_, Media> {
        self.0.lock().expect("simulated disk lock poisoned")
    }
}

impl Media {
    /// Whether the power is on as an operation completes; counts down to a
    /// planned cut.
    fn powered(&mut self) -> bool {
        match self.power {
            Power::On => true,
            Power::CutIn(0) => {
                self.cut();
                false
            }
            Power::CutIn(left) => {
                self.power = Power::CutIn(left - 1);
                true
            }
            Power::Cut { .. } => false,
        }
    }

    /// Cuts the power: every completed write survives, and each sector of
    /// every write in flight lands or not at random.
    fn cut(&mut self) {
        let mut image = self.bytes.clone();
        let mut rng = thread_rng();
        for write in &self.in_flight {
            for (index, sector) in write.bytes.chunks(self.sector_len).enumerate() {
                if rng.gen_bool(0.5) {
                    let offset = write.offset + (index * self.sector_len) as u64;
                    write_at(&mut image, offset, sector);
                }
            }
        }
        self.power = Power::Cut { image };
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
        write_at(&mut self.bytes, write.offset, &write.bytes);
        write.bytes.len()
    }
}

fn write_at(media: &mut Vec<u8>, offset: u64, bytes: &[u8]) {
    let start = offset as usize;
    let end = start + bytes.len();
    if media.len() < end {
        media.resize(end, 0);
    }
    media[start..end].copy_from_slice(bytes);
}
