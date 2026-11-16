pub(crate) struct SimDisk {
    bytes: Vec<u8>,
    file: Option<std::fs::File>,
    #[cfg(test)]
    write_gate: Option<PausedWrite>,
}

#[cfg(test)]
struct PausedWrite {
    started: crate::io::sync::mpsc::Sender<()>,
    release: crate::io::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct WritePause {
    started: crate::io::sync::mpsc::Receiver<()>,
    release: crate::io::sync::mpsc::Sender<()>,
}

#[cfg(test)]
impl WritePause {
    pub(crate) fn wait_until_started(&self) {
        self.started.recv().unwrap();
    }

    pub(crate) fn resume(self) {
        self.release.send(()).unwrap();
    }
}

pub(super) enum Completion<'a> {
    Read { offset: u64, buf: &'a mut [u8] },
    Write { offset: u64, bytes: &'a [u8] },
}

impl SimDisk {
    pub(crate) fn new() -> Self {
        Self {
            bytes: Vec::new(),
            file: None,
            #[cfg(test)]
            write_gate: None,
        }
    }

    /// The first write completes only once the test resumes it.
    #[cfg(test)]
    pub(crate) fn with_paused_writes() -> (Self, WritePause) {
        let (announce, started) = crate::io::sync::mpsc::channel();
        let (release, resume) = crate::io::sync::mpsc::channel();
        let disk = Self {
            write_gate: Some(PausedWrite {
                started: announce,
                release: resume,
            }),
            ..Self::new()
        };
        (disk, WritePause { started, release })
    }

    pub(super) fn on_file(file: std::fs::File) -> Self {
        Self {
            file: Some(file),
            ..Self::new()
        }
    }

    pub(super) fn complete(&mut self, completion: Completion<'_>) -> i32 {
        match completion {
            Completion::Read { offset, buf } => {
                if let Some(file) = &self.file {
                    use std::os::unix::fs::FileExt;
                    return file.read_at(buf, offset).map(|n| n as i32).unwrap_or(-5);
                }
                self.read(offset, buf) as i32
            }
            Completion::Write { offset, bytes } => self.write(offset, bytes) as i32,
        }
    }

    fn read(&self, offset: u64, buf: &mut [u8]) -> usize {
        let start = (offset as usize).min(self.bytes.len());
        let len = buf.len().min(self.bytes.len() - start);
        buf[..len].copy_from_slice(&self.bytes[start..start + len]);
        len
    }

    fn write(&mut self, offset: u64, bytes: &[u8]) -> usize {
        #[cfg(test)]
        if let Some(gate) = self.write_gate.take() {
            gate.started.send(()).unwrap();
            gate.release.recv().unwrap();
        }
        if let Some(file) = &self.file {
            use std::os::unix::fs::FileExt;
            file.write_all_at(bytes, offset).unwrap();
            return bytes.len();
        }
        let start = offset as usize;
        let end = start + bytes.len();
        if self.bytes.len() < end {
            self.bytes.resize(end, 0);
        }
        self.bytes[start..end].copy_from_slice(bytes);
        bytes.len()
    }
}
