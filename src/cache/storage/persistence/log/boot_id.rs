use std::io;

use crate::io::file_system::FileSystem;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache::storage::persistence) struct BootId([u8; 16]);

impl BootId {
    pub(in crate::cache::storage::persistence) fn current(
        file_system: &FileSystem,
    ) -> io::Result<Self> {
        let text = file_system.boot_id()?;
        Self::parse(text.trim()).ok_or_else(|| io::ErrorKind::InvalidData.into())
    }

    pub(in crate::cache::storage::persistence) fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    fn parse(text: &str) -> Option<Self> {
        if text.len() != 36 {
            return None;
        }
        let mut bytes = [0; 16];
        let mut digits = 0;
        for (at, byte) in text.bytes().enumerate() {
            if [8, 13, 18, 23].contains(&at) {
                if byte != b'-' {
                    return None;
                }
                continue;
            }
            let hex = char::from(byte).to_digit(16)? as u8;
            bytes[digits / 2] = (bytes[digits / 2] << 4) | hex;
            digits += 1;
        }
        Some(Self(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boot_ids_are_checked_uuids_and_stable_within_a_boot() {
        let uuid = "01234567-89ab-cdef-0123-456789abcdef";
        assert_eq!(BootId::parse(uuid), BootId::parse(&uuid.to_uppercase()));
        for invalid in [
            "",
            "0123456789abcdef0123456789abcdef",
            "01234567_89ab-cdef-0123-456789abcdef",
            "g1234567-89ab-cdef-0123-456789abcdef",
        ] {
            assert_eq!(BootId::parse(invalid), None);
        }
        let file_system = FileSystem::default();
        let current = || BootId::current(&file_system).unwrap();
        assert_eq!(current(), current());
    }
}
