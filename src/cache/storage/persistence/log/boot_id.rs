use std::io;

#[derive(Clone, Copy, Default)]
pub(in crate::cache) enum BootIdReader {
    #[default]
    System,
    #[cfg(all(test, feature = "shuttle"))]
    Unreadable,
}

impl BootIdReader {
    pub(in crate::cache::storage::persistence) fn read(self) -> io::Result<BootId> {
        match self {
            Self::System => BootId::current(),
            #[cfg(all(test, feature = "shuttle"))]
            Self::Unreadable => Err(io::ErrorKind::PermissionDenied.into()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cache::storage::persistence) struct BootId([u8; 16]);

impl BootId {
    pub(in crate::cache::storage::persistence) fn current() -> io::Result<Self> {
        let text = Self::read()?;
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

    #[cfg(target_os = "linux")]
    fn read() -> io::Result<String> {
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
    }

    #[cfg(target_os = "macos")]
    fn read() -> io::Result<String> {
        let mut bytes = [0u8; 64];
        let mut len = bytes.len();
        // SAFETY: the name is terminated and both output pointers describe live storage.
        if unsafe {
            libc::sysctlbyname(
                c"kern.bootsessionuuid".as_ptr(),
                bytes.as_mut_ptr().cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        } == -1
        {
            return Err(io::Error::last_os_error());
        }
        let text = std::str::from_utf8(&bytes[..len]).map_err(|_| io::ErrorKind::InvalidData)?;
        Ok(text.trim_end_matches('\0').to_owned())
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn read() -> io::Result<String> {
        Err(io::ErrorKind::Unsupported.into())
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
        assert_eq!(BootId::current().unwrap(), BootId::current().unwrap());
    }
}
