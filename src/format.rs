use crate::error::{Error, Result};

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;
pub const MAGIC: [u8; 4] = *b"BTF4";
pub const VERSION: u16 = 1;
pub const INDEX_PAGE_HEADER_SIZE: usize = 32;
const ENTRY_HEADER_SIZE: usize = 20;

pub const FLAG_LIVE: u8 = 0;
pub const FLAG_TOMBSTONE: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: u64,
    pub length: u64,
    pub flags: u8,
}

impl IndexEntry {
    pub fn live(key: Vec<u8>, offset: u64, length: u64) -> Self {
        Self {
            key,
            offset,
            length,
            flags: FLAG_LIVE,
        }
    }

    pub fn tombstone(key: Vec<u8>) -> Self {
        Self {
            key,
            offset: 0,
            length: 0,
            flags: FLAG_TOMBSTONE,
        }
    }

    pub fn serialized_len(&self) -> usize {
        ENTRY_HEADER_SIZE + self.key.len()
    }

    pub fn encode_into(&self, dst: &mut [u8]) -> Result<usize> {
        let key_len: u16 = self
            .key
            .len()
            .try_into()
            .map_err(|_| Error::KeyTooLarge(self.key.len()))?;
        let total = self.serialized_len();
        if dst.len() < total {
            return Err(Error::Format("entry buffer too small".into()));
        }
        dst[0..2].copy_from_slice(&key_len.to_le_bytes());
        dst[2] = self.flags;
        dst[3] = 0;
        dst[4..12].copy_from_slice(&self.offset.to_le_bytes());
        dst[12..20].copy_from_slice(&self.length.to_le_bytes());
        dst[20..20 + self.key.len()].copy_from_slice(&self.key);
        Ok(total)
    }

    pub fn decode_from(src: &[u8]) -> Result<(Self, usize)> {
        if src.len() < ENTRY_HEADER_SIZE {
            return Err(Error::Format("entry truncated".into()));
        }
        let key_len = u16::from_le_bytes([src[0], src[1]]) as usize;
        let total = ENTRY_HEADER_SIZE + key_len;
        if src.len() < total {
            return Err(Error::Format("entry key truncated".into()));
        }
        let flags = src[2];
        let offset = u64::from_le_bytes(src[4..12].try_into().unwrap());
        let length = u64::from_le_bytes(src[12..20].try_into().unwrap());
        let key = src[20..20 + key_len].to_vec();
        Ok((
            Self {
                key,
                offset,
                length,
                flags,
            },
            total,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPage {
    pub next_page: u64,
    pub entries: Vec<IndexEntry>,
}

impl Default for IndexPage {
    fn default() -> Self {
        Self::empty()
    }
}

impl IndexPage {
    pub fn empty() -> Self {
        Self {
            next_page: 0,
            entries: Vec::new(),
        }
    }

    pub fn used_bytes(&self) -> usize {
        INDEX_PAGE_HEADER_SIZE
            + self
                .entries
                .iter()
                .map(IndexEntry::serialized_len)
                .sum::<usize>()
    }

    pub fn can_fit(&self, entry: &IndexEntry) -> bool {
        self.used_bytes() + entry.serialized_len() <= PAGE_SIZE
    }

    pub fn push(&mut self, entry: IndexEntry) -> bool {
        if !self.can_fit(&entry) {
            return false;
        }
        self.entries.push(entry);
        true
    }

    pub fn to_bytes(&self) -> Result<[u8; PAGE_SIZE]> {
        let mut out = [0_u8; PAGE_SIZE];
        if self.used_bytes() > PAGE_SIZE {
            return Err(Error::Format("index page overflow".into()));
        }

        out[0..4].copy_from_slice(&MAGIC);
        out[4..6].copy_from_slice(&VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&0_u16.to_le_bytes());
        out[8..16].copy_from_slice(&self.next_page.to_le_bytes());
        out[16..20].copy_from_slice(&(self.entries.len() as u32).to_le_bytes());
        out[20..24].copy_from_slice(&0_u32.to_le_bytes());
        out[24..32].copy_from_slice(&0_u64.to_le_bytes());

        let mut cursor = INDEX_PAGE_HEADER_SIZE;
        for entry in &self.entries {
            let written = entry.encode_into(&mut out[cursor..])?;
            cursor += written;
        }
        Ok(out)
    }

    pub fn from_bytes(src: &[u8]) -> Result<Self> {
        if src.len() != PAGE_SIZE {
            return Err(Error::Format("index page must be 4096 bytes".into()));
        }
        if src[0..4] != MAGIC {
            return Err(Error::Format("bad magic".into()));
        }
        let version = u16::from_le_bytes([src[4], src[5]]);
        if version != VERSION {
            return Err(Error::Format(format!("unsupported version {version}")));
        }
        let next_page = u64::from_le_bytes(src[8..16].try_into().unwrap());
        let entry_count = u32::from_le_bytes(src[16..20].try_into().unwrap()) as usize;

        let mut entries = Vec::with_capacity(entry_count);
        let mut cursor = INDEX_PAGE_HEADER_SIZE;
        for _ in 0..entry_count {
            let (entry, consumed) = IndexEntry::decode_from(&src[cursor..])?;
            cursor += consumed;
            if cursor > PAGE_SIZE {
                return Err(Error::Format("entry overran index page".into()));
            }
            entries.push(entry);
        }

        Ok(Self { next_page, entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_round_trip() {
        let mut page = IndexPage::empty();
        page.next_page = 8192;
        assert!(page.push(IndexEntry::live(b"alpha".to_vec(), 4096, 123)));
        assert!(page.push(IndexEntry::tombstone(b"beta".to_vec())));

        let bytes = page.to_bytes().unwrap();
        let decoded = IndexPage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, page);
    }

    #[test]
    fn page_overflow_detection() {
        let mut page = IndexPage::empty();
        let mut i = 0_u64;
        while page.push(IndexEntry::live(vec![b'k'; 64], i * 4096, 64)) {
            i += 1;
        }
        assert!(i > 0);
        assert!(!page.can_fit(&IndexEntry::live(vec![1; 128], 0, 1)));
    }
}
