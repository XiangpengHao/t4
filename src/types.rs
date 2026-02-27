use std::borrow::Borrow;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct T4Key(Vec<u8>);

impl T4Key {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn from_wal_bytes(bytes: Vec<u8>) -> Result<Self> {
        Self::try_from(bytes)
    }
}

impl AsRef<[u8]> for T4Key {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<[u8]> for T4Key {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for T4Key {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        if value.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge(value.len()));
        }
        Ok(Self(value))
    }
}

impl TryFrom<&[u8]> for T4Key {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        Self::try_from(value.to_vec())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct T4KeyRef<'a>(&'a [u8]);

impl<'a> T4KeyRef<'a> {
    pub fn as_bytes(self) -> &'a [u8] {
        self.0
    }

    pub fn len(self) -> usize {
        self.0.len()
    }

    pub fn is_empty(self) -> bool {
        self.0.is_empty()
    }
}

impl<'a> AsRef<[u8]> for T4KeyRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

impl<'a> TryFrom<&'a [u8]> for T4KeyRef<'a> {
    type Error = Error;

    fn try_from(value: &'a [u8]) -> Result<Self> {
        if value.len() > u16::MAX as usize {
            return Err(Error::KeyTooLarge(value.len()));
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct T4Value {
    bytes: Vec<u8>,
    len_u32: u32,
}

impl T4Value {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn len_u32(&self) -> u32 {
        self.len_u32
    }

    pub fn is_empty(&self) -> bool {
        self.len_u32 == 0
    }
}

impl TryFrom<Vec<u8>> for T4Value {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        let len_u32: u32 = value
            .len()
            .try_into()
            .map_err(|_| Error::InvalidArgument("value length exceeds u32"))?;
        Ok(Self {
            bytes: value,
            len_u32,
        })
    }
}

impl TryFrom<&[u8]> for T4Value {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        Self::try_from(value.to_vec())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeRequest {
    start: u32,
    len: u32,
    end: u32,
}

impl RangeRequest {
    pub fn new(start: u64, len: u64) -> Result<Self> {
        let start: u32 = start.try_into().map_err(|_| Error::RangeOutOfBounds)?;
        let len: u32 = len.try_into().map_err(|_| Error::RangeOutOfBounds)?;
        let end = start.checked_add(len).ok_or(Error::RangeOutOfBounds)?;
        Ok(Self { start, len, end })
    }

    pub fn checked_against(self, upper_bound: u32) -> Result<CheckedRange> {
        if self.end > upper_bound {
            return Err(Error::RangeOutOfBounds);
        }
        Ok(CheckedRange {
            start: self.start,
            len: self.len,
            end: self.end,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckedRange {
    start: u32,
    len: u32,
    end: u32,
}

impl CheckedRange {
    pub fn start(self) -> u32 {
        self.start
    }

    pub fn len(self) -> u32 {
        self.len
    }

    pub fn end(self) -> u32 {
        self.end
    }

    pub fn is_empty(self) -> bool {
        self.len == 0
    }
}
