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

pub type RangeRequest = proof_core::RangeRequestU32;
pub type CheckedRange = proof_core::CheckedRangeU32;
