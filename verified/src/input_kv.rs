use std::borrow::Borrow;

use vstd::{prelude::*, slice::slice_to_vec};

verus! {

#[derive(Debug)]
pub enum InputError {
    KeyTooLarge(usize),
    ValueTooLarge(usize),
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct T4Key(Vec<u8>);

impl T4Key {
    #[verifier::type_invariant]
    spec fn type_inv(&self) -> bool {
        self.0.len() <= u8::MAX as usize
    }

    pub fn as_bytes(&self) -> (result: &[u8])
        ensures
            result.len() <= u8::MAX as usize,
    {
        proof {
            use_type_invariant(&*self);
        }
        self.0.as_slice()
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

    #[allow(clippy::should_implement_trait)]
    // verus doesn't allow inherit clone
    pub fn clone(&self) -> Self {
        proof {
            use_type_invariant(&*self);
        }
        Self(self.0.clone())
    }

    pub fn try_from_vec(value: Vec<u8>) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u8::MAX as usize,
    {
        if value.len() > u8::MAX as usize {
            return Err(InputError::KeyTooLarge(value.len()));
        }
        Ok(Self(value))
    }

    pub fn try_from_slice(value: &[u8]) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u8::MAX as usize,
    {
        if value.len() > u8::MAX as usize {
            return Err(InputError::KeyTooLarge(value.len()));
        }
        Ok(Self(slice_to_vec(value)))
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

    pub closed spec fn wf(self) -> bool {
        self.0.len() <= u8::MAX as usize
    }

    pub fn from_slice(value: &'a [u8]) -> (result: Self)
        requires
            value.len() <= u8::MAX as usize,
        ensures
            result.wf(),
    {
        Self(value)
    }

    pub fn try_from_slice(value: &'a [u8]) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u8::MAX as usize,
            result.is_ok() ==> result.unwrap().wf(),
    {
        if value.len() > u8::MAX as usize {
            return Err(InputError::KeyTooLarge(value.len()));
        }
        Ok(Self(value))
    }
}

impl<'a> AsRef<[u8]> for T4KeyRef<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq)]
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

    #[allow(clippy::should_implement_trait)]
    // verus doesn't allow inherit clone
    pub fn clone(&self) -> Self {
        Self { bytes: self.bytes.clone(), len_u32: self.len_u32 }
    }

    pub fn try_from_vec(value: Vec<u8>) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u32::MAX as usize,
    {
        if value.len() > u32::MAX as usize {
            return Err(InputError::ValueTooLarge(value.len()));
        }
        let len_u32: u32 = value.len() as u32;
        Ok(Self { bytes: value, len_u32 })
    }

    pub fn try_from_slice(value: &[u8]) -> (result: Result<Self, InputError>)
        ensures
            result.is_ok() <==> value.len() <= u32::MAX as usize,
    {
        if value.len() > u32::MAX as usize {
            return Err(InputError::ValueTooLarge(value.len()));
        }
        Ok(Self { bytes: slice_to_vec(value), len_u32: value.len() as u32 })
    }
}

} // verus!
