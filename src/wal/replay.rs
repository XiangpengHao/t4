#![allow(clippy::while_let_loop)] // verus doesn't support while let loop
#![allow(clippy::single_match)] // verus doesn't support single match
use vstd::prelude::*;

use super::allocate_next_lsn;
use super::page::{WalEntryRef, WalEntryState, WalPage};
use crate::PAGE_SIZE;
use crate::buffer::align_up_u64;
use crate::input_kv::{FileHoles, T4Key, ValueRef};

verus! {

#[derive(Debug)]
pub enum ReplayError {
    NonMonotonicLsn,
    Overflow,
    UnknownFlag,
    InvalidKey,
    InvalidValueRef,
}

/// Where replay puts each key's latest live value. The proofs say nothing
/// about its contents, so the store can replay straight into its own index.
pub trait ReplayIndex {
    /// Record `key`'s live value, written by the entry at `lsn`; returns the
    /// value it replaces.
    fn insert(&mut self, key: T4Key, lsn: u64, value: ValueRef) -> Option<ValueRef>;

    /// Forget `key`; returns the value it had.
    fn remove(&mut self, key: &[u8]) -> Option<ValueRef>;
}

#[derive(Debug)]
pub struct ReplayState {
    pub max_data_end: u64,
    pub max_wal_end: u64,
    pub previous_lsn: Option<u64>,
    pub holes: FileHoles,
}

impl ReplayState {
    pub closed spec fn wf(self) -> bool {
        self.holes.wf()
    }

    /// Fresh state before any pages have been scanned.
    /// Both bounds start at PAGE_SIZE because page 0 occupies [0, PAGE_SIZE).
    pub fn init() -> (result: Self)
        ensures
            result.max_data_end == PAGE_SIZE as u64,
            result.max_wal_end == PAGE_SIZE as u64,
            result.previous_lsn.is_none(),
            result.wf(),
    {
        Self {
            max_data_end: PAGE_SIZE as u64,
            max_wal_end: PAGE_SIZE as u64,
            previous_lsn: None,
            holes: FileHoles::empty(),
        }
    }

    /// Process a single WAL entry: verify LSN monotonicity, update data-end
    /// tracking, and apply key effects into `index`.
    fn process_entry<I: ReplayIndex>(self, entry: &WalEntryRef, index: &mut I) -> (result: Result<
        Self,
        ReplayError,
    >)
        requires
            self.wf(),
        ensures
            result.is_ok() ==> result.unwrap().max_data_end >= self.max_data_end,
            result.is_ok() ==> result.unwrap().max_wal_end == self.max_wal_end,
            result.is_ok() ==> result.unwrap().previous_lsn == Some(entry.lsn),
            result.is_ok() ==> self.previous_lsn.is_some() ==> self.previous_lsn.unwrap()
                < result.unwrap().previous_lsn.unwrap(),
            result.is_ok() ==> result.unwrap().wf(),
    {
        let prev_max_data_end = self.max_data_end;
        let max_wal_end = self.max_wal_end;
        let previous_lsn = self.previous_lsn;
        let mut holes = self.holes;

        match previous_lsn {
            Some(prev) if entry.lsn <= prev => {
                return Err(ReplayError::NonMonotonicLsn);
            },
            _ => {},
        }
        let max_data_end = match entry.state() {
            WalEntryState::Live => {
                proof {
                    assert(PAGE_SIZE as u64 & sub(PAGE_SIZE as u64, 1) == 0u64) by (bit_vector);
                }
                let padded = align_up_u64(entry.value_length as u64, PAGE_SIZE as u64).unwrap();
                let data_end = match entry.offset.checked_add(padded) {
                    Some(v) => v,
                    None => {
                        return Err(ReplayError::Overflow);
                    },
                };
                let key = T4Key::try_from_slice(entry.key.as_bytes()).unwrap();
                holes.consume(entry.offset, padded);
                let value = match ValueRef::try_new(entry.offset, entry.value_length) {
                    Some(value) => value,
                    None => {
                        return Err(ReplayError::InvalidValueRef);
                    },
                };
                let old = index.insert(key, entry.lsn, value);
                match old {
                    Some(value) => holes.release_value(value),
                    None => {},
                }
                if data_end > prev_max_data_end {
                    data_end
                } else {
                    prev_max_data_end
                }
            },
            WalEntryState::Tombstone => {
                let old = index.remove(entry.key.as_bytes());
                match old {
                    Some(value) => holes.release_value(value),
                    None => {},
                }
                prev_max_data_end
            },
        };

        Ok(ReplayState { max_data_end, max_wal_end, previous_lsn: Some(entry.lsn), holes })
    }

    /// Record that a WAL page at `page_offset` was read.
    fn advance_wal_end(self, page_offset: u64) -> (result: Result<Self, ReplayError>)
        requires
            self.wf(),
        ensures
            result.is_ok() ==> result.unwrap().max_wal_end >= self.max_wal_end,
            result.is_ok() ==> result.unwrap().max_data_end == self.max_data_end,
            result.is_ok() ==> result.unwrap().previous_lsn == self.previous_lsn,
            result.is_ok() ==> result.unwrap().wf(),
    {
        let prev_max_wal_end = self.max_wal_end;
        let max_data_end = self.max_data_end;
        let previous_lsn = self.previous_lsn;
        let holes = self.holes;
        let wal_end = match page_offset.checked_add(PAGE_SIZE as u64) {
            Some(v) => v,
            None => {
                return Err(ReplayError::Overflow);
            },
        };
        let max_wal_end = if wal_end > prev_max_wal_end {
            wal_end
        } else {
            prev_max_wal_end
        };
        Ok(ReplayState { max_data_end, max_wal_end, previous_lsn, holes })
    }

    pub fn process_page<I: ReplayIndex>(self, page: &WalPage, index: &mut I) -> (result: Result<
        (Self, Option<u64>),
        ReplayError,
    >)
        requires
            page.wf(),
            self.wf(),
        ensures
            result.is_ok() ==> result.unwrap().0.wf(),
    {
        let mut state = self;
        let mut iter = page.iter();
        loop
            invariant
                state.wf(),
            decreases iter.remaining(),
        {
            let entry = match iter.next_impl() {
                Some(v) => v,
                None => {
                    break;
                },
            };
            state = state.process_entry(&entry, index)?;
        }
        if page.next_page() != 0 {
            state = state.advance_wal_end(page.next_page())?;
            Ok((state, Some(page.next_page())))
        } else {
            Ok((state, None))
        }
    }

    /// Compute final file_tail and next_lsn from the accumulated replay state.
    pub fn finalize(self, file_len: u64) -> (result: Result<(u64, u64, FileHoles), ReplayError>)
        requires
            self.wf(),
        ensures
            result.is_ok() ==> result.unwrap().0 >= self.max_data_end,
            result.is_ok() ==> result.unwrap().0 >= self.max_wal_end,
            result.is_ok() ==> result.unwrap().0 >= file_len,
            result.is_ok() ==> result.unwrap().0 & sub(PAGE_SIZE as u64, 1) == 0,
            result.is_ok() ==> result.unwrap().2.wf(),
    {
        let a = if file_len > self.max_data_end {
            file_len
        } else {
            self.max_data_end
        };
        let highest = if a > self.max_wal_end {
            a
        } else {
            self.max_wal_end
        };
        proof {
            assert(PAGE_SIZE as u64 & sub(PAGE_SIZE as u64, 1) == 0u64) by (bit_vector);
        }
        let file_tail = match align_up_u64(highest, PAGE_SIZE as u64) {
            Some(v) => v,
            None => {
                return Err(ReplayError::Overflow);
            },
        };
        let next_lsn = match self.previous_lsn {
            Some(lsn) => {
                match allocate_next_lsn(lsn) {
                    Some(v) => v,
                    None => {
                        return Err(ReplayError::Overflow);
                    },
                }
            },
            None => 0u64,
        };
        Ok((file_tail, next_lsn, self.holes))
    }
}

} // verus!
