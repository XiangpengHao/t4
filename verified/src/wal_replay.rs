#![allow(clippy::while_let_loop)] // verus doesn't support while let loop
use std::collections::HashMap;

use vstd::prelude::*;

use crate::input_kv::{T4Key, ValueRef};
use crate::wal::{WalEntryRef, WalEntryState, WalPage};
use crate::{PAGE_SIZE, align_up_u64, allocate_next_lsn};

verus! {

pub const FLAG_LIVE: u8 = 0;

pub const FLAG_TOMBSTONE: u8 = 1;

#[derive(Debug)]
pub enum ReplayError {
    NonMonotonicLsn,
    Overflow,
    UnknownFlag,
    InvalidKey,
}

#[derive(Debug)]
pub struct ReplayState {
    pub max_data_end: u64,
    pub max_wal_end: u64,
    pub previous_lsn: Option<u64>,
    pub index: HashMap<T4Key, ValueRef>,
}

impl ReplayState {
    /// Fresh state before any pages have been scanned.
    /// Both bounds start at PAGE_SIZE because page 0 occupies [0, PAGE_SIZE).
    pub fn init() -> (result: Self)
        ensures
            result.max_data_end == PAGE_SIZE as u64,
            result.max_wal_end == PAGE_SIZE as u64,
            result.previous_lsn.is_none(),
    {
        Self {
            max_data_end: PAGE_SIZE as u64,
            max_wal_end: PAGE_SIZE as u64,
            previous_lsn: None,
            index: HashMap::new(),
        }
    }

    /// Process a single WAL entry: verify LSN monotonicity, update data-end
    /// tracking, and apply key effects into the hash index.
    fn process_entry(self, entry: &WalEntryRef) -> (result: Result<Self, ReplayError>)
        ensures
            result.is_ok() ==> result.unwrap().max_data_end >= self.max_data_end,
            result.is_ok() ==> result.unwrap().max_wal_end == self.max_wal_end,
            result.is_ok() ==> result.unwrap().previous_lsn == Some(entry.lsn),
            result.is_ok() ==> self.previous_lsn.is_some() ==> self.previous_lsn.unwrap()
                < result.unwrap().previous_lsn.unwrap(),
    {
        let prev_max_data_end = self.max_data_end;
        let max_wal_end = self.max_wal_end;
        let previous_lsn = self.previous_lsn;
        let mut index = self.index;

        if let Some(prev) = previous_lsn {
            if entry.lsn <= prev {
                return Err(ReplayError::NonMonotonicLsn);
            }
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
                index.insert(key, ValueRef { offset: entry.offset, length: entry.value_length });
                if data_end > prev_max_data_end {
                    data_end
                } else {
                    prev_max_data_end
                }
            },
            WalEntryState::Tombstone => {
                index.remove(entry.key.as_bytes());
                prev_max_data_end
            },
        };

        Ok(ReplayState { max_data_end, max_wal_end, previous_lsn: Some(entry.lsn), index })
    }

    /// Record that a WAL page at `page_offset` was read.
    fn advance_wal_end(self, page_offset: u64) -> (result: Result<Self, ReplayError>)
        ensures
            result.is_ok() ==> result.unwrap().max_wal_end >= self.max_wal_end,
            result.is_ok() ==> result.unwrap().max_data_end == self.max_data_end,
            result.is_ok() ==> result.unwrap().previous_lsn == self.previous_lsn,
    {
        let prev_max_wal_end = self.max_wal_end;
        let max_data_end = self.max_data_end;
        let previous_lsn = self.previous_lsn;
        let index = self.index;
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
        Ok(ReplayState { max_data_end, max_wal_end, previous_lsn, index })
    }

    pub fn process_page(self, page: &WalPage) -> (result: Result<(Self, Option<u64>), ReplayError>)
        requires
            page.wf(),
    {
        let mut state = self;
        let mut iter = page.iter();
        loop
            decreases iter.remaining(),
        {
            let entry = match iter.next() {
                Some(v) => v,
                None => {
                    break ;
                },
            };
            state = state.process_entry(&entry)?;
        }
        if page.next_page() != 0 {
            state = state.advance_wal_end(page.next_page())?;
            Ok((state, Some(page.next_page())))
        } else {
            Ok((state, None))
        }
    }

    /// Compute final file_tail and next_lsn from the accumulated replay state.
    pub fn finalize(self, file_len: u64) -> (result: Result<
        (u64, u64, HashMap<T4Key, ValueRef>),
        ReplayError,
    >)
        ensures
            result.is_ok() ==> result.unwrap().0 >= self.max_data_end,
            result.is_ok() ==> result.unwrap().0 >= self.max_wal_end,
            result.is_ok() ==> result.unwrap().0 >= file_len,
            result.is_ok() ==> result.unwrap().0 & sub(PAGE_SIZE as u64, 1) == 0,
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
        Ok((file_tail, next_lsn, self.index))
    }
}

} // verus!
