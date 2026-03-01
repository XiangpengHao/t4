use std::num::NonZeroU32;

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_U32: u32 = PAGE_SIZE as u32;
pub const PAGE_SIZE_NZ_U32: NonZeroU32 = match NonZeroU32::new(PAGE_SIZE_U32) {
    Some(value) => value,
    None => panic!("PAGE_SIZE must be non-zero"),
};
pub const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;
