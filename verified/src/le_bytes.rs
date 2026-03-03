use vstd::prelude::*;

use crate::PAGE_SIZE;

verus! {

pub fn u16_from_le_bytes(bytes: &[u8]) -> (result: u16)
    requires
        bytes.len() == 2,
    ensures
        result == (bytes[0] as usize | (bytes[1] as usize) << 8) as u16,
{
    (bytes[0] as usize | (bytes[1] as usize) << 8) as u16
}

#[verifier::external_body]
pub fn u64_from_le_bytes(bytes: &[u8]) -> (result: u64)
    requires
        bytes.len() == 8,
{
    u64::from_le_bytes(bytes.try_into().expect("entry bytes must be present"))
}

#[verifier::external_body]
pub fn u32_from_le_bytes(bytes: &[u8]) -> (result: u32)
    requires
        bytes.len() == 4,
    ensures
        result == u32_from_le_bytes_seq(bytes@),
{
    u32::from_le_bytes(bytes.try_into().expect("entry bytes must be present"))
}

pub open spec fn u32_from_le_bytes_seq(bytes: Seq<u8>) -> u32
    recommends
        bytes.len() == 4,
{
    u32_from_4(bytes[0], bytes[1], bytes[2], bytes[3])
}

pub open spec fn u32_from_4(b0: u8, b1: u8, b2: u8, b3: u8) -> u32 {
    (b0 as u32) | ((b1 as u32) << 8) | ((b2 as u32) << 16) | ((b3 as u32) << 24)
}

pub fn write_u16_le(bytes: &mut [u8; PAGE_SIZE], off: usize, v: u16)
    requires
        off + 1 < PAGE_SIZE,
{
    bytes[off] = (v & 0x00ff) as u8;
    bytes[off + 1] = ((v >> 8) & 0x00ff) as u8;
}

pub fn write_u32_le(bytes: &mut [u8; PAGE_SIZE], off: usize, v: u32)
    requires
        off + 3 < PAGE_SIZE,
    ensures
        u32_from_4(
            bytes@[off as int],
            bytes@[off as int + 1],
            bytes@[off as int + 2],
            bytes@[off as int + 3],
        ) == v,
        forall|i: int|
            0 <= i < PAGE_SIZE as int && !(off as int <= i <= off as int + 3) ==> bytes@[i] == old(
                bytes,
            )@[i],
{
    bytes[off] = (v & 0x000000ff) as u8;
    bytes[off + 1] = ((v >> 8) & 0x000000ff) as u8;
    bytes[off + 2] = ((v >> 16) & 0x000000ff) as u8;
    bytes[off + 3] = ((v >> 24) & 0x000000ff) as u8;
    proof {
        assert(((v & 0xffu32) as u8 as u32) | ((((v >> 8) & 0xffu32) as u8 as u32) << 8) | ((((v
            >> 16) & 0xffu32) as u8 as u32) << 16) | ((((v >> 24) & 0xffu32) as u8 as u32) << 24)
            == v) by (bit_vector);
    }
}

pub fn write_u64_le(bytes: &mut [u8; PAGE_SIZE], off: usize, v: u64)
    requires
        off + 7 < PAGE_SIZE,
{
    bytes[off] = (v & 0x00000000000000ff) as u8;
    bytes[off + 1] = ((v >> 8) & 0x00000000000000ff) as u8;
    bytes[off + 2] = ((v >> 16) & 0x00000000000000ff) as u8;
    bytes[off + 3] = ((v >> 24) & 0x00000000000000ff) as u8;
    bytes[off + 4] = ((v >> 32) & 0x00000000000000ff) as u8;
    bytes[off + 5] = ((v >> 40) & 0x00000000000000ff) as u8;
    bytes[off + 6] = ((v >> 48) & 0x00000000000000ff) as u8;
    bytes[off + 7] = ((v >> 56) & 0x00000000000000ff) as u8;
}

} // verus!
