//! The memory pool under shuttle: readers racing the eviction and reuse of
//! the chunk they read, and the replacement policy's sweep, on one chunk's
//! state word.

use std::num::NonZeroU32;

use super::memory_pool::MemoryPool;
use crate::cache::{BlockKey, ChunkId, FileVersionId};
use crate::io::sync::{Arc, thread};

fn block(file: u64) -> BlockKey {
    BlockKey::new(FileVersionId::new(file), 0)
}

/// A one-chunk pool, the chunk holding `block` published, every byte its
/// file version id's.
fn pool_holding(block: BlockKey) -> (Arc<MemoryPool>, ChunkId) {
    let pool = Arc::new(MemoryPool::new(NonZeroU32::MIN).unwrap());
    let chunk = publish(&pool, block).unwrap();
    (pool, chunk)
}

fn publish(pool: &MemoryPool, block: BlockKey) -> Option<ChunkId> {
    let mut writer = pool.allocate()?;
    writer.bytes_mut().fill(block.file_version().get() as u8);
    Some(writer.publish(block).chunk())
}

/// Reads `block` from `chunk`, if it still holds it, and checks the bytes
/// are `block`'s.
fn read(pool: &MemoryPool, chunk: ChunkId, block: BlockKey) {
    if let Some(reader) = pool.read(chunk, block) {
        let bytes = reader.bytes();
        let tag = block.file_version().get() as u8;
        assert!(bytes[0] == tag && bytes[bytes.len() - 1] == tag);
    }
}

/// A reader racing the eviction of its block, and the reuse of its chunk
/// for another, reads its own block's bytes or nothing.
#[test]
fn a_reader_racing_reuse_reads_its_block_or_nothing() {
    shuttle::check_pct(
        || {
            let (pool, chunk) = pool_holding(block(1));
            let reader = {
                let pool = Arc::clone(&pool);
                thread::spawn(move || read(&pool, chunk, block(1)))
            };
            let evictor = {
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    let victim = pool.pin_for_eviction(chunk).unwrap();
                    assert!(victim.unpublish());
                    drop(victim);
                    // Free again only once the reader, if it got in, is out.
                    publish(&pool, block(2))
                })
            };
            reader.join().unwrap();
            let reused = evictor.join().unwrap();
            assert!(pool.read(chunk, block(1)).is_none());
            assert_eq!(pool.free_count(), u32::from(reused.is_none()));
        },
        1000,
        3,
    );
}

/// The access bit shares the state word with the open bit and the reader
/// count: setting and taking it, between pins and unpins and the close,
/// changes neither.
#[test]
fn access_bits_never_disturb_the_readers() {
    shuttle::check_pct(
        || {
            let (pool, chunk) = pool_holding(block(1));
            let mut handles: Vec<_> = (0..2)
                .map(|_| {
                    let pool = Arc::clone(&pool);
                    thread::spawn(move || read(&pool, chunk, block(1)))
                })
                .collect();
            handles.push({
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    pool.take_accessed(chunk);
                    pool.take_accessed(chunk);
                })
            });
            handles.push({
                let pool = Arc::clone(&pool);
                thread::spawn(move || {
                    assert!(pool.pin_for_eviction(chunk).unwrap().unpublish());
                })
            });
            for handle in handles {
                handle.join().unwrap();
            }
            // Freed once every reader left: the count came back to zero, and
            // the close held.
            assert_eq!(pool.free_count(), 1);
            assert!(pool.read(chunk, block(1)).is_none());
        },
        1000,
        3,
    );
}

/// Two readers and an evictor: whichever leaves last frees the chunk, once.
#[test]
fn the_last_to_leave_frees_the_chunk_once() {
    shuttle::check_pct(
        || {
            let (pool, chunk) = pool_holding(block(1));
            let handles: Vec<_> = (0..3)
                .map(|role| {
                    let pool = Arc::clone(&pool);
                    thread::spawn(move || {
                        if role == 0 {
                            assert!(pool.pin_for_eviction(chunk).unwrap().unpublish());
                        } else {
                            read(&pool, chunk, block(1));
                        }
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(pool.free_count(), 1);
        },
        1000,
        3,
    );
}
