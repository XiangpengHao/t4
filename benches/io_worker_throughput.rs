use std::num::NonZero;

use divan::counter::BytesCount;
use pollster::block_on;
use tempfile::tempfile;

use t4::__bench::{AlignedBuf, IoWorker, PageWrite};
use t4::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U64};

fn main() {
    divan::main();
}

const QUEUE_DEPTH: u32 = 128;
const BATCH_SIZES: &[usize] = &[1, 4, 16, 64];

fn new_worker() -> IoWorker {
    let file = tempfile().expect("tempfile");
    IoWorker::new(NonZero::new(QUEUE_DEPTH).unwrap(), file).expect("io worker")
}

fn page_writes(batch: usize, base_offset: u64) -> Vec<PageWrite> {
    (0..batch)
        .map(|i| PageWrite {
            buf: AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap(),
            offset: base_offset + i as u64 * PAGE_SIZE_U64,
        })
        .collect()
}

#[divan::bench(args = BATCH_SIZES)]
fn write(bencher: divan::Bencher, batch: usize) {
    let worker = new_worker();
    bencher
        .counter(BytesCount::new(batch * PAGE_SIZE_U64 as usize))
        .with_inputs(|| page_writes(batch, 0))
        .bench_values(|writes| {
            block_on(worker.write(writes)).unwrap();
        });
}

#[divan::bench(args = BATCH_SIZES)]
fn read(bencher: divan::Bencher, batch: usize) {
    let worker = new_worker();
    block_on(worker.write(page_writes(batch, 0))).unwrap();
    block_on(worker.fsync()).unwrap();

    bencher
        .counter(BytesCount::new(batch * PAGE_SIZE_U64 as usize))
        .bench(|| {
            for i in 0..batch {
                let buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
                let out = block_on(worker.read_exact_at(buf, i as u64 * PAGE_SIZE_U64)).unwrap();
                divan::black_box(out);
            }
        });
}

#[divan::bench]
fn fsync(bencher: divan::Bencher) {
    let worker = new_worker();
    block_on(worker.write(page_writes(1, 0))).unwrap();
    bencher.bench(|| {
        block_on(worker.fsync()).unwrap();
    });
}
