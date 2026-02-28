#![no_main]

use std::collections::HashMap;
use std::path::Path;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use pollster::block_on;
use t4::{MountOptions, Store};

#[derive(Debug)]
struct FuzzInput {
    operations: Vec<Operation>,
}

impl<'a> Arbitrary<'a> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let op_count = usize::from(u8::arbitrary(u)?);
        let mut operations = Vec::with_capacity(op_count);
        for _ in 0..op_count {
            operations.push(Operation::arbitrary(u)?);
        }
        Ok(Self { operations })
    }
}

#[derive(Debug)]
enum Operation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    GetRange { key: Vec<u8>, start: u16, len: u16 },
    Remove { key: Vec<u8> },
    Sync,
    Remount,
}

impl<'a> Arbitrary<'a> for Operation {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let key = limited_bytes(u, 64)?;
        match u8::arbitrary(u)? % 6 {
            0 => Ok(Self::Put {
                key,
                value: limited_bytes(u, 2048)?,
            }),
            1 => Ok(Self::Get { key }),
            2 => Ok(Self::GetRange {
                key,
                start: u16::arbitrary(u)?,
                len: u16::arbitrary(u)?,
            }),
            3 => Ok(Self::Remove { key }),
            4 => Ok(Self::Sync),
            _ => Ok(Self::Remount),
        }
    }
}

fn limited_bytes(u: &mut Unstructured<'_>, max_len: usize) -> arbitrary::Result<Vec<u8>> {
    let len = usize::from(u16::arbitrary(u)?) % (max_len + 1);
    let mut out = vec![0_u8; len];
    for byte in &mut out {
        *byte = u8::arbitrary(u)?;
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedGetRange {
    NotFound,
    RangeOutOfBounds,
}

fn test_options() -> MountOptions {
    MountOptions {
        queue_depth: 32,
        direct_io: false,
        dsync: false,
    }
}

fn mount_or_skip(path: &Path) -> Option<Store> {
    match block_on(t4::mount_with_options(path, test_options())) {
        Ok(store) => Some(store),
        Err(t4::Error::Io(err))
            if matches!(
                err.raw_os_error(),
                Some(code) if code == libc::EPERM || code == libc::ENOSYS
            ) =>
        {
            None
        }
        Err(err) => panic!("mount failed: {err}"),
    }
}

fn assert_model_matches_store(store: &Store, model: &HashMap<Vec<u8>, Vec<u8>>) {
    assert_eq!(block_on(store.len()).unwrap(), model.len());
    assert_eq!(block_on(store.is_empty()).unwrap(), model.is_empty());
    for (key, expected) in model {
        let value = block_on(store.get(key)).unwrap();
        assert_eq!(&value, expected);
    }
}

fn expected_get_range(
    model: &HashMap<Vec<u8>, Vec<u8>>,
    key: &[u8],
    start: u16,
    len: u16,
) -> Result<Vec<u8>, ExpectedGetRange> {
    let Some(value) = model.get(key) else {
        return Err(ExpectedGetRange::NotFound);
    };

    let start = usize::from(start);
    let len = usize::from(len);
    let end = start.saturating_add(len);
    if end > value.len() {
        return Err(ExpectedGetRange::RangeOutOfBounds);
    }
    Ok(value[start..end].to_vec())
}

fuzz_target!(|input: FuzzInput| {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("model-equivalence.t4");

    let Some(mut store) = mount_or_skip(&path) else {
        return;
    };
    let mut model = HashMap::<Vec<u8>, Vec<u8>>::new();

    for op in input.operations {
        match op {
            Operation::Put { key, value } => {
                block_on(store.put(key.clone(), value.clone())).unwrap();
                model.insert(key, value);
            }
            Operation::Get { key } => {
                let actual = block_on(store.get(&key));
                match model.get(&key) {
                    Some(expected) => assert_eq!(actual.unwrap(), *expected),
                    None => assert!(matches!(actual, Err(t4::Error::NotFound))),
                }
            }
            Operation::GetRange { key, start, len } => {
                let actual = block_on(store.get_range(&key, u64::from(start), u64::from(len)));
                let expected = expected_get_range(&model, &key, start, len);
                match (actual, expected) {
                    (Ok(actual_bytes), Ok(expected_bytes)) => {
                        assert_eq!(actual_bytes, expected_bytes)
                    }
                    (Err(t4::Error::NotFound), Err(ExpectedGetRange::NotFound)) => {}
                    (Err(t4::Error::RangeOutOfBounds), Err(ExpectedGetRange::RangeOutOfBounds)) => {
                    }
                    (left, right) => panic!("get_range mismatch: {left:?} vs {right:?}"),
                }
            }
            Operation::Remove { key } => {
                let actual = block_on(store.remove(&key)).unwrap();
                let expected = model.remove(&key).is_some();
                assert_eq!(actual, expected);
            }
            Operation::Sync => {
                block_on(store.sync()).unwrap();
                assert_model_matches_store(&store, &model);
            }
            Operation::Remount => {
                block_on(store.sync()).unwrap();
                drop(store);
                store = mount_or_skip(&path).expect("mount failed after remount");
                assert_model_matches_store(&store, &model);
            }
        }
    }

    block_on(store.sync()).unwrap();
    drop(store);
    store = mount_or_skip(&path).expect("mount failed during final verification");
    assert_model_matches_store(&store, &model);
});
