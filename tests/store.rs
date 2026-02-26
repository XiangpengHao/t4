use pollster::block_on;
use t4::{MountOptions, Store};

fn test_options() -> MountOptions {
    MountOptions {
        queue_depth: 32,
        direct_io: false,
        dsync: false,
    }
}

fn mount_or_skip(path: &std::path::Path) -> Option<Store> {
    match block_on(t4::mount_with_options(path, test_options())) {
        Ok(engine) => Some(engine),
        Err(t4::Error::Io(err)) if matches!(err.raw_os_error(), Some(code) if code == libc::EPERM || code == libc::ENOSYS) =>
        {
            eprintln!("skipping io_uring integration test: {}", err);
            None
        }
        Err(err) => panic!("mount failed: {err}"),
    }
}

#[test]
fn engine_roundtrip_range_remove_and_remount() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("roundtrip.t4");

    {
        let Some(store) = mount_or_skip(&path) else {
            return;
        };
        block_on(store.put(b"a".to_vec(), b"small".to_vec())).unwrap();
        block_on(store.put(b"b".to_vec(), vec![7_u8; 5000])).unwrap();
        block_on(store.put(b"c".to_vec(), Vec::new())).unwrap();

        assert_eq!(block_on(store.get(b"a".to_vec())).unwrap(), b"small");
        assert_eq!(
            block_on(store.get(b"b".to_vec())).unwrap(),
            vec![7_u8; 5000]
        );
        assert_eq!(
            block_on(store.get(b"c".to_vec())).unwrap(),
            Vec::<u8>::new()
        );
        assert_eq!(
            block_on(store.get_range(b"b".to_vec(), 100, 64)).unwrap(),
            vec![7_u8; 64]
        );
        assert!(block_on(store.remove(b"a".to_vec())).unwrap());
        assert!(!block_on(store.remove(b"missing".to_vec())).unwrap());
        assert!(matches!(
            block_on(store.get(b"a".to_vec())),
            Err(t4::Error::NotFound)
        ));
        block_on(store.sync()).unwrap();
    }

    {
        let Some(store) = mount_or_skip(&path) else {
            return;
        };
        assert!(matches!(
            block_on(store.get(b"a".to_vec())),
            Err(t4::Error::NotFound)
        ));
        assert_eq!(
            block_on(store.get(b"b".to_vec())).unwrap(),
            vec![7_u8; 5000]
        );
        assert_eq!(
            block_on(store.get(b"c".to_vec())).unwrap(),
            Vec::<u8>::new()
        );
        assert_eq!(block_on(store.len()).unwrap(), 2);
    }
}

#[test]
fn index_page_growth_across_many_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("growth.t4");

    {
        let Some(store) = mount_or_skip(&path) else {
            return;
        };
        for i in 0..300_u32 {
            let key = format!("key-{i:04}");
            let val = vec![(i % 255) as u8; 128];
            block_on(store.put(key.into_bytes(), val)).unwrap();
        }

        for i in [0_u32, 17, 149, 299] {
            let key = format!("key-{i:04}");
            assert_eq!(
                block_on(store.get(key.into_bytes())).unwrap(),
                vec![(i % 255) as u8; 128]
            );
        }
        block_on(store.sync()).unwrap();
    }

    {
        let Some(store) = mount_or_skip(&path) else {
            return;
        };
        assert_eq!(block_on(store.len()).unwrap(), 300);
        assert_eq!(
            block_on(store.get(b"key-0299".to_vec())).unwrap(),
            vec![44_u8; 128]
        );
    }
}
