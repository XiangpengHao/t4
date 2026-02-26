use t4::{Engine, MountOptions};

fn test_options() -> MountOptions {
    MountOptions {
        queue_depth: 32,
        direct_io: false,
        dsync: false,
    }
}

fn mount_or_skip(path: &std::path::Path) -> Option<Engine> {
    match Engine::mount_with_options(path, test_options()) {
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
        let Some(mut engine) = mount_or_skip(&path) else {
            return;
        };
        engine.put(b"a", b"small").unwrap();
        engine.put(b"b", &vec![7_u8; 5000]).unwrap();
        engine.put(b"c", b"").unwrap();

        assert_eq!(engine.get(b"a").unwrap(), b"small");
        assert_eq!(engine.get(b"b").unwrap(), vec![7_u8; 5000]);
        assert_eq!(engine.get(b"c").unwrap(), Vec::<u8>::new());
        assert_eq!(engine.get_range(b"b", 100, 64).unwrap(), vec![7_u8; 64]);
        assert!(engine.remove(b"a").unwrap());
        assert!(!engine.remove(b"missing").unwrap());
        assert!(matches!(engine.get(b"a"), Err(t4::Error::NotFound)));
        engine.sync().unwrap();
    }

    {
        let Some(mut engine) = mount_or_skip(&path) else {
            return;
        };
        assert!(matches!(engine.get(b"a"), Err(t4::Error::NotFound)));
        assert_eq!(engine.get(b"b").unwrap(), vec![7_u8; 5000]);
        assert_eq!(engine.get(b"c").unwrap(), Vec::<u8>::new());
        assert_eq!(engine.len(), 2);
    }
}

#[test]
fn index_page_growth_across_many_entries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("growth.t4");

    {
        let Some(mut engine) = mount_or_skip(&path) else {
            return;
        };
        for i in 0..300_u32 {
            let key = format!("key-{i:04}");
            let val = vec![(i % 255) as u8; 128];
            engine.put(key.as_bytes(), &val).unwrap();
        }

        for i in [0_u32, 17, 149, 299] {
            let key = format!("key-{i:04}");
            assert_eq!(
                engine.get(key.as_bytes()).unwrap(),
                vec![(i % 255) as u8; 128]
            );
        }
        engine.sync().unwrap();
    }

    {
        let Some(mut engine) = mount_or_skip(&path) else {
            return;
        };
        assert_eq!(engine.len(), 300);
        assert_eq!(engine.get(b"key-0299").unwrap(), vec![44_u8; 128]);
    }
}
