use t4::{Engine, MountOptions};

fn demo_options() -> MountOptions {
    MountOptions {
        queue_depth: 32,
        direct_io: false,
        dsync: false,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("range.t4");

    let mut store = Engine::mount_with_options(&path, demo_options())?;
    store.put(b"blob", b"hello-0123456789-world")?;

    let range = store.get_range(b"blob", 6, 10)?;
    println!("range = {}", String::from_utf8_lossy(&range));
    assert_eq!(range, b"0123456789");

    Ok(())
}
