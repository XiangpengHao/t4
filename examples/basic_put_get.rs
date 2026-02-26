use t4::{Engine, MountOptions};

fn demo_options() -> MountOptions {
    // Disable O_DIRECT/O_DSYNC to keep the example runnable on more systems.
    MountOptions {
        queue_depth: 32,
        direct_io: false,
        dsync: false,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("basic.t4");

    let mut store = Engine::mount_with_options(&path, demo_options())?;
    store.put(b"greeting", b"Hello, t4!")?;

    let value = store.get(b"greeting")?;
    println!("greeting = {}", String::from_utf8_lossy(&value));

    let removed = store.remove(b"greeting")?;
    println!("removed = {removed}");

    Ok(())
}
