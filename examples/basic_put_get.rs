use t4::MountOptions;

fn demo_options() -> MountOptions {
    // Disable O_DIRECT/O_DSYNC to keep the example runnable on more systems.
    MountOptions {
        queue_depth: 32,
        direct_io: false,
        dsync: false,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pollster::block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("basic.t4");

    let store = t4::mount_with_options(&path, demo_options()).await?;
    store
        .put(b"greeting".to_vec(), b"Hello, t4!".to_vec())
        .await?;

    let value = store.get(b"greeting".to_vec()).await?;
    println!("greeting = {}", String::from_utf8_lossy(&value));

    let removed = store.remove(b"greeting".to_vec()).await?;
    println!("removed = {removed}");

    Ok(())
}
