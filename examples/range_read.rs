use t4::MountOptions;

fn demo_options() -> MountOptions {
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
    let path = dir.path().join("range.t4");

    let store = t4::mount_with_options(&path, demo_options()).await?;
    store
        .put(b"blob".to_vec(), b"hello-0123456789-world".to_vec())
        .await?;

    let range = store.get_range(b"blob".to_vec(), 6, 10).await?;
    println!("range = {}", String::from_utf8_lossy(&range));
    assert_eq!(range, b"0123456789");

    Ok(())
}
