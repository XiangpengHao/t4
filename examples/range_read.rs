fn main() -> Result<(), Box<dyn std::error::Error>> {
    pollster::block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("range.t4");

    let store = t4::mount(&path).await?;
    store
        .put(b"blob".to_vec(), b"hello-0123456789-world".to_vec())
        .await?;

    let range = store.get_range(b"blob", 6, 10).await?;
    println!("range = {}", String::from_utf8_lossy(&range));
    assert_eq!(range, b"0123456789");

    Ok(())
}
