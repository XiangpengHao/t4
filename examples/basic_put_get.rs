fn main() -> Result<(), Box<dyn std::error::Error>> {
    pollster::block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("basic.t4");

    let store = t4::mount(&path).await?;
    store
        .put(b"greeting".to_vec(), b"Hello, t4!".to_vec())
        .await?;

    let value = store.get(b"greeting").await?;
    println!("greeting = {}", String::from_utf8_lossy(&value));

    let removed = store.remove(b"greeting").await?;
    println!("removed = {removed}");

    Ok(())
}
