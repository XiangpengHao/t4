fn main() -> Result<(), Box<dyn std::error::Error>> {
    pollster::block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("remount.t4");
    {
        let store = t4::mount(&path).await?;
        store.put(b"user:1".to_vec(), b"Alice".to_vec()).await?;
        store.put(b"user:2".to_vec(), b"Bob".to_vec()).await?;
        store.remove(b"user:2").await?;
        store.sync().await?;
    }

    {
        let store = t4::mount(&path).await?;
        let user1 = store.get(b"user:1").await?;
        println!("user:1 = {}", String::from_utf8_lossy(&user1));

        match store.get(b"user:2").await {
            Ok(_) => println!("unexpected: user:2 still exists"),
            Err(t4::Error::NotFound) => {
                println!("user:2 was deleted (tombstone applied on remount)")
            }
            Err(err) => return Err(Box::new(err)),
        }
    }

    Ok(())
}
