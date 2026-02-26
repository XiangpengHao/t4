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
    let path = dir.path().join("remount.t4");

    {
        let mut store = Engine::mount_with_options(&path, demo_options())?;
        store.put(b"user:1", b"Alice")?;
        store.put(b"user:2", b"Bob")?;
        store.remove(b"user:2")?;
        store.sync()?;
    }

    {
        let mut store = Engine::mount_with_options(&path, demo_options())?;
        let user1 = store.get(b"user:1")?;
        println!("user:1 = {}", String::from_utf8_lossy(&user1));

        match store.get(b"user:2") {
            Ok(_) => println!("unexpected: user:2 still exists"),
            Err(t4::Error::NotFound) => {
                println!("user:2 was deleted (tombstone applied on remount)")
            }
            Err(err) => return Err(Box::new(err)),
        }
    }

    Ok(())
}
