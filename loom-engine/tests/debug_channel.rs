#[tokio::main]
async fn main() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<i32>(64);
    let handle = tokio::task::spawn_blocking(move || {
        while let Some(i) = rx.blocking_recv() {
            println!("Got {}", i);
        }
        println!("Finished");
    });
    tx.send(1).await.unwrap();
    drop(tx);
    handle.await.unwrap();
    println!("Done!");
}
