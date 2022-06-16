use std::time::Duration;

use rand::{thread_rng, Rng};
use rusqlite::Connection;

use crate::{Cache, CacheConfig};
use tracing_test::traced_test;

#[test]
#[traced_test]
fn test_new_drop() {
    let cache = Cache::new(
        CacheConfig::default(),
        Connection::open_in_memory().unwrap(),
    )
    .unwrap();
    drop(cache);
}

#[traced_test]
#[tokio::test]
async fn test_get_update() {
    let cache = Cache::new(
        CacheConfig::default(),
        Connection::open_in_memory().unwrap(),
    )
    .unwrap();
    let topic = cache.topic("test-topic").unwrap();
    cache.topic("test-topic-2").unwrap(); // not used
    assert!(topic.get("hello").unwrap().is_none());

    let (updater, value) = topic.get_for_update("hello").await.unwrap();
    assert!(value.is_none());
    updater.write(b"world", Duration::from_secs(1)).unwrap();
    assert!(&topic.get("hello").unwrap().unwrap().data[..] == b"world");
    topic.delete("hello").unwrap();
    assert!(topic.get("hello").unwrap().is_none());
}

#[traced_test]
#[tokio::test]
async fn test_update_lock() {
    let cache = Cache::new(
        CacheConfig::default(),
        Connection::open_in_memory().unwrap(),
    )
    .unwrap();
    let topic = cache.topic("test-topic").unwrap();
    let (updater, _) = topic.get_for_update("hello").await.unwrap();
    tokio::select! {
        _ = topic.get_for_update("hello") => {
            panic!("lock failed");
        }
        _ = tokio::time::sleep(Duration::from_millis(100)) => {

        }
    }

    let attempt = {
        let topic = topic.clone();
        tokio::task::spawn(async move { topic.get_for_update("hello").await.unwrap().1 })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    updater.write(b"world", Duration::from_secs(1)).unwrap();
    let value = attempt.await.unwrap().unwrap();
    assert_eq!(&value.data[..], b"world");
}

#[traced_test]
#[tokio::test]
async fn test_gc() {
    let cache = Cache::new(
        CacheConfig {
            flush_interval: Duration::from_millis(100),
            flush_gc_ratio: 5,
            ..CacheConfig::default()
        },
        Connection::open_in_memory().unwrap(),
    )
    .unwrap();
    let topic = cache.topic("test-topic").unwrap();
    let (updater, value) = topic.get_for_update("hello").await.unwrap();
    assert!(value.is_none());
    updater
        .write(b"world", Duration::from_millis(1000))
        .unwrap();
    assert!(&topic.get("hello").unwrap().unwrap().data[..] == b"world");

    for _ in 0..20 {
        if thread_rng().gen_bool(0.5) {
            assert!(&topic.get("hello").unwrap().unwrap().data[..] == b"world");
        } else {
            topic
                .set("hello", b"world", Duration::from_millis(1000))
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(topic.get("hello").unwrap().is_none());
}
