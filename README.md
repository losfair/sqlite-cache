# sqlite-cache

[![crates.io](https://img.shields.io/crates/v/sqlite-cache.svg)](https://crates.io/crates/sqlite-cache)

SQLite-based on-disk cache for Rust.

## Usage

```rust
let cache = Cache::new(
    CacheConfig::default(),
    rusqlite::Connection::open_in_memory().unwrap(),
).unwrap();
let topic = cache.topic("test-topic").unwrap();
assert!(topic.get("hello").unwrap().is_none());
topic.set("hello", b"world", Duration::from_secs(60))
assert!(&topic.get("hello").unwrap().unwrap().data[..] == b"world");
```

### Locked updates

This library supports *locked updates* to prevent the [thundering herd problem](https://en.wikipedia.org/wiki/Thundering_herd_problem) on cache misses. The `get_for_update` API acquires a per-key lock and returns a `KeyUpdater`; subsequent `get_for_update` calls on the same key will block until the previous `KeyUpdater` is dropped.

```rust
let (updater, current_value) = topic.get_for_update("hello").await.unwrap();
let new_value = expensive_computation(current_value).await;
updater.write(new_value, Duration::from_secs(60)).unwrap();
```
