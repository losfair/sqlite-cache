use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use data_encoding::BASE64;
use rand::{
    prelude::{SliceRandom, ThreadRng},
    thread_rng, RngCore,
};
use rusqlite::Connection;
use sqlite_cache::{Cache, CacheConfig};

fn gen_pairs(rng: &mut ThreadRng, size: usize) -> Vec<(String, Vec<u8>)> {
    (0..size)
        .map(|_| {
            let mut k = vec![0u8; (rng.next_u64() % 100 + 32) as usize];
            let mut v = vec![0u8; (rng.next_u64() % 1000) as usize];
            rng.fill_bytes(&mut k);
            rng.fill_bytes(&mut v);
            (BASE64.encode(&k), v)
        })
        .collect::<Vec<_>>()
}

fn bench_get_mt(b: &mut Bencher, size: usize, num_threads: usize) {
    let mut rng = thread_rng();
    let cache = Cache::new(
        CacheConfig::default(),
        Connection::open_in_memory().unwrap(),
    )
    .unwrap();
    let topic = cache.topic("test").unwrap();
    let pairs = gen_pairs(&mut rng, size);
    for (k, v) in &pairs {
        topic.set(k, v, Duration::from_secs(3600)).unwrap();
    }

    let pairs = Arc::new(pairs);

    b.iter_custom(|n| {
        let start = Instant::now();
        let handles = (0..num_threads)
            .map(|_| {
                let pairs = pairs.clone();
                let topic = topic.clone();
                std::thread::spawn(move || {
                    let mut rng = thread_rng();
                    for _ in 0..n {
                        let (k, v) = pairs.choose(&mut rng).unwrap();
                        let got_value = topic.get(k).unwrap().unwrap();
                        assert_eq!(&got_value.data, v);
                    }
                })
            })
            .collect::<Vec<_>>();
        for h in handles {
            h.join().unwrap();
        }
        start.elapsed()
    });
}

fn bench_get(b: &mut Bencher, size: usize) {
    bench_get_mt(b, size, 1)
}

fn bench_set_mt(b: &mut Bencher, size: usize, num_threads: usize) {
    let mut rng = thread_rng();
    let cache = Cache::new(
        CacheConfig::default(),
        Connection::open_in_memory().unwrap(),
    )
    .unwrap();
    let topic = cache.topic("test").unwrap();
    let pairs = gen_pairs(&mut rng, size);

    let pairs = Arc::new(pairs);

    b.iter_custom(|n| {
        let start = Instant::now();
        let handles = (0..num_threads)
            .map(|_| {
                let pairs = pairs.clone();
                let topic = topic.clone();
                std::thread::spawn(move || {
                    let mut rng = thread_rng();
                    for _ in 0..n {
                        let (k, v) = pairs.choose(&mut rng).unwrap();
                        topic.set(k, v, Duration::from_secs(3600)).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for h in handles {
            h.join().unwrap();
        }
        start.elapsed()
    });
}

fn bench_set(b: &mut Bencher, size: usize) {
    bench_set_mt(b, size, 1)
}

fn criterion_benchmark(c: &mut Criterion) {
    //c.bench_function("lookup - cache size 1000", |b| bench_get(b, 1000));
    c.bench_function("lookup - cache size 10000", |b| bench_get(b, 10000));
    c.bench_function("lookup mt(4) - cache size 10000", |b| {
        bench_get_mt(b, 10000, 4)
    });
    //c.bench_function("lookup - cache size 50000", |b| bench_get(b, 50000));
    //c.bench_function("insert - cache size 1000", |b| bench_set(b, 1000));
    c.bench_function("insert - cache size 10000", |b| bench_set(b, 10000));
    c.bench_function("insert mt(4) - cache size 10000", |b| {
        bench_set_mt(b, 10000, 4)
    });
    //c.bench_function("insert - cache size 50000", |b| bench_set(b, 50000));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
