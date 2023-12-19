#[cfg(test)]
mod lib_test;

use data_encoding::BASE32_NOPAD;
use futures::channel::oneshot::{channel, Receiver, Sender};
pub use rusqlite;

use std::{
    collections::HashMap,
    sync::{mpsc, Arc, Mutex, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension};

#[derive(Clone)]
pub struct Cache {
    inner: Arc<CacheImpl>,
}

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub flush_interval: Duration,
    pub flush_gc_ratio: u64,
    pub max_ttl: Option<Duration>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            flush_interval: Duration::from_secs(10),
            flush_gc_ratio: 30,
            max_ttl: None,
        }
    }
}

#[derive(Clone)]
pub struct Topic {
    inner: Arc<TopicImpl>,
}

struct CacheImpl {
    config: CacheConfig,
    conn: Mutex<Connection>,
    lazy_expiry_update: Mutex<HashMap<(Arc<str>, String), u64>>,
    stop_tx: Mutex<mpsc::Sender<()>>,
    completion_rx: Mutex<mpsc::Receiver<()>>,
}

struct TopicImpl {
    cache: Cache,
    table_name: Arc<str>,
    listeners: Mutex<HashMap<String, Vec<Sender<()>>>>,
}

impl Drop for CacheImpl {
    fn drop(&mut self) {
        self.stop_tx.lock().unwrap().send(()).unwrap();
        self.completion_rx.lock().unwrap().recv().unwrap();
    }
}

impl Cache {
    pub fn new(config: CacheConfig, conn: Connection) -> Result<Self, rusqlite::Error> {
        assert!(config.flush_gc_ratio > 0);
        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        let (completion_tx, completion_rx) = mpsc::channel::<()>();
        conn.execute_batch("pragma journal_mode = wal;")?;
        let inner = Arc::new(CacheImpl {
            conn: Mutex::new(conn),
            config: config.clone(),
            lazy_expiry_update: Mutex::new(HashMap::new()),
            stop_tx: Mutex::new(stop_tx),
            completion_rx: Mutex::new(completion_rx),
        });
        let w = Arc::downgrade(&inner);
        std::thread::spawn(move || periodic_task(config, stop_rx, completion_tx, w));
        Ok(Self { inner })
    }

    fn flush(&self) {
        let lazy_expiry_update = std::mem::take(&mut *self.inner.lazy_expiry_update.lock().unwrap());
        for ((table_name, key), expiry) in lazy_expiry_update {
            let res = self.inner.conn.lock().unwrap().execute(
                &format!("update {} set expiry = ? where k = ?", table_name),
                rusqlite::params![expiry, key],
            );
            if let Err(e) = res {
                tracing::error!(table = &*table_name, key = key.as_str(), error = %e, "error updating expiry");
            }
        }
    }

    fn gc(&self) -> Result<(), rusqlite::Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let tables = self
            .inner
            .conn
            .lock()
            .unwrap()
            .unchecked_transaction()?
            .prepare("select name from sqlite_master where type = 'table' and name like 'topic_%'")?
            .query_map(rusqlite::params![], |x| x.get::<_, String>(0))?
            .collect::<Result<Vec<String>, rusqlite::Error>>()?;
        let mut total = 0usize;
        for table in tables {
            let count = self.inner.conn.lock().unwrap().execute(
                &format!("delete from {} where expiry < ?", table),
                rusqlite::params![now],
            )?;
            total += count;
        }
        if total != 0 {
            tracing::info!(total = total, "gc deleted rows");
        }
        Ok(())
    }

    pub fn topic(&self, key: &str) -> Result<Topic, rusqlite::Error> {
        let table_name = format!("topic_{}", BASE32_NOPAD.encode(key.as_bytes()));
        self.inner.conn.lock().unwrap().execute_batch(&format!(
            r#"
begin transaction;
create table if not exists {} (
    k text primary key not null,
    v blob not null,
    created_at integer not null default (cast(strftime('%s', 'now') as integer)),
    expiry integer not null,
    ttl integer not null
);
create index if not exists {}_by_expiry on {} (expiry);
commit;
"#,
            table_name, table_name, table_name,
        ))?;
        Ok(Topic {
            inner: Arc::new(TopicImpl {
                cache: self.clone(),
                table_name: Arc::from(table_name),
                listeners: Mutex::new(HashMap::new()),
            }),
        })
    }
}

pub struct Value {
    pub data: Vec<u8>,
    pub created_at: u64,
}

impl Topic {
    pub fn get(&self, key: &str) -> Result<Option<Value>, rusqlite::Error> {
        let conn = self.inner.cache.inner.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(&format!(
            "select v, created_at, ttl from {} where k = ?",
            self.inner.table_name,
        ))?;
        let rsp: Option<(Vec<u8>, u64, u64)> = stmt
            .query_row(rusqlite::params![key], |x| {
                Ok((x.get(0)?, x.get(1)?, x.get(2)?))
            })
            .optional()?;
        if let Some((data, created_at, ttl)) = rsp {
            self.inner
                .cache
                .inner
                .lazy_expiry_update
                .lock()
                .unwrap()
                .insert(
                    (self.inner.table_name.clone(), key.to_string()),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        .saturating_add(ttl)
                        .min(i64::MAX as u64),
                );
            Ok(Some(Value { data, created_at }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_for_update(
        &self,
        key: &str,
    ) -> Result<(KeyUpdater, Option<Value>), rusqlite::Error> {
        loop {
            let receiver: Option<Receiver<()>>;
            {
                let mut listeners = self.inner.listeners.lock().unwrap();
                if let Some(arr) = listeners.get_mut(key) {
                    let (tx, rx) = channel();
                    arr.push(tx);
                    receiver = Some(rx);
                } else {
                    receiver = None;
                    listeners.insert(key.to_string(), vec![]);
                }
            }

            if let Some(receiver) = receiver {
                let _ = receiver.await;
            } else {
                break;
            }
        }

        let data = self.get(key)?;
        Ok((
            KeyUpdater {
                topic: self.clone(),
                key: key.to_string(),
            },
            data,
        ))
    }

    pub fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), rusqlite::Error> {
        let conn = self.inner.cache.inner.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(&format!(
            "replace into {} (k, v, expiry, ttl) values(?, ?, ?, ?)",
            self.inner.table_name
        ))?;
        let mut ttl = ttl.as_secs();
        if let Some(max_ttl) = self.inner.cache.inner.config.max_ttl {
            let max_ttl = max_ttl.as_secs();
            ttl = ttl.min(max_ttl);
        }
        ttl = ttl.min(i64::MAX as u64);
        let expiry = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_add(ttl)
            .min(i64::MAX as u64);
        stmt.execute(rusqlite::params![key, value, expiry, ttl])?;
        self.inner
            .cache
            .inner
            .lazy_expiry_update
            .lock()
            .unwrap()
            .remove(&(self.inner.table_name.clone(), key.to_string()));
        Ok(())
    }

    pub fn delete(&self, key: &str) -> Result<(), rusqlite::Error> {
        let conn = self.inner.cache.inner.conn.lock().unwrap();
        let mut stmt = conn.prepare_cached(&format!(
            "delete from {} where k = ?",
            self.inner.table_name
        ))?;
        stmt.execute(rusqlite::params![key])?;
        Ok(())
    }
}

pub struct KeyUpdater {
    topic: Topic,
    key: String,
}

impl Drop for KeyUpdater {
    fn drop(&mut self) {
        let mut listeners = self.topic.inner.listeners.lock().unwrap();
        listeners.remove(self.key.as_str()).unwrap();
    }
}

impl KeyUpdater {
    pub fn write(self, value: &[u8], ttl: Duration) -> Result<(), rusqlite::Error> {
        self.topic.set(&self.key, value, ttl)?;
        Ok(())
    }
}

fn periodic_task(
    config: CacheConfig,
    stop_rx: mpsc::Receiver<()>,
    completion_tx: mpsc::Sender<()>,
    w: Weak<CacheImpl>,
) {
    let mut gc_ratio_counter = 0u64;
    loop {
        let tx = stop_rx.recv_timeout(config.flush_interval);
        if tx.is_ok() {
            break;
        }

        let inner = if let Some(x) = w.upgrade() {
            x
        } else {
            break;
        };
        let cache = Cache { inner };
        cache.flush();
        gc_ratio_counter += 1;
        if gc_ratio_counter == config.flush_gc_ratio {
            gc_ratio_counter = 0;
            if let Err(e) = cache.gc() {
                tracing::error!(error = %e, "gc failed");
            }
        }
    }
    tracing::info!("exiting periodic task");
    completion_tx.send(()).unwrap();
}
