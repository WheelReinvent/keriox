//! Where redb databases live.
//!
//! Natively each database is a file. In the browser (wasm32) there are no
//! files: databases live in memory, registered under their path, and the
//! embedder saves and restores their bytes ([snapshot] / [restore]), e.g. to
//! IndexedDB. Natively the memory mode can be switched on too ([use_memory]),
//! which is how it is tested.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use redb::{Database, DatabaseError, StorageBackend};

static MEMORY: AtomicBool = AtomicBool::new(cfg!(target_arch = "wasm32"));
/// Bumped on every write: the embedder saves when it changed.
static GENERATION: AtomicU64 = AtomicU64::new(0);

type Bytes = Arc<RwLock<Vec<u8>>>;

fn store() -> &'static Mutex<BTreeMap<String, Bytes>> {
    static STORE: OnceLock<Mutex<BTreeMap<String, Bytes>>> = OnceLock::new();
    STORE.get_or_init(Default::default)
}

/// Keep databases in memory instead of files (always on in wasm32).
pub fn use_memory(on: bool) {
    MEMORY.store(on, Ordering::SeqCst);
}

pub fn in_memory() -> bool {
    MEMORY.load(Ordering::SeqCst)
}

/// Opens or creates the database at [path]: a file, or the in-memory one
/// registered under that path.
pub fn open(path: &Path) -> Result<Database, DatabaseError> {
    if !in_memory() {
        return Database::create(path);
    }
    let key = path.to_string_lossy().replace('\\', "/");
    let bytes = store().lock().unwrap_or_else(|p| p.into_inner()).entry(key).or_default().clone();
    Database::builder().create_with_backend(Memory(bytes))
}

/// Creates the parent folder of a database file (nothing to do in memory).
pub fn ensure_parent(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(parent) if !in_memory() => std::fs::create_dir_all(parent),
        _ => Ok(()),
    }
}

/// A change counter: different from the last saved value means "save again".
pub fn generation() -> u64 {
    GENERATION.load(Ordering::SeqCst)
}

/// Every in-memory database: (path, bytes).
pub fn snapshot() -> Vec<(String, Vec<u8>)> {
    let store = store().lock().unwrap_or_else(|p| p.into_inner());
    store
        .iter()
        .map(|(k, v)| (k.clone(), v.read().unwrap_or_else(|p| p.into_inner()).clone()))
        .collect()
}

/// Puts databases back before they are opened (a later [open] of the same
/// path finds them). Replaces what is registered under those paths.
pub fn restore(entries: Vec<(String, Vec<u8>)>) {
    let mut store = store().lock().unwrap_or_else(|p| p.into_inner());
    for (k, v) in entries {
        store.insert(k, Arc::new(RwLock::new(v)));
    }
}

/// Small files kept next to the databases (the embedder's JSON stores):
/// real files natively, entries of the same in-memory store in the browser,
/// so one [snapshot] saves everything.
pub mod files {
    use super::*;

    fn key(path: &Path) -> String {
        format!("file:{}", path.to_string_lossy().replace('\\', "/"))
    }

    fn not_found() -> io::Error {
        io::Error::new(io::ErrorKind::NotFound, "no such file")
    }

    pub fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
        if !in_memory() {
            return std::fs::read(path);
        }
        let store = store().lock().unwrap_or_else(|p| p.into_inner());
        let bytes = store.get(&key(path.as_ref())).ok_or_else(not_found)?;
        let data = bytes.read().unwrap_or_else(|p| p.into_inner()).clone();
        Ok(data)
    }

    pub fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
        String::from_utf8(read(path)?).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn write(path: impl AsRef<Path>, data: impl AsRef<[u8]>) -> io::Result<()> {
        if !in_memory() {
            return std::fs::write(path, data);
        }
        store()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key(path.as_ref()), Arc::new(RwLock::new(data.as_ref().to_vec())));
        GENERATION.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
        if !in_memory() {
            return std::fs::rename(from, to);
        }
        let mut store = store().lock().unwrap_or_else(|p| p.into_inner());
        let bytes = store.remove(&key(from.as_ref())).ok_or_else(not_found)?;
        store.insert(key(to.as_ref()), bytes);
        GENERATION.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn remove(path: impl AsRef<Path>) -> io::Result<()> {
        if !in_memory() {
            return std::fs::remove_file(path);
        }
        store().lock().unwrap_or_else(|p| p.into_inner()).remove(&key(path.as_ref())).ok_or_else(not_found)?;
        GENERATION.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
        if in_memory() {
            return Ok(());
        }
        std::fs::create_dir_all(path)
    }
}

/// Forgets every in-memory database (tests, "sign out").
pub fn clear() {
    store().lock().unwrap_or_else(|p| p.into_inner()).clear();
}

#[derive(Debug)]
struct Memory(Bytes);

impl Memory {
    fn lock_err() -> io::Error {
        io::Error::new(io::ErrorKind::Other, "memory database lock poisoned")
    }
}

impl StorageBackend for Memory {
    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.0.read().map_err(|_| Self::lock_err())?.len() as u64)
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let data = self.0.read().map_err(|_| Self::lock_err())?;
        let start = offset as usize;
        data.get(start..start + len)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "read past the end"))
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.0.write().map_err(|_| Self::lock_err())?.resize(len as usize, 0);
        GENERATION.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn sync_data(&self, _eventual: bool) -> Result<(), io::Error> {
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut bytes = self.0.write().map_err(|_| Self::lock_err())?;
        let start = offset as usize;
        let end = start + data.len();
        if bytes.len() < end {
            bytes.resize(end, 0);
        }
        bytes[start..end].copy_from_slice(data);
        GENERATION.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::TableDefinition;

    const T: TableDefinition<&str, u64> = TableDefinition::new("t");

    #[test]
    fn memory_databases_survive_snapshot_and_restore() {
        use_memory(true);
        let path = Path::new("mem-test/db");
        {
            let db = open(path).unwrap();
            let w = db.begin_write().unwrap();
            w.open_table(T).unwrap().insert("a", 7).unwrap();
            w.commit().unwrap();
        }
        let saved = snapshot();
        clear();
        restore(saved);
        let db = open(path).unwrap();
        let r = db.begin_read().unwrap();
        assert_eq!(r.open_table(T).unwrap().get("a").unwrap().unwrap().value(), 7);
        use_memory(false);
    }
}
