//! The Disk Manager provides a layer of abstraction between the disk
//! and the rest of the database functionality, managing page-level I/O.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use common::DiskError;

/// Represents a unique identifier for a page in the database.
pub type PageId = u64;

pub type Result<T> = std::result::Result<T, DiskError>;

/// Disk manager for a single database file with fixed-size pages.
///
/// This layer handles low-level page I/O, ensuring that data is correctly
/// read from and written to the underlying storage medium.
pub struct DiskManager {
    _db_path: PathBuf,
    file: File,
    page_size: usize,
    #[cfg(test)]
    sync_data_calls: AtomicUsize,
}

impl DiskManager {
    /// Opens or creates the database file at the specified path.
    pub fn new(path: impl AsRef<Path>, page_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // make .write implicit behavior explicit
            .truncate(false)
            .open(&path)?;

        Ok(Self {
            _db_path: path.as_ref().to_path_buf(),
            file,
            page_size,
            #[cfg(test)]
            sync_data_calls: AtomicUsize::new(0),
        })
    }

    /// Returns the number of pages currently in the database file.
    pub fn num_pages(&self) -> Result<u64> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len() / self.page_size as u64)
    }

    /// Reads a page from the database file into the provided buffer.
    ///
    /// The buffer must be pre-allocated and its length must exactly match
    /// the `page_size` configured for this `DiskManager`.
    ///
    /// # Errors
    ///
    /// * Returns [`DiskError::InvalidPageSize`] if the buffer length does not
    ///   match the configured page size.
    /// * Returns [`DiskError::Io`] if an I/O error occurs during reading.
    pub fn read_page(&self, page_id: PageId, buffer: &mut [u8]) -> Result<()> {
        if buffer.len() != self.page_size {
            return Err(DiskError::InvalidPageSize);
        }

        let offset = page_id * self.page_size as u64;

        #[cfg(unix)]
        self.file.read_exact_at(buffer, offset)?;
        #[cfg(not(unix))]
        {
            // this is not as threadsafe as pread
            use std::io::{Read, Seek, SeekFrom};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(buffer)?;
        }
        Ok(())
    }

    /// Writes a page from the provided data buffer to the database file.
    ///
    /// The data buffer's length must exactly match the `page_size` configured
    /// for this `DiskManager`. The write is not guaranteed to be durable until
    /// [`DiskManager::sync_data`] is called.
    ///
    /// # Errors
    ///
    /// * Returns [`DiskError::InvalidPageSize`] if the data buffer length does not
    ///   match the configured page size.
    /// * Returns [`DiskError::Io`] if an I/O error occurs during writing.
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        if data.len() != self.page_size {
            return Err(DiskError::InvalidPageSize);
        }
        let offset = page_id * self.page_size as u64;

        #[cfg(unix)]
        self.file.write_all_at(data, offset)?;
        #[cfg(not(unix))]
        {
            // this is not even as threadsafe as pwrite
            use std::io::{Seek, SeekFrom};
            let mut file = &self.file;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(data)?;
        }
        Ok(())
    }

    /// Flushes file data to disk (equivalent to `fdatasync`).
    pub fn sync_data(&self) -> Result<()> {
        #[cfg(test)]
        self.sync_data_calls.fetch_add(1, Ordering::Relaxed);
        self.file.sync_data()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn sync_data_call_count(&self) -> usize {
        self.sync_data_calls.load(Ordering::Relaxed)
    }

    /// Performs an atomic write for small "whole file" updates, such as catalogs or manifests.
    ///
    /// The process involves:
    /// 1. Writing to a temporary file.
    /// 2. Syncing the temporary file to disk.
    /// 3. Renaming the temporary file to the destination path (atomic).
    /// 4. Syncing the parent directory to ensure the metadata update is durable.
    pub fn atomic_write_file(&self, path: &Path, data: &[u8]) -> Result<()> {
        let temp_path = path.with_extension("tmp");

        let mut f = File::create(&temp_path)?;
        f.write_all(data)?;

        f.sync_all()?;
        drop(f);

        fs::rename(&temp_path, path)?;

        if let Some(parent) = path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        };

        Ok(())
    }

    /// Flushes both the file and its parent directory to ensure metadata
    /// (such as rename or creation) is durable.
    pub fn sync_file_and_dir(file: &File, file_path: &Path) -> Result<()> {
        file.sync_all()?;
        if let Some(parent) = file_path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const PAGE_SIZE: usize = 4096;

    fn make_manager(name: &str) -> (DiskManager, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join(name);
        let dm = DiskManager::new(&path, PAGE_SIZE).unwrap();
        (dm, dir)
    }

    #[test]
    fn write_and_read_page_roundtrip() {
        let (dm, _dir) = make_manager("db.bin");
        let data: Vec<u8> = (0..PAGE_SIZE).map(|i| (i % 256) as u8).collect();

        dm.write_page(0, &data).unwrap();

        let mut buf = vec![0u8; PAGE_SIZE];
        dm.read_page(0, &mut buf).unwrap();

        assert_eq!(buf, data);
    }

    #[test]
    fn multiple_pages_are_independent() {
        let (dm, _dir) = make_manager("db.bin");
        let page0 = vec![1u8; PAGE_SIZE];
        let page1 = vec![2u8; PAGE_SIZE];

        dm.write_page(0, &page0).unwrap();
        dm.write_page(1, &page1).unwrap();

        let mut buf = vec![0u8; PAGE_SIZE];
        dm.read_page(0, &mut buf).unwrap();
        assert_eq!(buf, page0);

        dm.read_page(1, &mut buf).unwrap();
        assert_eq!(buf, page1);
    }

    #[test]
    fn write_rejects_wrong_size_buffer() {
        let (dm, _dir) = make_manager("db.bin");
        let bad = vec![0u8; PAGE_SIZE - 1];
        assert!(matches!(
            dm.write_page(0, &bad),
            Err(DiskError::InvalidPageSize)
        ));
    }

    #[test]
    fn read_rejects_wrong_size_buffer() {
        let (dm, _dir) = make_manager("db.bin");
        let data = vec![0u8; PAGE_SIZE];
        dm.write_page(0, &data).unwrap();

        let mut bad = vec![0u8; PAGE_SIZE + 1];
        assert!(matches!(
            dm.read_page(0, &mut bad),
            Err(DiskError::InvalidPageSize)
        ));
    }

    #[test]
    fn overwrite_page_reflects_new_data() {
        let (dm, _dir) = make_manager("db.bin");
        let first = vec![0xAAu8; PAGE_SIZE];
        let second = vec![0xBBu8; PAGE_SIZE];

        dm.write_page(0, &first).unwrap();
        dm.write_page(0, &second).unwrap();

        let mut buf = vec![0u8; PAGE_SIZE];
        dm.read_page(0, &mut buf).unwrap();
        assert_eq!(buf, second);
    }

    #[test]
    fn atomic_write_file_creates_and_replaces() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("db.bin");
        let dm = DiskManager::new(&db_path, PAGE_SIZE).unwrap();

        let target = dir.path().join("manifest.bin");
        let v1 = b"version1";
        dm.atomic_write_file(&target, v1).unwrap();
        assert_eq!(fs::read(&target).unwrap(), v1);

        let v2 = b"version2";
        dm.atomic_write_file(&target, v2).unwrap();
        assert_eq!(fs::read(&target).unwrap(), v2);

        // temp file must not linger
        assert!(!target.with_extension("tmp").exists());
    }

    #[test]
    fn sync_data_does_not_error() {
        let (dm, _dir) = make_manager("db.bin");
        let data = vec![0u8; PAGE_SIZE];
        dm.write_page(0, &data).unwrap();
        assert!(dm.sync_data().is_ok());
    }
}
