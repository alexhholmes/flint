use std::fs::{File, OpenOptions};
use std::io::{self, Result};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

/// Alignment requirement for Direct I/O (4KB on most systems)
pub const ALIGNMENT: usize = 4096;

pub struct Disk {
    file: File,
}

impl Disk {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Disk> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // Enable Direct I/O (platform-specific)
        #[cfg(target_os = "linux")]
        unsafe {
            let fd = file.as_raw_fd();
            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags == -1 {
                return Err(io::Error::last_os_error());
            }
            if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_DIRECT) == -1 {
                return Err(io::Error::last_os_error());
            }
        }

        #[cfg(target_os = "macos")]
        unsafe {
            let fd = file.as_raw_fd();
            if libc::fcntl(fd, libc::F_NOCACHE, 1) == -1 {
                return Err(io::Error::last_os_error());
            }
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            tracing::warn!("Direct I/O not supported on this platform, using buffered I/O");
        }

        Ok(Disk { file })
    }

    /// Read aligned data at a specific offset
    ///
    /// On Linux, uses O_DIRECT if available. On macOS, uses F_NOCACHE.
    /// Offset and buffer must be aligned to ALIGNMENT.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        // Validate alignment
        if offset as usize % ALIGNMENT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("offset {} not aligned to {}", offset, ALIGNMENT),
            ));
        }
        if buf.len() % ALIGNMENT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("buffer length {} not aligned to {}", buf.len(), ALIGNMENT),
            ));
        }
        if (buf.as_ptr() as usize) % ALIGNMENT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer pointer not aligned",
            ));
        }

        self.file.read_at(buf, offset)
    }

    /// Write aligned data at a specific offset
    ///
    /// On Linux, uses O_DIRECT if available. On macOS, uses F_NOCACHE.
    /// Offset and buffer must be aligned to ALIGNMENT.
    pub fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        // Validate alignment
        if offset as usize % ALIGNMENT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("offset {} not aligned to {}", offset, ALIGNMENT),
            ));
        }
        if buf.len() % ALIGNMENT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("buffer length {} not aligned to {}", buf.len(), ALIGNMENT),
            ));
        }
        if (buf.as_ptr() as usize) % ALIGNMENT != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer pointer not aligned",
            ));
        }

        self.file.write_at(buf, offset)
    }
}

/// Allocate an aligned buffer for Direct I/O
pub fn alloc_aligned(size: usize) -> Vec<u8> {
    // Ensure size is aligned
    let aligned_size = (size + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;

    // Allocate with alignment
    let layout = std::alloc::Layout::from_size_align(aligned_size, ALIGNMENT)
        .expect("invalid layout");

    unsafe {
        let ptr = std::alloc::alloc(layout);
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Vec::from_raw_parts(ptr, aligned_size, aligned_size)
    }
}
