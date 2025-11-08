use std::io::{self, Result};
use crate::storage::base::TuplePointer;
use bincode::{Encode, Decode};

/// Index page size (4KB)
pub const INDEX_PAGE_SIZE: usize = 4096;

/// Index page header (64 bytes)
#[repr(C)]
#[derive(Debug, Clone, Copy, Encode, Decode)]
pub struct IndexPageHeader {
    /// Magic number for validation
    pub magic: u32,
    /// True if this is a leaf node
    pub is_leaf: bool,
    /// Number of keys in this node
    pub num_keys: u16,
    /// Padding to reach 64 bytes
    pub _reserved: [u8; 57],
}

impl IndexPageHeader {
    const MAGIC: u32 = 0x494E4458; // "INDX"

    pub fn new(is_leaf: bool) -> Self {
        IndexPageHeader {
            magic: Self::MAGIC,
            is_leaf,
            num_keys: 0,
            _reserved: [0; 57],
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != Self::MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid index page magic",
            ));
        }
        Ok(())
    }
}

/// Single entry in index page: key (u64) + pointer (TuplePointer, 7 bytes)
/// Total: 15 bytes per entry
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct IndexEntry {
    pub key: u64,
    pub segment_id: u32,
    pub block_id: u8,
    pub slot_id: u16,
}

impl IndexEntry {
    pub fn new(key: u64, ptr: TuplePointer) -> Self {
        IndexEntry {
            key,
            segment_id: ptr.segment_id,
            block_id: ptr.block_id,
            slot_id: ptr.slot_id,
        }
    }

    pub fn as_tuple_pointer(&self) -> TuplePointer {
        TuplePointer {
            segment_id: self.segment_id,
            block_id: self.block_id,
            slot_id: self.slot_id,
        }
    }
}

/// Index page (4KB in-memory buffer)
#[derive(Debug)]
pub struct IndexPage {
    pub data: Vec<u8>,
}

impl IndexPage {
    /// Create new empty index page
    pub fn new(is_leaf: bool) -> Self {
        let mut data = vec![0u8; INDEX_PAGE_SIZE];
        let header = IndexPageHeader::new(is_leaf);

        // Write header at offset 0
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                &header as *const IndexPageHeader as *const u8,
                std::mem::size_of::<IndexPageHeader>(),
            )
        };
        data[..header_bytes.len()].copy_from_slice(header_bytes);

        IndexPage { data }
    }

    /// Read header from page
    pub fn header(&self) -> io::Result<IndexPageHeader> {
        if self.data.len() < std::mem::size_of::<IndexPageHeader>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Index page too small",
            ));
        }

        let header = unsafe {
            std::ptr::read(self.data.as_ptr() as *const IndexPageHeader)
        };
        header.validate()?;
        Ok(header)
    }

    /// Write header to page
    fn write_header(&mut self, header: &IndexPageHeader) -> io::Result<()> {
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                header as *const IndexPageHeader as *const u8,
                std::mem::size_of::<IndexPageHeader>(),
            )
        };
        self.data[..header_bytes.len()].copy_from_slice(header_bytes);
        Ok(())
    }

    /// Calculate maximum entries per page
    /// (INDEX_PAGE_SIZE - header) / entry_size = (4096 - 64) / 15 ≈ 269
    pub fn max_entries() -> usize {
        (INDEX_PAGE_SIZE - std::mem::size_of::<IndexPageHeader>()) / std::mem::size_of::<IndexEntry>()
    }

    /// Get entry at position
    pub fn get_entry(&self, pos: usize) -> io::Result<IndexEntry> {
        let header = self.header()?;
        if pos >= header.num_keys as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Entry index {} out of range ({})", pos, header.num_keys),
            ));
        }

        let offset = std::mem::size_of::<IndexPageHeader>() + pos * std::mem::size_of::<IndexEntry>();
        if offset + std::mem::size_of::<IndexEntry>() > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Entry offset out of bounds",
            ));
        }

        let entry = unsafe {
            std::ptr::read(self.data.as_ptr().add(offset) as *const IndexEntry)
        };
        Ok(entry)
    }

    /// Binary search for key position
    /// Returns: (found, position_to_insert)
    pub fn binary_search(&self, key: u64) -> io::Result<(bool, usize)> {
        let header = self.header()?;
        let count = header.num_keys as usize;

        if count == 0 {
            return Ok((false, 0));
        }

        let mut left = 0;
        let mut right = count;

        while left < right {
            let mid = (left + right) / 2;
            let mid_entry = self.get_entry(mid)?;

            if mid_entry.key == key {
                return Ok((true, mid));
            } else if mid_entry.key < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        Ok((false, left))
    }

    /// Insert entry at position (shifts others right)
    /// Returns error if page is full
    pub fn insert_at(&mut self, pos: usize, entry: IndexEntry) -> io::Result<()> {
        let mut header = self.header()?;
        let max = Self::max_entries();

        if header.num_keys as usize >= max {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Index page full",
            ));
        }

        if pos > header.num_keys as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Insert position out of range",
            ));
        }

        // Shift entries right
        let header_size = std::mem::size_of::<IndexPageHeader>();
        let entry_size = std::mem::size_of::<IndexEntry>();
        let count = header.num_keys as usize;

        for i in (pos..count).rev() {
            let src_offset = header_size + i * entry_size;
            let dst_offset = header_size + (i + 1) * entry_size;
            self.data.copy_within(src_offset..src_offset + entry_size, dst_offset);
        }

        // Write new entry
        let offset = header_size + pos * entry_size;
        let entry_bytes = unsafe {
            std::slice::from_raw_parts(
                &entry as *const IndexEntry as *const u8,
                entry_size,
            )
        };
        self.data[offset..offset + entry_size].copy_from_slice(entry_bytes);

        // Update header
        header.num_keys += 1;
        self.write_header(&header)?;

        Ok(())
    }

    /// Get all entries (for splitting)
    pub fn entries(&self) -> io::Result<Vec<IndexEntry>> {
        let header = self.header()?;
        let mut result = Vec::new();

        for i in 0..header.num_keys as usize {
            result.push(self.get_entry(i)?);
        }

        Ok(result)
    }

    /// Clear page and set new entries
    pub fn set_entries(&mut self, is_leaf: bool, entries: Vec<IndexEntry>) -> io::Result<()> {
        if entries.len() > Self::max_entries() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Too many entries for page",
            ));
        }

        let mut header = IndexPageHeader::new(is_leaf);
        header.num_keys = entries.len() as u16;

        self.data.fill(0);
        self.write_header(&header)?;

        let header_size = std::mem::size_of::<IndexPageHeader>();
        let entry_size = std::mem::size_of::<IndexEntry>();

        for (i, entry) in entries.iter().enumerate() {
            let offset = header_size + i * entry_size;
            let entry_bytes = unsafe {
                std::slice::from_raw_parts(
                    entry as *const IndexEntry as *const u8,
                    entry_size,
                )
            };
            self.data[offset..offset + entry_size].copy_from_slice(entry_bytes);
        }

        Ok(())
    }
}