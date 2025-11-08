mod io;
mod base;
mod internal;

use std::collections::HashMap;
use crate::types::{Row, Schema};
use self::internal::DatabaseFile;
use self::base::SegmentId;
use bincode::{Encode, Decode};

pub type Result<T> = std::result::Result<T, String>;

/// Catalog header for metadata persistence
#[derive(Debug, Clone, Encode, Decode)]
pub struct CatalogHeader {
    pub catalog_version: u32,
    pub num_tables: u32,
    pub table_offsets: Vec<(String, u32)>, // (table_name, byte_offset)
    pub checksum: u64,
}

impl CatalogHeader {
    pub fn new() -> Self {
        CatalogHeader {
            catalog_version: 1,
            num_tables: 0,
            table_offsets: Vec::new(),
            checksum: 0,
        }
    }
}

/// Table metadata
#[derive(Debug, Clone, Encode, Decode)]
pub struct TableMetadata {
    pub schema: Schema,
    pub segments: Vec<SegmentId>,
}

/// Database with file-based storage
pub struct Database {
    file: DatabaseFile,
    tables: HashMap<String, TableMetadata>,
    next_segment_id: SegmentId,
}

impl Database {
    pub fn new() -> Self {
        let file = DatabaseFile::open("data.db")
            .expect("Failed to open database file");

        let mut db = Database {
            file,
            tables: HashMap::new(),
            next_segment_id: 1, // segment 0 reserved for metadata
        };

        // Try to load existing metadata from segment 0
        let _ = db.load_catalog();

        db
    }

    /// Load catalog from segment 0
    fn load_catalog(&mut self) -> Result<()> {
        // Read segment 0 header block
        let header_block = self.file.read_block(0, 0)
            .map_err(|e| format!("Failed to read catalog header: {}", e))?;

        // Deserialize catalog header from first part of block
        let (catalog, bytes_read): (CatalogHeader, usize) = bincode::decode_from_slice(&header_block.data, bincode::config::standard())
            .map_err(|e| format!("Failed to decode catalog header: {}", e))?;

        // Load each table metadata
        let mut offset = bytes_read;
        for (table_name, _) in &catalog.table_offsets {
            // Decode table metadata from segment 0 data area
            let metadata_bytes = &header_block.data[offset..];
            let (metadata, bytes_read): (TableMetadata, usize) = bincode::decode_from_slice(metadata_bytes, bincode::config::standard())
                .map_err(|e| format!("Failed to decode table {}: {}", table_name, e))?;

            self.tables.insert(table_name.clone(), metadata);
            offset += bytes_read;

            // Update next_segment_id based on highest segment seen
            if let Some(table) = self.tables.get(table_name) {
                if let Some(&max_seg) = table.segments.iter().max() {
                    self.next_segment_id = self.next_segment_id.max(max_seg + 1);
                }
            }
        }

        Ok(())
    }

    /// Check if catalog would fit in a block with current tables
    fn catalog_fits(&self) -> Result<()> {
        let mut catalog = CatalogHeader::new();
        catalog.num_tables = self.tables.len() as u32;

        let mut metadata_bytes = Vec::new();
        let mut offsets = Vec::new();

        for (table_name, table_meta) in &self.tables {
            offsets.push((table_name.clone(), metadata_bytes.len() as u32));
            let encoded = bincode::encode_to_vec(table_meta, bincode::config::standard())
                .map_err(|e| format!("Failed to encode table {}: {}", table_name, e))?;
            metadata_bytes.extend_from_slice(&encoded);
        }
        catalog.table_offsets = offsets;

        let header_bytes = bincode::encode_to_vec(&catalog, bincode::config::standard())
            .map_err(|e| format!("Failed to encode catalog: {}", e))?;

        if header_bytes.len() + metadata_bytes.len() > base::BLOCK_SIZE {
            return Err(format!("Catalog too large: {} bytes (max {})",
                header_bytes.len() + metadata_bytes.len(), base::BLOCK_SIZE));
        }

        Ok(())
    }

    /// Save catalog to segment 0
    fn save_catalog(&mut self) -> Result<()> {
        // Build catalog header
        let mut catalog = CatalogHeader::new();
        catalog.num_tables = self.tables.len() as u32;

        // Serialize all tables into a buffer
        let mut metadata_bytes = Vec::new();
        let mut offsets = Vec::new();

        for (table_name, table_meta) in &self.tables {
            offsets.push((table_name.clone(), metadata_bytes.len() as u32));
            let encoded = bincode::encode_to_vec(table_meta, bincode::config::standard())
                .map_err(|e| format!("Failed to encode table {}: {}", table_name, e))?;
            metadata_bytes.extend_from_slice(&encoded);
        }
        catalog.table_offsets = offsets;

        // Encode catalog header
        let header_bytes = bincode::encode_to_vec(&catalog, bincode::config::standard())
            .map_err(|e| format!("Failed to encode catalog: {}", e))?;

        // Write to segment 0
        let mut block = self.file.read_block(0, 0)
            .map_err(|e| format!("Failed to read block 0,0: {}", e))?;

        // Copy header and metadata into block
        if header_bytes.len() + metadata_bytes.len() > base::BLOCK_SIZE {
            return Err(format!("Catalog too large: {} bytes (max {})",
                header_bytes.len() + metadata_bytes.len(), base::BLOCK_SIZE));
        }

        // Clear block and write new data
        block.data.fill(0);
        block.data[..header_bytes.len()].copy_from_slice(&header_bytes);
        block.data[header_bytes.len()..header_bytes.len() + metadata_bytes.len()].copy_from_slice(&metadata_bytes);

        self.file.write_block(0, 0, &block)
            .map_err(|e| format!("Failed to write catalog: {}", e))?;

        Ok(())
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table already exists: {}", name));
        }

        // Pre-flight: check if new table would fit in catalog before modifying state
        let segment_id = self.next_segment_id;
        let metadata = TableMetadata {
            schema,
            segments: vec![segment_id],
        };

        // Temporarily insert to check if catalog fits
        self.tables.insert(name.clone(), metadata.clone());
        if let Err(e) = self.catalog_fits() {
            // Rollback on catalog size check failure
            self.tables.remove(&name);
            return Err(e);
        }

        // Now proceed with actual initialization
        self.next_segment_id += 1;

        // Initialize segment
        self.file.initialize_segment(segment_id)
            .map_err(|e| {
                // Rollback on initialization failure
                self.tables.remove(&name);
                format!("Failed to initialize segment: {}", e)
            })?;

        // Persist catalog to segment 0
        self.save_catalog()
            .map_err(|e| {
                // Rollback on persist failure
                self.tables.remove(&name);
                self.next_segment_id -= 1;
                e
            })?;

        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<&TableMetadata> {
        self.tables
            .get(name)
            .ok_or_else(|| format!("Table not found: {}", name))
    }

    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<()> {
        let metadata = self.tables.get(table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?;

        // Validate row against schema
        if row.len() != metadata.schema.len() {
            return Err(format!(
                "Row has {} columns but schema expects {}",
                row.len(),
                metadata.schema.len()
            ));
        }

        // Serialize row to bytes (simplified - TODO: proper serialization)
        let row_bytes = bincode::encode_to_vec(&row, bincode::config::standard())
            .map_err(|e| format!("Serialization error: {}", e))?;

        // Find segment with space
        let segment_id = *metadata.segments.last()
            .ok_or_else(|| "No segments for table".to_string())?;

        // Try to allocate block in segment
        let block_id = self.file.allocate_block(segment_id)
            .map_err(|e| format!("Failed to allocate block: {}", e))?
            .ok_or_else(|| "Segment full - need to allocate new segment".to_string())?;

        // Read block, append tuple, write back
        let mut block = self.file.read_block(segment_id, block_id)
            .map_err(|e| format!("Failed to read block: {}", e))?;

        block.append_tuple(&row_bytes)
            .ok_or_else(|| "Block full".to_string())?;

        self.file.write_block(segment_id, block_id, &block)
            .map_err(|e| format!("Failed to write block: {}", e))?;

        Ok(())
    }

    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Row>> {
        let metadata = self.get_table(table_name)?;

        let mut rows = Vec::new();

        // Scan all segments for table
        for &segment_id in &metadata.segments {
            let header = self.file.read_segment_header(segment_id)
                .map_err(|e| format!("Failed to read segment header: {}", e))?;

            // Scan all used blocks
            for block_id in 0..base::BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
                if !header.is_block_free(block_id) {
                    let block = self.file.read_block(segment_id, block_id)
                        .map_err(|e| format!("Failed to read block: {}", e))?;

                    // Read all slots in block
                    let slot_count = block.header().slot_count;
                    for slot_id in 0..slot_count {
                        if let Some(tuple_bytes) = block.read_tuple(slot_id) {
                            let (row, _): (Row, usize) = bincode::decode_from_slice(tuple_bytes, bincode::config::standard())
                                .map_err(|e| format!("Deserialization error: {}", e))?;
                            rows.push(row);
                        }
                    }
                }
            }
        }

        Ok(rows)
    }

    pub fn get_schema(&self, table_name: &str) -> Result<Schema> {
        let metadata = self.get_table(table_name)?;
        Ok(metadata.schema.clone())
    }
}