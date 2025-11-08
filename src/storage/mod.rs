mod io;
mod base;
mod internal;

use std::collections::HashMap;
use crate::types::{Row, Schema};
use self::internal::DatabaseFile;
use self::base::{SegmentId, Block, TuplePointer};

pub type Result<T> = std::result::Result<T, String>;

/// Table metadata
#[derive(Debug, Clone)]
struct TableMetadata {
    schema: Schema,
    segments: Vec<SegmentId>,
}

/// Database with file-based storage
pub struct Database {
    file: DatabaseFile,
    tables: HashMap<String, TableMetadata>,
    next_segment_id: SegmentId,
}

impl Database {
    pub fn new() -> Self {
        // Temporary: use in-memory file path
        let file = DatabaseFile::open("data.db")
            .expect("Failed to open database file");

        Database {
            file,
            tables: HashMap::new(),
            next_segment_id: 0,
        }
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table already exists: {}", name));
        }

        // Allocate first segment for table
        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        // Initialize segment
        self.file.initialize_segment(segment_id)
            .map_err(|e| format!("Failed to initialize segment: {}", e))?;

        let metadata = TableMetadata {
            schema,
            segments: vec![segment_id],
        };

        self.tables.insert(name, metadata);
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
        let row_bytes = bincode::serialize(&row)
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
                            let row: Row = bincode::deserialize(tuple_bytes)
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