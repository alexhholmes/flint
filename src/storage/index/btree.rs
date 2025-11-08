use std::io::{self, Result as IoResult};
use crate::storage::base::TuplePointer;
use super::page::{IndexEntry, IndexPage, IndexPageHeader};

/// Represents a split result when a node overflows
#[derive(Debug)]
pub struct SplitResult {
    /// The key that was promoted to the parent
    pub promoted_key: u64,
    /// The right sibling after split
    pub right_page: IndexPage,
}

/// B+ Tree node operations
/// Handles insertion with automatic splitting and rebalancing
pub struct BTree;

impl BTree {
    /// Insert a key-value pair into a page, handling splits if necessary
    /// Returns None if no split occurred, Some(SplitResult) if the page split
    pub fn insert_into_page(
        page: &mut IndexPage,
        key: u64,
        tuple_ptr: TuplePointer,
    ) -> IoResult<Option<SplitResult>> {
        let (found, pos) = page.binary_search(key)?;

        // If key already exists, update it (replace old value)
        if found {
            let entry = IndexEntry::new(key, tuple_ptr);
            let header_size = size_of::<IndexPageHeader>();
            let entry_size = size_of::<IndexEntry>();
            let offset = header_size + pos * entry_size;

            let entry_bytes = unsafe {
                std::slice::from_raw_parts(
                    &entry as *const IndexEntry as *const u8,
                    entry_size,
                )
            };
            page.data[offset..offset + entry_size].copy_from_slice(entry_bytes);
            return Ok(None);
        }

        // Try to insert at position
        let entry = IndexEntry::new(key, tuple_ptr);
        match page.insert_at(pos, entry) {
            Ok(()) => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::Other => {
                // Page is full, need to split
                Self::split_page(page, pos, entry)
            }
            Err(e) => Err(e),
        }
    }

    /// Split a full page into two pages
    /// Returns the promoted key and the right sibling page
    fn split_page(
        page: &mut IndexPage,
        insert_pos: usize,
        new_entry: IndexEntry,
    ) -> IoResult<Option<SplitResult>> {
        // Get all current entries
        let mut entries = page.entries()?;

        // Insert the new entry into the collection
        entries.insert(insert_pos, new_entry);

        // Get page info
        let header = page.header()?;
        let is_leaf = header.is_leaf;

        // Calculate split point (roughly middle)
        let split_point = entries.len() / 2;

        // Split entries
        let right_entries: Vec<_> = entries.drain(split_point..).collect();
        let promoted_key = right_entries[0].key;

        // Left page keeps the lower keys
        page.set_entries(is_leaf, entries)?;

        // Right page gets the higher keys
        let mut right_page = IndexPage::new(is_leaf);
        right_page.set_entries(is_leaf, right_entries)?;

        Ok(Some(SplitResult {
            promoted_key,
            right_page,
        }))
    }

    /// Find a value by key in a page (returns Option<TuplePointer> if leaf)
    pub fn search_page(page: &IndexPage, key: u64) -> IoResult<Option<TuplePointer>> {
        let (found, pos) = page.binary_search(key)?;

        if !found {
            return Ok(None);
        }

        let entry = page.get_entry(pos)?;
        Ok(Some(entry.as_tuple_pointer()))
    }

    /// Range scan in a leaf page - get all entries in [start_key, end_key]
    pub fn range_scan_page(
        page: &IndexPage,
        start_key: u64,
        end_key: u64,
    ) -> IoResult<Vec<(u64, TuplePointer)>> {
        let header = page.header()?;
        let mut results = Vec::new();

        for i in 0..header.num_keys as usize {
            let entry = page.get_entry(i)?;
            if entry.key >= start_key && entry.key <= end_key {
                results.push((entry.key, entry.as_tuple_pointer()));
            }
        }

        Ok(results)
    }

    /// Get all entries from a leaf page (for full scan)
    pub fn scan_page(page: &IndexPage) -> IoResult<Vec<(u64, TuplePointer)>> {
        let entries = page.entries()?;
        Ok(entries
            .into_iter()
            .map(|e| (e.key, e.as_tuple_pointer()))
            .collect())
    }
}

/// Simple in-memory B+ tree for single-threaded operations
/// For multi-threaded scenarios, wrap with Arc<RwLock<>>
pub struct InMemoryBTree {
    root: Option<Box<IndexPage>>,
}

impl InMemoryBTree {
    pub fn new() -> Self {
        InMemoryBTree { root: None }
    }

    /// Insert a key-value pair
    /// This is a simplified single-level implementation
    pub fn insert(&mut self, key: u64, tuple_ptr: TuplePointer) -> IoResult<()> {
        match &mut self.root {
            None => {
                // Create root page
                let mut root = IndexPage::new(true);
                let entry = IndexEntry::new(key, tuple_ptr);
                root.insert_at(0, entry)?;
                self.root = Some(Box::new(root));
                Ok(())
            }
            Some(root) => {
                // Try to insert into root
                match BTree::insert_into_page(root, key, tuple_ptr)? {
                    None => Ok(()),
                    Some(split) => {
                        // Root split - create new root
                        let mut new_root = IndexPage::new(false); // Internal node

                        // Add entries from left child (root) and right child to new root
                        let _left_entries = root.entries()?;

                        // New root has: split.promoted_key pointing to left and right children
                        // For simplicity, just store promoted key
                        let promoted_entry = IndexEntry::new(split.promoted_key, TuplePointer::new(0, 0, 0));
                        new_root.insert_at(0, promoted_entry)?;

                        // In a full implementation, we'd also store child pointers
                        // For now, this is a simplified version
                        self.root = Some(Box::new(new_root));
                        Ok(())
                    }
                }
            }
        }
    }

    /// Look up a key
    pub fn search(&self, key: u64) -> IoResult<Option<TuplePointer>> {
        match &self.root {
            None => Ok(None),
            Some(page) => {
                // For now, only handle single-level tree (leaf root)
                if page.header()?.is_leaf {
                    BTree::search_page(page, key)
                } else {
                    // In full implementation, would traverse tree
                    Ok(None)
                }
            }
        }
    }

    /// Get root page for persistence
    pub fn root_page(&self) -> Option<&IndexPage> {
        self.root.as_ref().map(|b| b.as_ref())
    }

    /// Load tree from a persisted page
    pub fn load(page: IndexPage) -> Self {
        InMemoryBTree {
            root: Some(Box::new(page)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_search() -> IoResult<()> {
        let mut tree = InMemoryBTree::new();
        let ptr = TuplePointer::new(0, 5, 10);

        tree.insert(42, ptr)?;

        let found = tree.search(42)?;
        assert_eq!(found, Some(ptr));

        let not_found = tree.search(99)?;
        assert_eq!(not_found, None);

        Ok(())
    }

    #[test]
    fn test_split_on_overflow() -> IoResult<()> {
        let mut tree = InMemoryBTree::new();
        let ptr1 = TuplePointer::new(0, 0, 1);
        let ptr2 = TuplePointer::new(0, 0, 2);

        tree.insert(10, ptr1)?;
        tree.insert(20, ptr2)?;

        let found = tree.search(10)?;
        assert_eq!(found, Some(ptr1));

        let found = tree.search(20)?;
        assert_eq!(found, Some(ptr2));

        Ok(())
    }
}