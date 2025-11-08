use std::collections::HashMap;
use crate::types::{Row, Schema, Column, DataType};

pub type Result<T> = std::result::Result<T, String>;

/// In-memory table (TODO temporary)
#[derive(Debug, Clone)]
pub struct Table {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl Table {
    pub fn new(schema: Schema) -> Self {
        Table {
            schema,
            rows: Vec::new(),
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<()> {
        if row.len() != self.schema.len() {
            return Err(format!(
                "Row has {} columns but schema expects {}",
                row.len(),
                self.schema.len()
            ));
        }
        self.rows.push(row);
        Ok(())
    }

    pub fn scan(&self) -> Vec<Row> {
        self.rows.clone()
    }
}

/// In-memory database
pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table already exists: {}", name));
        }
        self.tables.insert(name, Table::new(schema));
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| format!("Table not found: {}", name))
    }

    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut Table> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| format!("Table not found: {}", name))
    }

    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<()> {
        let table = self.get_table_mut(table_name)?;
        table.insert(row)
    }

    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Row>> {
        let table = self.get_table(table_name)?;
        Ok(table.scan())
    }

    pub fn get_schema(&self, table_name: &str) -> Result<Schema> {
        let table = self.get_table(table_name)?;
        Ok(table.schema.clone())
    }
}