use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};

/// A single column value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

impl Value {
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Int(n) => Some(*n as i32),
            _ => None,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Int(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
        }
    }
}

/// A single row (ordered list of values)
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Row { values }
    }

    pub fn get(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Column metadata
#[derive(Debug, Clone, Encode, Decode)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

/// SQL data types
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum DataType {
    Int,
    Float,
    String,
    Bool,
    Null,
}

/// Table schema
#[derive(Debug, Clone, Encode, Decode)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Schema { columns }
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}