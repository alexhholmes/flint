# Extensions

## Initialization flow

```
bin/flint.rs:main()
↓
Server::new(config)
↓
Server::start() → HandlerFactory::new()
↓
HandlerFactory::new() → Executor::new()
↓
Executor::new() → Database::new()  ← EXTENSION REGISTRATION POINT
```

## The Architecture

Based on your current code and requirements, here's the Rust-native, trait-based extension system:
1. Core Extension Traits (in src/extensions/mod.rs)
   use crate::types::{Value, DataType, Schema};
   use crate::storage::base::TuplePointer;
   use std::any::Any;

```rust
/// Type extension trait - allows custom data types
pub trait TypeExtension: Send + Sync {
/// PostgreSQL-compatible type OID
fn type_oid(&self) -> u32;

    /// Type name (e.g., "vector", "jsonb")
    fn type_name(&self) -> &str;
    
    /// Category for type coercion (numeric, string, etc.)
    fn type_category(&self) -> TypeCategory;
    
    /// Serialize extension value to bytes for storage
    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String>;
    
    /// Deserialize bytes back to extension value
    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String>;
    
    /// Convert to pgwire Type for protocol
    fn to_pgwire_type(&self) -> pgwire::api::Type;
}

/// Operator extension trait - custom operators like <-> for vector distance
pub trait OperatorExtension: Send + Sync {
/// Operator symbol (e.g., "<->", "<#>", "@>")
fn operator_symbol(&self) -> &str;

    /// Check if this operator can handle these types
    fn can_handle(&self, left_type: &DataType, right_type: &DataType) -> bool;
    
    /// Execute the operator
    fn execute(&self, left: &Value, right: &Value) -> Result<Value, String>;
    
    /// Return type given input types
    fn return_type(&self, left_type: &DataType, right_type: &DataType) -> DataType;
}

/// Function extension trait - scalar functions like vector_dims()
pub trait FunctionExtension: Send + Sync {
/// Function name
fn name(&self) -> &str;

    /// Execute the function
    fn execute(&self, args: &[Value]) -> Result<Value, String>;
    
    /// Return type given argument types
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, String>;
}

/// Index extension trait - custom index types (HNSW, GIN, etc.)
pub trait IndexExtension: Send + Sync {
/// Index type name (e.g., "hnsw", "ivfflat", "gin")
fn index_type(&self) -> &str;

    /// Insert a key-value pair
    fn insert(&mut self, key: &Value, pointer: TuplePointer) -> Result<(), String>;
    
    /// Standard lookup (for point queries)
    fn search(&self, key: &Value) -> Result<Vec<TuplePointer>, String>;
    
    /// k-NN search (for vector similarity, returns k nearest)
    fn knn_search(&self, query: &Value, k: usize) -> Result<Vec<(TuplePointer, f64)>, String>;
    
    /// Serialize index to bytes for persistence
    fn serialize(&self) -> Result<Vec<u8>, String>;
    
    /// Deserialize index from bytes
    fn deserialize(bytes: &[u8]) -> Result<Box<dyn IndexExtension>, String>
    where
        Self: Sized;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TypeCategory {
Numeric,
String,
Boolean,
Temporal,
Array,
Composite,
Extension,
}
```

2. Registry Pattern (in src/extensions/registry.rs)
   use super::*;
   use std::collections::HashMap;

```rust
/// Type registry manages all type extensions
pub struct TypeRegistry {
types: HashMap<u32, Box<dyn TypeExtension>>,  // OID -> TypeExtension
names: HashMap<String, u32>,                   // name -> OID
}

impl TypeRegistry {
pub fn new() -> Self {
let mut registry = TypeRegistry {
types: HashMap::new(),
names: HashMap::new(),
};

        // Register built-in types
        registry.register_builtin_types();
        
        registry
    }
    
    pub fn register(&mut self, ext: Box<dyn TypeExtension>) {
        let oid = ext.type_oid();
        let name = ext.type_name().to_string();
        
        self.types.insert(oid, ext);
        self.names.insert(name, oid);
    }
    
    pub fn get_by_oid(&self, oid: u32) -> Option<&dyn TypeExtension> {
        self.types.get(&oid).map(|b| &**b)
    }
    
    pub fn get_by_name(&self, name: &str) -> Option<&dyn TypeExtension> {
        self.names.get(name)
            .and_then(|oid| self.types.get(oid))
            .map(|b| &**b)
    }
    
    fn register_builtin_types(&mut self) {
        // Built-in types: Int, Float, String, Bool, Null
        // Each gets a TypeExtension implementation
    }
}

/// Operator registry
pub struct OperatorRegistry {
operators: Vec<Box<dyn OperatorExtension>>,
}

impl OperatorRegistry {
pub fn new() -> Self {
let mut registry = OperatorRegistry {
operators: Vec::new(),
};

        // Register built-in operators (+, -, *, /, =, !=, etc.)
        registry.register_builtin_operators();
        
        registry
    }
    
    pub fn register(&mut self, ext: Box<dyn OperatorExtension>) {
        self.operators.push(ext);
    }
    
    pub fn find(&self, symbol: &str, left: &DataType, right: &DataType) -> Option<&dyn OperatorExtension> {
        self.operators.iter()
            .find(|op| op.operator_symbol() == symbol && op.can_handle(left, right))
            .map(|b| &**b)
    }
}

/// Function registry
pub struct FunctionRegistry {
functions: HashMap<String, Box<dyn FunctionExtension>>,
}

impl FunctionRegistry {
pub fn new() -> Self {
FunctionRegistry {
functions: HashMap::new(),
}
}

    pub fn register(&mut self, ext: Box<dyn FunctionExtension>) {
        self.functions.insert(ext.name().to_string(), ext);
    }
    
    pub fn get(&self, name: &str) -> Option<&dyn FunctionExtension> {
        self.functions.get(name).map(|b| &**b)
    }
}

/// Index builder registry (for CREATE INDEX ... USING hnsw)
pub struct IndexBuilderRegistry {
builders: HashMap<String, Box<dyn Fn() -> Box<dyn IndexExtension>>>,
}

impl IndexBuilderRegistry {
pub fn new() -> Self {
let mut registry = IndexBuilderRegistry {
builders: HashMap::new(),
};

        // Register built-in B-tree
        registry.register_builtin_btree();
        
        registry
    }
    
    pub fn register<F>(&mut self, index_type: &str, builder: F)
    where
        F: Fn() -> Box<dyn IndexExtension> + 'static,
    {
        self.builders.insert(index_type.to_string(), Box::new(builder));
    }
    
    pub fn build(&self, index_type: &str) -> Option<Box<dyn IndexExtension>> {
        self.builders.get(index_type).map(|builder| builder())
    }
    
    fn register_builtin_btree(&mut self) {
        // Register BTree index as default
    }
}
```

3. Database Integration (updated src/storage/mod.rs)
   use crate::extensions::{TypeRegistry, OperatorRegistry, FunctionRegistry, IndexBuilderRegistry};

```rust
pub struct Database {
file: DatabaseFile,
tables: HashMap<String, TableMetadata>,
next_segment_id: SegmentId,
metadata_mgr: MetadataManager,

    // Extension registries
    pub type_registry: Arc<TypeRegistry>,
    pub operator_registry: Arc<OperatorRegistry>,
    pub function_registry: Arc<FunctionRegistry>,
    pub index_builder_registry: Arc<IndexBuilderRegistry>,
}

impl Database {
pub fn new() -> Self {
// Create registries
let mut type_registry = TypeRegistry::new();
let mut operator_registry = OperatorRegistry::new();
let mut function_registry = FunctionRegistry::new();
let mut index_builder_registry = IndexBuilderRegistry::new();

        // Register extensions based on features
        #[cfg(feature = "vector")]
        {
            use crate::contrib::vector;
            vector::register_vector_extension(
                &mut type_registry,
                &mut operator_registry,
                &mut function_registry,
                &mut index_builder_registry,
            );
        }
        
        #[cfg(feature = "json")]
        {
            use crate::contrib::json;
            json::register_json_extension(
                &mut type_registry,
                &mut operator_registry,
                &mut function_registry,
                &mut index_builder_registry,
            );
        }
        
        let file = DatabaseFile::open("data.db")
            .expect("Failed to open database file");

        let mut db = Database {
            file,
            tables: HashMap::new(),
            next_segment_id: 2,
            metadata_mgr: MetadataManager::new(),
            type_registry: Arc::new(type_registry),
            operator_registry: Arc::new(operator_registry),
            function_registry: Arc::new(function_registry),
            index_builder_registry: Arc::new(index_builder_registry),
        };

        let _ = db.load_catalog();
        db
    }
}
```

4. Extension Value Wrapper (updated src/types.rs)
   use std::any::Any;

```rust
#[derive(Debug, Clone)]
pub enum Value {
// Built-in types (fast path)
Int(i64),
Float(f64),
String(String),
Bool(bool),
Null,

    // Extension types (trait object path)
    Extension {
        type_oid: u32,
        data: Arc<dyn Any + Send + Sync>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
// Built-ins
Int,
Float,
String,
Bool,
Null,

    // Extension type reference
    Extension {
        type_oid: u32,
        type_name: String,
    },
}
```

5. Example: contrib/vector (the proof)
   // In contrib/vector/mod.rs

```rust
use crate::extensions::*;
use crate::types::{Value, DataType};

/// Vector type implementation
pub struct VectorValue {
pub dimensions: u16,
pub data: Vec<f32>,
}

/// Vector type extension
pub struct VectorType;

impl TypeExtension for VectorType {
fn type_oid(&self) -> u32 { 16384 }  // Custom OID range
fn type_name(&self) -> &str { "vector" }
fn type_category(&self) -> TypeCategory { TypeCategory::Extension }

    fn serialize(&self, value: &dyn Any) -> Result<Vec<u8>, String> {
        let vec = value.downcast_ref::<VectorValue>()
            .ok_or("Invalid vector value")?;
        
        let mut buf = Vec::with_capacity(2 + vec.data.len() * 4);
        buf.extend_from_slice(&vec.dimensions.to_le_bytes());
        for &f in &vec.data {
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Ok(buf)
    }
    
    fn deserialize(&self, bytes: &[u8]) -> Result<Box<dyn Any>, String> {
        if bytes.len() < 2 {
            return Err("Invalid vector data".into());
        }
        
        let dimensions = u16::from_le_bytes([bytes[0], bytes[1]]);
        let mut data = Vec::with_capacity(dimensions as usize);
        
        for chunk in bytes[2..].chunks_exact(4) {
            let f = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            data.push(f);
        }
        
        Ok(Box::new(VectorValue { dimensions, data }))
    }
    
    fn to_pgwire_type(&self) -> pgwire::api::Type {
        pgwire::api::Type::UNKNOWN  // Or custom type
    }
}

/// Cosine distance operator (<->)
pub struct CosineDistanceOp;

impl OperatorExtension for CosineDistanceOp {
fn operator_symbol(&self) -> &str { "<->" }

    fn can_handle(&self, left: &DataType, right: &DataType) -> bool {
        matches!((left, right), 
            (DataType::Extension { type_oid: 16384, .. }, 
             DataType::Extension { type_oid: 16384, .. }))
    }
    
    fn execute(&self, left: &Value, right: &Value) -> Result<Value, String> {
        // Extract vectors and compute cosine distance
        // ...
        Ok(Value::Float(distance))
    }
    
    fn return_type(&self, _left: &DataType, _right: &DataType) -> DataType {
        DataType::Float
    }
}

/// Registration function called during Database::new()
pub fn register_vector_extension(
types: &mut TypeRegistry,
operators: &mut OperatorRegistry,
functions: &mut FunctionRegistry,
indexes: &mut IndexBuilderRegistry,
) {
// Register vector type
types.register(Box::new(VectorType));

    // Register operators
    operators.register(Box::new(CosineDistanceOp));
    
    // Register HNSW index
    indexes.register("hnsw", || Box::new(HNSWIndex::new()));
}
```

6. Cargo.toml Features

```
   [features]
   default = []
   vector = []
   json = []
   uuid = []
   full_text = []
   geospatial = []
```

Or convenience bundles

```all-extensions = ["vector", "json", "uuid", "full_text", "geospatial"]```

Key Design Decisions
✅ What This Gives You
Built-ins stay fast: Value::Int(42) is stack-allocated, no virtual dispatch
Extensions are type-safe: Compiler checks trait implementations
Zero overhead when unused: Feature flags compile out unused code completely
Single registration point: All extensions register in Database::new()
Composable: Multiple extensions work together through registries
No C FFI: Pure Rust, no dynamic loading nightmares
⚠️ Trade-offs
Extension values use trait objects: Arc<dyn Any> adds heap allocation + vtable dispatch
Downcasting required: Extensions must downcast from &dyn Any (runtime check)
Static linking only: No dynamic .so loading (but this is a FEATURE for simplicity)
🎯 Your Positioning
"Flint extensions are Rust modules compiled with the database. No C FFI, no dynamic loading, no compatibility nightmares. Just features you enable at build time."