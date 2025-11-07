use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::debug;

use crate::executor::error::ExecutorError;

pub fn parse(query: &str) -> Result<Vec<Statement>, ExecutorError> {
    let dialect = PostgreSqlDialect {};
    debug!(query_len = query.len(), "parsing SQL");

    Parser::parse_sql(&dialect, query)
        .map_err(|e| {
            debug!(error = %e, "parse failed");
            ExecutorError::Parse(format!("Parse error: {}", e))
        })
}