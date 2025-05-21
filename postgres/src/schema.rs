use std::fmt;

use pg_escape::quote_identifier;
use tokio_postgres::types::Type;

/// A fully qualified PostgreSQL table name consisting of a schema and table name.
///
/// This type represents a table identifier in PostgreSQL, which requires both a schema name
/// and a table name. It provides methods for formatting the name in different contexts.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableName {
    /// The schema name containing the table
    pub schema: String,
    /// The name of the table within the schema
    pub name: String,
}

impl TableName {
    /// Returns the table name as a properly quoted PostgreSQL identifier.
    ///
    /// This method ensures the schema and table names are properly escaped according to
    /// PostgreSQL identifier quoting rules.
    pub fn as_quoted_identifier(&self) -> String {
        let quoted_schema = quote_identifier(&self.schema);
        let quoted_name = quote_identifier(&self.name);
        format!("{quoted_schema}.{quoted_name}")
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{0}.{1}", self.schema, self.name))
    }
}

/// A type alias for PostgreSQL type modifiers.
///
/// Type modifiers in PostgreSQL are used to specify additional type-specific attributes,
/// such as length for varchar or precision for numeric types.
type TypeModifier = i32;

/// Represents the schema of a single column in a PostgreSQL table.
///
/// This type contains all metadata about a column including its name, data type,
/// type modifier, nullability, and whether it's part of the primary key.
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// The name of the column
    pub name: String,
    /// The PostgreSQL data type of the column
    pub typ: Type,
    /// Type-specific modifier value (e.g., length for varchar)
    pub modifier: TypeModifier,
    /// Whether the column can contain NULL values
    pub nullable: bool,
    /// Whether the column is part of the table's primary key
    pub primary: bool,
}

/// A type alias for PostgreSQL table OIDs.
///
/// Table OIDs are unique identifiers assigned to tables in PostgreSQL.
pub type TableId = u32;

/// Represents the complete schema of a PostgreSQL table.
///
/// This type contains all metadata about a table including its name, OID,
/// and the schemas of all its columns.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// The fully qualified name of the table
    pub table_name: TableName,
    /// The PostgreSQL OID of the table
    pub table_id: TableId,
    /// The schemas of all columns in the table
    pub column_schemas: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Returns whether the table has any primary key columns.
    ///
    /// This method checks if any column in the table is marked as part of the primary key.
    pub fn has_primary_keys(&self) -> bool {
        self.column_schemas.iter().any(|cs| cs.primary)
    }
}
