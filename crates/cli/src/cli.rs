use clap::{Args, Parser, Subcommand};

// Let's start with basic SQL commands first

// SELECT - extracts data from a database
// UPDATE - updates data in a database
// DELETE - deletes data from a database
// INSERT INTO - inserts new data into a database
// CREATE DATABASE - creates a new database
// CREATE TABLE - creates a new table
// DROP TABLE - deletes a table
// CREATE INDEX - creates an index (search key)
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Query records (eventually maps to B+Tree lookups / range scans).
    Select(SelectArgs),
    // Update a record by primary key.
    // Update(UpdateArgs),

    // Good future additions for an OLTP DB CLI:
    // Start(StartArgs),
    // Insert(InsertArgs),
    // Delete(DeleteArgs),
    // CreateTable(CreateTableArgs),
    // Scan(ScanArgs),
}

#[derive(Args, Debug)]
pub struct SelectArgs {
    /// Table name.
    pub table: String,

    /// Primary key lookup (fast path).
    #[arg(long)]
    pub pk: Option<String>,

    /// Optional filter expression (placeholder; replace with structured filters later).
    #[arg(long, value_name = "EXPR")]
    pub r#where: Option<String>,

    /// Limit number of rows returned.
    #[arg(long)]
    pub limit: Option<u32>,
}

#[derive(Parser, Debug)]
#[command(name = "fluxdb", version, about = "FluxDB CLI")]
pub struct Cli {
    /// Address of the FluxDB server.
    #[arg(long, default_value = "127.0.0.1:9000")]
    pub endpoint: String,

    #[command(subcommand)]
    pub command: Command,
}
