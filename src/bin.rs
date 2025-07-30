pub mod overpass_parser;
use std::io;

use overpass_parser::{parse_query, request::Request};
use sql_dialect::sql_dialect::SqlDialect;
pub mod sql_dialect;

pub fn main() {
    let dialect = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "postgres".to_string());
    let sql_dialect: &(dyn SqlDialect + Send + Sync) = match dialect.as_str() {
        "postgres" => &sql_dialect::postgres::postgres::Postgres::default(),
        "duckdb" => &sql_dialect::duckdb::duckdb::Duckdb,
        _ => panic!("Unsupported SQL dialect: {dialect}"),
    };

    // read stdin
    match io::read_to_string(io::stdin()).ok() {
        Some(query0) => {
            let query = query0.as_str();
            let out = match parse_query(query) {
                Ok(request) => Request::to_sql(&request, sql_dialect, "4326", None),
                Err(e) => panic!("Error parsing query: {e}"),
            };
            println!("{out}");
        }
        None => {
            eprintln!("Failed to read from stdin");
        }
    }
}
