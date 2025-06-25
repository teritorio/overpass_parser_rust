use sql_dialect::{postgres::postgres::Postgres, sql_dialect::SqlDialect};
use wasm_bindgen::prelude::*;
pub mod overpass_parser;
use overpass_parser::{parse_query, request::Request};
pub mod sql_dialect;

#[wasm_bindgen]
pub fn parse_query_json(query: &str) -> String {
    match parse_query(query) {
        Ok(request) => Request::to_sql(
            &request,
            &(Box::new(Postgres::default()) as Box<dyn SqlDialect>),
            "4326",
            None,
        ),
        Err(e) => format!("Error parsing query: {}", e),
    }
}
