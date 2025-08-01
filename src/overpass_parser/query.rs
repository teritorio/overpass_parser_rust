use crate::sql_dialect::sql_dialect::SqlDialect;
use pest::iterators::Pair;

use super::Rule;

pub trait Query {
    // Returns the default asignation for the query
    // Return None, when explicit asignation is declared
    fn default_asignation(&self) -> Option<&str>;

    fn asignation(&self) -> &str;

    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>>;

    fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        default_set: &str,
    ) -> String;
}
