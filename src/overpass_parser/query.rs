use crate::sql_dialect::sql_dialect::SqlDialect;
use pest::iterators::Pair;

use super::Rule;

pub trait Query {
    fn asignation(&self) -> &str;

    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>>;

    fn to_sql(&self, sql_dialect: &Box<dyn SqlDialect>, srid: &str, default_set: &str) -> String;
}
