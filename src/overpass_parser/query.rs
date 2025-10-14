use crate::sql_dialect::sql_dialect::SqlDialect;
use pest::iterators::Pair;

use super::{Rule, subrequest::SubrequestJoin};

pub trait Query {
    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>>;

    fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        default_set: &str,
    ) -> Vec<SubrequestJoin>;
}
