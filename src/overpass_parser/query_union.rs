use crate::sql_dialect::sql_dialect::SqlDialect;
use pest::iterators::Pair;

use derivative::Derivative;
use regex::Regex;

use super::{Rule, query::Query, subrequest::QueryType};

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct QueryUnion {
    pub queries: Vec<Box<QueryType>>,
    #[derivative(Default(
        value = "COUNTER.fetch_add(1, Ordering::SeqCst).to_string().as_str().into()"
    ))]
    pub default_asignation: Box<str>,
    pub asignation: Option<Box<str>>,
}

impl Query for QueryUnion {
    fn default_asignation(&self) -> Option<&str> {
        match self.asignation {
            None => Some(&self.default_asignation),
            _ => None,
        }
    }

    fn asignation(&self) -> &str {
        self.asignation
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or(&self.default_asignation)
    }

    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>> {
        let mut query_union = QueryUnion::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::query_sequence => {
                    for query in inner_pair.into_inner() {
                        match QueryType::from_pest(query) {
                            Ok(query_type) => query_union.queries.push(query_type),
                            Err(e) => return Err(e),
                        }
                    }
                }
                Rule::asignation => {
                    query_union.asignation = Some(
                        inner_pair
                            .into_inner()
                            .find(|p| p.as_rule() == Rule::ID)
                            .map(|p| p.as_str())
                            .unwrap()
                            .into(),
                    );
                }
                _ => panic!("Unexpected rule in QueryUnion: {:?}", inner_pair.as_rule()),
            }
        }
        Ok(Box::new(query_union))
    }

    fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        default_set: &str,
    ) -> String {
        let mut default_set = default_set;
        let replace = Regex::new(r"^").unwrap();

        let with = self
            .queries
            .iter()
            .map(|query| {
                let mut sql = query.to_sql(sql_dialect, srid, default_set);
                sql = replace.replace_all(&sql, "").to_string();
                default_set = query.asignation();
                format!("_{default_set} AS (\n{sql}\n)")
            })
            .collect::<Vec<String>>()
            .join(",\n");

        let asignations = self
            .queries
            .iter()
            .map(|query| format!("(SELECT * FROM _{})", query.asignation()))
            .collect::<Vec<String>>()
            .join(" UNION\n    ");

        format!(
            "WITH
{with}
SELECT DISTINCT ON(osm_type, id)
    *
FROM (
    {asignations}
) AS t
ORDER BY
    osm_type, id"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        overpass_parser::{parse_query, subrequest::SubrequestType},
        sql_dialect::postgres::postgres::Postgres,
    };
    use pretty_assertions::assert_eq;

    fn parse(query: &str) -> QueryUnion {
        match parse_query(query) {
            Ok(parsed) => match parsed.subrequest.queries[0].as_ref() {
                SubrequestType::QueryType(query_type) => match query_type {
                    QueryType::QueryUnion(query_union) => query_union.clone(),
                    _ => panic!(
                        "Expected QueryUnion, found {:?}",
                        parsed.subrequest.queries[1]
                    ),
                },
                _ => panic!(
                    "Expected QueryUnion, found {:?}",
                    parsed.subrequest.queries[0]
                ),
            },
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_matches_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(
            "WITH
_a AS (
SELECT
    *
FROM
    node_by_geom
WHERE
    osm_type = 'n'
),
_b AS (
SELECT
    *
FROM
    way_by_geom
WHERE
    osm_type = 'w'
)
SELECT DISTINCT ON(osm_type, id)
    *
FROM (
    (SELECT * FROM _a) UNION
    (SELECT * FROM _b)
) AS t
ORDER BY
    osm_type, id",
            parse("(node->.a;way->.b;);").to_sql(d, "4326", "_")
        )
    }
}
