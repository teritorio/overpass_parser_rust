use crate::sql_dialect::sql_dialect::SqlDialect;
use pest::iterators::Pair;

use derivative::Derivative;
use regex::Regex;

use super::{
    Rule,
    query::Query,
    subrequest::{QueryType, SubrequestJoin},
};

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct QueryUnion {
    pub queries: Vec<Box<QueryType>>,
    pub asignation: Option<Box<str>>,
}

impl Query for QueryUnion {
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
    ) -> SubrequestJoin {
        let mut precomputed = Vec::new();
        let mut previous_default_set = default_set.to_string();
        let replace = Regex::new(r"^").unwrap();

        let clauses = self
            .queries
            .iter()
            .map(|query| {
                let sj = query.to_sql(sql_dialect, srid, previous_default_set.as_str());
                precomputed.extend(sj.precompute.unwrap_or_default());
                let set = match query.asignation() {
                    Some(asignation) => asignation.to_string(),
                    None => {
                        previous_default_set = COUNTER.fetch_add(1, Ordering::SeqCst).to_string();
                        previous_default_set.clone()
                    }
                };
                (set, sj.clauses)
            })
            .collect::<Vec<(String, String)>>();

        let with = clauses
            .iter()
            .map(|(set, sql)| format!("_{set} AS (\n{}\n)", replace.replace_all(sql, "")))
            .collect::<Vec<String>>()
            .join(",\n");

        let asignations = clauses
            .iter()
            .map(|(set, _sql)| format!("(SELECT * FROM _{set})"))
            .collect::<Vec<String>>()
            .join(" UNION\n    ");

        SubrequestJoin {
            precompute: Some(precomputed),
            from: None,
            clauses: format!(
                "WITH
{with}
SELECT DISTINCT ON(osm_type, id)
    *
FROM (
    {asignations}
) AS t
ORDER BY
    osm_type, id"
            ),
        }
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
            parse("(node->.a;way->.b;);").to_sql(d, "4326", "_").clauses
        )
    }
}
