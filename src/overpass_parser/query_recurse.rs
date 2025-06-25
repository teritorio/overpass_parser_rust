use pest::iterators::Pair;

use derivative::Derivative;

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::{Rule, query::Query};

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct QueryRecurse {
    pub recurse: Box<str>,
    #[derivative(Default(
        value = "COUNTER.fetch_add(1, Ordering::SeqCst).to_string().as_str().into()"
    ))]
    pub asignation: Box<str>,
}

impl Query for QueryRecurse {
    fn asignation(&self) -> &str {
        self.asignation.as_ref()
    }

    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>> {
        let mut query_recurse = QueryRecurse::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::query_recurse => {
                    query_recurse.recurse = inner_pair.as_str().into();
                }
                Rule::asignation => {
                    query_recurse.asignation = inner_pair
                        .into_inner()
                        .find(|p| p.as_rule() == Rule::ID)
                        .map(|p| p.as_str())
                        .unwrap()
                        .into()
                }
                _ => {}
            }
        }
        Ok(Box::new(query_recurse))
    }

    fn to_sql(&self, _sql_dialect: &Box<dyn SqlDialect>, _srid: &str, default_set: &str) -> String {
        format!("SELECT
    way.*
FROM
    {default_set} AS way
    JOIN node ON
        node.id = ANY(way.nodes) AND
        node.geom && way.geom
WHERE
    way.osm_type = 'w'
UNION ALL
SELECT
    node.*
FROM
    {default_set} AS relation
    JOIN LATERAL (
        SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'n'
    ) AS members ON
        type = 'w'
    JOIN node ON
        node.id = members.ref
WHERE
    relation.osm_type = 'r'
UNION ALL
SELECT
    way.*
FROM
    {default_set} AS relation
    JOIN LATERAL (
        SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'w'
    ) AS members ON
        true
    JOIN way ON
        way.id = members.ref
WHERE
    relation.osm_type = 'r'"
)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overpass_parser::parse_query;
    use crate::overpass_parser::request::QueryType;
    
    use crate::sql_dialect::postgres::postgres::Postgres;
    use pretty_assertions::assert_eq;

    fn parse(query: &str) -> QueryRecurse {
        match parse_query(query) {
            Ok(parsed) => match parsed.queries[1].as_ref() {
                QueryType::QueryRecurse(query_recurse) => query_recurse.clone(),
                _ => panic!("Expected QueryRecurse, found {:?}", parsed.queries[1]),
            },
            Err(e) => panic!("Failed to parse query: {}", e),
        }
    }

    #[test]
    fn test_matches_to_sql() {
        let d = Box::new(Postgres::default()) as Box<dyn SqlDialect>;

        assert_eq!(
            "SELECT
    way.*
FROM
    _ AS way
    JOIN node ON
        node.id = ANY(way.nodes) AND
        node.geom && way.geom
WHERE
    way.osm_type = 'w'
UNION ALL
SELECT
    node.*
FROM
    _ AS relation
    JOIN LATERAL (
        SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'n'
    ) AS members ON
        type = 'w'
    JOIN node ON
        node.id = members.ref
WHERE
    relation.osm_type = 'r'
UNION ALL
SELECT
    way.*
FROM
    _ AS relation
    JOIN LATERAL (
        SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'w'
    ) AS members ON
        true
    JOIN way ON
        way.id = members.ref
WHERE
    relation.osm_type = 'r'",
            parse("way;>;")
                .to_sql(&d, "4326", "_"))
    }
}
