use pest::iterators::Pair;

use derivative::Derivative;

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::{Rule, query::Query, subrequest::SubrequestJoin};

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct QueryRecurse {
    pub set: Option<Box<str>>,
    pub recurse: Box<str>,
    pub asignation: Option<Box<str>>,
}

impl Query for QueryRecurse {
    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>> {
        let mut query_recurse = QueryRecurse::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::ID => {
                    query_recurse.set = Some(inner_pair.as_str().into());
                }
                Rule::query_recurse => {
                    query_recurse.recurse = inner_pair.as_str().into();
                }
                Rule::asignation => {
                    query_recurse.asignation = Some(
                        inner_pair
                            .into_inner()
                            .find(|p| p.as_rule() == Rule::ID)
                            .map(|p| p.as_str())
                            .unwrap()
                            .into(),
                    )
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!(
                                "Invalid rule {:?} for QueryRecurse",
                                inner_pair.as_rule()
                            ),
                        },
                        inner_pair.as_span(),
                    ));
                }
            }
        }
        Ok(Box::new(query_recurse))
    }

    fn to_sql(
        &self,
        _sql_dialect: &(dyn SqlDialect + Send + Sync),
        _srid: &str,
        default_set: &str,
    ) -> SubrequestJoin {
        let from = if self.set.is_none() {
            default_set
        } else {
            self.set.as_ref().unwrap()
        };

        SubrequestJoin{
            precompute: None,
            from: None,
            clauses: format!("SELECT
    way.*
FROM
    _{from} AS way
    JOIN node_by_id AS node ON
        node.id = ANY(way.nodes)
WHERE
    way.osm_type = 'w'
UNION ALL
SELECT
    node.*
FROM
    _{from} AS relation
    JOIN LATERAL (
        SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'n'
    ) AS members ON
        type = 'w'
    JOIN node_by_id AS node ON
        node.id = members.ref
WHERE
    relation.osm_type = 'r'
UNION ALL
SELECT
    way.*
FROM
    _{from} AS relation
    JOIN LATERAL (
        SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'w'
    ) AS members ON
        true
    JOIN way_by_id AS way ON
        way.id = members.ref
WHERE
    relation.osm_type = 'r'"
           )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overpass_parser::parse_query;
    use crate::overpass_parser::subrequest::{QueryType, SubrequestType};

    use crate::sql_dialect::postgres::postgres::Postgres;
    use pretty_assertions::assert_eq;

    fn parse(query: &str) -> QueryRecurse {
        match parse_query(query) {
            Ok(parsed) => match parsed.subrequest.queries[1].as_ref() {
                SubrequestType::QueryType(query_type) => match query_type {
                    QueryType::QueryRecurse(query_recurse) => query_recurse.clone(),
                    _ => panic!(
                        "Expected QueryRecurse, found {:?}",
                        parsed.subrequest.queries[1]
                    ),
                },
                _ => panic!(
                    "Expected QueryRecurse, found {:?}",
                    parsed.subrequest.queries[1]
                ),
            },
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_matches_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(
            "SELECT
    way.*
FROM
    __ AS way
    JOIN node_by_id AS node ON
        node.id = ANY(way.nodes)
WHERE
    way.osm_type = 'w'
UNION ALL
SELECT
    node.*
FROM
    __ AS relation
    JOIN LATERAL (
        SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'n'
    ) AS members ON
        type = 'w'
    JOIN node_by_id AS node ON
        node.id = members.ref
WHERE
    relation.osm_type = 'r'
UNION ALL
SELECT
    way.*
FROM
    __ AS relation
    JOIN LATERAL (
        SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'w'
    ) AS members ON
        true
    JOIN way_by_id AS way ON
        way.id = members.ref
WHERE
    relation.osm_type = 'r'",
            parse("way;>;")
                .to_sql(d, "4326", "_").clauses)
    }
}
