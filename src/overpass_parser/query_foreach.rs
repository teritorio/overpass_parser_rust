use pest::iterators::Pair;
use regex::Regex;

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::{
    Rule,
    query::Query,
    subrequest::{Subrequest, SubrequestJoin},
};

#[derive(Debug, Clone)]
pub struct QueryForeach {
    pub input_set: Option<Box<str>>,
    pub loop_var: Option<Box<str>>,
    pub body: Subrequest,
}

impl Query for QueryForeach {
    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>> {
        let mut query_foreach = QueryForeach {
            input_set: None,
            loop_var: None,
            body: Subrequest::default(),
        };
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::ID => {
                    query_foreach.input_set = Some(inner_pair.as_str().to_string().into());
                }
                Rule::asignation => {
                    query_foreach.loop_var = Some(
                        inner_pair
                            .into_inner()
                            .find(|p| p.as_rule() == Rule::ID)
                            .map(|p| p.as_str())
                            .unwrap()
                            .into(),
                    );
                }
                Rule::subrequest => {
                    query_foreach.body = Subrequest::from_pest(inner_pair)?;
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!(
                                "Invalid rule {:?} for QueryForeach",
                                inner_pair.as_rule()
                            ),
                        },
                        inner_pair.as_span(),
                    ));
                }
            }
        }
        Ok(Box::new(query_foreach))
    }

    fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        _default_set: &str,
    ) -> Vec<SubrequestJoin> {
        let body_sqls = self.body.to_sql(sql_dialect, srid, "input");
        let mut inner = body_sqls
            .last()
            .map(|s| s.trim_end_matches("\n;").trim_end().to_string())
            .unwrap_or_default()
            .replace(r"WITH", "WITH _input AS (SELECT _input.*),");
        inner = Regex::new(r"(?m)^")
            .unwrap()
            .replace_all(&inner, "        ")
            .to_string();
        let input = self.input_set.as_deref().unwrap_or(_default_set);
        let clause = format!(
            "SELECT
    _body.*
FROM
    _{input} AS _input
    JOIN LATERAL (
{inner}
    ) AS _body ON true"
        );
        vec![SubrequestJoin {
            precompute_set: None,
            precompute: None,
            from: None,
            clauses: clause.to_string(),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overpass_parser::parse_query;
    use crate::overpass_parser::subrequest::{QueryType, SubrequestType};
    use crate::sql_dialect::postgres::postgres::Postgres;
    use pretty_assertions::assert_eq;

    fn parse(query: &str) -> QueryForeach {
        match parse_query(query) {
            Ok(parsed) => match parsed.subrequest.queries[0].as_ref() {
                SubrequestType::QueryType(query_type) => match query_type {
                    QueryType::QueryForeach(query_foreach) => query_foreach.clone(),
                    _ => panic!(
                        "Expected QueryForeach, found {:?}",
                        parsed.subrequest.queries[0]
                    ),
                },
                _ => panic!(
                    "Expected QueryForeach, found {:?}",
                    parsed.subrequest.queries[0]
                ),
            },
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_parse_simple_foreach() {
        // foreach with parentheses as block delimiter
        let query = "
            foreach(
                out;
            );";
        match parse_query(query) {
            Ok(_) => {}
            Err(e) => panic!("Failed to parse foreach query: {e}"),
        }
    }

    #[test]
    fn test_parse_foreach_with_input_and_loop_var() {
        let foreach = parse(
            "foreach->.b(
                out;
            );",
        );
        assert_eq!(foreach.loop_var, Some("b".into()));
        assert_eq!(foreach.body.queries.len(), 1);
    }

    #[test]
    fn test_parse_foreach_complex_body() {
        let query = "
            foreach(
                rel(bn)->.r;
                out center meta;
            );";
        match parse_query(query) {
            Ok(parsed) => match parsed.subrequest.queries[0].as_ref() {
                SubrequestType::QueryType(QueryType::QueryForeach(fe)) => {
                    assert_eq!(fe.body.queries.len(), 2);
                }
                other => panic!("Expected QueryForeach, got {:?}", other),
            },
            Err(e) => panic!("Failed to parse complex foreach: {e}"),
        }
    }

    #[test]
    fn test_foreach_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);
        let query = "
            foreach.a ->.n(
                node.n ->.m;
            );
            .n out center ids;";
        match parse_query(query) {
            Ok(request) => {
                let sql = request.to_sql(d, "9999", None)[1].clone();
                assert_eq!(
                    "WITH
_n AS (
    SELECT
        _body.*
    FROM
        _a AS _input
        JOIN LATERAL (
            WITH _input AS (SELECT _input.*),
            _m AS (
                SELECT
                    _n.*
                FROM
                    _n
                WHERE
                    _n.osm_type = 'n'
            )
            SELECT * FROM _m
        ) AS _body ON true
),
_out_n AS (
    SELECT
        jsonb_strip_nulls(jsonb_build_object(
        'type', CASE osm_type WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation' WHEN 'a' THEN 'area' END,
        'id', id,
        'lon', CASE osm_type WHEN 'n' THEN ST_X(ST_Transform(geom, 4326))::numeric END,
        'lat', CASE osm_type WHEN 'n' THEN ST_Y(ST_Transform(geom, 4326))::numeric END,
        'center', CASE osm_type = 'w' OR osm_type = 'r'
            WHEN true THEN jsonb_build_object(
                'lon', ST_X(ST_PointOnSurface(ST_Transform(geom, 4326)))::numeric,
                'lat', ST_Y(ST_PointOnSurface(ST_Transform(geom, 4326)))::numeric
            )
        END)) AS j
    FROM
        _n
)
SELECT * FROM _out_n
;",
                    sql
                );
            }
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_foreach_with_out_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        let query = "
            foreach->.n(
                node.n ->.m;
            );
            .n out center ids;";
        match parse_query(query) {
            Ok(request) => {
                let sql = request.to_sql(d, "9999", None)[1].clone();
                assert_eq!(
                        "WITH
_n AS (
    SELECT
        _body.*
    FROM
        __ AS _input
        JOIN LATERAL (
            WITH _input AS (SELECT _input.*),
            _m AS (
                SELECT
                    _n.*
                FROM
                    _n
                WHERE
                    _n.osm_type = 'n'
            )
            SELECT * FROM _m
        ) AS _body ON true
),
_out_n AS (
    SELECT
        jsonb_strip_nulls(jsonb_build_object(
        'type', CASE osm_type WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation' WHEN 'a' THEN 'area' END,
        'id', id,
        'lon', CASE osm_type WHEN 'n' THEN ST_X(ST_Transform(geom, 4326))::numeric END,
        'lat', CASE osm_type WHEN 'n' THEN ST_Y(ST_Transform(geom, 4326))::numeric END,
        'center', CASE osm_type = 'w' OR osm_type = 'r'
            WHEN true THEN jsonb_build_object(
                'lon', ST_X(ST_PointOnSurface(ST_Transform(geom, 4326)))::numeric,
                'lat', ST_Y(ST_PointOnSurface(ST_Transform(geom, 4326)))::numeric
            )
        END)) AS j
    FROM
        _n
)
SELECT * FROM _out_n
;",
                    sql
                );
            }
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }
}
