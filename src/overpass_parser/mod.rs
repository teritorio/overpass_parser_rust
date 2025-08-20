pub mod filters;
pub mod out;
pub mod query;
pub mod query_objects;
pub mod query_recurse;
pub mod query_union;
pub mod request;
pub mod selectors;
pub mod subrequest;

use pest::Parser;
use pest_derive::Parser;
use request::Request;

#[derive(Parser)]
#[grammar = "overpass.pest"]
pub struct OverpassParser;

pub fn parse_query(query: &str) -> Result<Request, pest::error::Error<Rule>> {
    match OverpassParser::parse(Rule::request, query) {
        Ok(mut pairs) => Request::from_pest(pairs.next().unwrap()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        overpass_parser::parse_query,
        sql_dialect::{postgres::postgres::Postgres, sql_dialect::SqlDialect},
    };
    use pretty_assertions::assert_eq;

    // TODO autres test

    #[test]
    fn test_to_sql() {
        let query = "[out:json][timeout:25];
        area(3600166718)->.a;
        (
          nwr[a=\"Ñ'\"][b='\"'](area.a)->.x;
          nwr[c](area.a)->.z;
        )->.k;
        .k out center meta;";

        let request = parse_query(query).expect("Failed to parse query");
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        let sql = request.to_sql(d, "4326", None);
        assert_eq!("SET statement_timeout = 25000;
WITH
_a AS (
    SELECT
        *
    FROM
        area_by_id
    WHERE
        id = ANY (ARRAY[3600166718])
),
_k AS (
    WITH
    _x AS (
    SELECT
        *
    FROM
        nwr_by_geom
    WHERE
        (tags?'a' AND tags->>'a' = 'Ñ''') AND (tags?'b' AND tags->>'b' = '\"') AND
        ST_Intersects(geom, (SELECT ST_Union(geom) FROM _a))
    ),
    _z AS (
    SELECT
        *
    FROM
        nwr_by_geom
    WHERE
        tags?'c' AND
        ST_Intersects(geom, (SELECT ST_Union(geom) FROM _a))
    )
    SELECT DISTINCT ON(osm_type, id)
        *
    FROM (
        (SELECT * FROM _x) UNION
        (SELECT * FROM _z)
    ) AS t
    ORDER BY
        osm_type, id
),
_out_k AS (
    SELECT
        jsonb_strip_nulls(jsonb_build_object(
        'type', CASE osm_type WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation' WHEN 'a' THEN 'area' END,
        'id', id,
        'lon', CASE osm_type WHEN 'n' THEN ST_X(ST_Transform(geom, 4326))::numeric END,
        'lat', CASE osm_type WHEN 'n' THEN ST_Y(ST_Transform(geom, 4326))::numeric END,
        'timestamp', created,
        'version', version,
        'changeset', changeset,
        'user', \"user\",
        'uid', uid,
        'center', CASE osm_type = 'w' OR osm_type = 'r'
            WHEN true THEN jsonb_build_object(
                'lon', ST_X(ST_PointOnSurface(ST_Transform(geom, 4326)))::numeric,
                'lat', ST_Y(ST_PointOnSurface(ST_Transform(geom, 4326)))::numeric
            )
        END,
        'nodes', nodes,
        'members', members,
        'tags', tags)) AS j
    FROM
        _k
)
SELECT * FROM _out_k
;",
sql);
    }
}
