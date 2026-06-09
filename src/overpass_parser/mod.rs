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
        sql_dialect::{
            duckdb::duckdb::Duckdb, postgres::postgres::Postgres, sql_dialect::SqlDialect,
        },
    };
    use pretty_assertions::assert_eq;

    // TODO other tests

    #[test]
    fn test_to_sql() {
        let query = "[out:json][timeout:25];
        area(3600166718)->.a;
        (
          nwr[a=\"Ñ'\"][b='\"'](poly:\"1 2 3 4\")(area.a)->.x;
          nwr[c](area.a)->.z;
        )->.k;
        .k out center meta;";

        let request = parse_query(query).expect("Failed to parse query");
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        let sql = request.to_sql(d, "9999", None);
        assert_eq!(vec!["SET statement_timeout = 25000;",
"WITH
_a AS (
    SELECT
        area_by_id.*
    FROM
        area_by_id
    WHERE
        area_by_id.id = ANY (ARRAY[3600166718])
),
_poly_11689077968748950118 AS (
    SELECT
        geom
    FROM
        (VALUES (ST_Transform('SRID=4326;POLYGON((2 1, 4 3))'::geometry, 9999))) AS p(geom)
),
_k AS (
    WITH
    _x AS (
    SELECT
        nwr_by_geom.*
    FROM
        nwr_by_geom
            JOIN _poly_11689077968748950118 ON true
        JOIN _a ON true
    WHERE
        (nwr_by_geom.tags?'a' AND nwr_by_geom.tags->>'a' = 'Ñ''') AND (nwr_by_geom.tags?'b' AND nwr_by_geom.tags->>'b' = '\"') AND
        ST_Intersects(
            _poly_11689077968748950118.geom,
            nwr_by_geom.geom
        ) AND
        ST_Intersects(
            _a.geom,
            nwr_by_geom.geom
        )
    ),
    _z AS (
    SELECT
        nwr_by_geom.*
    FROM
        nwr_by_geom
            JOIN _a ON true
    WHERE
        nwr_by_geom.tags?'c' AND
        ST_Intersects(
            _a.geom,
            nwr_by_geom.geom
        )
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
;"].join("\n"),
sql.join("\n"));

        let d = &Duckdb as &(dyn SqlDialect + Send + Sync);

        let sql = request.to_sql(d, "9999", None);
        assert_eq!(vec!["CREATE TEMP TABLE _a AS
SELECT
    area_by_id.*
FROM
    area_by_id
WHERE
    (area_by_id.id = 3600166718)
;", "SET variable _a_bbox = (
    SELECT
        STRUCT_PACK(
            xmin := min(bbox.xmin),
            ymin := min(bbox.ymin),
            xmax := max(bbox.xmax),
            ymax := max(bbox.ymax),
            geom := ST_Union_Agg(geom)
        ) AS bbox_geom
    FROM
        _a
)
;", "CREATE TEMP TABLE _poly_17221393697116889690 AS
SELECT
    geom,
    STRUCT_PACK(
        xmin := ST_XMin(geom),
        ymin := ST_YMin(geom),
        xmax := ST_XMax(geom),
        ymax := ST_YMax(geom)
    ) AS bbox
FROM
    (VALUES (ST_Transform('SRID=4326;POLYGON((2 1, 4 3))'::geometry, 'EPSG:4326', 'EPSG:9999'))) AS p(geom)
;", "SET variable _poly_17221393697116889690_bbox = (
    SELECT
        STRUCT_PACK(
            xmin := min(bbox.xmin),
            ymin := min(bbox.ymin),
            xmax := max(bbox.xmax),
            ymax := max(bbox.ymax),
            geom := ST_Union_Agg(geom)
        ) AS bbox_geom
    FROM
        _poly_17221393697116889690
)
;", "WITH
_k AS (
    WITH
    _x AS (
    SELECT
        nwr_by_geom.*
    FROM
        nwr_by_geom
    WHERE
        ((nwr_by_geom.tags->>'a') IS NOT NULL AND (nwr_by_geom.tags->>'a') = 'Ñ''') AND ((nwr_by_geom.tags->>'b') IS NOT NULL AND (nwr_by_geom.tags->>'b') = '\"') AND
        nwr_by_geom.bbox.xmin <= getvariable('_poly_17221393697116889690_bbox').xmax AND
        nwr_by_geom.bbox.xmax >= getvariable('_poly_17221393697116889690_bbox').xmin AND
        nwr_by_geom.bbox.ymin <= getvariable('_poly_17221393697116889690_bbox').ymax AND
        nwr_by_geom.bbox.ymax >= getvariable('_poly_17221393697116889690_bbox').ymin AND
        ST_Intersects(
            getvariable('_poly_17221393697116889690_bbox').geom,
            nwr_by_geom.geom
        ) AND
        nwr_by_geom.bbox.xmin <= getvariable('_a_bbox').xmax AND
        nwr_by_geom.bbox.xmax >= getvariable('_a_bbox').xmin AND
        nwr_by_geom.bbox.ymin <= getvariable('_a_bbox').ymax AND
        nwr_by_geom.bbox.ymax >= getvariable('_a_bbox').ymin AND
        ST_Intersects(
            getvariable('_a_bbox').geom,
            nwr_by_geom.geom
        )
    ),
    _z AS (
    SELECT
        nwr_by_geom.*
    FROM
        nwr_by_geom
    WHERE
        (nwr_by_geom.tags->>'c') IS NOT NULL AND
        nwr_by_geom.bbox.xmin <= getvariable('_a_bbox').xmax AND
        nwr_by_geom.bbox.xmax >= getvariable('_a_bbox').xmin AND
        nwr_by_geom.bbox.ymin <= getvariable('_a_bbox').ymax AND
        nwr_by_geom.bbox.ymax >= getvariable('_a_bbox').ymin AND
        ST_Intersects(
            getvariable('_a_bbox').geom,
            nwr_by_geom.geom
        )
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
        (json_object(
        'type', CASE osm_type WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation' WHEN 'a' THEN 'area' END,
        'id', id,
        'lon', CASE osm_type WHEN 'n' THEN ST_X(ST_Transform(geom, 'EPSG:9999', 'EPSG:4326'))::numeric END,
        'lat', CASE osm_type WHEN 'n' THEN ST_Y(ST_Transform(geom, 'EPSG:9999', 'EPSG:4326'))::numeric END,
        'timestamp', created,
        'version', version,
        'changeset', changeset,
        'user', \"user\",
        'uid', uid,
        'center', CASE osm_type = 'w' OR osm_type = 'r'
            WHEN true THEN json_object(
                'lon', ST_X(ST_PointOnSurface(ST_Transform(geom, 'EPSG:9999', 'EPSG:4326')))::numeric,
                'lat', ST_Y(ST_PointOnSurface(ST_Transform(geom, 'EPSG:9999', 'EPSG:4326')))::numeric
            )
        END,
        'nodes', nodes,
        'members', members,
        'tags', tags)) AS j
    FROM
        _k
)
SELECT * FROM _out_k
;"].join("\n"),
sql.join("\n"));

        let query = "[out:json][timeout:25];
        area(3602364330)->.a;
        .a out center meta;
        nwr[highway=bus_stop][operator=Citalis](area.a);
        foreach->.k(
            rel(bn)->.r;
            convert node
            ::=::,
            ::id=id(),
            osm_type=type(),
            route_ref=r.set(t['ref']),
            !highway
            ;
        );
        .k out center meta;
";

        let request = parse_query(query).expect("Failed to parse query");
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        let sql = request.to_sql(d, "9999", None);
        let re = Regex::new(r"_[0-9]+").unwrap();
        assert_eq!(
            re.replace_all(&vec!["SET statement_timeout = 25000;",
"WITH
_a AS (
    SELECT
        area_by_id.*
    FROM
        area_by_id
    WHERE
        area_by_id.id = ANY (ARRAY[3602364330])
),
_out_a AS (
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
        _a
),
_125 AS (
    SELECT
        nwr_by_geom.*
    FROM
        nwr_by_geom
        JOIN _a ON true
    WHERE
        (nwr_by_geom.tags?'highway' AND nwr_by_geom.tags->>'highway' = 'bus_stop') AND (nwr_by_geom.tags?'operator' AND nwr_by_geom.tags->>'operator' = 'Citalis') AND
        ST_Intersects(
            _a.geom,
            nwr_by_geom.geom
        )
),
_k AS (
    SELECT
        _body.*
    FROM
        _127 AS _input
        JOIN LATERAL (
            WITH _input AS (SELECT _input.*),
            _r AS (
                SELECT
                    relation_by_geom.*
                FROM
                    relation_by_geom
                    JOIN _input AS bn ON _.osm_type = 'n' AND (
                    bn.osm_type = 'w' AND array[bn.id] <@ relation_by_geom.nodes OR
                    bn.osm_type = 'r' AND array[bn.id] <@ osm_base_idx_nodes_members(relation_by_geom.members, 'n')
                )
                WHERE
                    relation_by_geom.osm_type = 'r' AND
                    true
            ),
            _125 AS (
                SELECT
                    tags - 'highway' || jsonb_build_object('osm_type', osm_type) || jsonb_build_object('route_ref', (SELECT string_agg(tags ->> 'ref', ',') FROM _r)) AS tags,
                    changeset AS changeset,
                    created AS created,
                    id AS id,
                    osm_type AS osm_type,
                    uid AS uid,
                    user AS user,
                    version AS version,
                    nodes,
                    members,
                    geom AS geom
                FROM
                    _input
            )
            SELECT * FROM _125
        ) AS _body ON true
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
SELECT * FROM _out_a
UNION ALL
SELECT * FROM _out_k
;"].join("\n"), "_999").to_string(),
            re.replace_all(&sql.join("\n"), "_999").to_string()
        );

        let query = "[out:json][timeout:25];
        relation[ref=523][network='Cars Régionaux 64 - Pyrénées-Atlantiques'];
        node(r)[highway=bus_stop];
        out center meta;
";

        let request = parse_query(query).expect("Failed to parse query");
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        let sql = request.to_sql(d, "9999", None);
        let re = Regex::new(r"_[0-9]+").unwrap();
        println!("{}", re.replace_all(&sql.join("\n"), "_999").to_string());
        assert_eq!(
            re.replace_all(&vec!["SET statement_timeout = 25000;",
"WITH
_133 AS (
    SELECT
        relation_by_geom.*
    FROM
        relation_by_geom
    WHERE
        relation_by_geom.osm_type = 'r' AND
        (relation_by_geom.tags?'ref' AND relation_by_geom.tags->>'ref' = '523') AND (relation_by_geom.tags?'network' AND relation_by_geom.tags->>'network' = 'Cars Régionaux 64 - Pyrénées-Atlantiques')
),
_999 AS (
    SELECT
        node_by_geom.*
    FROM
        node_by_geom
        JOIN _999 AS r ON r.osm_type = 'r'
        JOIN LATERAL jsonb_to_recordset(r.members) AS m(type text, ref bigint, role text) ON m.type = node_by_geom.osm_type AND m.ref = node_by_geom.id
    WHERE
        node_by_geom.osm_type = 'n' AND
        (node_by_geom.tags?'highway' AND node_by_geom.tags->>'highway' = 'bus_stop') AND
        true
),
_out_999 AS (
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
        _999
)
SELECT * FROM _out_999
;"].join("\n"), "_999").to_string(),
            re.replace_all(&sql.join("\n"), "_999").to_string()
        );
    }
}
