use pest::iterators::Pair;

use derivative::Derivative;
use regex::Regex;

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::Rule;

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Out {
    pub set: Option<Box<str>>,

    #[derivative(Default(value = "\"geom\".into()"))]
    pub geom: Box<str>,

    #[derivative(Default(value = "\"body\".into()"))]
    pub level_of_details: Box<str>,
}

impl Out {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut out = Out::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::ID => {
                    out.set = Some(inner_pair.as_str().into());
                }
                Rule::out_geom => {
                    out.geom = inner_pair.as_str().into();
                }
                Rule::out_level_of_details => {
                    out.level_of_details = inner_pair.as_str().into();
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!("Invalid rule {:?} for Out", inner_pair.as_rule()),
                        },
                        inner_pair.as_span(),
                    ));
                }
            }
        }
        Ok(out)
    }

    pub fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        default_set: &str,
    ) -> String {
        let way_member_nodes = matches!(self.level_of_details.as_ref(), "skel" | "body" | "meta");
        let relations_members = matches!(self.level_of_details.as_ref(), "skel" | "body" | "meta");
        let tags = matches!(self.level_of_details.as_ref(), "body" | "tags" | "meta");
        let meta = self.level_of_details.as_ref() == "meta";

        let json_strip_nulls = sql_dialect.json_strip_nulls();
        let json_build_object = sql_dialect.json_build_object();
        let st_dump_points = sql_dialect.st_dump_points();
        let st_transform_reverse = sql_dialect.st_transform_reverse("geom", srid);
        let st_asgeojson = sql_dialect.st_asgeojson(&st_transform_reverse, 7);
        let jsonb_agg = sql_dialect.jsonb_agg();

        let replace = Regex::new(r"\n").unwrap();

        let meta_fields = if meta {
            ",\n    'timestamp', created,
    'version', version,
    'changeset', changeset,
    'user', \"user\",
    'uid', uid"
        } else {
            ""
        };

        let geom_center = if self.geom.as_ref() == "center" {
            format!(
                ",
    'center', CASE osm_type = 'w' OR osm_type = 'r'
        WHEN true THEN {json_build_object}(
            'lon', ST_X(ST_PointOnSurface({st_transform_reverse}))::numeric,
            'lat', ST_Y(ST_PointOnSurface({st_transform_reverse}))::numeric
        )
    END"
            )
        } else {
            "".to_string()
        };

        let geom_bb_geom = if self.geom.as_ref() == "bb" || self.geom.as_ref() == "geom" {
            format!(
                ",
    'bounds', CASE osm_type = 'w' OR osm_type = 'r'
    WHEN true THEN {}
    END",
                sql_dialect.json_build_bbox("geom", srid).replace("\n", "\n    ")
            )
        } else {
            "".to_string()
        };
        let geom = if self.geom.as_ref() == "geom" {
            let a = ",
    'geometry', CASE osm_type
        WHEN 'w' THEN ";

            let w = if st_dump_points.is_some() {
                let st_dump_points = st_dump_points.unwrap();
                format!(
                    "(SELECT \
{jsonb_agg}({json_build_object}(\
'lon', ST_X({st_transform_reverse})::numeric, \
'lat', ST_Y({st_transform_reverse})::numeric)) \
FROM {st_dump_points}(geom))",
                )
                .to_string()
            } else {
                format!(
                    "
        replace(replace(replace(replace(replace((
            CASE ST_GeometryType(geom)
            WHEN 'LINESTRING' THEN {st_asgeojson}->'coordinates'
            ELSE {st_asgeojson}->'coordinates'->0
            END
        )::text, {}",
                    " '[', '{\"lon\":'), \
',', ',\"lat\":'), \
'{\"lon\":{\"lon\":', '[{\"lon\":'), \
'],\"lat\":{\"lon\":', '},{\"lon\":'), \
']]', '}]')::json"
                )
            };
            format!(
                "{a}{w}
    END"
            )
        } else {
            "".to_string()
        };
        let way_member_nodes_field = if way_member_nodes {
            ",\n    'nodes', nodes"
        } else {
            ""
        };

        let relations_members_field = if relations_members {
            ",\n    'members', members"
        } else {
            ""
        };

        let tags_field = if tags { ",\n    'tags', tags" } else { "" };

        format!("SELECT
    {json_strip_nulls}({json_build_object}(
    'type', CASE osm_type WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation' WHEN 'a' THEN 'area' END,
    'id', id,
    'lon', CASE osm_type WHEN 'n' THEN ST_X({st_transform_reverse})::numeric END,
    'lat', CASE osm_type WHEN 'n' THEN ST_Y({st_transform_reverse})::numeric END{meta_fields}{geom_center}{geom_bb_geom}{geom}{way_member_nodes_field}{relations_members_field}{tags_field})) AS j
FROM
    _{}", self.set.clone().unwrap_or(default_set.into()))
    }
}

#[cfg(test)]
mod tests {
    use crate::{overpass_parser::parse_query, sql_dialect::postgres::postgres::Postgres};

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_multiple_output() {
        let query = "
            [out:json][timeout:25];
            node(1573900912)->.a;
            .a out geom;
            node(1573900912)->.b;
            .b out geom;
        ";
        match parse_query(query) {
            Ok(request) => {
                let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);
                let sql = request.to_sql(d, "9999", None);
                assert_eq!(vec!["SET statement_timeout = 25000;", "WITH
_a AS (
    SELECT
        *
    FROM
        node_by_id
    WHERE
        osm_type = 'n' AND
        id = ANY (ARRAY[1573900912])
),
_out_a AS (
    SELECT
        jsonb_strip_nulls(jsonb_build_object(
        'type', CASE osm_type WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation' WHEN 'a' THEN 'area' END,
        'id', id,
        'lon', CASE osm_type WHEN 'n' THEN ST_X(ST_Transform(geom, 4326))::numeric END,
        'lat', CASE osm_type WHEN 'n' THEN ST_Y(ST_Transform(geom, 4326))::numeric END,
        'bounds', CASE osm_type = 'w' OR osm_type = 'r'
        WHEN true THEN jsonb_build_object(
            'minlon', ST_XMin(ST_Transform(geom, 4326))::numeric,
            'minlat', ST_YMin(ST_Transform(geom, 4326))::numeric,
            'maxlon', ST_XMax(ST_Transform(geom, 4326))::numeric,
            'maxlat', ST_YMax(ST_Transform(geom, 4326))::numeric
        )
        END,
        'geometry', CASE osm_type
            WHEN 'w' THEN (SELECT jsonb_agg(jsonb_build_object('lon', ST_X(ST_Transform(geom, 4326))::numeric, 'lat', ST_Y(ST_Transform(geom, 4326))::numeric)) FROM ST_DumpPoints(geom))
        END,
        'nodes', nodes,
        'members', members,
        'tags', tags)) AS j
    FROM
        _a
),
_b AS (
    SELECT
        *
    FROM
        node_by_id
    WHERE
        osm_type = 'n' AND
        id = ANY (ARRAY[1573900912])
),
_out_b AS (
    SELECT
        jsonb_strip_nulls(jsonb_build_object(
        'type', CASE osm_type WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation' WHEN 'a' THEN 'area' END,
        'id', id,
        'lon', CASE osm_type WHEN 'n' THEN ST_X(ST_Transform(geom, 4326))::numeric END,
        'lat', CASE osm_type WHEN 'n' THEN ST_Y(ST_Transform(geom, 4326))::numeric END,
        'bounds', CASE osm_type = 'w' OR osm_type = 'r'
        WHEN true THEN jsonb_build_object(
            'minlon', ST_XMin(ST_Transform(geom, 4326))::numeric,
            'minlat', ST_YMin(ST_Transform(geom, 4326))::numeric,
            'maxlon', ST_XMax(ST_Transform(geom, 4326))::numeric,
            'maxlat', ST_YMax(ST_Transform(geom, 4326))::numeric
        )
        END,
        'geometry', CASE osm_type
            WHEN 'w' THEN (SELECT jsonb_agg(jsonb_build_object('lon', ST_X(ST_Transform(geom, 4326))::numeric, 'lat', ST_Y(ST_Transform(geom, 4326))::numeric)) FROM ST_DumpPoints(geom))
        END,
        'nodes', nodes,
        'members', members,
        'tags', tags)) AS j
    FROM
        _b
)
SELECT * FROM _out_a
UNION ALL
SELECT * FROM _out_b
;"], sql);
            }
            Err(e) => {
                println!("Error parsing query: {e}");
                panic!("Parsing fails");
            }
        };
    }
}
