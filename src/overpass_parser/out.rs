use pest::iterators::Pair;

use derivative::Derivative;

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::Rule;

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Out {
    #[derivative(Default(value = "\"geom\".into()"))]
    geom: Box<str>,

    #[derivative(Default(value = "\"body\".into()"))]
    level_of_details: Box<str>,
}

impl Out {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut out = Out::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::out_geom => {
                    out.geom = inner_pair.as_str().into();
                }
                Rule::out_level_of_details => {
                    out.level_of_details = inner_pair.as_str().into();
                }
                _ => {}
            }
        }
        Ok(out)
    }

    pub fn to_sql(&self, sql_dialect: &Box<dyn SqlDialect>, srid: &str) -> String {
        let way_member_nodes = matches!(self.level_of_details.as_ref(), "skel" | "body" | "meta");
        let relations_members = matches!(self.level_of_details.as_ref(), "skel" | "body" | "meta");
        let tags = matches!(self.level_of_details.as_ref(), "body" | "tags" | "meta");
        let meta = self.level_of_details.as_ref() == "meta";

        let json_strip_nulls = sql_dialect.json_strip_nulls();
        let json_build_object = sql_dialect.json_build_object();
        let st_dump_points = sql_dialect.st_dump_points();
        let st_transform_reverse = sql_dialect.st_transform_reverse("geom", srid);
        let st_asgeojson = sql_dialect.st_asgeojson(&st_transform_reverse, 7);

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
            ",
    'bounds', CASE osm_type = 'w' OR osm_type = 'r'
      WHEN true THEN {json_build_object}(
        'minlon', ST_XMin(ST_Envelope({st_transform_reverse}))::numeric,
        'minlat', ST_YMin(ST_Envelope({st_transform_reverse}))::numeric,
        'maxlon', ST_XMax(ST_Envelope({st_transform_reverse}))::numeric,
        'maxlat', ST_YMax(ST_Envelope({st_transform_reverse}))::numeric
      )
    END"
        } else {
            ""
        };
        let geom = if self.geom.as_ref() == "geom" {
            let a = ",
     'geometry', CASE osm_type
      WHEN 'w' THEN ";

            let w = if st_dump_points.is_some() {
                "(SELECT \
#{sql_dialect.jsonb_agg}({json_build_object}(\
'lon', ST_X({st_transform_reverse})::numeric, \
'lat', ST_Y({st_transform_reverse})::numeric)) \
FROM {st_dump_points}(geom))"
                    .to_string()
            } else {
                format!(
                    ",
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
            format!("{a}{w}")
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
    'lat', CASE osm_type WHEN 'n' THEN ST_Y({st_transform_reverse})::numeric END{meta_fields}{geom_center}{geom_bb_geom}{geom}{way_member_nodes_field}{relations_members_field}{tags_field})) AS j")
    }
}
