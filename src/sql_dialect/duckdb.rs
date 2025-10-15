pub mod duckdb {
    use crate::sql_dialect::sql_dialect::SqlDialect;

    use derivative::Derivative;

    #[derive(Derivative)]
    #[derivative(Default)]
    pub struct Duckdb;

    impl SqlDialect for Duckdb {
        fn escape_literal(&self, string: &str) -> String {
            format!("'{}'", string.replace('\'', "''"))
        }

        fn statement_timeout(&self, _timeout: u32) -> Option<String> {
            None
        }

        fn make_geom_fields(&self)  -> String {
            "geom,
    STRUCT_PACK(
        xmin := ST_XMin(geom),
        ymin := ST_YMin(geom),
        xmax := ST_XMax(geom),
        ymax := ST_YMax(geom)
    ) AS bbox".to_string()
        }

        fn is_precompute(&self) -> bool {
            true
        }

        fn precompute(&self, set: &str, sql: &str) -> Option<Vec<String>> {
            Some(vec![
                format!("CREATE TEMP TABLE _{set} AS\n{sql}\n;"),
                format!(
                    "SET variable _{set}_bbox = (
    SELECT
        STRUCT_PACK(
            xmin := min(bbox.xmin),
            ymin := min(bbox.ymin),
            xmax := max(bbox.xmax),
            ymax := max(bbox.ymax),
            geom := ST_Union_Agg(geom)
        ) AS bbox_geom
    FROM
        _{set}
)
;"
                ),
            ])
        }

        fn id_in_list(&self, field: &str, values: &Vec<i64>) -> String {
            let sql = values
                .iter()
                .map(|value| format!("{field} = {value}"))
                .collect::<Vec<String>>()
                .join(" OR ");
            format!("({sql})")
        }

        fn hash_exists(&self, key: &str) -> String {
            format!("(tags->>{}) IS NOT NULL", self.escape_literal(key))
        }

        fn hash_get(&self, key: &str) -> String {
            format!("(tags->>{})", self.escape_literal(key))
        }

        fn json_strip_nulls(&self) -> String {
            "".to_string()
        }

        fn json_build_object(&self) -> String {
            "json_object".to_string()
        }

        fn json_build_bbox(&self, geom: &str, srid: &str) -> String {
            let g = self.st_transform_reverse(geom, srid);
            if g == "geom" {
                format!(
                    "{}(
    'minlon', bbox.xmin,
    'minlat', bbox.ymin,
    'maxlon', bbox.xmax,
    'maxlat', bbox.ymax
)",
                    self.json_build_object()
                )
            } else {
                format!(
                    "{}(
    'minlon', ST_XMin({g}),
    'minlat', ST_YMin({g}),
    'maxlon', ST_XMax({g}),
    'maxlat', ST_YMax({g})
)",
                    self.json_build_object()
                )
            }
        }

        fn jsonb_agg(&self) -> String {
            "json_group_array".to_string()
        }

        fn st_union(&self) -> String {
            "".to_string() // 'ST_Union_Agg'
        }

        fn st_dump_points(&self) -> Option<String> {
            None
        }

        fn table_precompute_geom(&self, other: &str) -> String {
            format!("getvariable('_{other}_bbox').geom")
        }

        fn st_intersects_with_geom(&self, table: &str, other: &str) -> String {
            [
                self.st_intersects_extent_with_geom(table, other.replace(".geom", "").as_str()),
                format!(
                    "ST_Intersects(
    {other},
    {table}.geom
)"
                ),
            ]
            .join(" AND\n")
        }

        fn st_intersects_extent_with_geom(&self, table: &str, other: &str) -> String {
            format!(
                "{table}.bbox.xmin <= {other}.xmax AND
{table}.bbox.xmax >= {other}.xmin AND
{table}.bbox.ymin <= {other}.ymax AND
{table}.bbox.ymax >= {other}.ymin"
            )
        }

        fn st_transform(&self, geom: &str, srid: &str) -> String {
            if srid == "4326" {
                geom.to_string()
            } else {
                format!("ST_Transform({geom}, 'EPSG:4326', 'EPSG:{srid}')")
            }
        }

        fn st_transform_reverse(&self, geom: &str, srid: &str) -> String {
            if srid == "4326" {
                geom.to_string()
            } else {
                format!("ST_Transform({geom}, 'EPSG:{srid}', 'EPSG:4326')")
            }
        }

        fn st_asgeojson(&self, geom: &str, _max_decimal_digits: usize) -> String {
            format!("ST_AsGeoJSON({geom})")
        }
    }
}
