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

        fn statement_timeout(&self, _timeout: u32) -> String {
            "".to_string()
        }

        fn id_in_list(&self, field: &str, values: &Vec<i64>) -> String {
            let sql = values
                .iter()
                .map(|value| format!("{field} = {}", value.to_string()))
                .collect::<Vec<String>>()
                .join(" OR ");
            format!("({})", sql)
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

        fn jsonb_agg(&self) -> String {
            "json_group_array".to_string()
        }

        fn st_union(&self) -> String {
            "".to_string() // 'ST_Union_Agg'
        }

        fn st_dump_points(&self) -> Option<String> {
            None
        }

        fn st_intersects_with_geom(&self, geom: &str) -> String {
            [
                self.st_intersects_extent_with_geom(geom),
                format!(
                    "ST_Intersects(
        {geom},
        geom
    )"
                ),
            ]
            .join(" AND\n")
        }

        fn st_intersects_extent_with_geom(&self, geom: &str) -> String {
            format!(
                "bbox.xmin <= ST_XMax({geom}) AND
bbox.xmax >= ST_XMin({geom}) AND
bbox.ymin <= ST_YMax({geom}) AND
bbox.ymax >= ST_YMin({geom})"
            )
        }

        fn st_transform(&self, geom: &str, srid: &str) -> String {
            format!("ST_Transform({geom}, 'EPSG:4326', 'EPSG:{srid}')")
        }

        fn st_transform_reverse(&self, geom: &str, srid: &str) -> String {
            format!("ST_Transform({geom}, 'EPSG:{srid}', 'EPSG:4326')")
        }

        fn st_asgeojson(&self, geom: &str, _max_decimal_digits: usize) -> String {
            format!("ST_AsGeoJSON({geom})")
        }
    }
}
