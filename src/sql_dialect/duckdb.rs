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

        fn hash_exists(&self, key: &str) -> String {
            format!("(tags->>'{}') IS NOT NULL", self.escape_literal(key))
        }

        fn hash_get(&self, key: &str) -> String {
            format!("(tags->>'{}')", self.escape_literal(key))
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

        fn st_intersects(&self) -> String {
            "ST_Intersects".to_string()
        }

        fn st_intersects_extent(&self) -> String {
            "ST_Intersects_Extent".to_string()
        }

        fn st_transform(&self, geom: &str, srid: &str) -> String {
            format!("ST_Transform({}, 'EPSG:4326', 'EPSG:{}')", geom, srid)
        }

        fn st_transform_reverse(&self, geom: &str, srid: &str) -> String {
            format!("ST_Transform({}, 'EPSG:{}', 'EPSG:4326')", geom, srid)
        }

        fn st_asgeojson(&self, geom: &str, _max_decimal_digits: usize) -> String {
            format!("ST_AsGeoJSON({})", geom)
        }
    }
}
