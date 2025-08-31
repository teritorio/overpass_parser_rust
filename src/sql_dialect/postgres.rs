pub mod postgres {
    use crate::sql_dialect::sql_dialect::SqlDialect;

    use derivative::Derivative;

    #[derive(Derivative)]
    #[derivative(Default)]
    // #[derive(Debug)]
    pub struct Postgres {
        pub postgres_escape_literal: Option<Box<dyn Fn(&str) -> String + Send + Sync>>,
    }

    impl SqlDialect for Postgres {
        fn escape_literal(&self, string: &str) -> String {
            if self.postgres_escape_literal.is_some() {
                (self.postgres_escape_literal.as_ref().unwrap())(string)
            } else {
                format!("'{}'", string.replace('\'', "''"))
            }
        }

        fn statement_timeout(&self, timeout: u32) -> String {
            format!("SET statement_timeout = {timeout};")
        }

        fn id_in_list(&self, field: &str, values: &Vec<i64>) -> String {
            format!(
                "{field} = ANY (ARRAY[{}])",
                values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }

        fn hash_exists(&self, key: &str) -> String {
            format!("tags?{}", self.escape_literal(key))
        }

        fn hash_get(&self, key: &str) -> String {
            format!("tags->>{}", self.escape_literal(key))
        }

        fn json_strip_nulls(&self) -> String {
            "jsonb_strip_nulls".to_string()
        }

        fn json_build_object(&self) -> String {
            "jsonb_build_object".to_string()
        }

        fn jsonb_agg(&self) -> String {
            "jsonb_agg".to_string()
        }

        fn st_union(&self) -> String {
            "ST_Union".to_string()
        }

        fn st_dump_points(&self) -> Option<String> {
            Some("ST_DumpPoints".to_string())
        }

        fn st_intersects_with_geom(&self, table: &str, geom: &str) -> String {
            format!(
                "ST_Intersects(
    {geom},
    {table}.geom
)"
            )
        }

        fn st_intersects_extent_with_geom(&self, table: &str, geom: &str) -> String {
            format!(
                "ST_Intersects(
    {geom},
    {table}.geom
)"
            )
        }

        fn st_transform(&self, geom: &str, srid: &str) -> String {
            format!("ST_Transform({geom}, {srid})")
        }

        fn st_transform_reverse(&self, geom: &str, _srid: &str) -> String {
            format!("ST_Transform({geom}, 4326)")
        }

        fn st_asgeojson(&self, geom: &str, max_decimal_digits: usize) -> String {
            format!("ST_AsGeoJSON({geom}, {max_decimal_digits})")
        }
    }
}
