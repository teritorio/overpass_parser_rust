pub trait SqlDialect: Send + Sync {
    fn escape_literal(&self, string: &str) -> String {
        format!("'{}'", string.replace('\'', "''"))
    }

    fn statement_timeout(&self, timeout: u32) -> String;

    fn hash_exists(&self, key: &str) -> String;

    fn hash_get(&self, key: &str) -> String;

    fn json_strip_nulls(&self) -> String;

    fn json_build_object(&self) -> String;

    fn jsonb_agg(&self) -> String;

    fn st_union(&self) -> String;

    fn st_dump_points(&self) -> Option<String>;

    fn st_intersects(&self) -> String;

    fn st_intersects_extent(&self) -> String;

    fn st_transform(&self, geom: &str, srid: &str) -> String;

    fn st_transform_reverse(&self, geom: &str, srid: &str) -> String;

    fn st_asgeojson(&self, geom: &str, max_decimal_digits: usize) -> String;
}
