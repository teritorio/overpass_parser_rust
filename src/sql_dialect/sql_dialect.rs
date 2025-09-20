pub trait SqlDialect: Send + Sync {
    fn escape_literal(&self, string: &str) -> String {
        format!("'{}'", string.replace('\'', "''"))
    }

    fn statement_timeout(&self, timeout: u32) -> Option<String>;

    fn is_precompute(&self) -> bool;

    fn precompute(&self, set: &str, sql: &str) -> Option<Vec<String>>;

    fn id_in_list(&self, field: &str, values: &Vec<i64>) -> String;

    fn hash_exists(&self, key: &str) -> String;

    fn hash_get(&self, key: &str) -> String;

    fn json_strip_nulls(&self) -> String;

    fn json_build_object(&self) -> String;

    fn json_build_bbox(&self, geom: &str, srid: &str) -> String;

    fn jsonb_agg(&self) -> String;

    fn st_union(&self) -> String;

    fn st_dump_points(&self) -> Option<String>;

    fn table_precompute_geom(&self, other: &str) -> String;

    fn st_intersects_with_geom(&self, table: &str, other: &str) -> String;

    fn st_intersects_extent_with_geom(&self, table: &str, other: &str) -> String;

    fn st_transform(&self, geom: &str, srid: &str) -> String;

    fn st_transform_reverse(&self, geom: &str, srid: &str) -> String;

    fn st_asgeojson(&self, geom: &str, max_decimal_digits: usize) -> String;
}
