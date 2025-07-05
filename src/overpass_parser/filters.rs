use pest::iterators::Pair;

use derivative::Derivative;
use regex::Regex;

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::Rule;

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct FilterAround {
    core: Box<str>,
    radius: f64,
}

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Filter {
    bbox: Option<(f64, f64, f64, f64)>,
    poly: Option<Vec<(f64, f64)>>,
    ids: Option<Vec<i64>>,
    area_id: Option<Box<str>>,
    around: Option<FilterAround>,
}

impl Filter {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut filter = Filter::default();
        match pair.as_rule() {
            Rule::filter_bbox => {
                let coords: Vec<f64> = pair
                    .as_str()
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();
                if coords.len() == 4 {
                    filter.bbox = Some((coords[0], coords[1], coords[2], coords[3]));
                }
            }
            Rule::filter_poly => {
                let a = pair.into_inner().next().unwrap().as_str();
                let points: Vec<(f64, f64)> = Regex::new(r"\s+")
                    .unwrap()
                    .split(&(a[1..a.len() - 1]))
                    .map(|s| s.parse::<f64>().ok().unwrap())
                    .collect::<Vec<f64>>()
                    .chunks(2)
                    .map(|chunk| {
                        if chunk.len() == 2 {
                            (chunk[0], chunk[1])
                        } else {
                            panic!("Invalid point in poly filter: {chunk:?}");
                        }
                    })
                    .collect::<Vec<(f64, f64)>>();
                filter.poly = Some(points);
            }
            Rule::filter_osm_id => {
                if let Ok(id) = pair.as_str().parse::<i64>() {
                    filter.ids = Some(vec![id]);
                }
            }
            Rule::filter_osm_ids => {
                let ids: Vec<i64> = pair
                    .into_inner()
                    .filter_map(|id_pair| id_pair.as_str().parse().ok())
                    .collect();
                filter.ids = Some(ids);
            }
            Rule::filter_area => {
                filter.area_id = pair
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::ID)
                    .map(|p| p.as_str().into());
            }
            Rule::filter_around => {
                let mut around = FilterAround::default();
                for around_inner in pair.into_inner() {
                    match around_inner.as_rule() {
                        Rule::filter_around_core => {
                            around.core = around_inner
                                .into_inner()
                                .find(|p| p.as_rule() == Rule::ID)
                                .map(|p| p.as_str())
                                .unwrap()
                                .into();
                        }
                        Rule::filter_around_radius => {
                            if let Ok(radius) = around_inner.as_str().parse::<f64>() {
                                around.radius = radius;
                            }
                        }
                        _ => {}
                    }
                }
                filter.around = Some(around);
            }
            _ => {}
        }
        Ok(filter)
    }

    fn bbox_clauses(
        sql_dialect: &Box<dyn SqlDialect + Send + Sync>,
        bbox: (f64, f64, f64, f64),
        srid: &str,
    ) -> String {
        format!(
            "{}({}, geom)",
            sql_dialect.st_intersects_extent(),
            sql_dialect.st_transform(
                &format!(
                    "ST_Envelope('SRID=4326;LINESTRING({} {}, {} {})'::geometry)",
                    bbox.1, bbox.0, bbox.3, bbox.2
                ),
                srid
            )
        )
    }

    fn poly_clauses(sql_dialect: &Box<dyn SqlDialect + Send + Sync>, poly: &[(f64, f64)], srid: &str) -> String {
        let coords = poly
            .iter()
            .map(|&(lat, lon)| format!("{lon} {lat}"))
            .collect::<Vec<String>>()
            .join(", ");
        format!(
            "{}({}, geom)",
            sql_dialect.st_intersects(),
            sql_dialect.st_transform(&format!("'SRID=4326;POLYGON({coords})'::geometry"), srid)
        )
    }

    fn around_clause(
        &self,
        sql_dialect: &Box<dyn SqlDialect + Send + Sync>,
        srid: &str,
        around: &FilterAround,
    ) -> String {
        let core_geom = format!(
            "(SELECT {}(geom) FROM _{})",
            sql_dialect.st_union(),
            around.core
        );
        let utm_zone = format!(
            "
                -- Calculate UTM zone from
                32600 +
                CASE WHEN ST_Y(ST_Centroid(
                    {core_geom}
                )) >= 0 THEN 1 ELSE 31 END +
                floor(ST_X(ST_Centroid(
                    {core_geom}
                ) + 180) / 6)
            "
        );
        format!(
            "{}(
    geom,
    {}
)",
            sql_dialect.st_intersects(),
            sql_dialect.st_transform(
                &format!(
                    "
        ST_Buffer(
            {},
            {}
        )",
                    sql_dialect.st_transform(
                        &format!(
                            "
                {core_geom}"
                        ),
                        &utm_zone
                    ),
                    around.radius
                ),
                srid
            ),
        )
    }

    pub fn to_sql(&self, sql_dialect: &Box<dyn SqlDialect + Send + Sync>, srid: &str) -> String {
        let mut clauses = Vec::new();

        if let Some(bbox) = self.bbox {
            clauses.push(Self::bbox_clauses(sql_dialect, bbox, srid));
        }
        if let Some(poly) = &self.poly {
            clauses.push(Self::poly_clauses(sql_dialect, poly, srid));
        }
        if let Some(ids) = &self.ids {
            clauses.push(format!(
                "id = ANY (ARRAY[{}])",
                ids.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ));
        }
        if let Some(area_id) = &self.area_id {
            clauses.push(format!(
                "{}(geom, (SELECT {}(geom) FROM _{}))",
                sql_dialect.st_intersects(),
                sql_dialect.st_union(),
                area_id
            ));
        }
        if let Some(around) = &self.around {
            clauses.push(self.around_clause(sql_dialect, srid, around));
        }

        clauses.join(" AND ")
    }
}

#[derive(Debug, Clone)]
pub struct Filters {
    pub filters: Vec<Filter>,
}

impl Filters {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut filters = Vec::new();
        for inner_pair in pair.into_inner() {
            filters.push(Filter::from_pest(inner_pair)?);
        }
        Ok(Filters { filters })
    }

    pub fn to_sql(&self, sql_dialect: &Box<dyn SqlDialect + Send + Sync>, srid: &str) -> String {
        self.filters
            .iter()
            .map(|filter| filter.to_sql(sql_dialect, srid))
            .collect::<Vec<String>>()
            .join(" AND ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        overpass_parser::{parse_query, request::QueryType},
        sql_dialect::postgres::postgres::Postgres,
    };
    use pretty_assertions::assert_eq;

    fn parse(query: &str) -> Filter {
        match parse_query(format!("node{query};").as_str()) {
            Ok(parsed) => match parsed.queries[0].as_ref() {
                QueryType::QueryObjects(query_objets) => query_objets
                    .filters
                    .as_ref()
                    .unwrap()
                    .filters
                    .first()
                    .unwrap()
                    .clone(),
                _ => panic!("Expected a QueryObjects, got {:?}", parsed.queries[0]),
            },
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_matches_to_sql() {
        let d = Box::new(Postgres::default()) as Box<dyn SqlDialect + Send + Sync>;

        assert_eq!(
            "ST_Intersects(ST_Transform(ST_Envelope('SRID=4326;LINESTRING(2 -1.1, 4 3)'::geometry), 4326), geom)",
            parse("(-1.1,2,3,4)").to_sql(&d, "4326")
        );
        assert_eq!(
            "id = ANY (ARRAY[11111111111111])",
            parse("(11111111111111)").to_sql(&d, "4326")
        );
        assert_eq!(
            "id = ANY (ARRAY[1, 2, 3])",
            parse("(id:1,2,3)").to_sql(&d, "4326")
        );
        assert_eq!(
            "ST_Intersects(geom, (SELECT ST_Union(geom) FROM _a))",
            parse("(area.a)").to_sql(&d, "4326")
        );
        assert_eq!(
            "ST_Intersects(
    geom,
    ST_Transform(
        ST_Buffer(
            ST_Transform(
                (SELECT ST_Union(geom) FROM _a),\x20
                -- Calculate UTM zone from
                32600 +
                CASE WHEN ST_Y(ST_Centroid(
                    (SELECT ST_Union(geom) FROM _a)
                )) >= 0 THEN 1 ELSE 31 END +
                floor(ST_X(ST_Centroid(
                    (SELECT ST_Union(geom) FROM _a)
                ) + 180) / 6)
            ),
            12.3
        ), 4326)
)",
            parse("(around.a:12.3)").to_sql(&d, "4326")
        );
    }
}
