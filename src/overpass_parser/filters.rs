use pest::iterators::Pair;

use derivative::Derivative;
use regex::Regex;

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::Rule;

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct FilterAround {
    pub core: Box<str>,
    pub radius: f64,
}

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Filter {
    pub bbox: Option<(f64, f64, f64, f64)>,
    pub poly: Option<Vec<(f64, f64)>>,
    pub ids: Option<Vec<i64>>,
    pub area_id: Option<Box<str>>,
    pub around: Option<FilterAround>,
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
                        _ => {
                            return Err(pest::error::Error::new_from_span(
                                pest::error::ErrorVariant::CustomError {
                                    message: format!(
                                        "Invalid rule {:?} for FilterAround",
                                        around_inner.as_rule()
                                    ),
                                },
                                around_inner.as_span(),
                            ));
                        }
                    }
                }
                filter.around = Some(around);
            }
            _ => {
                return Err(pest::error::Error::new_from_span(
                    pest::error::ErrorVariant::CustomError {
                        message: format!("Invalid rule {:?} for Filter", pair.as_rule()),
                    },
                    pair.as_span(),
                ));
            }
        }
        Ok(filter)
    }

    fn bbox_clauses(
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        table: &str,
        bbox: (f64, f64, f64, f64),
        srid: &str,
    ) -> String {
        sql_dialect.st_intersects_extent_with_geom(
            table,
            sql_dialect
                .st_transform(
                    &format!(
                        "ST_Envelope('SRID=4326;LINESTRING({} {}, {} {})'::geometry)",
                        bbox.1, bbox.0, bbox.3, bbox.2
                    ),
                    srid,
                )
                .as_str(),
        )
    }

    fn poly_clauses(
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        set: &str,
        poly: &[(f64, f64)],
        srid: &str,
    ) -> String {
        let coords = poly
            .iter()
            .map(|&(lat, lon)| format!("{lon} {lat}"))
            .collect::<Vec<String>>()
            .join(", ");
        sql_dialect.st_intersects_with_geom(
            set,
            &sql_dialect.st_transform(&format!("'SRID=4326;POLYGON({coords})'::geometry"), srid),
        )
    }

    fn around_clause(
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        set: &str,
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
        sql_dialect.st_intersects_with_geom(
            set,
            &sql_dialect.st_transform(
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
                srid,
            ),
        )
    }

    fn area_id_clause(
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        set: &str,
        area_id: &str,
    ) -> (Option<String>, String) {
        (
            Some(format!(
                "JOIN (SELECT {}(geom) AS geom FROM _{area_id}) AS _{area_id}_geom ON true",
                sql_dialect.st_union()
            )),
            sql_dialect.st_intersects_with_geom(set, format!("_{area_id}_geom.geom").as_str()),
        )
    }

    pub fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        set: &str,
        srid: &str,
    ) -> (String, String) {
        let mut clauses = Vec::new();

        if let Some(bbox) = self.bbox {
            clauses.push((None, Self::bbox_clauses(sql_dialect, set, bbox, srid)));
        }
        if let Some(poly) = &self.poly {
            clauses.push((None, Self::poly_clauses(sql_dialect, set, poly, srid)));
        }
        if let Some(ids) = &self.ids {
            clauses.push((None, sql_dialect.id_in_list("id", ids)))
        }
        if let Some(area_id) = &self.area_id {
            clauses.push(Self::area_id_clause(sql_dialect, set, area_id));
        }
        if let Some(around) = &self.around {
            clauses.push((None, Self::around_clause(sql_dialect, set, srid, around)));
        }

        let from = clauses
            .iter()
            .filter_map(|c| c.0.clone())
            .collect::<Vec<String>>()
            .join("\n");
        let clauses = clauses
            .into_iter()
            .map(|c| c.1.replace("\n", "\n    "))
            .collect::<Vec<String>>()
            .join(" AND ");
        (from, clauses)
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

    pub fn has_ids(&self) -> bool {
        self.filters.iter().any(|f| f.ids.is_some())
    }

    pub fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        set: &str,
        srid: &str,
    ) -> (String, String) {
        let s = self
            .filters
            .iter()
            .map(|filter| filter.to_sql(sql_dialect, set, srid))
            .collect::<Vec<(String, String)>>();
        let from = s
            .iter()
            .map(|(from, _)| from.clone())
            .collect::<Vec<String>>()
            .join(" AND ");
        let clauses = s
            .iter()
            .map(|(_, clauses)| clauses.clone())
            .collect::<Vec<String>>()
            .join(" AND ");
        (from, clauses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        overpass_parser::{
            parse_query,
            subrequest::{QueryType, SubrequestType},
        },
        sql_dialect::postgres::postgres::Postgres,
    };
    use pretty_assertions::assert_eq;

    fn parse(query: &str) -> Filter {
        match parse_query(format!("node{query};").as_str()) {
            Ok(parsed) => match parsed.subrequest.queries[0].as_ref() {
                SubrequestType::QueryType(query_type) => match query_type {
                    QueryType::QueryObjects(query_objets) => query_objets
                        .filters
                        .as_ref()
                        .unwrap()
                        .filters
                        .first()
                        .unwrap()
                        .clone(),
                    _ => panic!(
                        "Expected a QueryObjects, got {:?}",
                        parsed.subrequest.queries[0]
                    ),
                },
                _ => panic!(
                    "Expected a QueryType, got {:?}",
                    parsed.subrequest.queries[0]
                ),
            },
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_matches_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(
            "ST_Intersects(
        ST_Transform(ST_Envelope('SRID=4326;LINESTRING(2 -1.1, 4 3)'::geometry), 4326),
        _.geom
    )",
            parse("(-1.1,2,3,4)").to_sql(d, "_", "4326").1
        );
        assert_eq!(
            "id = ANY (ARRAY[11111111111111])",
            parse("(11111111111111)").to_sql(d, "_", "4326").1
        );
        assert_eq!(
            "id = ANY (ARRAY[1, 2, 3])",
            parse("(id:1,2,3)").to_sql(d, "_", "4326").1
        );
        assert_eq!(
            "ST_Intersects(
        _a_geom.geom,
        _.geom
    )",
            parse("(area.a)").to_sql(d, "_", "4326").1
        );
        assert_eq!(
            "ST_Intersects(
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
            ), 4326),
        _.geom
    )",
            parse("(around.a:12.3)").to_sql(d, "_", "4326").1
        );
    }
}
