use pest::iterators::Pair;

use derivative::Derivative;
use regex::Regex;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::{Rule, subrequest::SubrequestJoin};

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
    ) -> (SubrequestJoin, SubrequestJoin) {
        let coords = poly
            .iter()
            .map(|&(lat, lon)| format!("{lon} {lat}"))
            .collect::<Vec<String>>()
            .join(", ");
        let poly =
            &sql_dialect.st_transform(&format!("'SRID=4326;POLYGON(({coords}))'::geometry"), srid);

        let mut hasher = DefaultHasher::new();
        poly.hash(&mut hasher);
        let poly_id = format!("poly_{}", hasher.finish());

        (
            SubrequestJoin {
                precompute_set: Some(poly_id.to_string()),
                precompute: None,
                from: None,
                clauses: format!(
                    "SELECT
    geom,
    STRUCT_PACK(
        xmin := ST_XMin(geom),
        ymin := ST_YMin(geom),
        xmax := ST_XMax(geom),
        ymax := ST_YMax(geom)
    ) AS bbox
FROM
    VALUES(({poly})) AS p(geom)"
                )
                .to_string(),
            },
            SubrequestJoin {
                precompute_set: None,
                precompute: sql_dialect
                    .is_precompute()
                    .then(|| vec![poly_id.to_string()]),
                from: (!sql_dialect.is_precompute())
                    .then(|| format!("    JOIN _{poly_id} ON true")),
                clauses: sql_dialect.st_intersects_with_geom(
                    set,
                    sql_dialect.table_precompute_geom(poly_id.as_str()).as_str(),
                ),
            },
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
    ) -> SubrequestJoin {
        SubrequestJoin {
            precompute_set: None,
            precompute: sql_dialect
                .is_precompute()
                .then(|| vec![area_id.to_string()]),
            from: (!sql_dialect.is_precompute()).then(|| format!("    JOIN _{area_id} ON true")),
            clauses: sql_dialect
                .st_intersects_with_geom(set, sql_dialect.table_precompute_geom(area_id).as_str()),
        }
    }

    pub fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        set: &str,
        srid: &str,
    ) -> (Option<SubrequestJoin>, SubrequestJoin) {
        let mut pre: Option<SubrequestJoin> = None;
        let mut clauses = Vec::new();

        if let Some(bbox) = self.bbox {
            clauses.push(SubrequestJoin {
                precompute_set: None,
                precompute: None,
                from: None,
                clauses: Self::bbox_clauses(sql_dialect, set, bbox, srid),
            });
        }
        if let Some(poly) = &self.poly {
            let (preee, clause) = Self::poly_clauses(sql_dialect, set, poly, srid);
            pre = Some(preee);
            clauses.push(clause);
        }
        if let Some(ids) = &self.ids {
            clauses.push(SubrequestJoin {
                precompute_set: None,
                precompute: None,
                from: None,
                clauses: sql_dialect.id_in_list("id", ids),
            })
        }
        if let Some(area_id) = &self.area_id {
            clauses.push(Self::area_id_clause(sql_dialect, set, area_id));
        }
        if let Some(around) = &self.around {
            clauses.push(SubrequestJoin {
                precompute_set: None,
                precompute: None,
                from: None,
                clauses: Self::around_clause(sql_dialect, set, srid, around),
            });
        }

        let precompute = clauses
            .iter()
            .filter_map(|c| c.precompute.clone())
            .flatten()
            .collect();
        let from = clauses
            .iter()
            .filter_map(|c| c.from.clone())
            .collect::<Vec<String>>();
        let clauses_join = clauses
            .into_iter()
            .map(|c| c.clauses.replace("\n", "\n    "))
            .collect::<Vec<String>>()
            .join(" AND ");

        (
            pre,
            SubrequestJoin {
                precompute_set: None,
                precompute: Some(precompute),
                from: (!from.is_empty()).then(|| from.join("\n")),
                clauses: clauses_join,
            },
        )
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
    ) -> (Option<SubrequestJoin>, SubrequestJoin) {
        let mut pre: Option<SubrequestJoin> = None;
        let s = self
            .filters
            .iter()
            .map(|filter| {
                let (preee, clause) = filter.to_sql(sql_dialect, set, srid);
                if preee.is_some() {
                    pre = preee;
                }
                clause
            })
            .collect::<Vec<SubrequestJoin>>();
        let from = s
            .iter()
            .filter_map(|sj| sj.from.clone())
            .collect::<Vec<String>>()
            .join("\n");
        let clauses = s
            .iter()
            .map(|sj| sj.clauses.clone())
            .collect::<Vec<String>>()
            .join(" AND\n    ");

        (
            pre,
            SubrequestJoin {
                precompute_set: None,
                precompute: Some(
                    s.iter()
                        .filter_map(|c| c.precompute.clone())
                        .flatten()
                        .collect(),
                ),
                from: (!from.is_empty()).then_some(from),
                clauses,
            },
        )
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

    fn parse(query: &str) -> Filters {
        match parse_query(format!("node{query};").as_str()) {
            Ok(parsed) => match parsed.subrequest.queries[0].as_ref() {
                SubrequestType::QueryType(query_type) => match query_type {
                    QueryType::QueryObjects(query_objets) => {
                        query_objets.filters.as_ref().unwrap().clone()
                    }
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
        ST_Transform(ST_Envelope('SRID=4326;LINESTRING(2 -1.1, 4 3)'::geometry), 9999),
        _.geom
    )",
            parse("(-1.1,2,3,4)").to_sql(d, "_", "9999").1.clauses
        );
        assert_eq!(
            "ST_Intersects(
        _poly_11689077968748950118.geom,
        _.geom
    )",
            parse("(poly:\"1 2 3 4\")").to_sql(d, "_", "9999").1.clauses
        );
        assert_eq!(
            "id = ANY (ARRAY[11111111111111])",
            parse("(11111111111111)").to_sql(d, "_", "9999").1.clauses
        );
        assert_eq!(
            "id = ANY (ARRAY[1, 2, 3])",
            parse("(id:1,2,3)").to_sql(d, "_", "9999").1.clauses
        );
        assert_eq!(
            "ST_Intersects(
        _a.geom,
        _.geom
    )",
            parse("(area.a)").to_sql(d, "_", "9999").1.clauses
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
            ), 9999),
        _.geom
    )",
            parse("(around.a:12.3)").to_sql(d, "_", "9999").1.clauses
        );

        // Combined filters
        assert_eq!(
            "ST_Intersects(
        _poly_11689077968748950118.geom,
        _.geom
    ) AND
    ST_Intersects(
        _a.geom,
        _.geom
    )",
            parse("(poly:\"1 2 3 4\")(area.a)")
                .to_sql(d, "_", "9999")
                .1
                .clauses
        );
    }
}
