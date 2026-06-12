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
    pub recurse: Option<Box<str>>,
}

impl Filter {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut filter = Filter::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::filter_bbox => {
                    let coords: Vec<f64> = inner_pair
                        .as_str()
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    if coords.len() == 4 {
                        filter.bbox = Some((coords[0], coords[1], coords[2], coords[3]));
                    }
                }
                Rule::filter_poly => {
                    let a = inner_pair.into_inner().next().unwrap().as_str();
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
                    if let Ok(id) = inner_pair.as_str().parse::<i64>() {
                        filter.ids = Some(vec![id]);
                    }
                }
                Rule::filter_osm_ids => {
                    let ids: Vec<i64> = inner_pair
                        .into_inner()
                        .filter_map(|id_pair| id_pair.as_str().parse().ok())
                        .collect();
                    filter.ids = Some(ids);
                }
                Rule::filter_area => {
                    filter.area_id = inner_pair
                        .into_inner()
                        .find(|p| p.as_rule() == Rule::ID)
                        .map(|p| p.as_str().into());
                }
                Rule::filter_around => {
                    let mut around = FilterAround::default();
                    for around_inner in inner_pair.into_inner() {
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
                Rule::filter_recurse => {
                    filter.recurse = Some(inner_pair.as_str().into());
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!("Invalid rule {:?} for Filter", inner_pair.as_rule()),
                        },
                        inner_pair.as_span(),
                    ));
                }
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
    {}
FROM
    (VALUES ({poly})) AS p(geom)",
                    sql_dialect.make_geom_fields()
                )
                .to_string(),
            },
            SubrequestJoin {
                precompute_set: None,
                precompute: sql_dialect
                    .is_precompute()
                    .then(|| vec![poly_id.to_string()]),
                from: (!sql_dialect.is_precompute()).then(|| format!("JOIN _{poly_id} ON true")),
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
        let join = sql_dialect.st_subdivide(
            &sql_dialect.st_dump_geom(&sql_dialect.st_transform_reverse(
                &sql_dialect.st_buffer(
                    &sql_dialect.st_transform(&sql_dialect.st_union_agg("geom"), srid),
                    around.radius,
                ),
                srid,
            )),
            1000,
        );
        let on = sql_dialect.st_intersects_with_geom("subdivided_geom", &format!("{set}.geom"));

        format!(
            "JOIN (SELECT {join} AS geom FROM _{}) AS subdivided_geom ON
    {on}",
            around.core
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
            from: (!sql_dialect.is_precompute()).then(|| format!("JOIN _{area_id} ON true")),
            clauses: sql_dialect
                .st_intersects_with_geom(set, sql_dialect.table_precompute_geom(area_id).as_str()),
        }
    }

    fn recurse_clause(recurse: &str, set: &str, default_set: &str) -> String {
        match recurse {
            // forward from ways: select nodes that are members of ways in the input set
            "w" => format!(
                "JOIN _{default_set} AS w ON w.osm_type = 'w' AND {set}.osm_type = 'n' AND array[{set}.id] <@ w.nodes"
            ),
            // forward from relations: select elements that are members of relations in the input set
                "r" => format!(
                "JOIN _{default_set} AS r ON r.osm_type = 'r'
    JOIN LATERAL jsonb_to_recordset(r.members) AS m(type text, ref bigint, role text) ON m.type = {set}.osm_type AND m.ref = {set}.id"
            ),
            // backward from nodes: select ways/relations that contain nodes from the input set
            "bn" => format!(
                "JOIN _{default_set} AS bn ON _.osm_type = 'n' AND (
    bn.osm_type = 'w' AND array[bn.id] <@ {set}.nodes OR
    bn.osm_type = 'r' AND array[bn.id] <@ osm_base_idx_nodes_members({set}.members, 'n')
)"
            ),
            // backward from ways: select relations that contain ways from the input set
            "bw" => format!(
                "JOIN _{default_set} AS bw ON {set}.osm_type = 'w' AND bw.osm_type = 'r' AND array[bw.id] <@ osm_base_idx_nodes_members({set}.members, 'w')"
            ),
            // backward from relations: select relations that contain relations from the input set
            "br" => format!(
                "JOIN _{default_set} AS br ON {set}.osm_type = 'r' AND br.osm_type = 'r' AND array[br.id] <@ osm_base_idx_nodes_members({set}.members, 'r')"
            ),
            _ => panic!("Invalid or Not Implemented recurse type: {recurse}"),
        }
    }

    pub fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        set: &str,
        default_set: &str,
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
                clauses: sql_dialect.id_in_list(set, "id", ids),
            })
        }
        if let Some(area_id) = &self.area_id {
            clauses.push(Self::area_id_clause(sql_dialect, set, area_id));
        }
        if let Some(around) = &self.around {
            clauses.push(SubrequestJoin {
                precompute_set: None,
                precompute: None,
                from: Some(Self::around_clause(sql_dialect, set, srid, around)),
                clauses: "true".to_string(),
            });
        }
        if let Some(recurse_type) = &self.recurse {
            clauses.push(SubrequestJoin {
                precompute_set: None,
                precompute: None,
                from: Some(Self::recurse_clause(recurse_type, set, default_set)),
                clauses: "true".to_string(),
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

#[derive(Derivative)]
#[derivative(Default)]
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
        default_set: &str,
        srid: &str,
    ) -> (Option<SubrequestJoin>, SubrequestJoin) {
        let mut pre: Option<SubrequestJoin> = None;
        let s = self
            .filters
            .iter()
            .map(|filter| {
                let (preee, clause) = filter.to_sql(sql_dialect, set, default_set, srid);
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
                    QueryType::QueryObjects(query_objets) => query_objets.filters.clone(),
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
            parse("(-1.1,2,3,4)").to_sql(d, "_", "_d", "9999").1.clauses
        );
        assert_eq!(
            "ST_Intersects(
        _poly_11689077968748950118.geom,
        _.geom
    )",
            parse("(poly:\"1 2 3 4\")")
                .to_sql(d, "_", "_d", "9999")
                .1
                .clauses
        );
        assert_eq!(
            "_.id = ANY (ARRAY[11111111111111])",
            parse("(11111111111111)")
                .to_sql(d, "_", "_d", "9999")
                .1
                .clauses
        );
        assert_eq!(
            "_.id = ANY (ARRAY[1, 2, 3])",
            parse("(id:1,2,3)").to_sql(d, "_", "_d", "9999").1.clauses
        );
        assert_eq!(
            "ST_Intersects(
        _a.geom,
        _.geom
    )",
            parse("(area.a)").to_sql(d, "_", "_d", "9999").1.clauses
        );
        assert_eq!(
            "JOIN (SELECT ST_Subdivide((ST_Dump(ST_Transform(ST_Buffer(ST_Transform(ST_Union(geom), 9999), 12.3), 4326))).geom, 1000) AS geom FROM _a) AS subdivided_geom ON
    ST_Intersects(
    _.geom,
    subdivided_geom.geom
)",
            parse("(around.a:12.3)")
                .to_sql(d, "_", "_d", "9999")
                .1
                .from
                .unwrap()
        );

        // recurse filters — use table-prefixed set so object type can be inferred
        assert_eq!(
            "JOIN _d AS br ON _.osm_type = 'r' AND br.osm_type = 'r' AND array[br.id] <@ osm_base_idx_nodes_members(_.members, 'r')",
            parse("(br)").to_sql(d, "_", "d", "9999").1.from.unwrap()
        );
        assert_eq!(
            "JOIN _d AS bn ON _.osm_type = 'n' AND (
    bn.osm_type = 'w' AND array[bn.id] <@ _.nodes OR
    bn.osm_type = 'r' AND array[bn.id] <@ osm_base_idx_nodes_members(_.members, 'n')
)",
            parse("(bn)").to_sql(d, "_", "d", "9999").1.from.unwrap()
        );

        println!(
            "{}",
            parse("(poly:\"1 2 3 4\")(area.a)")
                .to_sql(d, "_", "d", "9999")
                .1
                .clauses
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
                .to_sql(d, "_", "d", "9999")
                .1
                .clauses
        );
    }
}
