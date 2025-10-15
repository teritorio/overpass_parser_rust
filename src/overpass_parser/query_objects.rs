use crate::{
    overpass_parser::{filters::Filters, selectors::Selector},
    sql_dialect::sql_dialect::SqlDialect,
};
use pest::iterators::Pair;

use derivative::Derivative;

use super::{Rule, query::Query, selectors::Selectors, subrequest::SubrequestJoin};

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct QueryObjects {
    pub object_type: Box<str>,
    pub selectors: Selectors,
    pub filters: Option<Filters>,
    pub set: Option<Box<str>>,
    pub asignation: Option<Box<str>>,
}

impl Query for QueryObjects {
    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>> {
        match pair.as_rule() {
            Rule::query_object => {
                let mut query_objects = QueryObjects::default();
                for inner_pair in pair.into_inner() {
                    match inner_pair.as_rule() {
                        Rule::object_type => {
                            query_objects.object_type = inner_pair.as_str().into();
                        }
                        Rule::selector => {
                            query_objects
                                .selectors
                                .selectors
                                .push(Selector::from_pest(inner_pair)?);
                        }
                        Rule::filters => {
                            query_objects.filters = Some(Filters::from_pest(inner_pair)?);
                        }
                        Rule::ID => {
                            query_objects.set = Some(inner_pair.as_str().into());
                        }
                        Rule::asignation => {
                            query_objects.asignation = Some(
                                inner_pair
                                    .into_inner()
                                    .find(|p| p.as_rule() == Rule::ID)
                                    .map(|p| p.as_str())
                                    .unwrap()
                                    .into(),
                            );
                        }
                        _ => {
                            return Err(pest::error::Error::new_from_span(
                                pest::error::ErrorVariant::CustomError {
                                    message: format!(
                                        "Invalid rule {:?} for QueryObjects",
                                        inner_pair.as_rule()
                                    ),
                                },
                                inner_pair.as_span(),
                            ));
                        }
                    }
                }
                Ok(Box::new(query_objects))
            }
            _ => Err(pest::error::Error::new_from_span(
                pest::error::ErrorVariant::CustomError {
                    message: format!("Invalid rule {:?} for QueryObjects", pair.as_rule()),
                },
                pair.as_span(),
            )),
        }
    }

    fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        default_set: &str,
    ) -> Vec<SubrequestJoin> {
        let p: String;
        let from_table: String = if self.set.is_none() {
            let from: String = self.object_type.clone().into();
            if self.filters.is_some() && self.filters.as_ref().unwrap().has_ids() {
                format!("{from}_by_id")
            } else {
                format!("{from}_by_geom")
            }
        } else if self.set == Some("_".into()) {
            default_set.into()
        } else {
            p = format!("_{}", self.set.as_ref().unwrap());
            p
        };

        let mut where_clauses = Vec::new();

        if self.object_type.as_ref() == "nwr" {
        } else if self.object_type.as_ref() != "area" {
            where_clauses.push(format!(
                "osm_type = '{}'",
                self.object_type.chars().next().unwrap()
            ));
        }

        if !self.selectors.selectors.is_empty() {
            let selectors_sql = self
                .selectors
                .selectors
                .iter()
                .map(|selector| selector.to_sql(sql_dialect, srid))
                .collect::<Vec<String>>()
                .join(" AND ");
            where_clauses.push(selectors_sql);
        }

        let mut pre: Option<SubrequestJoin> = None;
        let mut precomputed = Vec::new();
        let mut from = from_table.clone();
        if let Some(filters) = &self.filters {
            let (pree, sj) = filters.to_sql(sql_dialect, &from, srid);
            if pree.is_some() {
                pre = pree;
            }
            precomputed = sj.precompute.unwrap_or_default();
            if let Some(sj_from) = sj.from {
                from = format!("{from}\n    {sj_from}");
            }
            where_clauses.push(sj.clauses);
        }

        let where_clause = format!("WHERE\n    {}", where_clauses.join(" AND\n    "));

        let mut ret = Vec::new();
        if let Some(r) = pre {
            ret.push(r);
        }
        ret.push(SubrequestJoin {
            precompute_set: None,
            precompute: Some(precomputed),
            from: None,
            clauses: format!(
                "SELECT
    {from_table}.*
FROM
    {from}
{where_clause}"
            ),
        });
        ret
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

    fn parse(query: &str) -> QueryObjects {
        match parse_query(format!("{query};").as_str()) {
            Ok(parsed) => match parsed.subrequest.queries[0].as_ref() {
                SubrequestType::QueryType(query_type) => match query_type {
                    QueryType::QueryObjects(query_objets) => query_objets.clone(),
                    _ => panic!(
                        "Expected a QueryObjects, got {:?}",
                        parsed.subrequest.queries[0]
                    ),
                },
                _ => panic!(
                    "Expected QueryObjects, found {:?}",
                    parsed.subrequest.queries[0]
                ),
            },

            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_matches_bbox_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(
            "SELECT
    _a.*
FROM
    _a
WHERE
    osm_type = 'n' AND
    (tags?'a' AND tags->>'a' = 'b') AND
    ST_Intersects(
        ST_Transform(ST_Envelope('SRID=4326;LINESTRING(2 1, 4 3)'::geometry), 9999),
        _a.geom
    )",
            parse("node.a[a=b](1,2,3,4)->.b").to_sql(d, "9999", "_")[0].clauses
        );
    }

    #[test]
    fn test_matches_poly_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(
            vec!(
                "SELECT
    geom
FROM
    VALUES((ST_Transform('SRID=4326;POLYGON((2 1, 4 3, 6 5))'::geometry, 9999))) AS p(geom)",
                "SELECT
    _a.*
FROM
    _a
        JOIN _poly_15599741043204530343 ON true
WHERE
    osm_type = 'n' AND
    ST_Intersects(
        _poly_15599741043204530343.geom,
        _a.geom
    )"
            ),
            parse("node.a(poly:'1 2 3 4 5 6')")
                .to_sql(d, "9999", "_")
                .iter()
                .map(|i| i.clauses.clone())
                .collect::<Vec<String>>()
        );
    }
}
