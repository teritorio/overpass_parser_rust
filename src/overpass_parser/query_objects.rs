use crate::{
    overpass_parser::{filters::Filters, selectors::Selector},
    sql_dialect::sql_dialect::SqlDialect,
};
use pest::iterators::Pair;

use derivative::Derivative;

use super::{Rule, query::Query, selectors::Selectors};

use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct QueryObjects {
    pub object_type: Box<str>,
    pub selectors: Selectors,
    pub filters: Option<Filters>,
    pub set: Option<Box<str>>,
    #[derivative(Default(
        value = "COUNTER.fetch_add(1, Ordering::SeqCst).to_string().as_str().into()"
    ))]
    pub default_asignation: Box<str>,
    pub asignation: Option<Box<str>>,
}

impl Query for QueryObjects {
    fn default_asignation(&self) -> Option<&str> {
        match self.asignation {
            None => Some(&self.default_asignation),
            _ => None,
        }
    }

    fn asignation(&self) -> &str {
        self.asignation
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or(&self.default_asignation)
    }

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
                        Rule::filter => {
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
    ) -> String {
        let p: String;
        let from = if self.set.is_none() {
            self.object_type.as_ref()
        } else if self.set == Some("_".into()) {
            default_set
        } else {
            p = format!("_{}", self.set.as_ref().unwrap());
            p.as_str()
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

        if let Some(filters) = &self.filters {
            where_clauses.push(filters.to_sql(sql_dialect, srid));
        }

        let where_clause = format!("WHERE\n    {}", where_clauses.join(" AND\n    "));

        format!(
            "SELECT
    *
FROM
    {from}
{where_clause}"
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
    *
FROM
    _a
WHERE
    osm_type = 'n' AND
    (tags?'a' AND tags->>'a' = 'b') AND
    ST_Intersects(ST_Transform(ST_Envelope('SRID=4326;LINESTRING(2 1, 4 3)'::geometry), 4326), geom)",
                      parse("node.a[a=b](1,2,3,4)->.b").to_sql(d, "4326", "_")
        );
    }

    #[test]
    fn test_matches_poly_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(
            "SELECT
    *
FROM
    _a
WHERE
    osm_type = 'n' AND
    ST_Intersects(ST_Transform('SRID=4326;POLYGON(2 1, 4 3, 6 5)'::geometry, 4326), geom)",
            parse("node.a(poly:'1 2 3 4 5 6')").to_sql(d, "4326", "_")
        );
    }
}
