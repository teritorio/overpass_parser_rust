use std::{
    borrow::Cow,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::overpass_parser::out::Out;
use pest::iterators::Pair;
use regex::Regex;

use crate::sql_dialect::sql_dialect::SqlDialect;

use derivative::Derivative;

use super::{
    Rule, query::Query, query_objects::QueryObjects, query_recurse::QueryRecurse,
    query_union::QueryUnion,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub enum QueryType {
    QueryObjects(QueryObjects),
    QueryUnion(QueryUnion),
    QueryRecurse(QueryRecurse),
}

impl Query for QueryType {
    fn asignation(&self) -> &str {
        match self {
            QueryType::QueryObjects(query) => query.asignation(),
            QueryType::QueryUnion(query) => query.asignation(),
            QueryType::QueryRecurse(query) => query.asignation(),
        }
    }

    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>> {
        match pair.as_rule() {
            Rule::query_object => {
                let query_objects = QueryObjects::from_pest(pair)?;
                Ok(Box::new(QueryType::QueryObjects(*query_objects)))
            }
            Rule::query_union => {
                let query_union = QueryUnion::from_pest(pair)?;
                Ok(Box::new(QueryType::QueryUnion(*query_union)))
            }
            Rule::query_recurse => {
                let query_recurse = QueryRecurse::from_pest(pair)?;
                Ok(Box::new(QueryType::QueryRecurse(*query_recurse)))
            }
            _ => Err(pest::error::Error::new_from_span(
                pest::error::ErrorVariant::CustomError {
                    message: format!("Invalid rule {:?} for QueryType", pair.as_rule()),
                },
                pair.as_span(),
            )),
        }
    }

    fn to_sql(
        &self,
        sql_dialect: &Box<dyn SqlDialect + Send + Sync>,
        srid: &str,
        default_set: &str,
    ) -> String {
        match self {
            QueryType::QueryObjects(query) => query.to_sql(sql_dialect, srid, default_set),
            QueryType::QueryUnion(query) => query.to_sql(sql_dialect, srid, default_set),
            QueryType::QueryRecurse(query) => query.to_sql(sql_dialect, srid, default_set),
        }
    }
}

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Subrequest {
    pub queries: Vec<Box<QueryType>>,
    pub out: Option<Out>,
    #[derivative(Default(
        value = "COUNTER.fetch_add(1, Ordering::SeqCst).to_string().as_str().into()"
    ))]
    pub asignation: Box<str>,
}

impl Subrequest {
    pub fn asignation(&self) -> &str {
        self.asignation.as_ref()
    }

    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut subrequest = Subrequest::default();
        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::query_sequence => {
                    for query in inner.into_inner() {
                        match QueryType::from_pest(query) {
                            Ok(query_type) => subrequest.queries.push(query_type),
                            Err(e) => return Err(e),
                        }
                    }
                }
                Rule::out => {
                    subrequest.out = Some(Out::from_pest(inner)?);
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!("Invalid rule {:?} for Subrequest", inner.as_rule()),
                        },
                        inner.as_span(),
                    ));
                }
            }
        }
        Ok(subrequest)
    }

    pub fn to_sql(&self, sql_dialect: &Box<dyn SqlDialect + Send + Sync>, srid: &str) -> String {
        let mut default_set: Cow<str> = "_".into();
        let replace = Regex::new(r"(?m)^").unwrap();
        let with = self
            .queries
            .iter()
            .map(|query| {
                let mut sql = query.to_sql(sql_dialect, srid, &default_set);
                sql = replace.replace_all(&sql, "    ").to_string();
                default_set = format!("_{}", query.asignation()).into();
                format!("{default_set} AS (\n{sql}\n)")
            })
            .collect::<Vec<String>>();
        let with_join = with.join(",\n");
        let select = self
            .out
            .as_ref()
            .unwrap_or(&Out::default())
            .to_sql(sql_dialect, srid);
        format!(
            "WITH
{with_join}
{select}
FROM
    {default_set}"
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{overpass_parser::parse_query, sql_dialect::postgres::postgres::Postgres};

    use super::*;

    #[test]
    fn test_parse() {
        let query = "
            node
                [\"amenity\"=\"drinking_water\"]
                [!loop]
                [foo~\"bar|baz\"]
                (1, 2, 3, 4);
            out;";
        match parse_query(query) {
            Ok(request) => {
                let d = Box::new(Postgres::default()) as Box<dyn SqlDialect + Send + Sync>;
                let sql = request.to_sql(&d, "4326", None);
                assert_ne!("", sql);
            }
            Err(e) => {
                println!("Error parsing query: {e}");
                panic!("Parsing fails");
            }
        };
    }
}
