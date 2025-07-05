use crate::overpass_parser::out::Out;
use pest::iterators::Pair;
use regex::Regex;

use crate::sql_dialect::sql_dialect::SqlDialect;

use derivative::Derivative;

use super::{
    Rule, query::Query, query_objects::QueryObjects, query_recurse::QueryRecurse,
    query_union::QueryUnion,
};

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
                    message: "Invalid rule for QueryType".to_string(),
                },
                pair.as_span(),
            )),
        }
    }

    fn to_sql(&self, sql_dialect: &Box<dyn SqlDialect + Send + Sync>, srid: &str, default_set: &str) -> String {
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
pub struct Request {
    #[derivative(Default(value = "Some(160)"))]
    pub timeout: Option<u32>,
    pub queries: Vec<Box<QueryType>>,
    pub out: Option<Out>,
}

impl Request {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut request = Request::default();
        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::metadata => {
                    request.timeout = inner
                        .into_inner()
                        .find(|p| p.as_rule() == Rule::number)
                        .map(|p| p.as_str().parse::<u32>().ok().unwrap());
                }
                Rule::query_sequence => {
                    for query in inner.into_inner() {
                        match QueryType::from_pest(query) {
                            Ok(query_type) => request.queries.push(query_type),
                            Err(e) => return Err(e),
                        }
                    }
                }
                Rule::out => {
                    request.out = Some(Out::from_pest(inner)?);
                }
                _ => {}
            }
        }
        Ok(request)
    }

    pub fn to_sql(
        &self,
        sql_dialect: &Box<dyn SqlDialect + Send + Sync>,
        srid: &str,
        finalizer: Option<&str>,
    ) -> String {
        let mut default_set = "_";
        let replace = Regex::new(r"(?m)^").unwrap();
        let mut with = self
            .queries
            .iter()
            .map(|query| {
                let mut sql = query.to_sql(sql_dialect, srid, default_set);
                sql = replace.replace_all(&sql, "    ").to_string();
                default_set = query.asignation();
                format!("_{default_set} AS (\n{sql}\n)")
            })
            .collect::<Vec<String>>();
        if finalizer.is_some() {
            let mut finalizer = finalizer.unwrap().replace("{{query}}", default_set);
            finalizer = replace.replace_all(&finalizer, "  ").to_string();
            with.push(format!("__finalizer AS (\n{finalizer}\n)"));
            default_set = "__finalizer";
        };
        let with_join = with.join(",\n");
        let select = self
            .out
            .as_ref()
            .unwrap_or(&Out::default())
            .to_sql(sql_dialect, srid);
        let timeout = sql_dialect.statement_timeout(self.timeout.unwrap_or(180).min(500) * 1000);
        format!(
            "{timeout}
WITH
{with_join}
{select}
FROM
    _{default_set}
;"
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{overpass_parser::parse_query, sql_dialect::postgres::postgres::Postgres};

    use super::*;

    #[test]
    fn test_parse() {
        let queries = [
            //
            // Spaces
            "
            node

            ;",
            //
            // Overpasstube wirazrd
            "// @name Drinking Water

            /*
            This is an example Overpass query.
            Try it out by pressing the Run button above!
            You can find more examples with the Load tool.
            */
            [out:json];
            node
                [\"amenity\"=\"drinking_water\"]
                [!loop]
                [foo~\"bar|baz\"]
                (1, 2, 3, 4);
            out;",
        ];
        queries.map(|query| {
            match parse_query(query) {
                Ok(request) => {
                    let d = Box::new(Postgres::default()) as Box<dyn SqlDialect + Send + Sync>;
                    let sql = request.to_sql(&d, "4326", None);
                    assert_ne!("", sql);
                }
                Err(e) => {
                    println!("Error parsing query: {}", e);
                    panic!("Parsing fails");
                }
            };
        });
    }
}
