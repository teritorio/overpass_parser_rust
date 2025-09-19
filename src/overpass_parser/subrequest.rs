use std::sync::atomic::{AtomicU64, Ordering};

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

impl QueryType {
    pub fn asignation(&self) -> Option<Box<str>> {
        match self {
            QueryType::QueryObjects(query) => query.asignation.clone(),
            QueryType::QueryUnion(query) => query.asignation.clone(),
            QueryType::QueryRecurse(query) => query.asignation.clone(),
        }
    }
}

impl Query for QueryType {
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
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        default_set: &str,
    ) -> SubrequestJoin {
        match self {
            QueryType::QueryObjects(query) => query.to_sql(sql_dialect, srid, default_set),
            QueryType::QueryUnion(query) => query.to_sql(sql_dialect, srid, default_set),
            QueryType::QueryRecurse(query) => query.to_sql(sql_dialect, srid, default_set),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SubrequestType {
    QueryType(QueryType),
    Out(Out),
}

#[derive(Debug, Clone)]
pub struct SubrequestJoin {
    pub precompute: Option<Vec<String>>,
    pub from: Option<String>,
    pub clauses: String,
}

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Subrequest {
    pub queries: Vec<Box<SubrequestType>>,
    #[derivative(Default(
        value = "COUNTER.fetch_add(1, Ordering::SeqCst).to_string().as_str().into()"
    ))]
    pub asignation: Box<str>,
}

impl Subrequest {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut subrequest = Subrequest::default();
        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::query_sequence => {
                    for query in inner.into_inner() {
                        match QueryType::from_pest(query) {
                            Ok(query_type) => subrequest
                                .queries
                                .push(Box::new(SubrequestType::QueryType(*query_type))),
                            Err(e) => return Err(e),
                        }
                    }
                }
                Rule::out => match Out::from_pest(inner) {
                    Ok(out) => subrequest.queries.push(Box::new(SubrequestType::Out(out))),
                    Err(e) => return Err(e),
                },
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

    pub fn to_sql(&self, sql_dialect: &(dyn SqlDialect + Send + Sync), srid: &str) -> Vec<String> {
        let mut precomputed = Vec::new();
        let mut previous_default_set: String = "_".into();
        let replace = Regex::new(r"(?m)^").unwrap();
        let mut clauses = self
            .queries
            .iter()
            .map(|query| match query.as_ref() {
                SubrequestType::QueryType(query_type) => {
                    let sj = query_type.to_sql(sql_dialect, srid, previous_default_set.as_str());
                    precomputed.extend(sj.precompute.unwrap_or_default());
                    let set: String = match query_type.asignation() {
                        Some(asignation) => asignation.to_string(),
                        None => {
                            previous_default_set =
                                COUNTER.fetch_add(1, Ordering::SeqCst).to_string();
                            previous_default_set.clone()
                        }
                    };
                    (false, set, sj.clauses)
                }
                SubrequestType::Out(out) => (
                    true,
                    format!(
                        "out_{}",
                        out.set
                            .clone()
                            .unwrap_or(previous_default_set.as_str().into())
                    ),
                    out.to_sql(sql_dialect, srid, previous_default_set.as_str()),
                ),
            })
            .collect::<Vec<(bool, String, String)>>();
        let mut precomputed_sql = Vec::new();
        clauses = clauses
            .iter()
            .filter(|(is_out, set, sql)| {
                if *is_out || !precomputed.contains(set) {
                    true
                } else {
                    let p = sql_dialect.precompute(set, sql);
                    if p.is_some() {
                        precomputed_sql.append(&mut p.unwrap());
                        false
                    } else {
                        true
                    }
                }
            })
            .map(|(is_out, set, sql)| (*is_out, set.clone(), sql.clone()))
            .collect::<Vec<(bool, String, String)>>();

        let with_join = clauses
            .iter()
            .map(|(_, set, sql)| format!("_{set} AS (\n{}\n)", replace.replace_all(sql, "    ")))
            .collect::<Vec<String>>()
            .join(",\n");
        let select = clauses
            .iter()
            .filter(|(is_out, _, _)| *is_out)
            .map(|(_, set, _sql)| format!("SELECT * FROM _{set}"))
            .collect::<Vec<String>>()
            .join("\nUNION ALL\n");

        precomputed_sql.push(format!("WITH\n{with_join}\n{select}\n;"));
        precomputed_sql
    }
}

#[cfg(test)]
mod tests {
    use crate::{overpass_parser::parse_query, sql_dialect::postgres::postgres::Postgres};
    use pretty_assertions::assert_eq;

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
                let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);
                let sql = request.to_sql(d, "4326", None);
                assert_ne!(vec![""], sql);
            }
            Err(e) => {
                println!("Error parsing query: {e}");
                panic!("Parsing fails");
            }
        };
    }

    #[test]
    fn test_recursive() {
        let query = "
            node(id:1)->.a;
            .a >->.b;";
        match parse_query(query) {
            Ok(request) => {
                let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);
                let sql = request.to_sql(d, "4326", None);
                assert_eq!(vec!["SET statement_timeout = 160000;",
                "WITH
_a AS (
    SELECT
        *
    FROM
        node_by_id
    WHERE
        osm_type = 'n' AND
        id = ANY (ARRAY[1])
),
_b AS (
    SELECT
        way.*
    FROM
        _a AS way
        JOIN node_by_id AS node ON
            node.id = ANY(way.nodes)
    WHERE
        way.osm_type = 'w'
    UNION ALL
    SELECT
        node.*
    FROM
        _a AS relation
        JOIN LATERAL (
            SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'n'
        ) AS members ON
            type = 'w'
        JOIN node_by_id AS node ON
            node.id = members.ref
    WHERE
        relation.osm_type = 'r'
    UNION ALL
    SELECT
        way.*
    FROM
        _a AS relation
        JOIN LATERAL (
            SELECT * FROM jsonb_to_recordset(members) AS t(ref bigint, role text, type text) WHERE type = 'w'
        ) AS members ON
            true
        JOIN way_by_id AS way ON
            way.id = members.ref
    WHERE
        relation.osm_type = 'r'
)

;"], sql);
            }
            Err(e) => {
                println!("Error parsing query: {e}");
                panic!("Parsing fails");
            }
        };
    }

    #[test]
    fn test_out_set() {
        let query = "
            node(1)->.a;
            .a out;";
        match parse_query(query) {
            Ok(request) => {
                let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);
                let sql = request.to_sql(d, "4326", None);
                assert_ne!(vec![""], sql);
            }
            Err(e) => {
                println!("Error parsing query: {e}");
                panic!("Parsing fails");
            }
        };
    }
}
