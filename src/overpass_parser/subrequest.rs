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

impl Query for QueryType {
    fn default_asignation(&self) -> Option<&str> {
        match self {
            QueryType::QueryObjects(query) => query.default_asignation(),
            QueryType::QueryUnion(query) => query.default_asignation(),
            QueryType::QueryRecurse(query) => query.default_asignation(),
        }
    }

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
        sql_dialect: &(dyn SqlDialect + Send + Sync),
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

#[derive(Debug, Clone)]
pub enum SubrequestType {
    QueryType(QueryType),
    Out(Out),
}

impl SubrequestType {
    pub fn asignation(&self) -> &str {
        match self {
            SubrequestType::QueryType(query_type) => query_type.asignation(),
            SubrequestType::Out(out) => out.asignation(),
        }
    }
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
        let mut previous_default_set = "_".to_string();
        let mut subrequest = Subrequest::default();
        for inner in pair.into_inner() {
            match inner.as_rule() {
                Rule::query_sequence => {
                    for query in inner.into_inner() {
                        match QueryType::from_pest(query) {
                            Ok(query_type) => {
                                if let Some(default_asignation) = query_type.default_asignation() {
                                    previous_default_set = default_asignation.into();
                                }
                                subrequest
                                    .queries
                                    .push(Box::new(SubrequestType::QueryType(*query_type)))
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                Rule::out => match Out::from_pest(inner, previous_default_set.as_str()) {
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

    pub fn to_sql(&self, sql_dialect: &(dyn SqlDialect + Send + Sync), srid: &str) -> String {
        let mut previous_default_set = "_";
        let mut outs: Vec<&str> = Vec::new();
        let replace = Regex::new(r"(?m)^").unwrap();
        let with = self
            .queries
            .iter()
            .map(|query| {
                let mut sql = match query.as_ref() {
                    SubrequestType::QueryType(query_type) => {
                        let q = query_type.to_sql(sql_dialect, srid, previous_default_set);
                        if let Some(default_asignation) = query_type.default_asignation() {
                            previous_default_set = default_asignation;
                        }
                        q
                    }
                    SubrequestType::Out(out) => {
                        outs.push(&*out.set);
                        out.to_sql(sql_dialect, srid)
                    }
                };
                sql = replace.replace_all(&sql, "    ").to_string();
                let set = query.as_ref().asignation();
                format!("_{set} AS (\n{sql}\n)")
            })
            .collect::<Vec<String>>();
        let with_join = with.join(",\n");
        let select = self
            .queries
            .iter()
            .filter_map(|query| match query.as_ref() {
                SubrequestType::Out(out) => Some(format!("SELECT * FROM _{}", out.asignation())),
                _ => None,
            })
            .collect::<Vec<String>>()
            .join("\nUNION ALL\n");

        format!("WITH\n{with_join}\n{select}")
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
                assert_ne!("", sql);
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
                assert_eq!("SET statement_timeout = 160000;
WITH
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
        JOIN node ON
            node.id = ANY(way.nodes) AND
            node.geom && way.geom
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
        JOIN node ON
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
        JOIN way ON
            way.id = members.ref
    WHERE
        relation.osm_type = 'r'
)

;", sql);
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
                assert_ne!("", sql);
            }
            Err(e) => {
                println!("Error parsing query: {e}");
                panic!("Parsing fails");
            }
        };
    }
}
