use derivative::Derivative;
use pest::iterators::Pair;

use crate::sql_dialect::sql_dialect::SqlDialect;

use super::Rule;

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Evaluator {
    pub fn_call: Option<Box<str>>,
    pub static_value: Option<Box<str>>,
}

impl Evaluator {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut evaluator = Evaluator::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::ID => {
                    evaluator.fn_call = Some(inner_pair.as_str().to_string().into());
                }
                Rule::STRING => {
                    evaluator.static_value = Some(inner_pair.as_str().to_string().into());
                }
                Rule::number => {
                    evaluator.static_value = Some(inner_pair.as_str().to_string().into());
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!(
                                "Invalid rule {:?} for Evaluator",
                                inner_pair.as_rule()
                            ),
                        },
                        inner_pair.as_span(),
                    ));
                }
            }
        }
        Ok(evaluator)
    }

    pub fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        _srid: &str,
        _default_set: &str,
    ) -> String {
        if self.static_value.is_some() {
            sql_dialect.escape_literal(self.static_value.as_ref().unwrap())
        } else if let Some(fn_call) = &self.fn_call {
            let mut name = fn_call.as_ref();
            if name == "type" {
                name = "osm_type"
            } else if name == "timestamp" {
                name = "created"
            } else if name == "lon" {
                name = "ST_Y(ST_PointOnSurface(geom))"
            } else if name == "lat" {
                name = "ST_X(ST_PointOnSurface(geom))"
            }
            name.to_string()
        } else {
            panic!("Evaluator must have either a static value or a function call")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overpass_parser::parse_query;
    use crate::overpass_parser::subrequest::{QueryType, SubrequestType};
    use pretty_assertions::assert_eq;

    fn parse(query: &str) -> Evaluator {
        match parse_query(format!("convert node a={query};").as_str()) {
            Ok(parsed) => match parsed.subrequest.queries[0].as_ref() {
                SubrequestType::QueryType(query_type) => match query_type {
                    QueryType::QueryConvert(convert) => convert.converts[0].source.clone().unwrap(),
                    _ => panic!(
                        "Expected a QueryConvert, got {:?}",
                        parsed.subrequest.queries[0]
                    ),
                },
                _ => panic!(
                    "Expected QueryForeach, found {:?}",
                    parsed.subrequest.queries[0]
                ),
            },
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_parse_constant() {
        let query = "a";
        let eval = parse(query);
        assert_eq!(eval.static_value, Some("a".into()));
    }

    #[test]
    fn test_parse_function() {
        let query = "id()";
        let eval = parse(query);
        assert_eq!(eval.fn_call, Some("id".into()));
    }
}
