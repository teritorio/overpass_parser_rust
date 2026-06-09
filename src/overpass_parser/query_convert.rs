use std::collections::HashMap;

use pest::iterators::Pair;

use derivative::Derivative;

use crate::{overpass_parser::evaluator::Evaluator, sql_dialect::sql_dialect::SqlDialect};

use super::{Rule, query::Query, subrequest::SubrequestJoin};

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct ConvertItem {
    pub target: Box<str>,
    pub source: Option<Evaluator>,
}

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct QueryConvert {
    pub object_type: Box<str>,
    pub converts: Vec<Box<ConvertItem>>,
}

impl Query for QueryConvert {
    fn from_pest(pair: Pair<Rule>) -> Result<Box<Self>, pest::error::Error<Rule>> {
        let mut convert = QueryConvert::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::object_type => {
                    convert.object_type = inner_pair.as_str().into();
                }
                Rule::convert_tag_item => {
                    for convert_list_item_pair in inner_pair.into_inner() {
                        match convert_list_item_pair.as_rule() {
                            Rule::convert_key_eval => {
                                for convert_item_pair in convert_list_item_pair.into_inner() {
                                    match convert_item_pair.as_rule() {
                                        Rule::convert_key => {
                                            convert.converts.push(Box::new(ConvertItem {
                                                target: convert_item_pair.as_str().into(),
                                                source: None,
                                            }));
                                        }
                                        Rule::metadata_key => {
                                            if let Some(last) = convert.converts.last_mut() {
                                                last.source =
                                                    Some(Evaluator::from_pest(convert_item_pair)?);
                                            } else {
                                                return Err(pest::error::Error::new_from_span(
                                                    pest::error::ErrorVariant::CustomError {
                                                        message:
                                                            "Metadata key without a convert key"
                                                                .to_string(),
                                                    },
                                                    convert_item_pair.as_span(),
                                                ));
                                            }
                                        }
                                        Rule::eval => {
                                            if let Some(last) = convert.converts.last_mut() {
                                                last.source =
                                                    Some(Evaluator::from_pest(convert_item_pair)?);
                                            } else {
                                                return Err(pest::error::Error::new_from_span(
                                                    pest::error::ErrorVariant::CustomError {
                                                        message: "Eval without a convert key"
                                                            .to_string(),
                                                    },
                                                    convert_item_pair.as_span(),
                                                ));
                                            }
                                        }
                                        _ => {
                                            return Err(pest::error::Error::new_from_span(
                                                pest::error::ErrorVariant::CustomError {
                                                    message: format!(
                                                        "Invalid rule {:?} for ConvertItem",
                                                        convert_item_pair.as_rule()
                                                    ),
                                                },
                                                convert_item_pair.as_span(),
                                            ));
                                        }
                                    }
                                }
                            }
                            Rule::convert_generic_copy => {
                                convert.converts.push(Box::new(ConvertItem {
                                    target: "::".into(),
                                    source: None,
                                }));
                            }
                            Rule::convert_suppress_key => {
                                convert.converts.push(Box::new(ConvertItem {
                                    target: convert_list_item_pair
                                        .as_str()
                                        .trim_start_matches('!')
                                        .into(),
                                    source: None,
                                }));
                            }
                            _ => {
                                return Err(pest::error::Error::new_from_span(
                                    pest::error::ErrorVariant::CustomError {
                                        message: format!(
                                            "Invalid rule {:?} for ConvertItem",
                                            convert_list_item_pair.as_rule()
                                        ),
                                    },
                                    convert_list_item_pair.as_span(),
                                ));
                            }
                        }
                    }
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!(
                                "Invalid rule {:?} for QueryConvert",
                                inner_pair.as_rule()
                            ),
                        },
                        inner_pair.as_span(),
                    ));
                }
            }
        }
        Ok(Box::new(convert))
    }

    fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        default_set: &str,
    ) -> Vec<SubrequestJoin> {
        let mut keys = "{}::jsonb".to_string();

        // If ::=::, copy all the keys from source to target
        if self.converts.iter().any(|c| c.target == "::".into()) {
            keys = "tags".to_string();
        }

        // Remove keys that are suppressed
        for convert in &self.converts {
            if convert.source.is_none() && convert.target != "::".into() {
                keys = format!("{keys} - {}", sql_dialect.escape_literal(&convert.target));
            }
        }

        // Add keys that are converted from metadata
        let mut metadata = HashMap::from([
            ("osm_type", "osm_type".to_string()), // osm_type
            ("id", "id".to_string()),
            ("lon", "".to_string()),
            ("lat", "".to_string()),
            ("created", "created".to_string()),
            ("version", "version".to_string()),
            ("changeset", "changeset".to_string()),
            ("user", "user".to_string()),
            ("uid", "uid".to_string()),
        ]);
        for convert in &self.converts {
            if let Some(source) = &convert.source {
                let source_sql = source.to_sql(sql_dialect, srid, default_set);
                if convert.target.starts_with("::") {
                    let mut target_key = convert.target.trim_start_matches("::");
                    if target_key == "type" {
                        target_key = "osm_type"
                    } else if target_key == "timestamp" {
                        target_key = "created"
                    }
                    metadata
                        .entry(target_key)
                        .and_modify(|v| *v = source_sql.clone())
                        .or_insert(source_sql);
                } else {
                    keys = format!(
                        "{keys} || jsonb_build_object('{}', {})",
                        convert.target, source_sql
                    );
                }
            }
        }

        let mut geom = "geom".to_string();
        if !metadata["lon"].is_empty() && !metadata["lat"].is_empty() {
            geom = format!(
                "ST_SetSRID(ST_MakePoint({}, {}), {})",
                metadata["lon"], metadata["lat"], srid
            );
        }

        let mut metadata_sql_parts = metadata
            .iter()
            .filter(|(k, _)| **k != "lon" && **k != "lat")
            .map(|(k, v)| format!("{} AS {}", v, k))
            .collect::<Vec<_>>();
        metadata_sql_parts.sort();
        let metadata_sql = metadata_sql_parts.join(",\n    ");
        vec![SubrequestJoin {
            precompute_set: None,
            precompute: None,
            from: None,
            clauses: format!(
                "SELECT\n    {keys} AS tags,\n    {metadata_sql},\n    nodes,\n    members,\n    {geom} AS geom\nFROM\n    _{default_set}"
            ),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::overpass_parser::parse_query;
    use crate::overpass_parser::subrequest::{QueryType, SubrequestType};
    use crate::sql_dialect::postgres::postgres::Postgres;
    use pretty_assertions::assert_eq;

    fn parse(query: &str) -> QueryConvert {
        match parse_query(format!("{query};").as_str()) {
            Ok(parsed) => match parsed.subrequest.queries[0].as_ref() {
                SubrequestType::QueryType(query_type) => match query_type {
                    QueryType::QueryConvert(query_objets) => query_objets.clone(),
                    _ => panic!(
                        "Expected a QueryConvert, got {:?}",
                        parsed.subrequest.queries[0]
                    ),
                },
                _ => panic!(
                    "Expected QueryConvert, found {:?}",
                    parsed.subrequest.queries[0]
                ),
            },

            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_convert_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(
            "SELECT
    tags - 'highway' || jsonb_build_object('osm_type', osm_type) AS tags,
    changeset AS changeset,
    created AS created,
    id AS id,
    osm_type AS osm_type,
    uid AS uid,
    user AS user,
    version AS version,
    nodes,
    members,
    ST_SetSRID(ST_MakePoint(ST_Y(ST_PointOnSurface(geom)), ST_X(ST_PointOnSurface(geom))), 9999) AS geom
FROM
    _input",
            parse(
                "
                convert node
                ::=::,
                ::id=id(),
                ::lat=lat(),
                ::lon=lon(),
                osm_type=type(),
                !highway
                ;"
            )
            .to_sql(d, "9999", "input")[0]
                .clauses
        );
    }
}
