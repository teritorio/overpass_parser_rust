use pest::iterators::Pair;
use regex::Regex;

use derivative::Derivative;

use crate::sql_dialect::sql_dialect::SqlDialect;
use std::collections::HashMap;

use super::Rule;

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Selector {
    #[derivative(Default(value = "false"))]
    not: bool,
    key: Box<str>,
    operator: Option<Box<str>>,
    value: Option<Box<str>>,
    value_regex: Option<Regex>,
}

impl Selector {
    fn unquote(value: &str) -> &str {
        if (value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\''))
        {
            &value[1..value.len() - 1]
        } else {
            value
        }
    }

    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut selector = Selector::default();
        for inner_pair in pair.into_inner() {
            match inner_pair.as_rule() {
                Rule::not => {
                    selector.not = inner_pair.as_str() == "!";
                }
                Rule::key => {
                    selector.key = Self::unquote(inner_pair.as_str()).into();
                }
                Rule::operator => {
                    selector.operator = Some(inner_pair.as_str().into());
                }
                Rule::value => {
                    let value = Self::unquote(inner_pair.as_str());
                    let operator = selector.operator.as_deref().unwrap();
                    if operator == "~" || operator == "!~" {
                        selector.value_regex = Regex::new(value).ok();
                    } else {
                        selector.value = Some(value.into());
                    }
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!(
                                "Invalid rule {:?} for Selector",
                                inner_pair.as_rule()
                            ),
                        },
                        inner_pair.as_span(),
                    ));
                }
            }
        }
        Ok(selector)
    }

    pub fn matches(&self, tags: &HashMap<&str, &str>) -> Option<Vec<&str>> {
        let m = if self.operator.is_none() {
            let mut c = tags.contains_key(self.key.as_ref());
            if self.not {
                if !c {
                    return Some(vec![]);
                } else {
                    c = !c;
                }
            }
            c
        } else if !tags.contains_key(self.key.as_ref()) {
            false
        } else {
            let value = tags[self.key.as_ref()];
            let operator = self.operator.as_deref().unwrap();
            if self.value.is_some() {
                let self_value = self.value.as_deref().unwrap();
                match operator {
                    "=" => value == self_value,
                    "!=" => value != self_value,
                    _ => panic!("unknow operator {:?}", self.operator),
                }
            } else {
                let self_value = self.value_regex.clone().unwrap();
                match operator {
                    "~" => self_value.is_match(value),
                    "!~" => self_value.is_match(value),
                    _ => panic!("unknow operator {self:?}"),
                }
            }
        };

        if m { Some(vec![&self.key]) } else { None }
    }

    pub fn to_sql(&self, sql_dialect: &(dyn SqlDialect + Send + Sync), _srid: &str) -> String {
        let key = sql_dialect.hash_exists(&self.key);
        if self.operator.is_none() {
            if self.not { format!("NOT {key}") } else { key }
        } else {
            let op = self.operator.as_deref().unwrap();
            let value = match self.value.as_deref() {
                Some(value) => sql_dialect.escape_literal(value),
                None => match self.value_regex.as_ref() {
                    Some(regex) => format!("'{}'", regex.as_str()),
                    None => panic!("Selector without value or value_regex"),
                },
            };
            match op {
                "=" => {
                    if value.is_empty() {
                        format!("NOT {key}")
                    } else {
                        format!(
                            "({} AND {} = {})",
                            key,
                            sql_dialect.hash_get(&self.key),
                            value
                        )
                    }
                }
                "!=" => {
                    format!(
                        "(NOT {} OR {} != {})",
                        key,
                        sql_dialect.hash_get(&self.key),
                        value
                    )
                }
                "~" => {
                    format!(
                        "({} AND {} ~ {})",
                        key,
                        sql_dialect.hash_get(&self.key),
                        value
                    )
                }
                "!~" => {
                    format!(
                        "(NOT {} OR {} !~ {})",
                        key,
                        sql_dialect.hash_get(&self.key),
                        value
                    )
                }
                _ => {
                    panic!(
                        "Unsupported operator '{}' for key '{}'",
                        self.operator.as_deref().unwrap_or(""),
                        self.key
                    )
                }
            }
        }
    }
}

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Selectors {
    pub selectors: Vec<Selector>,
}

impl Selectors {
    pub fn from_pest(pair: Pair<Rule>) -> Result<Self, pest::error::Error<Rule>> {
        let mut selectors = Vec::new();
        for inner_pair in pair.into_inner() {
            selectors.push(Selector::from_pest(inner_pair)?);
        }
        Ok(Selectors { selectors })
    }

    pub fn matches(&self, tags: &HashMap<&str, &str>) -> Option<Vec<&str>> {
        let m = self
            .selectors
            .iter()
            .map(|selector| selector.matches(tags))
            .collect::<Vec<Option<Vec<&str>>>>();
        if m.clone().into_iter().any(|n| n.is_none()) {
            None
        } else {
            let mut n = m
                .into_iter()
                .flat_map(|n| n.unwrap())
                .collect::<Vec<&str>>();
            n.sort();
            n.dedup();
            Some(n)
        }
    }

    pub fn to_sql(&self, sql_dialect: &(dyn SqlDialect + Send + Sync), srid: &str) -> String {
        self.selectors
            .iter()
            .map(|selector| selector.to_sql(sql_dialect, srid))
            .collect::<Vec<String>>()
            .join(" AND ")
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    use crate::{
        overpass_parser::{parse_query, subrequest::QueryType},
        sql_dialect::{
            postgres::postgres::Postgres, sql_dialect::SqlDialect,
        },
    };

    use super::Selectors;

    fn parse(query: &str) -> Selectors {
        match parse_query(format!("node{query};").as_str()) {
            Ok(parsed) => match parsed.subrequests[0].queries[0].as_ref() {
                QueryType::QueryObjects(query_objets) => query_objets.selectors.clone(),
                _ => panic!(
                    "Expected a QueryObjects, got {:?}",
                    parsed.subrequests[0].queries[0]
                ),
            },
            Err(e) => panic!("Failed to parse query: {e}"),
        }
    }

    #[test]
    fn test_match_value() {
        let selector = parse("[p]");
        assert_eq!(
            selector.matches(&HashMap::from([("p", "+48")])),
            Some(vec!["p"])
        );

        let selector = parse("[p=\"+48\"]");
        assert_eq!(
            selector.matches(&HashMap::from([("p", "+48")])),
            Some(vec!["p"])
        );
        assert_eq!(selector.matches(&HashMap::from([("p", "+4")])), None);

        let selector = parse("[p~4]");
        assert_eq!(
            selector.matches(&HashMap::from([("p", "+48")])),
            Some(vec!["p"])
        );
        assert_eq!(selector.matches(&HashMap::from([("p", "+5")])), None);

        let selector = parse("[highway=footway][footway=traffic_island]");
        assert_eq!(
            selector.matches(&HashMap::from([("footway", "highway")])),
            None
        );
        assert_eq!(
            selector.matches(&HashMap::from([
                ("highway", "footway"),
                ("footway", "traffic_island")
            ])),
            Some(vec!["footway", "highway"])
        );

        let selector = parse("[highway=footway][!footway]");
        assert_eq!(
            selector.matches(&HashMap::from([("highway", "footway")])),
            Some(vec!["highway"])
        );

        assert_eq!(
            selector.matches(&HashMap::from([
                ("highway", "footway"),
                ("footway", "traffic_island")
            ])),
            None
        );
    }

    // #[test]
    // fn test_matches_to_overpass() {
    //     let selector = parse("[amenity]");
    //     assert_eq!(selector.to_overpass(), "[amenity]");

    //     let selector = parse("[shop=florist]");
    //     assert_eq!(selector.to_overpass(), "[shop=florist]");

    //     let selector = parse(r#"[shop~"pizza.*"]"#);
    //     assert_eq!(selector.to_overpass(), r#"[shop~"pizza.*"]"#);

    //     let selector = parse("[highway=footway][footway=traffic_island]");
    //     assert_eq!(
    //         selector.to_overpass(),
    //         "[highway=footway][footway=traffic_island]"
    //     );

    //     let selector = parse("[!amenity]");
    //     assert_eq!(selector.to_overpass(), "[!amenity]");

    //     // Sort test
    //     let sorted_selector = parse("[amenity]").sort();
    //     assert_eq!(sorted_selector.to_overpass(), "[amenity]");
    // }

    #[test]
    fn test_matches_to_sql() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(parse("[\"amenity\"]").to_sql(d, "4326"), "tags?'amenity'");
        assert_eq!(parse("['amenity']").to_sql(d, "4326"), "tags?'amenity'");
        assert_eq!(
            parse("[shop=florist]").to_sql(d, "4326"),
            "(tags?'shop' AND tags->>'shop' = 'florist')"
        );
        assert_eq!(
            parse("[shop=\"florist\"]").to_sql(d, "4326"),
            "(tags?'shop' AND tags->>'shop' = 'florist')"
        );
        assert_eq!(
            parse(r#"[shop~"pizza.*"]"#).to_sql(d, "4326"),
            "(tags?'shop' AND tags->>'shop' ~ 'pizza.*')"
        );
        assert_eq!(
            parse("[highway=footway][footway=traffic_island]").to_sql(d, "4326"),
            "(tags?'highway' AND tags->>'highway' = 'footway') AND (tags?'footway' AND tags->>'footway' = 'traffic_island')"
        );
        assert_eq!(parse("[!amenity]").to_sql(d, "4326"), "NOT tags?'amenity'");
    }

    #[test]
    fn test_matches_to_sql_duckdb() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);

        assert_eq!(
            parse("[\"amenity\"]").to_sql(d, "4326"),
            "tags?'amenity'"
        );
        assert_eq!(
            parse("['amenity']").to_sql(d, "4326"),
            "tags?'amenity'"
        );
        assert_eq!(
            parse("[shop=florist]").to_sql(d, "4326"),
            "(tags?'shop' AND tags->>'shop' = 'florist')"
        );
    }

    #[test]
    fn test_matches_to_sql_quote() {
        let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);
        assert_eq!(
            parse(r#"[name="l'l"]"#).to_sql(d, "4326"),
            "(tags?'name' AND tags->>'name' = 'l''l')"
        );

        let d = &Postgres {
            postgres_escape_literal: Some(|s| format!("_{s}_")),
        } as &(dyn SqlDialect + Send + Sync);
        assert_eq!(
            parse(r#"[name="l'l"]"#).to_sql(d, "4326"),
            "(tags?_name_ AND tags->>_name_ = _l'l_)"
        );
    }

    // #[test]
    // fn test_sort() {
    // let d = sql_dialect::postgres::postgres::Postgres::default();
    //     assert_eq!(
    //         parse(r#"[power!~"no|cable|line|minor_line$"][power]"#)
    //             .sort()
    //             .to_sql(&d, 4326),
    //         "tags?'power' AND (NOT tags?'power' OR tags->>'power' !~ '(no|cable|line|minor_line$)')"
    //     );
    // }
}
