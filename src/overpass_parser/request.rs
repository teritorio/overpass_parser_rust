use pest::iterators::Pair;

use crate::sql_dialect::sql_dialect::SqlDialect;

use derivative::Derivative;

use super::{Rule, subrequest::Subrequest};

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Debug, Clone)]
pub struct Request {
    #[derivative(Default(value = "Some(160)"))]
    pub timeout: Option<u32>,
    pub subrequest: Subrequest,
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
                Rule::subrequest => {
                    match Subrequest::from_pest(inner) {
                        Ok(subrequest) => request.subrequest = subrequest,
                        Err(e) => return Err(e),
                    };
                }
                _ => {
                    return Err(pest::error::Error::new_from_span(
                        pest::error::ErrorVariant::CustomError {
                            message: format!("Invalid rule {:?} for Request", inner.as_rule()),
                        },
                        inner.as_span(),
                    ));
                }
            }
        }
        Ok(request)
    }

    pub fn to_sql(
        &self,
        sql_dialect: &(dyn SqlDialect + Send + Sync),
        srid: &str,
        finalizer: Option<&str>,
    ) -> String {
        let select = self.subrequest.to_sql(sql_dialect, srid);
        let timeout = sql_dialect.statement_timeout(self.timeout.unwrap_or(180).min(500) * 1000);
        format!("{timeout}\n{select}\n;")
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
                    let d = &Postgres::default() as &(dyn SqlDialect + Send + Sync);
                    let sql = request.to_sql(d, "4326", None);
                    assert_ne!("", sql);
                }
                Err(e) => {
                    println!("Error parsing query: {e}");
                    panic!("Parsing fails");
                }
            };
        });
    }
}
