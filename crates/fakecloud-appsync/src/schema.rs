//! Derived GraphQL-schema representations for `GetIntrospectionSchema`.
//!
//! AppSync's real `GetIntrospectionSchema` returns the API's schema either as
//! SDL text or as a JSON introspection document. fakecloud stores the raw SDL
//! ingested by `StartSchemaCreation`; this module derives the requested
//! representation from it:
//!
//! * `SDL` -> the stored SDL document verbatim.
//! * `JSON` -> a deterministic JSON *skeleton* that lists the type names parsed
//!   out of the SDL under a `__schema.types` array. This is a faithful,
//!   honestly-scoped projection of the stored schema, NOT a full GraphQL
//!   introspection response (field/arg/directive graphs are not reconstructed).

use serde_json::{json, Value};

/// Extract the top-level type names declared in an SDL document. Recognises the
/// `type`, `input`, `enum`, `interface`, `union`, and `scalar` keywords. This is
/// a lightweight lexical scan, sufficient to project a deterministic type list.
pub fn type_names(sdl: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in sdl.lines() {
        let t = line.trim();
        for kw in [
            "type ",
            "input ",
            "enum ",
            "interface ",
            "union ",
            "scalar ",
        ] {
            if let Some(rest) = t.strip_prefix(kw) {
                if let Some(name) = rest
                    .split(|c: char| c.is_whitespace() || c == '{' || c == '(' || c == '=')
                    .next()
                {
                    if !name.is_empty() {
                        names.push(name.to_string());
                    }
                }
            }
        }
    }
    names
}

/// Build the schema representation bytes for the requested `format`.
///
/// `format` is the `OutputType` query param (`SDL` or `JSON`). Any other value
/// falls back to SDL.
pub fn introspection_bytes(sdl: &str, format: &str) -> Vec<u8> {
    if format.eq_ignore_ascii_case("JSON") {
        let types: Vec<Value> = type_names(sdl)
            .into_iter()
            .map(|n| json!({ "kind": "OBJECT", "name": n }))
            .collect();
        let doc = json!({
            "__schema": {
                "queryType": { "name": "Query" },
                "types": types,
            }
        });
        serde_json::to_vec(&doc).unwrap_or_default()
    } else {
        sdl.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_type_names() {
        let sdl =
            "type Query { hello: String }\ntype Post { id: ID! }\ninput NewPost { t: String }";
        let names = type_names(sdl);
        assert!(names.contains(&"Query".to_string()));
        assert!(names.contains(&"Post".to_string()));
        assert!(names.contains(&"NewPost".to_string()));
    }

    #[test]
    fn sdl_format_is_verbatim() {
        let sdl = "type Query { hello: String }";
        assert_eq!(introspection_bytes(sdl, "SDL"), sdl.as_bytes());
    }

    #[test]
    fn json_format_lists_types() {
        let sdl = "type Query { hello: String }";
        let bytes = introspection_bytes(sdl, "JSON");
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["__schema"]["types"][0]["name"], json!("Query"));
    }
}
