use serde_json::{json, Map, Value};

/// Apply a transformer config (JSON array of processor objects) to a log event message.
/// The message is initially wrapped as `{"message": "<original>"}`, then each processor
/// is applied in sequence.
pub fn apply_transformer(config: &Value, message: &str) -> Value {
    let mut event = json!({ "message": message });

    let processors = match config.as_array() {
        Some(arr) => arr,
        None => return event,
    };

    for processor in processors {
        let obj = match processor.as_object() {
            Some(o) => o,
            None => continue,
        };
        // Each processor object has exactly one key indicating the type
        for (proc_type, proc_config) in obj {
            apply_processor(proc_type, proc_config, &mut event);
        }
    }

    event
}

fn apply_processor(proc_type: &str, config: &Value, event: &mut Value) {
    match proc_type {
        "parseJSON" => apply_parse_json(config, event),
        "addKeys" => apply_add_keys(config, event),
        "deleteKeys" => apply_delete_keys(config, event),
        "renameKeys" => apply_rename_keys(config, event),
        "moveKeys" => apply_move_keys(config, event),
        "copyValue" => apply_copy_value(config, event),
        "lowerCaseString" => apply_lower_case_string(config, event),
        "upperCaseString" => apply_upper_case_string(config, event),
        "trimString" => apply_trim_string(config, event),
        "splitString" => apply_split_string(config, event),
        "substituteString" => apply_substitute_string(config, event),
        "typeConverter" => apply_type_converter(config, event),
        "csv" => apply_csv(config, event),
        "parseKeyValue" => apply_parse_key_value(config, event),
        "dateTimeConverter" => apply_date_time_converter(config, event),
        _ => {} // Unknown processor, skip
    }
}

// --- Helpers for nested key access (dot-separated paths) ---

fn get_nested<'a>(event: &'a Value, key: &str) -> Option<&'a Value> {
    let parts: Vec<&str> = key.split('.').collect();
    let mut current = event;
    for part in parts {
        current = current.get(part)?;
    }
    Some(current)
}

fn set_nested(event: &mut Value, key: &str, value: Value) {
    let parts: Vec<&str> = key.split('.').collect();
    if parts.is_empty() {
        return;
    }
    let mut current = event;
    for part in &parts[..parts.len() - 1] {
        if !current.get(*part).is_some_and(|v| v.is_object()) {
            current[*part] = json!({});
        }
        current = current.get_mut(*part).unwrap();
    }
    current[*parts.last().unwrap()] = value;
}

fn remove_nested(event: &mut Value, key: &str) -> Option<Value> {
    let parts: Vec<&str> = key.split('.').collect();
    if parts.is_empty() {
        return None;
    }
    if parts.len() == 1 {
        return event.as_object_mut().and_then(|m| m.remove(parts[0]));
    }
    let mut current = event;
    for part in &parts[..parts.len() - 1] {
        current = current.get_mut(*part)?;
    }
    current
        .as_object_mut()
        .and_then(|m| m.remove(*parts.last().unwrap()))
}

// --- Processor implementations ---

fn apply_parse_json(config: &Value, event: &mut Value) {
    let source = config["source"].as_str().unwrap_or("message");
    let destination = config["destination"].as_str();

    let raw = match get_nested(event, source).and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return,
    };

    let parsed: Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return,
    };

    if let Some(dest) = destination {
        set_nested(event, dest, parsed);
    } else {
        // Merge parsed fields into event root
        if let Value::Object(map) = parsed {
            if let Value::Object(ref mut ev) = event {
                for (k, v) in map {
                    ev.insert(k, v);
                }
            }
        }
    }
}

fn apply_add_keys(config: &Value, event: &mut Value) {
    let entries = match config["entries"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for entry in entries {
        if let (Some(key), Some(value)) = (entry["key"].as_str(), entry.get("value")) {
            let val = match value {
                Value::String(s) => Value::String(s.clone()),
                other => other.clone(),
            };
            set_nested(event, key, val);
        }
    }
}

fn apply_delete_keys(config: &Value, event: &mut Value) {
    let keys = match config["withKeys"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for key in keys {
        if let Some(k) = key.as_str() {
            remove_nested(event, k);
        }
    }
}

fn apply_rename_keys(config: &Value, event: &mut Value) {
    let entries = match config["entries"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for entry in entries {
        if let (Some(old_key), Some(new_key)) = (entry["key"].as_str(), entry["renameTo"].as_str())
        {
            if let Some(val) = remove_nested(event, old_key) {
                set_nested(event, new_key, val);
            }
        }
    }
}

fn apply_move_keys(config: &Value, event: &mut Value) {
    let entries = match config["entries"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for entry in entries {
        if let (Some(source), Some(target)) = (entry["source"].as_str(), entry["target"].as_str()) {
            if let Some(val) = remove_nested(event, source) {
                set_nested(event, target, val);
            }
        }
    }
}

fn apply_copy_value(config: &Value, event: &mut Value) {
    let entries = match config["entries"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for entry in entries {
        if let (Some(source), Some(target)) = (entry["source"].as_str(), entry["target"].as_str()) {
            if let Some(val) = get_nested(event, source).cloned() {
                set_nested(event, target, val);
            }
        }
    }
}

fn apply_lower_case_string(config: &Value, event: &mut Value) {
    let keys = match config["withKeys"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for key in keys {
        if let Some(k) = key.as_str() {
            if let Some(val) = get_nested(event, k).and_then(|v| v.as_str()) {
                set_nested(event, k, Value::String(val.to_lowercase()));
            }
        }
    }
}

fn apply_upper_case_string(config: &Value, event: &mut Value) {
    let keys = match config["withKeys"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for key in keys {
        if let Some(k) = key.as_str() {
            if let Some(val) = get_nested(event, k).and_then(|v| v.as_str()) {
                set_nested(event, k, Value::String(val.to_uppercase()));
            }
        }
    }
}

fn apply_trim_string(config: &Value, event: &mut Value) {
    let keys = match config["withKeys"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for key in keys {
        if let Some(k) = key.as_str() {
            if let Some(val) = get_nested(event, k).and_then(|v| v.as_str()) {
                set_nested(event, k, Value::String(val.trim().to_string()));
            }
        }
    }
}

fn apply_split_string(config: &Value, event: &mut Value) {
    let entries = match config["entries"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for entry in entries {
        if let (Some(source), Some(delimiter)) =
            (entry["source"].as_str(), entry["delimiter"].as_str())
        {
            if let Some(val) = get_nested(event, source).and_then(|v| v.as_str()) {
                let parts: Vec<Value> = val
                    .split(delimiter)
                    .map(|s| Value::String(s.to_string()))
                    .collect();
                set_nested(event, source, Value::Array(parts));
            }
        }
    }
}

fn apply_substitute_string(config: &Value, event: &mut Value) {
    let entries = match config["entries"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for entry in entries {
        if let (Some(source), Some(from), Some(to)) = (
            entry["source"].as_str(),
            entry["from"].as_str(),
            entry["to"].as_str(),
        ) {
            if let Some(val) = get_nested(event, source).and_then(|v| v.as_str()) {
                let replaced = match regex::Regex::new(from) {
                    Ok(re) => re.replace_all(val, to).to_string(),
                    Err(_) => val.replace(from, to),
                };
                set_nested(event, source, Value::String(replaced));
            }
        }
    }
}

fn apply_type_converter(config: &Value, event: &mut Value) {
    let entries = match config["entries"].as_array() {
        Some(arr) => arr,
        None => return,
    };
    for entry in entries {
        if let (Some(key), Some(target_type)) = (entry["key"].as_str(), entry["type"].as_str()) {
            if let Some(val) = get_nested(event, key).cloned() {
                let converted = match target_type {
                    "integer" => {
                        if let Some(s) = val.as_str() {
                            s.parse::<i64>().ok().map(|n| json!(n))
                        } else {
                            val.as_f64().map(|f| json!(f as i64))
                        }
                    }
                    "double" | "float" => {
                        if let Some(s) = val.as_str() {
                            s.parse::<f64>().ok().map(|n| json!(n))
                        } else {
                            val.as_f64().map(|n| json!(n))
                        }
                    }
                    "string" => match &val {
                        Value::String(_) => Some(val),
                        Value::Number(n) => Some(Value::String(n.to_string())),
                        Value::Bool(b) => Some(Value::String(b.to_string())),
                        _ => Some(Value::String(val.to_string())),
                    },
                    "boolean" => {
                        if let Some(s) = val.as_str() {
                            match s.to_lowercase().as_str() {
                                "true" | "1" => Some(json!(true)),
                                "false" | "0" => Some(json!(false)),
                                _ => None,
                            }
                        } else if let Some(b) = val.as_bool() {
                            Some(json!(b))
                        } else {
                            val.as_i64().map(|n| json!(n != 0))
                        }
                    }
                    _ => None,
                };
                if let Some(c) = converted {
                    set_nested(event, key, c);
                }
            }
        }
    }
}

fn apply_csv(config: &Value, event: &mut Value) {
    let source = config["source"].as_str().unwrap_or("message");
    let delimiter = config["delimiter"].as_str().unwrap_or(",");
    let columns = match config["columns"].as_array() {
        Some(arr) => arr,
        None => return,
    };

    let raw = match get_nested(event, source).and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return,
    };

    let delim_char = delimiter.chars().next().unwrap_or(',');
    let values = parse_csv_fields(&raw, delim_char);

    for (i, col) in columns.iter().enumerate() {
        if let Some(col_name) = col.as_str() {
            if let Some(val) = values.get(i) {
                set_nested(event, col_name, Value::String(val.trim().to_string()));
            }
        }
    }
}

/// Parse CSV fields with RFC 4180 quoted field support.
/// Fields wrapped in double quotes may contain the delimiter and escaped quotes ("").
fn parse_csv_fields(input: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut chars = input.chars().peekable();
    let mut field = String::new();

    while chars.peek().is_some() {
        if chars.peek() == Some(&'"') {
            // Quoted field
            chars.next(); // consume opening quote
            loop {
                match chars.next() {
                    Some('"') => {
                        if chars.peek() == Some(&'"') {
                            // Escaped quote
                            field.push('"');
                            chars.next();
                        } else {
                            // End of quoted field
                            break;
                        }
                    }
                    Some(c) => field.push(c),
                    None => break, // Unterminated quote, end of input
                }
            }
            // Consume delimiter after quoted field if present
            if chars.peek() == Some(&delimiter) {
                chars.next();
            }
            fields.push(std::mem::take(&mut field));
        } else {
            // Unquoted field
            loop {
                match chars.peek() {
                    Some(&c) if c == delimiter => {
                        chars.next();
                        break;
                    }
                    Some(_) => field.push(chars.next().unwrap()),
                    None => break,
                }
            }
            fields.push(std::mem::take(&mut field));
        }
    }

    // Handle trailing delimiter producing an empty final field
    if !input.is_empty() && input.ends_with(delimiter) {
        fields.push(String::new());
    }

    // Handle empty input producing a single empty field
    if fields.is_empty() {
        fields.push(String::new());
    }

    fields
}

fn apply_parse_key_value(config: &Value, event: &mut Value) {
    let source = config["source"].as_str().unwrap_or("message");
    let destination = config["destination"].as_str();

    let raw = match get_nested(event, source).and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return,
    };

    let mut parsed = Map::new();
    // Parse key=value or key="value" pairs
    let re = regex::Regex::new(r#"(\w+)=(?:"([^"]*)"|(\S+))"#).unwrap();
    for caps in re.captures_iter(&raw) {
        let key = caps.get(1).unwrap().as_str();
        let value = caps
            .get(2)
            .or_else(|| caps.get(3))
            .map(|m| m.as_str())
            .unwrap_or("");
        parsed.insert(key.to_string(), Value::String(value.to_string()));
    }

    let parsed_value = Value::Object(parsed);
    if let Some(dest) = destination {
        set_nested(event, dest, parsed_value);
    } else {
        // Merge into root
        if let Value::Object(map) = parsed_value {
            if let Value::Object(ref mut ev) = event {
                for (k, v) in map {
                    ev.insert(k, v);
                }
            }
        }
    }
}

fn apply_date_time_converter(config: &Value, event: &mut Value) {
    let source = config["source"].as_str().unwrap_or("timestamp");
    let target = config["target"].as_str();
    let match_patterns = config["matchPatterns"].as_array();

    let raw = match get_nested(event, source) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        _ => return,
    };

    let patterns = match match_patterns {
        Some(arr) => arr,
        None => return,
    };

    // Try each pattern, use the first that succeeds
    let mut parsed_value: Option<String> = None;
    for pattern in patterns {
        if let Some(pat) = pattern.as_str() {
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&raw, pat) {
                parsed_value = Some(dt.format("%Y-%m-%dT%H:%M:%S").to_string());
                break;
            }
            // Also try NaiveDate for date-only patterns
            if let Ok(d) = chrono::NaiveDate::parse_from_str(&raw, pat) {
                parsed_value = Some(d.format("%Y-%m-%d").to_string());
                break;
            }
        }
    }

    if let Some(val) = parsed_value {
        let dest = target.unwrap_or(source);
        set_nested(event, dest, Value::String(val));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_json() {
        let config = json!([{"parseJSON": {"source": "message"}}]);
        let result = apply_transformer(&config, r#"{"level":"ERROR","msg":"fail"}"#);
        assert_eq!(result["level"], "ERROR");
        assert_eq!(result["msg"], "fail");
    }

    #[test]
    fn test_parse_json_with_destination() {
        let config = json!([{"parseJSON": {"source": "message", "destination": "parsed"}}]);
        let result = apply_transformer(&config, r#"{"level":"INFO"}"#);
        assert_eq!(result["parsed"]["level"], "INFO");
        assert_eq!(result["message"], r#"{"level":"INFO"}"#);
    }

    #[test]
    fn test_add_keys() {
        let config = json!([{"addKeys": {"entries": [{"key": "env", "value": "prod"}, {"key": "version", "value": "1.0"}]}}]);
        let result = apply_transformer(&config, "hello");
        assert_eq!(result["env"], "prod");
        assert_eq!(result["version"], "1.0");
        assert_eq!(result["message"], "hello");
    }

    #[test]
    fn test_delete_keys() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "tmp", "value": "x"}, {"key": "debug", "value": "y"}]}},
            {"deleteKeys": {"withKeys": ["tmp", "debug"]}}
        ]);
        let result = apply_transformer(&config, "hello");
        assert!(result.get("tmp").is_none());
        assert!(result.get("debug").is_none());
        assert_eq!(result["message"], "hello");
    }

    #[test]
    fn test_rename_keys() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "oldName", "value": "val"}]}},
            {"renameKeys": {"entries": [{"key": "oldName", "renameTo": "newName"}]}}
        ]);
        let result = apply_transformer(&config, "hello");
        assert!(result.get("oldName").is_none());
        assert_eq!(result["newName"], "val");
    }

    #[test]
    fn test_move_keys() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "a.b", "value": "val"}]}},
            {"moveKeys": {"entries": [{"source": "a.b", "target": "c.d"}]}}
        ]);
        let result = apply_transformer(&config, "hello");
        assert_eq!(result["c"]["d"], "val");
        assert!(result["a"].get("b").is_none());
    }

    #[test]
    fn test_copy_value() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "src", "value": "data"}]}},
            {"copyValue": {"entries": [{"source": "src", "target": "dst"}]}}
        ]);
        let result = apply_transformer(&config, "hello");
        assert_eq!(result["src"], "data");
        assert_eq!(result["dst"], "data");
    }

    #[test]
    fn test_lower_case_string() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "name", "value": "HELLO World"}]}},
            {"lowerCaseString": {"withKeys": ["name"]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["name"], "hello world");
    }

    #[test]
    fn test_upper_case_string() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "name", "value": "hello"}]}},
            {"upperCaseString": {"withKeys": ["name"]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["name"], "HELLO");
    }

    #[test]
    fn test_trim_string() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "val", "value": "  hello  "}]}},
            {"trimString": {"withKeys": ["val"]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["val"], "hello");
    }

    #[test]
    fn test_split_string() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "tags", "value": "a,b,c"}]}},
            {"splitString": {"entries": [{"source": "tags", "delimiter": ","}]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["tags"], json!(["a", "b", "c"]));
    }

    #[test]
    fn test_substitute_string() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "msg", "value": "error: bad thing"}]}},
            {"substituteString": {"entries": [{"source": "msg", "from": "error", "to": "warning"}]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["msg"], "warning: bad thing");
    }

    #[test]
    fn test_substitute_string_regex() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "msg", "value": "id=123 id=456"}]}},
            {"substituteString": {"entries": [{"source": "msg", "from": "\\d+", "to": "***"}]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["msg"], "id=*** id=***");
    }

    #[test]
    fn test_type_converter_integer() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "count", "value": "42"}]}},
            {"typeConverter": {"entries": [{"key": "count", "type": "integer"}]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["count"], 42);
    }

    #[test]
    fn test_type_converter_boolean() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "flag", "value": "true"}]}},
            {"typeConverter": {"entries": [{"key": "flag", "type": "boolean"}]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["flag"], true);
    }

    #[test]
    fn test_type_converter_string() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "num", "value": "99"}]}},
            {"typeConverter": {"entries": [{"key": "num", "type": "integer"}]}},
            {"typeConverter": {"entries": [{"key": "num", "type": "string"}]}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["num"], "99");
    }

    #[test]
    fn test_csv() {
        let config = json!([{"csv": {"source": "message", "delimiter": ",", "columns": ["name", "age", "city"]}}]);
        let result = apply_transformer(&config, "Alice,30,NYC");
        assert_eq!(result["name"], "Alice");
        assert_eq!(result["age"], "30");
        assert_eq!(result["city"], "NYC");
    }

    #[test]
    fn test_csv_quoted_fields() {
        let config = json!([{"csv": {"source": "message", "delimiter": ",", "columns": ["name", "desc", "city"]}}]);
        let result = apply_transformer(&config, r#"Alice,"hello, world",NYC"#);
        assert_eq!(result["name"], "Alice");
        assert_eq!(result["desc"], "hello, world");
        assert_eq!(result["city"], "NYC");
    }

    #[test]
    fn test_csv_escaped_quotes() {
        let config =
            json!([{"csv": {"source": "message", "delimiter": ",", "columns": ["name", "quote"]}}]);
        let result = apply_transformer(&config, r#"Alice,"She said ""hi"""#);
        assert_eq!(result["name"], "Alice");
        assert_eq!(result["quote"], r#"She said "hi""#);
    }

    #[test]
    fn test_csv_quoted_field_with_custom_delimiter() {
        let config =
            json!([{"csv": {"source": "message", "delimiter": "|", "columns": ["a", "b", "c"]}}]);
        let result = apply_transformer(&config, r#"one|"two|three"|four"#);
        assert_eq!(result["a"], "one");
        assert_eq!(result["b"], "two|three");
        assert_eq!(result["c"], "four");
    }

    #[test]
    fn test_parse_csv_fields_unit() {
        let fields = parse_csv_fields(r#"a,"b,c",d"#, ',');
        assert_eq!(fields, vec!["a", "b,c", "d"]);

        let fields = parse_csv_fields(r#""escaped ""quotes""",normal"#, ',');
        assert_eq!(fields, vec![r#"escaped "quotes""#, "normal"]);

        // Simple unquoted
        let fields = parse_csv_fields("a,b,c", ',');
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_key_value() {
        let config = json!([{"parseKeyValue": {"source": "message", "destination": "parsed"}}]);
        let result = apply_transformer(&config, r#"user=alice status=200 path="/api""#);
        assert_eq!(result["parsed"]["user"], "alice");
        assert_eq!(result["parsed"]["status"], "200");
        assert_eq!(result["parsed"]["path"], "/api");
    }

    #[test]
    fn test_parse_key_value_no_destination() {
        let config = json!([{"parseKeyValue": {"source": "message"}}]);
        let result = apply_transformer(&config, "host=localhost port=8080");
        assert_eq!(result["host"], "localhost");
        assert_eq!(result["port"], "8080");
    }

    #[test]
    fn test_date_time_converter() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "ts", "value": "2024-01-15"}]}},
            {"dateTimeConverter": {"source": "ts", "matchPatterns": ["%Y-%m-%d"], "target": "parsedTime"}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["parsedTime"], "2024-01-15");
    }

    #[test]
    fn test_date_time_converter_datetime() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "ts", "value": "2024-01-15 10:30:00"}]}},
            {"dateTimeConverter": {"source": "ts", "matchPatterns": ["%Y-%m-%d %H:%M:%S"], "target": "parsedTime"}}
        ]);
        let result = apply_transformer(&config, "test");
        assert_eq!(result["parsedTime"], "2024-01-15T10:30:00");
    }

    #[test]
    fn test_pipeline_add_delete_rename() {
        let config = json!([
            {"addKeys": {"entries": [{"key": "env", "value": "staging"}, {"key": "tmp", "value": "remove_me"}]}},
            {"deleteKeys": {"withKeys": ["tmp"]}},
            {"renameKeys": {"entries": [{"key": "message", "renameTo": "original_message"}]}}
        ]);
        let result = apply_transformer(&config, "hello world");
        assert_eq!(result["env"], "staging");
        assert!(result.get("tmp").is_none());
        assert!(result.get("message").is_none());
        assert_eq!(result["original_message"], "hello world");
    }

    #[test]
    fn test_empty_config() {
        let config = json!([]);
        let result = apply_transformer(&config, "hello");
        assert_eq!(result["message"], "hello");
    }

    #[test]
    fn test_non_array_config() {
        let config = json!("invalid");
        let result = apply_transformer(&config, "hello");
        assert_eq!(result["message"], "hello");
    }

    #[test]
    fn test_csv_trailing_empty_column() {
        let fields = parse_csv_fields("a,b,", ',');
        assert_eq!(fields, vec!["a", "b", ""]);
    }

    #[test]
    fn test_csv_empty_input() {
        let fields = parse_csv_fields("", ',');
        assert_eq!(fields, vec![""]);
    }

    #[test]
    fn test_csv_quoted_with_embedded_delimiter() {
        let fields = parse_csv_fields(r#""hello, world",bar"#, ',');
        assert_eq!(fields, vec!["hello, world", "bar"]);
    }
}
