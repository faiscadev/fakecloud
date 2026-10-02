//! Wire formats for DynamoDB S3 import and export: decompression, the three
//! import formats (DynamoDB JSON, Amazon Ion text, CSV) and the two export
//! formats (DynamoDB JSON, Amazon Ion text).
//!
//! The AWS attribute wire format is our storage format (`AttributeValue` is a
//! `serde_json::Value`), so DynamoDB JSON rows copy through verbatim; Ion and
//! CSV rows are converted into it here.

use base64::Engine;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use super::helpers::{
    canonical_number, parse_number, MAX_SCIENTIFIC_EXPONENT, MIN_SCIENTIFIC_EXPONENT,
};

/// One imported row, or the reason it could not form an item.
pub(crate) type ParsedRow = Result<HashMap<String, Value>, String>;

/// Decompress an S3 object per the import's `InputCompressionType`. AWS does
/// not sniff the content: the declared type wins, so a gzip object imported
/// with `NONE` fails to parse rather than being transparently decoded.
pub(crate) fn decompress(data: &[u8], compression: &str) -> Result<Vec<u8>, String> {
    use fakecloud_s3::compression::Codec;
    let codec = match compression {
        "GZIP" => Codec::Gzip,
        "ZSTD" => Codec::Zstd,
        _ => return Ok(data.to_vec()),
    };
    fakecloud_s3::compression::decompress(codec, data)
        .map_err(|e| format!("failed to decompress {compression} object: {e}"))
}

// ---------------------------------------------------------------------------
// DynamoDB JSON
// ---------------------------------------------------------------------------

/// Parse a DynamoDB JSON (JSON Lines) object: one `{"Item": {...}}` per line.
/// A line that is not JSON, or not an `Item` wrapper, is an item error.
pub(crate) fn parse_dynamodb_json(text: &str) -> Vec<ParsedRow> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(|line| {
            let parsed: Value =
                serde_json::from_str(line).map_err(|e| format!("Unable to parse item: {e}"))?;
            match parsed.get("Item").and_then(Value::as_object) {
                Some(item) => Ok(item.clone().into_iter().collect()),
                None => Err("Item is not in DynamoDB JSON format: missing \"Item\"".to_string()),
            }
        })
        .collect()
}

/// Render one item as a DynamoDB JSON export line (no trailing newline).
pub(crate) fn dynamodb_json_line(item: &HashMap<String, Value>) -> String {
    serde_json::to_string(&json!({ "Item": item })).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

/// How CSV columns map to attributes: the delimiter, an explicit header (when
/// given, the first line of each object is data), and the declared types of
/// the base-table and secondary-index key attributes. Every other column is
/// imported as a string.
pub(crate) struct CsvOptions {
    pub delimiter: char,
    pub header: Option<Vec<String>>,
    pub key_types: HashMap<String, String>,
}

/// Split CSV text into records per RFC 4180 (a quote inside a quoted field is
/// escaped by doubling it; quoted fields may span lines). A stray quote in an
/// unquoted field is malformed; the error ends parsing of the object, as AWS
/// skips the rest of an object it cannot parse.
fn csv_records(text: &str, delimiter: char) -> (Vec<Vec<String>>, Option<String>) {
    let mut records = Vec::new();
    let mut record: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut field_started_quoted = false;
    let mut chars = text.chars().peekable();
    let mut any_in_record = false;
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        match c {
            '"' if field.is_empty() && !field_started_quoted => {
                in_quotes = true;
                field_started_quoted = true;
                any_in_record = true;
            }
            '"' => {
                return (
                    records,
                    Some("Unescaped double quote in CSV field".to_string()),
                );
            }
            c if c == delimiter => {
                record.push(std::mem::take(&mut field));
                field_started_quoted = false;
                any_in_record = true;
            }
            '\r' if chars.peek() == Some(&'\n') => {}
            '\n' | '\r' => {
                if any_in_record || !field.is_empty() {
                    record.push(std::mem::take(&mut field));
                    records.push(std::mem::take(&mut record));
                }
                field_started_quoted = false;
                any_in_record = false;
            }
            other => {
                if field_started_quoted {
                    return (
                        records,
                        Some("Unexpected character after closing quote in CSV field".to_string()),
                    );
                }
                field.push(other);
                any_in_record = true;
            }
        }
    }
    if in_quotes {
        return (records, Some("Unterminated quoted CSV field".to_string()));
    }
    if any_in_record || !field.is_empty() {
        record.push(field);
        records.push(record);
    }
    (records, None)
}

/// Parse a CSV object into items.
pub(crate) fn parse_csv(text: &str, opts: &CsvOptions) -> Vec<ParsedRow> {
    let (mut records, trailing_error) = csv_records(text, opts.delimiter);
    let mut rows: Vec<ParsedRow> = Vec::new();
    let header: Vec<String> = match &opts.header {
        Some(h) => h.clone(),
        None => {
            if records.is_empty() {
                if let Some(err) = trailing_error {
                    rows.push(Err(err));
                }
                return rows;
            }
            records.remove(0)
        }
    };
    for record in records {
        if record.len() != header.len() {
            rows.push(Err(format!(
                "CSV row has {} columns but the header has {}",
                record.len(),
                header.len()
            )));
            continue;
        }
        let mut item = HashMap::new();
        for (name, value) in header.iter().zip(record) {
            // Empty columns are omitted rather than stored as empty strings.
            if value.is_empty() {
                continue;
            }
            let tag = match opts.key_types.get(name).map(String::as_str) {
                Some("N") => "N",
                Some("B") => "B",
                _ => "S",
            };
            item.insert(name.clone(), json!({ tag: value }));
        }
        rows.push(Ok(item));
    }
    if let Some(err) = trailing_error {
        rows.push(Err(err));
    }
    rows
}

// ---------------------------------------------------------------------------
// Amazon Ion (text)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Ion {
    Null,
    Bool(bool),
    /// Any Ion numeric (int, decimal, float), normalized to a plain decimal
    /// string DynamoDB accepts as a Number.
    Number(String),
    /// A well-formed numeric outside DynamoDB's range. Valid Ion, so parsing
    /// carries on; converting it to an attribute fails the row.
    NumberOutOfRange(String),
    String(String),
    Symbol(String),
    Blob(Vec<u8>),
    Clob(Vec<u8>),
    List(Vec<Annotated>),
    Sexp(Vec<Annotated>),
    Struct(Vec<(String, Annotated)>),
}

#[derive(Debug, Clone, PartialEq)]
struct Annotated {
    annotations: Vec<String>,
    value: Ion,
}

struct IonParser<'a> {
    /// The input as text, for decoding one char at the cursor in O(1).
    text: &'a str,
    src: &'a [u8],
    pos: usize,
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'$'
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

const OPERATOR_CHARS: &[u8] = b"!#%&*+-./;<=>?@^`|~";

impl<'a> IonParser<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text,
            src: text.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn peek_at(&self, off: usize) -> Option<u8> {
        self.src.get(self.pos + off).copied()
    }

    fn starts_with(&self, s: &str) -> bool {
        self.src[self.pos..].starts_with(s.as_bytes())
    }

    fn skip_ws(&mut self) -> Result<(), String> {
        loop {
            match self.peek() {
                Some(b) if b.is_ascii_whitespace() => self.pos += 1,
                Some(b'/') if self.peek_at(1) == Some(b'/') => {
                    while let Some(b) = self.peek() {
                        self.pos += 1;
                        if b == b'\n' {
                            break;
                        }
                    }
                }
                Some(b'/') if self.peek_at(1) == Some(b'*') => {
                    self.pos += 2;
                    loop {
                        if self.pos + 1 >= self.src.len() {
                            return Err("unterminated block comment".to_string());
                        }
                        if self.starts_with("*/") {
                            self.pos += 2;
                            break;
                        }
                        self.pos += 1;
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    fn at_end(&mut self) -> Result<bool, String> {
        self.skip_ws()?;
        Ok(self.pos >= self.src.len())
    }

    fn expect(&mut self, b: u8) -> Result<(), String> {
        self.skip_ws()?;
        if self.peek() == Some(b) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected '{}' at offset {}", b as char, self.pos))
        }
    }

    /// Parse one value, with any `annotation::` prefixes.
    fn value(&mut self) -> Result<Annotated, String> {
        let mut annotations = Vec::new();
        loop {
            self.skip_ws()?;
            let save = self.pos;
            // An annotation is a symbol (identifier or quoted) followed by `::`.
            let sym = match self.peek() {
                Some(b) if is_ident_start(b) => Some(self.identifier()),
                Some(b'\'') if !self.starts_with("'''") => Some(self.quoted('\'')?),
                _ => None,
            };
            if let Some(sym) = sym {
                self.skip_ws()?;
                if self.starts_with("::") {
                    self.pos += 2;
                    annotations.push(sym);
                    continue;
                }
            }
            self.pos = save;
            break;
        }
        let value = self.bare_value()?;
        Ok(Annotated { annotations, value })
    }

    fn bare_value(&mut self) -> Result<Ion, String> {
        self.skip_ws()?;
        let b = self.peek().ok_or("unexpected end of Ion input")?;
        match b {
            b'{' if self.peek_at(1) == Some(b'{') => self.lob(),
            b'{' => self.structure(),
            b'[' => self.list(),
            b'(' => self.sexp(),
            b'"' => Ok(Ion::String(self.quoted('"')?)),
            b'\'' if self.starts_with("'''") => Ok(Ion::String(self.long_string()?)),
            b'\'' => Ok(Ion::Symbol(self.quoted('\'')?)),
            b if is_ident_start(b) => self.keyword_or_symbol(),
            b'+' if self.starts_with("+inf") => {
                Err("Ion float +inf is not a DynamoDB number".into())
            }
            b'-' if self.starts_with("-inf") => {
                Err("Ion float -inf is not a DynamoDB number".into())
            }
            b if b.is_ascii_digit() || b == b'-' => self.number(),
            _ => Err(format!("unexpected character '{}' in Ion input", b as char)),
        }
    }

    fn identifier(&mut self) -> String {
        let start = self.pos;
        while matches!(self.peek(), Some(b) if is_ident_char(b)) {
            self.pos += 1;
        }
        String::from_utf8_lossy(&self.src[start..self.pos]).into_owned()
    }

    fn keyword_or_symbol(&mut self) -> Result<Ion, String> {
        let ident = self.identifier();
        match ident.as_str() {
            "true" => Ok(Ion::Bool(true)),
            "false" => Ok(Ion::Bool(false)),
            "nan" => Err("Ion float nan is not a DynamoDB number".into()),
            "null" => {
                // Typed nulls: `null.string`, `null.struct`, ...
                if self.peek() == Some(b'.')
                    && matches!(self.peek_at(1), Some(b) if is_ident_start(b))
                {
                    self.pos += 1;
                    self.identifier();
                }
                Ok(Ion::Null)
            }
            _ => Ok(Ion::Symbol(ident)),
        }
    }

    fn hex_escape(&mut self, digits: usize) -> Result<char, String> {
        let end = self.pos + digits;
        let hex = self
            .src
            .get(self.pos..end)
            .ok_or("truncated escape sequence")?;
        let code = u32::from_str_radix(std::str::from_utf8(hex).map_err(|e| e.to_string())?, 16)
            .map_err(|_| "invalid hex escape".to_string())?;
        self.pos = end;
        char::from_u32(code).ok_or_else(|| "invalid unicode escape".to_string())
    }

    /// The char at the cursor, decoded from the input text without
    /// re-validating the rest of it (`pos` always sits on a char boundary:
    /// it only advances by ASCII bytes or by a decoded char's length).
    fn next_char(&self) -> Option<char> {
        self.text.get(self.pos..)?.chars().next()
    }

    /// Read the body of a `"`- or `'`-quoted string/symbol (no long strings).
    fn quoted(&mut self, quote: char) -> Result<String, String> {
        self.pos += 1; // opening quote
        let mut out = String::new();
        loop {
            let c = self.next_char().ok_or("unterminated Ion string")?;
            self.pos += c.len_utf8();
            if c == quote {
                return Ok(out);
            }
            if c == '\n' && quote == '"' {
                return Err("newline in Ion short string".to_string());
            }
            if c == '\\' {
                self.escape(&mut out)?;
            } else {
                out.push(c);
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<(), String> {
        let e = self.peek().ok_or("truncated escape sequence")?;
        self.pos += 1;
        match e {
            b'n' => out.push('\n'),
            b't' => out.push('\t'),
            b'r' => out.push('\r'),
            b'0' => out.push('\0'),
            b'a' => out.push('\u{7}'),
            b'b' => out.push('\u{8}'),
            b'f' => out.push('\u{c}'),
            b'v' => out.push('\u{b}'),
            b'\\' => out.push('\\'),
            b'"' => out.push('"'),
            b'\'' => out.push('\''),
            b'/' => out.push('/'),
            b'?' => out.push('?'),
            b'x' => out.push(self.hex_escape(2)?),
            b'u' => out.push(self.hex_escape(4)?),
            b'U' => out.push(self.hex_escape(8)?),
            // Line continuation.
            b'\n' => {}
            b'\r' => {
                if self.peek() == Some(b'\n') {
                    self.pos += 1;
                }
            }
            other => return Err(format!("invalid escape '\\{}'", other as char)),
        }
        Ok(())
    }

    /// One or more adjacent `'''...'''` segments, concatenated.
    fn long_string(&mut self) -> Result<String, String> {
        let mut out = String::new();
        loop {
            self.pos += 3;
            loop {
                if self.starts_with("'''") {
                    self.pos += 3;
                    break;
                }
                let c = self.next_char().ok_or("unterminated Ion long string")?;
                self.pos += c.len_utf8();
                if c == '\\' {
                    self.escape(&mut out)?;
                } else {
                    out.push(c);
                }
            }
            let save = self.pos;
            self.skip_ws()?;
            if !self.starts_with("'''") {
                self.pos = save;
                return Ok(out);
            }
        }
    }

    fn lob(&mut self) -> Result<Ion, String> {
        self.pos += 2;
        self.skip_ws()?;
        let value = match self.peek() {
            Some(b'"') => Ion::Clob(self.quoted('"')?.into_bytes()),
            Some(b'\'') if self.starts_with("'''") => Ion::Clob(self.long_string()?.into_bytes()),
            _ => {
                let start = self.pos;
                while matches!(self.peek(), Some(b) if b != b'}') {
                    self.pos += 1;
                }
                let b64: String = String::from_utf8_lossy(&self.src[start..self.pos])
                    .chars()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                Ion::Blob(
                    base64::engine::general_purpose::STANDARD
                        .decode(b64.as_bytes())
                        .map_err(|e| format!("invalid Ion blob: {e}"))?,
                )
            }
        };
        self.skip_ws()?;
        if !self.starts_with("}}") {
            return Err("expected '}}' closing Ion lob".to_string());
        }
        self.pos += 2;
        Ok(value)
    }

    fn structure(&mut self) -> Result<Ion, String> {
        self.pos += 1;
        let mut fields = Vec::new();
        loop {
            self.skip_ws()?;
            if self.peek() == Some(b'}') {
                self.pos += 1;
                return Ok(Ion::Struct(fields));
            }
            let name = match self.peek() {
                Some(b'"') => self.quoted('"')?,
                Some(b'\'') if self.starts_with("'''") => self.long_string()?,
                Some(b'\'') => self.quoted('\'')?,
                Some(b) if is_ident_start(b) => self.identifier(),
                _ => return Err(format!("expected Ion field name at offset {}", self.pos)),
            };
            self.expect(b':')?;
            let value = self.value()?;
            fields.push((name, value));
            self.skip_ws()?;
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {}
                _ => return Err(format!("expected ',' or '}}' at offset {}", self.pos)),
            }
        }
    }

    fn list(&mut self) -> Result<Ion, String> {
        self.pos += 1;
        let mut items = Vec::new();
        loop {
            self.skip_ws()?;
            if self.peek() == Some(b']') {
                self.pos += 1;
                return Ok(Ion::List(items));
            }
            items.push(self.value()?);
            self.skip_ws()?;
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {}
                _ => return Err(format!("expected ',' or ']' at offset {}", self.pos)),
            }
        }
    }

    fn sexp(&mut self) -> Result<Ion, String> {
        self.pos += 1;
        let mut items = Vec::new();
        loop {
            self.skip_ws()?;
            match self.peek() {
                Some(b')') => {
                    self.pos += 1;
                    return Ok(Ion::Sexp(items));
                }
                Some(b)
                    if OPERATOR_CHARS.contains(&b)
                        && !(b == b'-'
                            && matches!(self.peek_at(1), Some(d) if d.is_ascii_digit())) =>
                {
                    let start = self.pos;
                    while matches!(self.peek(), Some(b) if OPERATOR_CHARS.contains(&b)) {
                        self.pos += 1;
                    }
                    items.push(Annotated {
                        annotations: Vec::new(),
                        value: Ion::Symbol(
                            String::from_utf8_lossy(&self.src[start..self.pos]).into_owned(),
                        ),
                    });
                }
                Some(_) => items.push(self.value()?),
                None => return Err("unterminated Ion s-expression".to_string()),
            }
        }
    }

    fn number(&mut self) -> Result<Ion, String> {
        let start = self.pos;
        while matches!(self.peek(), Some(b) if b.is_ascii_alphanumeric() || matches!(b, b'.' | b'+' | b'-' | b'_' | b':'))
        {
            self.pos += 1;
        }
        let token = std::str::from_utf8(&self.src[start..self.pos]).map_err(|e| e.to_string())?;
        match ion_number(token) {
            Ok(n) => Ok(Ion::Number(n)),
            Err(e) if e.starts_with("Number overflow") || e.starts_with("Number underflow") => {
                Ok(Ion::NumberOutOfRange(e))
            }
            Err(e) => Err(e),
        }
    }
}

/// Normalize an Ion numeric token (int incl. hex/binary, decimal with `d`
/// exponent, or float with `e` exponent) to a plain decimal string.
fn ion_number(token: &str) -> Result<String, String> {
    let bad = || format!("unsupported Ion value '{token}' (timestamps are not DynamoDB types)");
    let cleaned: String = token.chars().filter(|c| *c != '_').collect();
    let (neg, body) = match cleaned.strip_prefix('-') {
        Some(rest) => (true, rest.to_string()),
        None => (false, cleaned.clone()),
    };
    let radix = if body.starts_with("0x") || body.starts_with("0X") {
        Some(16)
    } else if body.starts_with("0b") || body.starts_with("0B") {
        Some(2)
    } else {
        None
    };
    if let Some(radix) = radix {
        let v = u128::from_str_radix(&body[2..], radix).map_err(|_| bad())?;
        return Ok(if neg && v != 0 {
            format!("-{v}")
        } else {
            v.to_string()
        });
    }
    let lower = body.to_ascii_lowercase();
    let (mantissa, exp) = match lower.find(['d', 'e']) {
        Some(i) => {
            let exp = &lower[i + 1..];
            let digits = exp.strip_prefix(['+', '-']).unwrap_or(exp);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad());
            }
            (&lower[..i], exp)
        }
        None => (lower.as_str(), "0"),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    if int_part.is_empty()
        || !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(bad());
    }
    // Decide the range from the coefficient and exponent and only then render
    // the plain decimal: expanding `1d999999999999` digit by digit would
    // allocate without bound and abort the process. An out-of-range number is
    // a row error (counted in ErrorCount), exactly as PutItem rejects it.
    let sign = if neg { "-" } else { "" };
    let literal = format!("{sign}{mantissa}e{exp}");
    let parsed = parse_number(&literal).ok_or_else(bad)?;
    if let Some(e) = parsed.scientific_exponent() {
        if e > MAX_SCIENTIFIC_EXPONENT {
            return Err(format!(
                "Number overflow. Attempting to store a number with magnitude larger than supported range: '{token}'"
            ));
        }
        if e < MIN_SCIENTIFIC_EXPONENT {
            return Err(format!(
                "Number underflow. Attempting to store a number with magnitude smaller than supported range: '{token}'"
            ));
        }
    }
    canonical_number(&literal).ok_or_else(bad)
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Convert an Ion value to a DynamoDB attribute value.
fn ion_to_attribute(v: &Annotated) -> Result<Value, String> {
    let set_kind = v
        .annotations
        .iter()
        .find_map(|a| a.strip_prefix("$dynamodb_"))
        .filter(|k| matches!(*k, "SS" | "NS" | "BS"));
    match (&v.value, set_kind) {
        (Ion::List(members), Some(kind)) => {
            let mut out = Vec::with_capacity(members.len());
            for m in members {
                let member = match (&m.value, kind) {
                    (Ion::String(s) | Ion::Symbol(s), "SS") => Value::String(s.clone()),
                    (Ion::Number(n), "NS") => Value::String(n.clone()),
                    (Ion::NumberOutOfRange(e), _) => return Err(e.clone()),
                    (Ion::Blob(b) | Ion::Clob(b), "BS") => Value::String(b64(b)),
                    _ => return Err(format!("invalid member in $dynamodb_{kind} set")),
                };
                out.push(member);
            }
            Ok(json!({ kind: out }))
        }
        (Ion::Null, _) => Ok(json!({ "NULL": true })),
        (Ion::Bool(b), _) => Ok(json!({ "BOOL": b })),
        (Ion::Number(n), _) => Ok(json!({ "N": n })),
        (Ion::NumberOutOfRange(e), _) => Err(e.clone()),
        (Ion::String(s) | Ion::Symbol(s), _) => Ok(json!({ "S": s })),
        (Ion::Blob(b) | Ion::Clob(b), _) => Ok(json!({ "B": b64(b) })),
        (Ion::List(members), None) => {
            let list: Result<Vec<Value>, String> = members.iter().map(ion_to_attribute).collect();
            Ok(json!({ "L": list? }))
        }
        (Ion::Struct(fields), _) => {
            let mut map = Map::new();
            for (k, fv) in fields {
                map.insert(k.clone(), ion_to_attribute(fv)?);
            }
            Ok(json!({ "M": map }))
        }
        (Ion::Sexp(_), _) => Err("Ion s-expressions have no DynamoDB representation".to_string()),
    }
}

/// One top-level `{Item: {...}}` struct to an item.
fn ion_item(v: &Annotated) -> ParsedRow {
    let Ion::Struct(fields) = &v.value else {
        return Err("Ion top-level value is not an {Item:...} struct".to_string());
    };
    let item = fields
        .iter()
        .find(|(k, _)| k == "Item")
        .map(|(_, v)| v)
        .ok_or("Item is not in DynamoDB Ion format: missing Item")?;
    let Ion::Struct(attrs) = &item.value else {
        return Err("Ion Item is not a struct".to_string());
    };
    let mut out = HashMap::new();
    for (name, value) in attrs {
        out.insert(name.clone(), ion_to_attribute(value)?);
    }
    Ok(out)
}

/// Parse an Amazon Ion text object: a stream of top-level values, each a
/// `{Item:{...}}` struct (or a list of them), with `$ion_1_0` version markers
/// ignored. A syntax error ends parsing of the object.
pub(crate) fn parse_ion(text: &str) -> Vec<ParsedRow> {
    let mut rows = Vec::new();
    let mut parser = IonParser::new(text);
    loop {
        match parser.at_end() {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => {
                rows.push(Err(e));
                break;
            }
        }
        let value = match parser.value() {
            Ok(v) => v,
            Err(e) => {
                rows.push(Err(format!("Unable to parse Ion: {e}")));
                break;
            }
        };
        match &value.value {
            Ion::Symbol(s) if value.annotations.is_empty() && s.starts_with("$ion_") => {}
            Ion::List(members) => rows.extend(members.iter().map(ion_item)),
            _ => rows.push(ion_item(&value)),
        }
    }
    rows
}

fn ion_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn ion_symbol(s: &str) -> String {
    let bytes = s.as_bytes();
    let keyword = matches!(s, "null" | "true" | "false" | "nan");
    if !keyword
        && !bytes.is_empty()
        && is_ident_start(bytes[0])
        && bytes.iter().all(|b| is_ident_char(*b))
    {
        return s.to_string();
    }
    let quoted = ion_string(s);
    format!("'{}'", quoted[1..quoted.len() - 1].replace('\'', "\\'"))
}

/// A DynamoDB Number as an Ion decimal: `5` -> `5.`, `1E5` -> `1d5`.
fn ion_decimal(n: &str) -> String {
    if n.contains(['e', 'E']) {
        n.replace(['e', 'E'], "d")
    } else if n.contains('.') {
        n.to_string()
    } else {
        format!("{n}.")
    }
}

fn ion_blob(b64_text: &str) -> String {
    format!("{{{{{b64_text}}}}}")
}

fn attribute_to_ion(v: &Value) -> String {
    let Some((tag, val)) = v.as_object().and_then(|o| o.iter().next()) else {
        return "null".to_string();
    };
    let strs = |val: &Value| -> Vec<String> {
        val.as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    };
    match tag.as_str() {
        "S" => ion_string(val.as_str().unwrap_or_default()),
        "N" => ion_decimal(val.as_str().unwrap_or("0")),
        "B" => ion_blob(val.as_str().unwrap_or_default()),
        "BOOL" => val.as_bool().unwrap_or(false).to_string(),
        "NULL" => "null".to_string(),
        "SS" => format!(
            "$dynamodb_SS::[{}]",
            strs(val)
                .iter()
                .map(|s| ion_string(s))
                .collect::<Vec<_>>()
                .join(",")
        ),
        "NS" => format!(
            "$dynamodb_NS::[{}]",
            strs(val)
                .iter()
                .map(|s| ion_decimal(s))
                .collect::<Vec<_>>()
                .join(",")
        ),
        "BS" => format!(
            "$dynamodb_BS::[{}]",
            strs(val)
                .iter()
                .map(|s| ion_blob(s))
                .collect::<Vec<_>>()
                .join(",")
        ),
        "L" => format!(
            "[{}]",
            val.as_array()
                .into_iter()
                .flatten()
                .map(attribute_to_ion)
                .collect::<Vec<_>>()
                .join(",")
        ),
        "M" => ion_struct(val.as_object().into_iter().flatten()),
        _ => "null".to_string(),
    }
}

fn ion_struct<'a>(fields: impl Iterator<Item = (&'a String, &'a Value)>) -> String {
    let mut fields: Vec<(&String, &Value)> = fields.collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));
    format!(
        "{{{}}}",
        fields
            .iter()
            .map(|(k, v)| format!("{}:{}", ion_symbol(k), attribute_to_ion(v)))
            .collect::<Vec<_>>()
            .join(",")
    )
}

/// Render one item as a DynamoDB Ion export line (no trailing newline):
/// `$ion_1_0 {Item:{...}}`.
pub(crate) fn ion_line(item: &HashMap<String, Value>) -> String {
    format!("$ion_1_0 {{Item:{}}}", ion_struct(item.iter()))
}

/// One incremental-export record (`Metadata`, `Keys`, optional `OldImage` /
/// `NewImage`) as an Ion line, mirroring the DynamoDB JSON record shape.
pub(crate) fn ion_record_line(record: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(ts) = record
        .pointer("/Metadata/WriteTimestampMicros/N")
        .and_then(Value::as_str)
    {
        parts.push(format!("Metadata:{{WriteTimestampMicros:{ts}}}"));
    }
    for field in ["Keys", "OldImage", "NewImage"] {
        if let Some(obj) = record.get(field).and_then(Value::as_object) {
            parts.push(format!("{field}:{}", ion_struct(obj.iter())));
        }
    }
    format!("$ion_1_0 {{{}}}", parts.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_rows(rows: Vec<ParsedRow>) -> Vec<HashMap<String, Value>> {
        rows.into_iter().map(|r| r.unwrap()).collect()
    }

    #[test]
    fn gzip_and_zstd_round_trip() {
        use std::io::Write;
        let data = b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n";
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(data).unwrap();
        let gz = enc.finish().unwrap();
        assert_eq!(decompress(&gz, "GZIP").unwrap(), data);
        let zs = zstd::stream::encode_all(&data[..], 0).unwrap();
        assert_eq!(decompress(&zs, "ZSTD").unwrap(), data);
        assert_eq!(decompress(data, "NONE").unwrap(), data);
        assert!(decompress(data, "GZIP").is_err());
    }

    #[test]
    fn dynamodb_json_requires_item_wrapper() {
        let rows = parse_dynamodb_json(
            "{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n\nnot json\n{\"pk\":{\"S\":\"b\"}}\n",
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].as_ref().unwrap()["pk"], json!({"S": "a"}));
        assert!(rows[1].is_err());
        assert!(rows[2].is_err());
    }

    #[test]
    fn csv_header_types_quotes_and_empty_columns() {
        let opts = CsvOptions {
            delimiter: ',',
            header: None,
            key_types: HashMap::from([
                ("pk".to_string(), "S".to_string()),
                ("sk".to_string(), "N".to_string()),
            ]),
        };
        let text = "pk,sk,title,year\n\"a,1\",5,\"Women's \"\"Full\"\" Dress\",\nb,6,\"multi\nline\",1973\r\n";
        let rows = ok_rows(parse_csv(text, &opts));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["pk"], json!({"S": "a,1"}));
        assert_eq!(rows[0]["sk"], json!({"N": "5"}));
        assert_eq!(rows[0]["title"], json!({"S": "Women's \"Full\" Dress"}));
        assert!(!rows[0].contains_key("year"), "empty column omitted");
        assert_eq!(rows[1]["title"], json!({"S": "multi\nline"}));
        // Non-key numeric-looking columns stay strings.
        assert_eq!(rows[1]["year"], json!({"S": "1973"}));
    }

    #[test]
    fn csv_explicit_header_and_delimiter() {
        let opts = CsvOptions {
            delimiter: '|',
            header: Some(vec!["pk".into(), "v".into()]),
            key_types: HashMap::new(),
        };
        let rows = parse_csv("a|1\nb|2|extra\n", &opts);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].as_ref().unwrap()["pk"], json!({"S": "a"}));
        assert!(rows[1].is_err(), "column count mismatch is an item error");
    }

    #[test]
    fn csv_unescaped_quote_is_an_error() {
        let opts = CsvOptions {
            delimiter: ',',
            header: None,
            key_types: HashMap::new(),
        };
        let rows = parse_csv("id,value\n\"123\",Women's Full \"Length\" Dress\n", &opts);
        assert!(rows.iter().any(|r| r.is_err()));
    }

    #[test]
    fn ion_parses_documented_example() {
        let text = r#"$ion_1_0
[
  {
    Item:{
      Authors:$dynamodb_SS::["Author1","Author2"],
      Dimensions:"8.5 x 11.0 x 1.5",
      Id:103.,
      InPublication:false,
      PageCount:6d2,
      Price:2d3,
      Ratio:1.50,
      Tiny:25d-3,
      Data:{{aGVsbG8=}},
      Nums:$dynamodb_NS::[1,2.5],
      Nested:{inner:[1, "x", null]},
      'quoted key':null.string,
      // a comment
      Esc:"tab\there é"
    }
  }
]
$ion_1_0 {Item:{Id:104.}}"#;
        let rows = ok_rows(parse_ion(text));
        assert_eq!(rows.len(), 2);
        let r = &rows[0];
        assert_eq!(r["Authors"], json!({"SS": ["Author1", "Author2"]}));
        assert_eq!(r["Id"], json!({"N": "103"}));
        assert_eq!(r["InPublication"], json!({"BOOL": false}));
        assert_eq!(r["PageCount"], json!({"N": "600"}));
        assert_eq!(r["Price"], json!({"N": "2000"}));
        assert_eq!(r["Ratio"], json!({"N": "1.5"}));
        assert_eq!(r["Tiny"], json!({"N": "0.025"}));
        assert_eq!(r["Data"], json!({"B": "aGVsbG8="}));
        assert_eq!(r["Nums"], json!({"NS": ["1", "2.5"]}));
        assert_eq!(
            r["Nested"],
            json!({"M": {"inner": {"L": [{"N": "1"}, {"S": "x"}, {"NULL": true}]}}})
        );
        assert_eq!(r["quoted key"], json!({"NULL": true}));
        assert_eq!(r["Esc"], json!({"S": "tab\there \u{e9}"}));
        assert_eq!(rows[1]["Id"], json!({"N": "104"}));
    }

    #[test]
    fn ion_syntax_error_is_reported() {
        let rows = parse_ion("$ion_1_0 {Item:{pk:\"a\"}}\n$ion_1_0 {Item:{pk:");
        assert_eq!(rows.len(), 2);
        assert!(rows[0].is_ok());
        assert!(rows[1].is_err());
        assert!(parse_ion("{Item:{t:2020-01-01T}}")[0].is_err());
    }

    #[test]
    fn ion_export_line_round_trips() {
        let item: HashMap<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "a \"quoted\"\nline"},
            "n": {"N": "5"},
            "f": {"N": "-1.25"},
            "e": {"N": "1E3"},
            "b": {"B": "aGVsbG8="},
            "ss": {"SS": ["x", "y"]},
            "ns": {"NS": ["1", "2"]},
            "bs": {"BS": ["aGVsbG8="]},
            "l": {"L": [{"BOOL": true}, {"NULL": true}]},
            "m": {"M": {"weird key": {"S": "v"}, "null": {"N": "0"}}}
        }))
        .unwrap();
        let line = ion_line(&item);
        assert!(line.starts_with("$ion_1_0 {Item:{"));
        let parsed = ok_rows(parse_ion(&line));
        let mut expected = item.clone();
        expected.insert("e".into(), json!({"N": "1000"}));
        assert_eq!(parsed[0], expected);
    }

    #[test]
    fn ion_numbers_normalize() {
        assert_eq!(ion_number("0x1F").unwrap(), "31");
        assert_eq!(ion_number("-0b101").unwrap(), "-5");
        assert_eq!(ion_number("1_000").unwrap(), "1000");
        assert_eq!(ion_number("-0.0").unwrap(), "0");
        assert_eq!(ion_number("1.5e2").unwrap(), "150");
        assert_eq!(ion_number("007.100").unwrap(), "7.1");
    }

    #[test]
    fn ion_number_range_is_checked_without_expanding_the_exponent() {
        let started = std::time::Instant::now();
        let err = ion_number("1d999999999999").unwrap_err();
        assert!(err.starts_with("Number overflow"), "{err}");
        let err = ion_number("1d-999999999999").unwrap_err();
        assert!(err.starts_with("Number underflow"), "{err}");
        let err = ion_number("1e9223372036854775807").unwrap_err();
        assert!(err.starts_with("Number overflow"), "{err}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "range checks must not materialize the digits"
        );
        assert_eq!(ion_number("1.5d3").unwrap(), "1500");
        assert_eq!(ion_number("-2.50d-1").unwrap(), "-0.25");
        assert_eq!(ion_number("9.9d125").unwrap().len(), 126);
        assert_eq!(ion_number("0d999999999999").unwrap(), "0");
    }

    #[test]
    fn out_of_range_ion_number_is_a_row_error_not_an_abort() {
        let rows = parse_ion(
            "$ion_1_0 {Item:{pk:\"a\",n:1d999999999999}}\n$ion_1_0 {Item:{pk:\"b\",n:2.}}\n",
        );
        assert_eq!(rows.len(), 2);
        assert!(rows[0].is_err());
        assert_eq!(rows[1].as_ref().unwrap()["n"], json!({"N": "2"}));
    }

    #[test]
    fn long_ion_strings_parse_in_linear_time() {
        let body = "x".repeat(1 << 20);
        let text = format!("$ion_1_0 {{Item:{{pk:\"{body}\",l:\'\'\'{body}\'\'\'}}}}\n");
        let started = std::time::Instant::now();
        let rows = parse_ion(&text);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "string decoding must not rescan the remaining input per char"
        );
        let row = rows[0].as_ref().unwrap();
        assert_eq!(row["pk"]["S"].as_str().unwrap().len(), 1 << 20);
        assert_eq!(row["l"]["S"].as_str().unwrap().len(), 1 << 20);
    }
}
