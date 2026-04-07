/// Helper to wrap an XML body with the standard XML declaration.
pub fn wrap_xml(inner: &str) -> String {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>{inner}")
}

/// Escape a string for safe embedding in XML content.
///
/// Handles the five standard XML entities plus control characters that are
/// invalid in XML 1.0 (everything below U+0020 except `\t`, `\n`, `\r`).
pub fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            // XML 1.0 allows \t, \n, \r as valid characters; all other control chars
            // need to be encoded as numeric character references.
            c if (c as u32) < 0x20 && c != '\t' && c != '\n' && c != '\r' => {
                out.push_str(&format!("&#x{:X};", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}
