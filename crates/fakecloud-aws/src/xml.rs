/// Helper to wrap an XML body with the standard XML declaration.
pub fn wrap_xml(inner: &str) -> String {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>{inner}")
}
