//! `S3Service` `website` family — extracted from service.rs by audit-2026-05-19.

use super::*;

/// What the website endpoint reads of a bucket's `WebsiteConfiguration`,
/// read as XML (entities and CDATA decoded, comments ignored, element names
/// matched whatever their namespace): the one reader of a stored
/// configuration, so no caller-supplied text is ever sliced by hand.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct WebsiteConfig {
    /// `IndexDocument/Suffix`.
    pub index_suffix: Option<String>,
    /// `ErrorDocument/Key`.
    pub error_key: Option<String>,
    /// Whether `RedirectAllRequestsTo` is present.
    redirect_all: bool,
}

fn malformed_xml() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "The XML you provided was not well-formed or did not validate against our published \
         schema",
    )
}

impl WebsiteConfig {
    /// Read a `WebsiteConfiguration` document. Not well-formed, another
    /// root, or a member the schema requires missing (`ErrorDocument/Key`,
    /// `RedirectAllRequestsTo/HostName`, `IndexDocument/Suffix`,
    /// `RoutingRule/Redirect`) is 400 MalformedXML.
    pub(crate) fn parse(xml: &str) -> Result<Self, AwsServiceError> {
        use quick_xml::events::Event;
        let xml = xml.strip_prefix('\u{feff}').unwrap_or(xml);
        let mut reader = quick_xml::Reader::from_str(xml);
        let mut path: Vec<Vec<u8>> = Vec::new();
        let mut roots = 0usize;
        let mut text = String::new();
        let mut config = WebsiteConfig::default();
        // Required children seen, per open container.
        let (mut index, mut error, mut redirect_all, mut rule) = (None, None, None, None);
        let at = |path: &[Vec<u8>], names: &[&str]| {
            path.len() == names.len()
                && path
                    .iter()
                    .zip(names)
                    .all(|(p, n)| p.as_slice() == n.as_bytes())
        };
        loop {
            match reader.read_event().map_err(|_| malformed_xml())? {
                Event::Start(e) => {
                    let name = e.local_name().as_ref().to_vec();
                    if path.is_empty() {
                        roots += 1;
                        if roots > 1 || name != b"WebsiteConfiguration" {
                            return Err(malformed_xml());
                        }
                    }
                    path.push(name);
                    text.clear();
                    let root = "WebsiteConfiguration";
                    if at(&path, &[root, "IndexDocument"]) {
                        index = Some(false);
                    } else if at(&path, &[root, "ErrorDocument"]) {
                        error = Some(false);
                    } else if at(&path, &[root, "RedirectAllRequestsTo"]) {
                        redirect_all = Some(false);
                        config.redirect_all = true;
                    } else if at(&path, &[root, "RoutingRules", "RoutingRule"]) {
                        rule = Some(false);
                    }
                }
                Event::Empty(e) => {
                    let name = e.local_name().as_ref().to_vec();
                    if path.is_empty() {
                        return Err(malformed_xml());
                    }
                    let mut child = path.clone();
                    child.push(name);
                    let root = "WebsiteConfiguration";
                    // A container written empty has none of its required
                    // children; an empty leaf is present and empty.
                    if at(&child, &[root, "IndexDocument"])
                        || at(&child, &[root, "ErrorDocument"])
                        || at(&child, &[root, "RedirectAllRequestsTo"])
                        || at(&child, &[root, "RoutingRules", "RoutingRule"])
                    {
                        return Err(malformed_xml());
                    }
                    if at(&child, &[root, "IndexDocument", "Suffix"]) {
                        config.index_suffix = Some(String::new());
                        index = Some(true);
                    } else if at(&child, &[root, "ErrorDocument", "Key"]) {
                        config.error_key = Some(String::new());
                        error = Some(true);
                    } else if at(&child, &[root, "RedirectAllRequestsTo", "HostName"]) {
                        redirect_all = Some(true);
                    } else if at(&child, &[root, "RoutingRules", "RoutingRule", "Redirect"]) {
                        rule = Some(true);
                    }
                }
                Event::Text(t) => {
                    let decoded = t.unescape().map_err(|_| malformed_xml())?;
                    if path.is_empty() && !decoded.trim().is_empty() {
                        return Err(malformed_xml());
                    }
                    text.push_str(&decoded);
                }
                Event::CData(c) => text.push_str(&String::from_utf8_lossy(&c.into_inner())),
                Event::End(_) => {
                    let root = "WebsiteConfiguration";
                    if at(&path, &[root, "IndexDocument", "Suffix"]) {
                        config.index_suffix = Some(std::mem::take(&mut text));
                        index = Some(true);
                    } else if at(&path, &[root, "ErrorDocument", "Key"]) {
                        config.error_key = Some(std::mem::take(&mut text));
                        error = Some(true);
                    } else if at(&path, &[root, "RedirectAllRequestsTo", "HostName"]) {
                        redirect_all = Some(true);
                    } else if at(&path, &[root, "RoutingRules", "RoutingRule", "Redirect"]) {
                        rule = Some(true);
                    } else if at(&path, &[root, "IndexDocument"]) {
                        if index != Some(true) {
                            return Err(malformed_xml());
                        }
                    } else if at(&path, &[root, "ErrorDocument"]) {
                        if error != Some(true) {
                            return Err(malformed_xml());
                        }
                    } else if at(&path, &[root, "RedirectAllRequestsTo"]) {
                        if redirect_all != Some(true) {
                            return Err(malformed_xml());
                        }
                    } else if at(&path, &[root, "RoutingRules", "RoutingRule"]) {
                        if rule != Some(true) {
                            return Err(malformed_xml());
                        }
                        rule = None;
                    }
                    if path.pop().is_none() {
                        return Err(malformed_xml());
                    }
                    text.clear();
                }
                Event::Eof => break,
                // Comments, processing instructions, the declaration.
                _ => {}
            }
        }
        if roots != 1 || !path.is_empty() {
            return Err(malformed_xml());
        }
        Ok(config)
    }

    /// The rules PutBucketWebsite applies beyond the schema (S3 API
    /// reference, `IndexDocument`: "The suffix must not be empty and must not
    /// include a slash character"; a configuration with no
    /// `RedirectAllRequestsTo` must name an index document).
    pub(crate) fn validate(self) -> Result<Self, AwsServiceError> {
        let invalid = |message: &str| {
            AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgument", message)
        };
        match self.index_suffix.as_deref() {
            None if !self.redirect_all => Err(invalid(
                "A value for IndexDocument Suffix must be provided if RedirectAllRequestsTo is empty",
            )),
            Some(suffix) if suffix.is_empty() || suffix.contains('/') => {
                Err(invalid("The IndexDocument Suffix is not well formed"))
            }
            _ => Ok(self),
        }
    }

    /// The configuration a bucket holds, read for serving. A stored
    /// document that does not read (one stored before it was validated)
    /// serves as no website configuration.
    pub(crate) fn of_stored(xml: &str) -> Option<Self> {
        Self::parse(xml).ok()
    }
}

impl S3Service {
    // ---- Website ----

    pub(crate) fn put_bucket_website(
        &self,
        account_id: &str,
        req: &AwsRequest,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body_str = std::str::from_utf8(&req.body).unwrap_or("").to_string();
        // Refused before anything is stored: what is stored is a
        // configuration the website endpoint can read.
        WebsiteConfig::parse(&body_str)?.validate()?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.website_config = Some(body_str.clone());
        self.store
            .put_bucket_subresource(bucket, BucketSubresource::Website, &body_str)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::OK))
    }

    pub(crate) fn get_bucket_website(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let __empty = crate::state::S3State::new(account_id, "us-east-1");
        let state = accts.get(account_id).unwrap_or(&__empty);
        let b = state
            .buckets
            .get(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        match &b.website_config {
            Some(config) => Ok(s3_xml(StatusCode::OK, config.clone())),
            None => Err(AwsServiceError::aws_error_with_fields(
                StatusCode::NOT_FOUND,
                "NoSuchWebsiteConfiguration",
                "The specified bucket does not have a website configuration",
                vec![("BucketName".to_string(), bucket.to_string())],
            )),
        }
    }

    pub(crate) fn delete_bucket_website(
        &self,
        account_id: &str,
        bucket: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accts = self.state.write();
        let state = accts.get_or_create(account_id);
        let b = state
            .buckets
            .get_mut(bucket)
            .ok_or_else(|| no_such_bucket(bucket))?;
        b.website_config = None;
        self.store
            .delete_bucket_subresource(bucket, BucketSubresource::Website)
            .map_err(crate::service::persistence_error)?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}
