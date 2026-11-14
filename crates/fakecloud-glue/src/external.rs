//! What Glue reads from neighbouring services. The server wires an
//! implementation backed by Lake Formation and S3 state; without one (unit
//! tests, a Glue-only embedding) the dependent checks are skipped and the
//! dependent output members are left out.

use chrono::{DateTime, Utc};

pub trait GlueExternalView: Send + Sync {
    /// The name of the default row-level security policy Lake Formation has
    /// attached to the table, rendered as `Table.RLSProperties`.
    fn default_rls_policy(
        &self,
        account_id: &str,
        catalog_id: &str,
        database: &str,
        table: &str,
    ) -> Option<String>;

    /// Whether the `s3://` location is under a resource registered with Lake
    /// Formation in the account.
    fn location_registered(&self, account_id: &str, location: &str) -> bool;

    /// The last-modified time of the S3 object at an `s3://bucket/key` URI,
    /// or `None` when there is no such object.
    fn s3_object_last_modified(&self, account_id: &str, uri: &str) -> Option<DateTime<Utc>>;
}
