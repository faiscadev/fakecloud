//! Answers Glue's questions about Lake Formation and S3 state: a table's
//! default row-level security policy, whether a location is registered with
//! Lake Formation, and an S3 object's last-modified time.

use chrono::{DateTime, Utc};
use fakecloud_glue::external::GlueExternalView;
use fakecloud_lakeformation::SharedLakeFormationState;
use fakecloud_s3::SharedS3State;

pub struct ServerGlueExternalView {
    lakeformation: SharedLakeFormationState,
    s3: SharedS3State,
}

impl ServerGlueExternalView {
    pub fn new(lakeformation: SharedLakeFormationState, s3: SharedS3State) -> Self {
        Self { lakeformation, s3 }
    }
}

impl GlueExternalView for ServerGlueExternalView {
    fn default_rls_policy(
        &self,
        account_id: &str,
        catalog_id: &str,
        database: &str,
        table: &str,
    ) -> Option<String> {
        self.lakeformation
            .read()
            .get(account_id)?
            .default_rls_policy(account_id, catalog_id, database, table)
    }

    fn location_registered(&self, account_id: &str, location: &str) -> bool {
        self.lakeformation
            .read()
            .get(account_id)
            .is_some_and(|st| st.location_registered(location))
    }

    fn s3_object_last_modified(&self, account_id: &str, uri: &str) -> Option<DateTime<Utc>> {
        let (bucket, key) = uri.strip_prefix("s3://")?.split_once('/')?;
        let s3 = self.s3.read();
        let object = s3.get(account_id)?.buckets.get(bucket)?.objects.get(key)?;
        (!object.is_delete_marker).then_some(object.last_modified)
    }
}
