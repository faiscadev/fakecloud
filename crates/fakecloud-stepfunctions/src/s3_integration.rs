//! S3 as a distributed Map reaches it: the requests behind `ItemReader` and
//! `ResultWriter`, through the same wire protocol `aws-sdk:s3:*` Tasks use.

use std::sync::Arc;

use bytes::Bytes;
use serde_json::{json, Value};

use crate::sdk_protocol::{Caller, DateStyle, SdkFailure, SdkService};

/// S3 as the execution's account in the execution's region.
pub(crate) struct S3Caller<'a> {
    pub(crate) caller: Caller<'a>,
    service: Arc<SdkService>,
}

impl<'a> S3Caller<'a> {
    pub(crate) fn new(caller: Caller<'a>) -> Option<Self> {
        Some(Self {
            caller,
            service: crate::sdk_protocol::service("s3")?,
        })
    }

    pub(crate) fn service(&self) -> &SdkService {
        &self.service
    }

    pub(crate) async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<Bytes, SdkFailure> {
        let mut params = json!({"Bucket": bucket, "Key": key});
        if let Some(version) = version_id {
            params["VersionId"] = json!(version);
        }
        let reply = self
            .service
            .send(&self.caller, "GetObject", &params)
            .await?;
        Ok(reply.body)
    }

    pub(crate) async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: String,
    ) -> Result<(), SdkFailure> {
        let params = json!({"Bucket": bucket, "Key": key, "Body": body});
        self.service
            .send(&self.caller, "PutObject", &params)
            .await?;
        Ok(())
    }

    /// One ListObjectsV2 page as its SDK shape.
    pub(crate) async fn list_objects_v2(&self, params: &Value) -> Result<Value, SdkFailure> {
        self.service
            .invoke(&self.caller, "ListObjectsV2", params, DateStyle::Iso)
            .await
    }
}
