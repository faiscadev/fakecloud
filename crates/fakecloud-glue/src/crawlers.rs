//! Crawlers, classifiers, crawler schedules, and crawler metrics.
//!
//! Crawlers carry a real READY -> RUNNING -> READY lifecycle: StartCrawler
//! flips state to RUNNING (rejecting a double-start with CrawlerRunningException),
//! StopCrawler flips it back (rejecting a stop-when-idle with
//! CrawlerNotRunningException).

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity, entity_not_found, now_ts, req_str};
use crate::generic;
use crate::service::GlueService;

const CRAWLER_FIELDS: &[&str] = &[
    "Name",
    "Role",
    "Targets",
    "DatabaseName",
    "Description",
    "Classifiers",
    "RecrawlPolicy",
    "SchemaChangePolicy",
    "LineageConfiguration",
    "TablePrefix",
    "Configuration",
    "CrawlerSecurityConfiguration",
    "LakeFormationConfiguration",
];

fn crawler_running(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "CrawlerRunningException",
        format!("Crawler {name} is already running"),
    )
}

fn crawler_not_running(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "CrawlerNotRunningException",
        format!("Crawler {name} is not running"),
    )
}

impl GlueService {
    pub(crate) fn create_crawler(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        req_str(&body, "Role")?;
        if body.get("Targets").is_none() {
            return Err(crate::common::missing("Targets"));
        }
        let now = now_ts();
        let stored = entity(
            &body,
            CRAWLER_FIELDS,
            vec![
                ("State", json!("READY")),
                ("CreationTime", json!(now)),
                ("LastUpdated", json!(now)),
                ("Version", json!(1)),
            ],
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(&mut state.crawlers, &name, stored, "Crawler")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_crawler(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let crawler = accounts
            .get(&req.account_id)
            .and_then(|s| s.crawlers.get(name))
            .ok_or_else(|| entity_not_found(format!("Crawler {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Crawler": crawler })))
    }

    pub(crate) fn get_crawlers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let crawlers: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.crawlers.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Crawlers": crawlers })))
    }

    pub(crate) fn batch_get_crawlers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let names = body["CrawlerNames"].as_array().cloned().unwrap_or_default();
        let accounts = self.state.read();
        let store = accounts.get(&req.account_id).map(|s| &s.crawlers);
        let mut found = Vec::new();
        let mut not_found = Vec::new();
        for n in &names {
            let Some(name) = n.as_str() else { continue };
            match store.and_then(|m| m.get(name)) {
                Some(c) => found.push(c.clone()),
                None => not_found.push(json!(name)),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "Crawlers": found,
            "CrawlersNotFound": not_found,
        })))
    }

    pub(crate) fn list_crawlers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let names: Vec<String> = accounts
            .get(&req.account_id)
            .map(|s| s.crawlers.keys().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "CrawlerNames": names })))
    }

    pub(crate) fn update_crawler(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut updates: Vec<(&str, Value)> = Vec::new();
        for f in CRAWLER_FIELDS {
            if let Some(v) = body.get(*f) {
                if !v.is_null() {
                    updates.push((f, v.clone()));
                }
            }
        }
        updates.push(("LastUpdated", json!(now_ts())));
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::update_merge(&mut state.crawlers, &name, "Crawler", updates)?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_crawler(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let crawler = state
            .crawlers
            .get(&name)
            .ok_or_else(|| entity_not_found(format!("Crawler {name} not found")))?;
        if crawler.get("State").and_then(|s| s.as_str()) == Some("RUNNING") {
            return Err(crawler_running(&name));
        }
        generic::delete(&mut state.crawlers, &name, "Crawler")?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn start_crawler(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let crawler = state
            .crawlers
            .get_mut(&name)
            .ok_or_else(|| entity_not_found(format!("Crawler {name} not found")))?;
        if crawler.get("State").and_then(|s| s.as_str()) == Some("RUNNING") {
            return Err(crawler_running(&name));
        }
        if let Some(obj) = crawler.as_object_mut() {
            obj.insert("State".into(), json!("RUNNING"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn stop_crawler(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let crawler = state
            .crawlers
            .get_mut(&name)
            .ok_or_else(|| entity_not_found(format!("Crawler {name} not found")))?;
        if crawler.get("State").and_then(|s| s.as_str()) != Some("RUNNING") {
            return Err(crawler_not_running(&name));
        }
        if let Some(obj) = crawler.as_object_mut() {
            obj.insert("State".into(), json!("READY"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn start_crawler_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.crawler_schedule_op(req, true)
    }

    pub(crate) fn stop_crawler_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.crawler_schedule_op(req, false)
    }

    fn crawler_schedule_op(
        &self,
        req: &AwsRequest,
        start: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "CrawlerName")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let crawler = state
            .crawlers
            .get_mut(&name)
            .ok_or_else(|| entity_not_found(format!("Crawler {name} not found")))?;
        if let Some(obj) = crawler.as_object_mut() {
            obj.insert(
                "Schedule".into(),
                json!({"State": if start { "SCHEDULED" } else { "NOT_SCHEDULED" }}),
            );
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn update_crawler_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "CrawlerName")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        state
            .crawlers
            .get(&name)
            .ok_or_else(|| entity_not_found(format!("Crawler {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_crawler_metrics(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let metrics: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.crawlers
                    .keys()
                    .map(|name| {
                        json!({
                            "CrawlerName": name,
                            "TablesCreated": 0,
                            "TablesUpdated": 0,
                            "TablesDeleted": 0,
                            "StillEstimating": false,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "CrawlerMetricsList": metrics }),
        ))
    }

    pub(crate) fn list_crawls(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        req_str(&body, "CrawlerName")?;
        Ok(AwsResponse::ok_json(json!({ "Crawls": [] })))
    }

    // --- classifiers ---

    pub(crate) fn create_classifier(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (kind, def) = classifier_def(&body)?;
        let name = def
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| crate::common::missing(&format!("{kind}.Name")))?
            .to_string();
        let now = now_ts();
        let mut stored = def.clone();
        if let Some(obj) = stored.as_object_mut() {
            obj.insert("CreationTime".into(), json!(now));
            obj.insert("LastUpdated".into(), json!(now));
            obj.insert("Version".into(), json!(1));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::create_unique(
            &mut state.classifiers,
            &name,
            json!({ kind: stored }),
            "Classifier",
        )?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_classifier(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?;
        let accounts = self.state.read();
        let c = accounts
            .get(&req.account_id)
            .and_then(|s| s.classifiers.get(name))
            .ok_or_else(|| entity_not_found(format!("Classifier {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Classifier": c })))
    }

    pub(crate) fn get_classifiers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let list: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.classifiers.values().cloned().collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "Classifiers": list })))
    }

    pub(crate) fn update_classifier(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (kind, def) = classifier_def(&body)?;
        let name = def
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| crate::common::missing(&format!("{kind}.Name")))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if !state.classifiers.contains_key(&name) {
            return Err(entity_not_found(format!("Classifier {name} not found")));
        }
        let mut stored = def.clone();
        if let Some(obj) = stored.as_object_mut() {
            obj.insert("LastUpdated".into(), json!(now_ts()));
        }
        state.classifiers.insert(name, json!({ kind: stored }));
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_classifier(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "Name")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        generic::delete(&mut state.classifiers, &name, "Classifier")?;
        Ok(AwsResponse::ok_json(json!({})))
    }
}

/// Pull whichever classifier sub-shape the request carried.
fn classifier_def(body: &Value) -> Result<(&'static str, &Value), AwsServiceError> {
    for kind in [
        "GrokClassifier",
        "XMLClassifier",
        "JsonClassifier",
        "CsvClassifier",
    ] {
        if let Some(v) = body.get(kind) {
            if v.is_object() {
                return Ok((kind, v));
            }
        }
    }
    Err(crate::common::invalid_input(
        "One of GrokClassifier, XMLClassifier, JsonClassifier, CsvClassifier is required",
    ))
}
