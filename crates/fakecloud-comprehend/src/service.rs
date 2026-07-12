//! Amazon Comprehend awsJson1_1 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: Comprehend_20171127.<Operation>`; dispatch
//! keys off `req.action`. Every operation runs model-driven input validation
//! first (required / length / range / enum / pattern), then real,
//! account-partitioned, persisted CRUD. Dereferencing a resource that does not
//! exist returns Comprehend's canonical `ResourceNotFoundException` (or
//! `JobNotFoundException` for analysis jobs); a duplicate name returns
//! `ResourceInUseException`.
//!
//! Honest NLP gap: fakecloud runs no natural-language inference. The
//! synchronous detection operations return well-formed, structurally-correct
//! result shapes with empty analysis lists; `DetectSentiment` returns the
//! neutral default rather than an invented judgement. No entities, key phrases,
//! syntax tokens, PII spans, or language probabilities are fabricated.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::shared::{hex32, now_epoch, parse_resource_arn, resource_arn};
use crate::state::{ComprehendData, SharedComprehendState};

/// Every operation name in the Amazon Comprehend Smithy model (85 operations).
pub const COMPREHEND_ACTIONS: &[&str] = &[
    "BatchDetectDominantLanguage",
    "BatchDetectEntities",
    "BatchDetectKeyPhrases",
    "BatchDetectSentiment",
    "BatchDetectSyntax",
    "BatchDetectTargetedSentiment",
    "ClassifyDocument",
    "ContainsPiiEntities",
    "CreateDataset",
    "CreateDocumentClassifier",
    "CreateEndpoint",
    "CreateEntityRecognizer",
    "CreateFlywheel",
    "DeleteDocumentClassifier",
    "DeleteEndpoint",
    "DeleteEntityRecognizer",
    "DeleteFlywheel",
    "DeleteResourcePolicy",
    "DescribeDataset",
    "DescribeDocumentClassificationJob",
    "DescribeDocumentClassifier",
    "DescribeDominantLanguageDetectionJob",
    "DescribeEndpoint",
    "DescribeEntitiesDetectionJob",
    "DescribeEntityRecognizer",
    "DescribeEventsDetectionJob",
    "DescribeFlywheel",
    "DescribeFlywheelIteration",
    "DescribeKeyPhrasesDetectionJob",
    "DescribePiiEntitiesDetectionJob",
    "DescribeResourcePolicy",
    "DescribeSentimentDetectionJob",
    "DescribeTargetedSentimentDetectionJob",
    "DescribeTopicsDetectionJob",
    "DetectDominantLanguage",
    "DetectEntities",
    "DetectKeyPhrases",
    "DetectPiiEntities",
    "DetectSentiment",
    "DetectSyntax",
    "DetectTargetedSentiment",
    "DetectToxicContent",
    "ImportModel",
    "ListDatasets",
    "ListDocumentClassificationJobs",
    "ListDocumentClassifierSummaries",
    "ListDocumentClassifiers",
    "ListDominantLanguageDetectionJobs",
    "ListEndpoints",
    "ListEntitiesDetectionJobs",
    "ListEntityRecognizerSummaries",
    "ListEntityRecognizers",
    "ListEventsDetectionJobs",
    "ListFlywheelIterationHistory",
    "ListFlywheels",
    "ListKeyPhrasesDetectionJobs",
    "ListPiiEntitiesDetectionJobs",
    "ListSentimentDetectionJobs",
    "ListTagsForResource",
    "ListTargetedSentimentDetectionJobs",
    "ListTopicsDetectionJobs",
    "PutResourcePolicy",
    "StartDocumentClassificationJob",
    "StartDominantLanguageDetectionJob",
    "StartEntitiesDetectionJob",
    "StartEventsDetectionJob",
    "StartFlywheelIteration",
    "StartKeyPhrasesDetectionJob",
    "StartPiiEntitiesDetectionJob",
    "StartSentimentDetectionJob",
    "StartTargetedSentimentDetectionJob",
    "StartTopicsDetectionJob",
    "StopDominantLanguageDetectionJob",
    "StopEntitiesDetectionJob",
    "StopEventsDetectionJob",
    "StopKeyPhrasesDetectionJob",
    "StopPiiEntitiesDetectionJob",
    "StopSentimentDetectionJob",
    "StopTargetedSentimentDetectionJob",
    "StopTrainingDocumentClassifier",
    "StopTrainingEntityRecognizer",
    "TagResource",
    "UntagResource",
    "UpdateEndpoint",
    "UpdateFlywheel",
];

/// Read-only verbs; any other action mutates persisted state and triggers a
/// snapshot after success. The inverse formulation guarantees no mutation is
/// ever missed if a new op is added.
fn is_mutating_action(action: &str) -> bool {
    const READ_PREFIXES: &[&str] = &[
        "Detect", "Batch", "Describe", "List", "Contains", "Classify",
    ];
    !READ_PREFIXES.iter().any(|p| action.starts_with(p))
}

/// A description of one asynchronous analysis-job family (there are nine): its
/// internal key, the ARN resource-type segment, and its `*JobProperties` wire
/// wrapper (the `Describe*` output member and the `List*` list member stem).
struct JobFamily {
    /// Internal family key stored in `job_families`.
    key: &'static str,
    /// ARN resource-type segment, e.g. `sentiment-detection-job`.
    arn_type: &'static str,
    /// The `*JobProperties` shape name (Describe output member).
    props: &'static str,
    /// The `*JobPropertiesList` list-output member.
    props_list: &'static str,
    /// Input members copied verbatim into the stored properties (a subset of the
    /// family's `*JobProperties` members; `copy_members` only pulls those the
    /// caller sent).
    copy: &'static [&'static str],
}

const JOB_FAMILIES: &[JobFamily] = &[
    JobFamily {
        key: "sentiment",
        arn_type: "sentiment-detection-job",
        props: "SentimentDetectionJobProperties",
        props_list: "SentimentDetectionJobPropertiesList",
        copy: &[
            "JobName",
            "InputDataConfig",
            "OutputDataConfig",
            "LanguageCode",
            "DataAccessRoleArn",
            "VolumeKmsKeyId",
            "VpcConfig",
        ],
    },
    JobFamily {
        key: "targeted-sentiment",
        arn_type: "targeted-sentiment-detection-job",
        props: "TargetedSentimentDetectionJobProperties",
        props_list: "TargetedSentimentDetectionJobPropertiesList",
        copy: &[
            "JobName",
            "InputDataConfig",
            "OutputDataConfig",
            "LanguageCode",
            "DataAccessRoleArn",
            "VolumeKmsKeyId",
            "VpcConfig",
        ],
    },
    JobFamily {
        key: "key-phrases",
        arn_type: "key-phrases-detection-job",
        props: "KeyPhrasesDetectionJobProperties",
        props_list: "KeyPhrasesDetectionJobPropertiesList",
        copy: &[
            "JobName",
            "InputDataConfig",
            "OutputDataConfig",
            "LanguageCode",
            "DataAccessRoleArn",
            "VolumeKmsKeyId",
            "VpcConfig",
        ],
    },
    JobFamily {
        key: "entities",
        arn_type: "entities-detection-job",
        props: "EntitiesDetectionJobProperties",
        props_list: "EntitiesDetectionJobPropertiesList",
        copy: &[
            "JobName",
            "EntityRecognizerArn",
            "InputDataConfig",
            "OutputDataConfig",
            "LanguageCode",
            "DataAccessRoleArn",
            "VolumeKmsKeyId",
            "VpcConfig",
            "FlywheelArn",
        ],
    },
    JobFamily {
        key: "dominant-language",
        arn_type: "dominant-language-detection-job",
        props: "DominantLanguageDetectionJobProperties",
        props_list: "DominantLanguageDetectionJobPropertiesList",
        copy: &[
            "JobName",
            "InputDataConfig",
            "OutputDataConfig",
            "DataAccessRoleArn",
            "VolumeKmsKeyId",
            "VpcConfig",
        ],
    },
    JobFamily {
        key: "pii",
        arn_type: "pii-entities-detection-job",
        props: "PiiEntitiesDetectionJobProperties",
        props_list: "PiiEntitiesDetectionJobPropertiesList",
        copy: &[
            "JobName",
            "InputDataConfig",
            "OutputDataConfig",
            "RedactionConfig",
            "LanguageCode",
            "DataAccessRoleArn",
            "Mode",
        ],
    },
    JobFamily {
        key: "events",
        arn_type: "events-detection-job",
        props: "EventsDetectionJobProperties",
        props_list: "EventsDetectionJobPropertiesList",
        copy: &[
            "JobName",
            "InputDataConfig",
            "OutputDataConfig",
            "LanguageCode",
            "DataAccessRoleArn",
            "TargetEventTypes",
        ],
    },
    JobFamily {
        key: "topics",
        arn_type: "topics-detection-job",
        props: "TopicsDetectionJobProperties",
        props_list: "TopicsDetectionJobPropertiesList",
        copy: &[
            "JobName",
            "InputDataConfig",
            "OutputDataConfig",
            "NumberOfTopics",
            "DataAccessRoleArn",
            "VolumeKmsKeyId",
            "VpcConfig",
        ],
    },
    JobFamily {
        key: "document-classification",
        arn_type: "document-classification-job",
        props: "DocumentClassificationJobProperties",
        props_list: "DocumentClassificationJobPropertiesList",
        copy: &[
            "JobName",
            "DocumentClassifierArn",
            "InputDataConfig",
            "OutputDataConfig",
            "DataAccessRoleArn",
            "VolumeKmsKeyId",
            "VpcConfig",
            "FlywheelArn",
        ],
    },
];

fn family(key: &str) -> &'static JobFamily {
    JOB_FAMILIES
        .iter()
        .find(|f| f.key == key)
        .expect("known job family")
}

pub struct ComprehendService {
    state: SharedComprehendState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl ComprehendService {
    pub fn new(state: SharedComprehendState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Run `f` against this account's mutable state.
    fn with_account_mut<R>(&self, req: &AwsRequest, f: impl FnOnce(&mut ComprehendData) -> R) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }
}

#[async_trait]
impl AwsService for ComprehendService {
    fn service_name(&self) -> &str {
        "comprehend"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            return Err(invalid_request(msg));
        }
        let result = self.dispatch(&action, &req);
        if is_mutating_action(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        COMPREHEND_ACTIONS
    }
}

impl ComprehendService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        match action {
            // Synchronous detection
            "DetectDominantLanguage" => self.detect_dominant_language(&body),
            "DetectEntities" => self.detect_entities(&body),
            "DetectKeyPhrases" => self.detect_key_phrases(&body),
            "DetectSyntax" => self.detect_syntax(&body),
            "DetectSentiment" => self.detect_sentiment(&body),
            "DetectTargetedSentiment" => self.detect_targeted_sentiment(&body),
            "DetectPiiEntities" => self.detect_pii_entities(&body),
            "ContainsPiiEntities" => self.contains_pii_entities(&body),
            "DetectToxicContent" => self.detect_toxic_content(&body),
            "ClassifyDocument" => self.classify_document(&body),
            // Batch detection
            "BatchDetectDominantLanguage" => self.batch_detect(&body, "Languages"),
            "BatchDetectEntities" => self.batch_detect(&body, "Entities"),
            "BatchDetectKeyPhrases" => self.batch_detect(&body, "KeyPhrases"),
            "BatchDetectSyntax" => self.batch_detect(&body, "SyntaxTokens"),
            "BatchDetectSentiment" => self.batch_detect(&body, "Sentiment"),
            "BatchDetectTargetedSentiment" => self.batch_detect(&body, "Entities"),
            // Asynchronous analysis jobs
            "StartDominantLanguageDetectionJob" => self.start_job(req, &body, "dominant-language"),
            "StartEntitiesDetectionJob" => self.start_job(req, &body, "entities"),
            "StartKeyPhrasesDetectionJob" => self.start_job(req, &body, "key-phrases"),
            "StartSentimentDetectionJob" => self.start_job(req, &body, "sentiment"),
            "StartTargetedSentimentDetectionJob" => {
                self.start_job(req, &body, "targeted-sentiment")
            }
            "StartPiiEntitiesDetectionJob" => self.start_job(req, &body, "pii"),
            "StartEventsDetectionJob" => self.start_job(req, &body, "events"),
            "StartTopicsDetectionJob" => self.start_job(req, &body, "topics"),
            "StartDocumentClassificationJob" => {
                self.start_job(req, &body, "document-classification")
            }
            "DescribeDominantLanguageDetectionJob" => {
                self.describe_job(req, &body, "dominant-language")
            }
            "DescribeEntitiesDetectionJob" => self.describe_job(req, &body, "entities"),
            "DescribeKeyPhrasesDetectionJob" => self.describe_job(req, &body, "key-phrases"),
            "DescribeSentimentDetectionJob" => self.describe_job(req, &body, "sentiment"),
            "DescribeTargetedSentimentDetectionJob" => {
                self.describe_job(req, &body, "targeted-sentiment")
            }
            "DescribePiiEntitiesDetectionJob" => self.describe_job(req, &body, "pii"),
            "DescribeEventsDetectionJob" => self.describe_job(req, &body, "events"),
            "DescribeTopicsDetectionJob" => self.describe_job(req, &body, "topics"),
            "DescribeDocumentClassificationJob" => {
                self.describe_job(req, &body, "document-classification")
            }
            "ListDominantLanguageDetectionJobs" => self.list_jobs(req, &body, "dominant-language"),
            "ListEntitiesDetectionJobs" => self.list_jobs(req, &body, "entities"),
            "ListKeyPhrasesDetectionJobs" => self.list_jobs(req, &body, "key-phrases"),
            "ListSentimentDetectionJobs" => self.list_jobs(req, &body, "sentiment"),
            "ListTargetedSentimentDetectionJobs" => {
                self.list_jobs(req, &body, "targeted-sentiment")
            }
            "ListPiiEntitiesDetectionJobs" => self.list_jobs(req, &body, "pii"),
            "ListEventsDetectionJobs" => self.list_jobs(req, &body, "events"),
            "ListTopicsDetectionJobs" => self.list_jobs(req, &body, "topics"),
            "ListDocumentClassificationJobs" => {
                self.list_jobs(req, &body, "document-classification")
            }
            "StopDominantLanguageDetectionJob" => self.stop_job(req, &body, "dominant-language"),
            "StopEntitiesDetectionJob" => self.stop_job(req, &body, "entities"),
            "StopKeyPhrasesDetectionJob" => self.stop_job(req, &body, "key-phrases"),
            "StopSentimentDetectionJob" => self.stop_job(req, &body, "sentiment"),
            "StopTargetedSentimentDetectionJob" => self.stop_job(req, &body, "targeted-sentiment"),
            "StopPiiEntitiesDetectionJob" => self.stop_job(req, &body, "pii"),
            "StopEventsDetectionJob" => self.stop_job(req, &body, "events"),
            // Document classifiers
            "CreateDocumentClassifier" => self.create_document_classifier(req, &body),
            "DescribeDocumentClassifier" => self.describe_document_classifier(req, &body),
            "DeleteDocumentClassifier" => self.delete_document_classifier(req, &body),
            "StopTrainingDocumentClassifier" => self.stop_training_document_classifier(req, &body),
            "ListDocumentClassifiers" => self.list_document_classifiers(req, &body),
            "ListDocumentClassifierSummaries" => {
                self.list_document_classifier_summaries(req, &body)
            }
            // Entity recognizers
            "CreateEntityRecognizer" => self.create_entity_recognizer(req, &body),
            "DescribeEntityRecognizer" => self.describe_entity_recognizer(req, &body),
            "DeleteEntityRecognizer" => self.delete_entity_recognizer(req, &body),
            "StopTrainingEntityRecognizer" => self.stop_training_entity_recognizer(req, &body),
            "ListEntityRecognizers" => self.list_entity_recognizers(req, &body),
            "ListEntityRecognizerSummaries" => self.list_entity_recognizer_summaries(req, &body),
            // Endpoints
            "CreateEndpoint" => self.create_endpoint(req, &body),
            "DescribeEndpoint" => self.describe_endpoint(req, &body),
            "UpdateEndpoint" => self.update_endpoint(req, &body),
            "DeleteEndpoint" => self.delete_endpoint(req, &body),
            "ListEndpoints" => self.list_endpoints(req, &body),
            // Flywheels + iterations
            "CreateFlywheel" => self.create_flywheel(req, &body),
            "DescribeFlywheel" => self.describe_flywheel(req, &body),
            "UpdateFlywheel" => self.update_flywheel(req, &body),
            "DeleteFlywheel" => self.delete_flywheel(req, &body),
            "ListFlywheels" => self.list_flywheels(req, &body),
            "StartFlywheelIteration" => self.start_flywheel_iteration(req, &body),
            "DescribeFlywheelIteration" => self.describe_flywheel_iteration(req, &body),
            "ListFlywheelIterationHistory" => self.list_flywheel_iteration_history(req, &body),
            // Datasets
            "CreateDataset" => self.create_dataset(req, &body),
            "DescribeDataset" => self.describe_dataset(req, &body),
            "ListDatasets" => self.list_datasets(req, &body),
            // Model import + resource policies
            "ImportModel" => self.import_model(req, &body),
            "PutResourcePolicy" => self.put_resource_policy(req, &body),
            "DescribeResourcePolicy" => self.describe_resource_policy(req, &body),
            "DeleteResourcePolicy" => self.delete_resource_policy(req, &body),
            // Tagging
            "TagResource" => self.tag_resource(req, &body),
            "UntagResource" => self.untag_resource(req, &body),
            "ListTagsForResource" => self.list_tags_for_resource(req, &body),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }
}

// ---- error / response helpers --------------------------------------------

fn invalid_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidRequestException",
        msg.into(),
    )
}

fn resource_not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        msg.into(),
    )
}

fn job_not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "JobNotFoundException", msg.into())
}

fn resource_in_use(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceInUseException",
        msg.into(),
    )
}

fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(value))
}

// ---- small member helpers -------------------------------------------------

/// Copy each named member from `body` into `obj` when present and non-null.
fn copy_members(body: &Value, obj: &mut Map<String, Value>, names: &[&str]) {
    for name in names {
        if let Some(v) = body.get(*name).filter(|v| !v.is_null()) {
            obj.insert((*name).to_string(), v.clone());
        }
    }
}

fn str_member<'a>(body: &'a Value, name: &str) -> Option<&'a str> {
    body.get(name).and_then(Value::as_str)
}

/// The neutral-default sentiment score fakecloud returns in place of a real
/// model judgement (see the crate-level NLP gap note).
fn neutral_sentiment_score() -> Value {
    json!({ "Positive": 0.0, "Negative": 0.0, "Neutral": 1.0, "Mixed": 0.0 })
}

/// Convert a `TagList` (`[{Key,Value}]`) into a sorted key/value map.
fn taglist_to_map(tags: &Value) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(arr) = tags.as_array() {
        for t in arr {
            if let (Some(k), Some(v)) = (str_member(t, "Key"), str_member(t, "Value")) {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

/// Render a key/value map back into a `TagList`.
fn map_to_taglist(map: &std::collections::BTreeMap<String, String>) -> Value {
    Value::Array(
        map.iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect(),
    )
}

impl ComprehendData {
    /// Record `Tags` from a create/start request under `arn`, if any.
    fn store_tags(&mut self, arn: &str, body: &Value) {
        if let Some(tags) = body.get("Tags").filter(|v| !v.is_null()) {
            let map = taglist_to_map(tags);
            if !map.is_empty() {
                self.tags.insert(arn.to_string(), map);
            }
        }
    }

    /// True if the resource named by an ARN's `(type, tail)` exists.
    fn resource_exists(&self, arn: &str) -> bool {
        self.document_classifiers.contains_key(arn)
            || self.entity_recognizers.contains_key(arn)
            || self.endpoints.contains_key(arn)
            || self.flywheels.contains_key(arn)
            || self.datasets.contains_key(arn)
    }
}

// ---- synchronous detection ------------------------------------------------

impl ComprehendService {
    fn detect_dominant_language(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        // No language detection is run; an empty result set is honest.
        ok(json!({ "Languages": [] }))
    }

    fn detect_entities(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "Entities": [] }))
    }

    fn detect_key_phrases(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "KeyPhrases": [] }))
    }

    fn detect_syntax(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "SyntaxTokens": [] }))
    }

    fn detect_sentiment(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({
            "Sentiment": "NEUTRAL",
            "SentimentScore": neutral_sentiment_score(),
        }))
    }

    fn detect_targeted_sentiment(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "Entities": [] }))
    }

    fn detect_pii_entities(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "Entities": [] }))
    }

    fn contains_pii_entities(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({ "Labels": [] }))
    }

    fn detect_toxic_content(&self, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        // One well-formed (empty) result per input text segment.
        let n = body
            .get("TextSegments")
            .and_then(Value::as_array)
            .map(|a| a.len())
            .unwrap_or(0);
        let results: Vec<Value> = (0..n)
            .map(|_| json!({ "Labels": [], "Toxicity": 0.0 }))
            .collect();
        ok(json!({ "ResultList": results }))
    }

    fn classify_document(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        // No custom-model inference is run; empty classes/labels are honest.
        ok(json!({ "Classes": [], "Labels": [] }))
    }

    /// Shared body for the six `BatchDetect*` operations: emit one well-formed,
    /// empty result per input string (indexed) and no per-document errors.
    fn batch_detect(&self, body: &Value, field: &str) -> Result<AwsResponse, AwsServiceError> {
        let n = body
            .get("TextList")
            .and_then(Value::as_array)
            .map(|a| a.len())
            .unwrap_or(0);
        let results: Vec<Value> = (0..n)
            .map(|i| {
                let mut obj = Map::new();
                obj.insert("Index".into(), json!(i as i64));
                if field == "Sentiment" {
                    obj.insert("Sentiment".into(), json!("NEUTRAL"));
                    obj.insert("SentimentScore".into(), neutral_sentiment_score());
                } else {
                    obj.insert(field.to_string(), json!([]));
                }
                Value::Object(obj)
            })
            .collect();
        ok(json!({ "ResultList": results, "ErrorList": [] }))
    }
}

// ---- asynchronous analysis jobs -------------------------------------------

impl ComprehendService {
    fn start_job(
        &self,
        req: &AwsRequest,
        body: &Value,
        family_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fam = family(family_key);
        let region = req.region.clone();
        let account = req.account_id.clone();
        let seed = str_member(body, "ClientRequestToken")
            .map(|t| format!("{account}/{family_key}/{t}"))
            .unwrap_or_default();
        let job_id = hex32(&seed);
        let job_arn = resource_arn(&region, &account, fam.arn_type, &job_id);
        let job_name = str_member(body, "JobName")
            .map(String::from)
            .unwrap_or_else(|| job_id.clone());
        self.with_account_mut(req, |d| {
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("JobId".into(), json!(job_id));
            obj.insert("JobArn".into(), json!(job_arn));
            obj.insert("JobName".into(), json!(job_name));
            obj.insert("JobStatus".into(), json!("SUBMITTED"));
            obj.insert("SubmitTime".into(), json!(now));
            copy_members(body, &mut obj, fam.copy);
            d.jobs.insert(job_id.clone(), Value::Object(obj));
            d.job_families.insert(job_id.clone(), fam.key.to_string());
            d.store_tags(&job_arn, body);
            ok(json!({
                "JobId": job_id,
                "JobArn": job_arn,
                "JobStatus": "SUBMITTED",
            }))
        })
    }

    fn describe_job(
        &self,
        req: &AwsRequest,
        body: &Value,
        family_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fam = family(family_key);
        let job_id = str_member(body, "JobId").unwrap_or_default().to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let belongs = d.job_families.get(&job_id).map(String::as_str) == Some(fam.key);
            let Some(job) = d.jobs.get(&job_id).filter(|_| belongs).cloned() else {
                return Err(job_not_found(
                    "The specified job was not found. Check the job ID and try again.",
                ));
            };
            ok(json!({ fam.props: job }))
        })
    }

    fn list_jobs(
        &self,
        req: &AwsRequest,
        body: &Value,
        family_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fam = family(family_key);
        let filter = body.get("Filter");
        let name_eq = filter.and_then(|f| str_member(f, "JobName"));
        let status_eq = filter.and_then(|f| str_member(f, "JobStatus"));
        self.with_account_mut(req, |d| {
            d.reconcile();
            let list: Vec<Value> = d
                .jobs
                .iter()
                .filter(|(id, _)| d.job_families.get(*id).map(String::as_str) == Some(fam.key))
                .map(|(_, v)| v)
                .filter(|v| {
                    name_eq
                        .map(|n| v.get("JobName").and_then(Value::as_str) == Some(n))
                        .unwrap_or(true)
                        && status_eq
                            .map(|s| v.get("JobStatus").and_then(Value::as_str) == Some(s))
                            .unwrap_or(true)
                })
                .cloned()
                .collect();
            ok(json!({ fam.props_list: list }))
        })
    }

    fn stop_job(
        &self,
        req: &AwsRequest,
        body: &Value,
        family_key: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fam = family(family_key);
        let job_id = str_member(body, "JobId").unwrap_or_default().to_string();
        self.with_account_mut(req, |d| {
            let belongs = d.job_families.get(&job_id).map(String::as_str) == Some(fam.key);
            let Some(job) = d.jobs.get_mut(&job_id).filter(|_| belongs) else {
                return Err(job_not_found(
                    "The specified job was not found. Check the job ID and try again.",
                ));
            };
            let obj = job.as_object_mut().expect("job is an object");
            let status = obj
                .get("JobStatus")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let new_status = if matches!(status.as_str(), "SUBMITTED" | "IN_PROGRESS") {
                obj.insert("JobStatus".into(), json!("STOP_REQUESTED"));
                "STOP_REQUESTED"
            } else {
                status.as_str()
            };
            ok(json!({ "JobId": job_id, "JobStatus": new_status }))
        })
    }
}

// ---- document classifiers -------------------------------------------------

impl ComprehendService {
    fn create_document_classifier(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "DocumentClassifierName")
            .unwrap_or_default()
            .to_string();
        let version = str_member(body, "VersionName").map(String::from);
        let region = req.region.clone();
        let account = req.account_id.clone();
        let tail = match &version {
            Some(v) => format!("{name}/version/{v}"),
            None => name.clone(),
        };
        let arn = resource_arn(&region, &account, "document-classifier", &tail);
        self.with_account_mut(req, |d| {
            if d.document_classifiers.contains_key(&arn) {
                return Err(resource_in_use(
                    "Concurrent modification of resources. A resource with the same name already exists.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("DocumentClassifierArn".into(), json!(arn));
            obj.insert("Status".into(), json!("SUBMITTED"));
            obj.insert("SubmitTime".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &[
                    "LanguageCode",
                    "InputDataConfig",
                    "OutputDataConfig",
                    "DataAccessRoleArn",
                    "VolumeKmsKeyId",
                    "VpcConfig",
                    "Mode",
                    "ModelKmsKeyId",
                    "VersionName",
                ],
            );
            d.document_classifiers.insert(arn.clone(), Value::Object(obj));
            d.store_tags(&arn, body);
            ok(json!({ "DocumentClassifierArn": arn }))
        })
    }

    fn describe_document_classifier(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "DocumentClassifierArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(v) = d.document_classifiers.get(&arn).cloned() else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            ok(json!({ "DocumentClassifierProperties": v }))
        })
    }

    fn delete_document_classifier(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "DocumentClassifierArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            if d.document_classifiers.remove(&arn).is_none() {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            }
            d.tags.remove(&arn);
            ok(json!({}))
        })
    }

    fn stop_training_document_classifier(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "DocumentClassifierArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            let Some(v) = d.document_classifiers.get_mut(&arn) else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            stop_training(v);
            ok(json!({}))
        })
    }

    fn list_document_classifiers(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = body.get("Filter");
        let status_eq = filter.and_then(|f| str_member(f, "Status"));
        let name_eq = filter.and_then(|f| str_member(f, "DocumentClassifierName"));
        self.with_account_mut(req, |d| {
            d.reconcile();
            let list: Vec<Value> = d
                .document_classifiers
                .values()
                .filter(|v| {
                    status_eq
                        .map(|s| v.get("Status").and_then(Value::as_str) == Some(s))
                        .unwrap_or(true)
                        && name_eq
                            .map(|n| classifier_name(v).as_deref() == Some(n))
                            .unwrap_or(true)
                })
                .cloned()
                .collect();
            ok(json!({ "DocumentClassifierPropertiesList": list }))
        })
    }

    fn list_document_classifier_summaries(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let summaries = summarize_versions(
                d.document_classifiers.values(),
                classifier_name,
                "DocumentClassifierName",
            );
            ok(json!({ "DocumentClassifierSummariesList": summaries }))
        })
    }
}

/// Extract the base classifier name from a `DocumentClassifierProperties` (the
/// segment before any `/version/...`).
fn classifier_name(v: &Value) -> Option<String> {
    let arn = v.get("DocumentClassifierArn").and_then(Value::as_str)?;
    let (_, tail) = parse_resource_arn(arn)?;
    Some(tail.split("/version/").next().unwrap_or(&tail).to_string())
}

/// Extract the base recognizer name from an `EntityRecognizerProperties`.
fn recognizer_name(v: &Value) -> Option<String> {
    let arn = v.get("EntityRecognizerArn").and_then(Value::as_str)?;
    let (_, tail) = parse_resource_arn(arn)?;
    Some(tail.split("/version/").next().unwrap_or(&tail).to_string())
}

/// Move a trainable resource into `STOP_REQUESTED` when its training is still in
/// flight (a no-op once terminal).
fn stop_training(v: &mut Value) {
    if let Some(obj) = v.as_object_mut() {
        let status = obj.get("Status").and_then(Value::as_str).unwrap_or("");
        if matches!(status, "SUBMITTED" | "TRAINING") {
            obj.insert("Status".into(), json!("STOP_REQUESTED"));
        }
    }
}

/// Build `*Summary` entries (one per base name) collapsing all versions of a
/// classifier / recognizer, mirroring `ListDocumentClassifierSummaries`.
fn summarize_versions<'a>(
    values: impl Iterator<Item = &'a Value>,
    name_of: fn(&Value) -> Option<String>,
    name_field: &str,
) -> Vec<Value> {
    use std::collections::BTreeMap;
    // name -> (count, latest_submit, latest_version, latest_status)
    let mut groups: BTreeMap<String, (i64, f64, Option<String>, Option<String>)> = BTreeMap::new();
    for v in values {
        let Some(name) = name_of(v) else { continue };
        let submit = v.get("SubmitTime").and_then(Value::as_f64).unwrap_or(0.0);
        let version = v
            .get("VersionName")
            .and_then(Value::as_str)
            .map(String::from);
        let status = v.get("Status").and_then(Value::as_str).map(String::from);
        let entry = groups.entry(name).or_insert((0, f64::MIN, None, None));
        entry.0 += 1;
        if submit >= entry.1 {
            entry.1 = submit;
            entry.2 = version;
            entry.3 = status;
        }
    }
    groups
        .into_iter()
        .map(|(name, (count, submit, version, status))| {
            let mut obj = Map::new();
            obj.insert(name_field.to_string(), json!(name));
            obj.insert("NumberOfVersions".into(), json!(count));
            if submit > f64::MIN {
                obj.insert("LatestVersionCreatedAt".into(), json!(submit));
            }
            if let Some(v) = version {
                obj.insert("LatestVersionName".into(), json!(v));
            }
            if let Some(s) = status {
                obj.insert("LatestVersionStatus".into(), json!(s));
            }
            Value::Object(obj)
        })
        .collect()
}

// ---- entity recognizers ---------------------------------------------------

impl ComprehendService {
    fn create_entity_recognizer(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "RecognizerName")
            .unwrap_or_default()
            .to_string();
        let version = str_member(body, "VersionName").map(String::from);
        let region = req.region.clone();
        let account = req.account_id.clone();
        let tail = match &version {
            Some(v) => format!("{name}/version/{v}"),
            None => name.clone(),
        };
        let arn = resource_arn(&region, &account, "entity-recognizer", &tail);
        self.with_account_mut(req, |d| {
            if d.entity_recognizers.contains_key(&arn) {
                return Err(resource_in_use(
                    "Concurrent modification of resources. A resource with the same name already exists.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("EntityRecognizerArn".into(), json!(arn));
            obj.insert("Status".into(), json!("SUBMITTED"));
            obj.insert("SubmitTime".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &[
                    "LanguageCode",
                    "InputDataConfig",
                    "DataAccessRoleArn",
                    "VolumeKmsKeyId",
                    "VpcConfig",
                    "ModelKmsKeyId",
                    "VersionName",
                ],
            );
            d.entity_recognizers.insert(arn.clone(), Value::Object(obj));
            d.store_tags(&arn, body);
            ok(json!({ "EntityRecognizerArn": arn }))
        })
    }

    fn describe_entity_recognizer(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "EntityRecognizerArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(v) = d.entity_recognizers.get(&arn).cloned() else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            ok(json!({ "EntityRecognizerProperties": v }))
        })
    }

    fn delete_entity_recognizer(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "EntityRecognizerArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            if d.entity_recognizers.remove(&arn).is_none() {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            }
            d.tags.remove(&arn);
            ok(json!({}))
        })
    }

    fn stop_training_entity_recognizer(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "EntityRecognizerArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            let Some(v) = d.entity_recognizers.get_mut(&arn) else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            stop_training(v);
            ok(json!({}))
        })
    }

    fn list_entity_recognizers(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = body.get("Filter");
        let status_eq = filter.and_then(|f| str_member(f, "Status"));
        let name_eq = filter.and_then(|f| str_member(f, "RecognizerName"));
        self.with_account_mut(req, |d| {
            d.reconcile();
            let list: Vec<Value> = d
                .entity_recognizers
                .values()
                .filter(|v| {
                    status_eq
                        .map(|s| v.get("Status").and_then(Value::as_str) == Some(s))
                        .unwrap_or(true)
                        && name_eq
                            .map(|n| recognizer_name(v).as_deref() == Some(n))
                            .unwrap_or(true)
                })
                .cloned()
                .collect();
            ok(json!({ "EntityRecognizerPropertiesList": list }))
        })
    }

    fn list_entity_recognizer_summaries(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let summaries = summarize_versions(
                d.entity_recognizers.values(),
                recognizer_name,
                "RecognizerName",
            );
            ok(json!({ "EntityRecognizerSummariesList": summaries }))
        })
    }
}

// ---- endpoints ------------------------------------------------------------

impl ComprehendService {
    fn create_endpoint(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "EndpointName")
            .unwrap_or_default()
            .to_string();
        let model_arn = str_member(body, "ModelArn").unwrap_or_default().to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        // The endpoint ARN's resource type mirrors the backing model type.
        let ep_type = if model_arn.contains("entity-recognizer") {
            "entity-recognizer-endpoint"
        } else {
            "document-classifier-endpoint"
        };
        let arn = resource_arn(&region, &account, ep_type, &name);
        self.with_account_mut(req, |d| {
            if d.endpoints.contains_key(&arn) {
                return Err(resource_in_use(
                    "Concurrent modification of resources. An endpoint with the same name already exists.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("EndpointArn".into(), json!(arn));
            obj.insert("Status".into(), json!("CREATING"));
            obj.insert("CurrentInferenceUnits".into(), json!(0));
            obj.insert("CreationTime".into(), json!(now));
            obj.insert("LastModifiedTime".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &[
                    "ModelArn",
                    "DesiredInferenceUnits",
                    "DataAccessRoleArn",
                    "FlywheelArn",
                ],
            );
            d.endpoints.insert(arn.clone(), Value::Object(obj));
            d.store_tags(&arn, body);
            ok(json!({ "EndpointArn": arn, "ModelArn": model_arn }))
        })
    }

    fn describe_endpoint(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "EndpointArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(v) = d.endpoints.get(&arn).cloned() else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            ok(json!({ "EndpointProperties": v }))
        })
    }

    fn update_endpoint(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "EndpointArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            let Some(v) = d.endpoints.get_mut(&arn) else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            let obj = v.as_object_mut().expect("endpoint is an object");
            let now = now_epoch();
            if let Some(m) = body.get("DesiredModelArn").filter(|v| !v.is_null()) {
                obj.insert("DesiredModelArn".into(), m.clone());
            }
            if let Some(u) = body.get("DesiredInferenceUnits").filter(|v| !v.is_null()) {
                obj.insert("DesiredInferenceUnits".into(), u.clone());
            }
            if let Some(r) = body.get("DesiredDataAccessRoleArn").filter(|v| !v.is_null()) {
                obj.insert("DesiredDataAccessRoleArn".into(), r.clone());
            }
            if let Some(f) = body.get("FlywheelArn").filter(|v| !v.is_null()) {
                obj.insert("FlywheelArn".into(), f.clone());
            }
            obj.insert("Status".into(), json!("UPDATING"));
            obj.insert("LastModifiedTime".into(), json!(now));
            let desired = obj.get("DesiredModelArn").cloned().unwrap_or(Value::Null);
            ok(json!({ "DesiredModelArn": desired }))
        })
    }

    fn delete_endpoint(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "EndpointArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            if d.endpoints.remove(&arn).is_none() {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            }
            d.tags.remove(&arn);
            ok(json!({}))
        })
    }

    fn list_endpoints(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = body.get("Filter");
        let status_eq = filter.and_then(|f| str_member(f, "Status"));
        let model_eq = filter.and_then(|f| str_member(f, "ModelArn"));
        self.with_account_mut(req, |d| {
            d.reconcile();
            let list: Vec<Value> = d
                .endpoints
                .values()
                .filter(|v| {
                    status_eq
                        .map(|s| v.get("Status").and_then(Value::as_str) == Some(s))
                        .unwrap_or(true)
                        && model_eq
                            .map(|m| v.get("ModelArn").and_then(Value::as_str) == Some(m))
                            .unwrap_or(true)
                })
                .cloned()
                .collect();
            ok(json!({ "EndpointPropertiesList": list }))
        })
    }
}

// ---- flywheels + iterations -----------------------------------------------

impl ComprehendService {
    fn create_flywheel(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "FlywheelName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        let arn = resource_arn(&region, &account, "flywheel", &name);
        let active_model = str_member(body, "ActiveModelArn").map(String::from);
        self.with_account_mut(req, |d| {
            if d.flywheels.contains_key(&arn) {
                return Err(resource_in_use(
                    "Concurrent modification of resources. A flywheel with the same name already exists.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("FlywheelArn".into(), json!(arn));
            obj.insert("Status".into(), json!("CREATING"));
            obj.insert("CreationTime".into(), json!(now));
            obj.insert("LastModifiedTime".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &[
                    "ActiveModelArn",
                    "DataAccessRoleArn",
                    "TaskConfig",
                    "ModelType",
                    "DataLakeS3Uri",
                    "DataSecurityConfig",
                ],
            );
            d.flywheels.insert(arn.clone(), Value::Object(obj));
            d.store_tags(&arn, body);
            ok(json!({ "FlywheelArn": arn, "ActiveModelArn": active_model }))
        })
    }

    fn describe_flywheel(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "FlywheelArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(v) = d.flywheels.get(&arn).cloned() else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            ok(json!({ "FlywheelProperties": v }))
        })
    }

    fn update_flywheel(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "FlywheelArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            let Some(v) = d.flywheels.get_mut(&arn) else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            let obj = v.as_object_mut().expect("flywheel is an object");
            for m in ["ActiveModelArn", "DataAccessRoleArn", "DataSecurityConfig"] {
                if let Some(val) = body.get(m).filter(|v| !v.is_null()) {
                    obj.insert(m.to_string(), val.clone());
                }
            }
            obj.insert("LastModifiedTime".into(), json!(now_epoch()));
            ok(json!({ "FlywheelProperties": Value::Object(obj.clone()) }))
        })
    }

    fn delete_flywheel(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "FlywheelArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            if d.flywheels.remove(&arn).is_none() {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            }
            d.tags.remove(&arn);
            // Drop the flywheel's iterations and datasets too.
            d.flywheel_iterations
                .retain(|k, _| !k.starts_with(&format!("{arn}#")));
            d.datasets
                .retain(|_, v| v.get("DatasetArn").and_then(Value::as_str)
                    .map(|a| !a.starts_with(&format!("{arn}/dataset/")))
                    .unwrap_or(true));
            ok(json!({}))
        })
    }

    fn list_flywheels(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = body.get("Filter");
        let status_eq = filter.and_then(|f| str_member(f, "Status"));
        self.with_account_mut(req, |d| {
            d.reconcile();
            let list: Vec<Value> = d
                .flywheels
                .values()
                .filter(|v| {
                    status_eq
                        .map(|s| v.get("Status").and_then(Value::as_str) == Some(s))
                        .unwrap_or(true)
                })
                .map(|v| {
                    // FlywheelSummary is a subset of FlywheelProperties.
                    let mut s = Map::new();
                    if let Some(obj) = v.as_object() {
                        for f in [
                            "FlywheelArn",
                            "ActiveModelArn",
                            "DataLakeS3Uri",
                            "Status",
                            "ModelType",
                            "Message",
                            "CreationTime",
                            "LastModifiedTime",
                            "LatestFlywheelIteration",
                        ] {
                            if let Some(val) = obj.get(f) {
                                s.insert(f.to_string(), val.clone());
                            }
                        }
                    }
                    Value::Object(s)
                })
                .collect();
            ok(json!({ "FlywheelSummaryList": list }))
        })
    }

    fn start_flywheel_iteration(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "FlywheelArn")
            .unwrap_or_default()
            .to_string();
        let iter_id = hex32(&format!(
            "{}/{}",
            arn,
            str_member(body, "ClientRequestToken").unwrap_or_default()
        ));
        self.with_account_mut(req, |d| {
            if !d.flywheels.contains_key(&arn) {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            }
            let now = now_epoch();
            let obj = json!({
                "FlywheelArn": arn,
                "FlywheelIterationId": iter_id,
                "CreationTime": now,
                "Status": "TRAINING",
            });
            d.flywheel_iterations
                .insert(format!("{arn}#{iter_id}"), obj);
            // Record the latest iteration on the flywheel.
            if let Some(fw) = d.flywheels.get_mut(&arn).and_then(Value::as_object_mut) {
                fw.insert("LatestFlywheelIteration".into(), json!(iter_id));
            }
            ok(json!({ "FlywheelArn": arn, "FlywheelIterationId": iter_id }))
        })
    }

    fn describe_flywheel_iteration(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "FlywheelArn")
            .unwrap_or_default()
            .to_string();
        let iter_id = str_member(body, "FlywheelIterationId")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(v) = d.flywheel_iterations.get(&format!("{arn}#{iter_id}")).cloned() else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            ok(json!({ "FlywheelIterationProperties": v }))
        })
    }

    fn list_flywheel_iteration_history(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "FlywheelArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            if !d.flywheels.contains_key(&arn) {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            }
            let prefix = format!("{arn}#");
            let list: Vec<Value> = d
                .flywheel_iterations
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v.clone())
                .collect();
            ok(json!({ "FlywheelIterationPropertiesList": list }))
        })
    }
}

// ---- datasets -------------------------------------------------------------

impl ComprehendService {
    fn create_dataset(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fw_arn = str_member(body, "FlywheelArn")
            .unwrap_or_default()
            .to_string();
        let name = str_member(body, "DatasetName")
            .unwrap_or_default()
            .to_string();
        let arn = format!("{fw_arn}/dataset/{name}");
        self.with_account_mut(req, |d| {
            if !d.flywheels.contains_key(&fw_arn) {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            }
            if d.datasets.contains_key(&arn) {
                return Err(resource_in_use(
                    "Concurrent modification of resources. A dataset with the same name already exists.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("DatasetArn".into(), json!(arn));
            obj.insert("DatasetName".into(), json!(name));
            obj.insert("Status".into(), json!("CREATING"));
            obj.insert("NumberOfDocuments".into(), json!(0));
            obj.insert("CreationTime".into(), json!(now));
            copy_members(body, &mut obj, &["DatasetType", "Description"]);
            d.datasets.insert(arn.clone(), Value::Object(obj));
            d.store_tags(&arn, body);
            ok(json!({ "DatasetArn": arn }))
        })
    }

    fn describe_dataset(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "DatasetArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(v) = d.datasets.get(&arn).cloned() else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            ok(json!({ "DatasetProperties": v }))
        })
    }

    fn list_datasets(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let fw_arn = str_member(body, "FlywheelArn").map(String::from);
        let filter = body.get("Filter");
        let status_eq = filter.and_then(|f| str_member(f, "Status"));
        self.with_account_mut(req, |d| {
            d.reconcile();
            let list: Vec<Value> = d
                .datasets
                .iter()
                .filter(|(arn, _)| {
                    fw_arn
                        .as_deref()
                        .map(|fw| arn.starts_with(&format!("{fw}/dataset/")))
                        .unwrap_or(true)
                })
                .map(|(_, v)| v)
                .filter(|v| {
                    status_eq
                        .map(|s| v.get("Status").and_then(Value::as_str) == Some(s))
                        .unwrap_or(true)
                })
                .cloned()
                .collect();
            ok(json!({ "DatasetPropertiesList": list }))
        })
    }
}

// ---- model import + resource policies -------------------------------------

impl ComprehendService {
    fn import_model(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let source = str_member(body, "SourceModelArn")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        // Import produces a copy of the source model (classifier or recognizer);
        // the type is inferred from the source ARN.
        let is_recognizer = source.contains("entity-recognizer");
        let rtype = if is_recognizer {
            "entity-recognizer"
        } else {
            "document-classifier"
        };
        let name = str_member(body, "ModelName")
            .map(String::from)
            .or_else(|| {
                parse_resource_arn(&source)
                    .map(|(_, tail)| tail.split("/version/").next().unwrap_or(&tail).to_string())
            })
            .unwrap_or_else(|| "imported-model".to_string());
        let version = str_member(body, "VersionName").map(String::from);
        let tail = match &version {
            Some(v) => format!("{name}/version/{v}"),
            None => name.clone(),
        };
        let arn = resource_arn(&region, &account, rtype, &tail);
        self.with_account_mut(req, |d| {
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("Status".into(), json!("TRAINED"));
            obj.insert("SubmitTime".into(), json!(now));
            obj.insert("TrainingStartTime".into(), json!(now));
            obj.insert("TrainingEndTime".into(), json!(now));
            obj.insert("EndTime".into(), json!(now));
            obj.insert("SourceModelArn".into(), json!(source));
            if let Some(v) = &version {
                obj.insert("VersionName".into(), json!(v));
            }
            copy_members(body, &mut obj, &["ModelKmsKeyId", "DataAccessRoleArn"]);
            if is_recognizer {
                obj.insert("EntityRecognizerArn".into(), json!(arn));
                d.entity_recognizers.insert(arn.clone(), Value::Object(obj));
            } else {
                obj.insert("DocumentClassifierArn".into(), json!(arn));
                d.document_classifiers
                    .insert(arn.clone(), Value::Object(obj));
            }
            d.store_tags(&arn, body);
            ok(json!({ "ModelArn": arn }))
        })
    }

    fn put_resource_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        let policy = str_member(body, "ResourcePolicy")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            let now = now_epoch();
            let revision = hex32(&format!("{arn}/{}", now));
            let created = d
                .resource_policies
                .get(&arn)
                .and_then(|p| p.get("CreationTime").cloned())
                .unwrap_or(json!(now));
            d.resource_policies.insert(
                arn.clone(),
                json!({
                    "ResourcePolicy": policy,
                    "PolicyRevisionId": revision,
                    "CreationTime": created,
                    "LastModifiedTime": now,
                }),
            );
            ok(json!({ "PolicyRevisionId": revision }))
        })
    }

    fn describe_resource_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            let Some(p) = d.resource_policies.get(&arn).cloned() else {
                return Err(resource_not_found(
                    "The specified resource ARN was not found. Check the ARN and try your request again.",
                ));
            };
            ok(p)
        })
    }

    fn delete_resource_policy(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.resource_policies.remove(&arn);
            ok(json!({}))
        })
    }
}

// ---- tagging --------------------------------------------------------------

impl ComprehendService {
    fn tag_resource(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&arn) {
                return Err(resource_not_found(format!(
                    "The specified resource ARN was not found: {arn}"
                )));
            }
            let entry = d.tags.entry(arn.clone()).or_default();
            for (k, v) in taglist_to_map(body.get("Tags").unwrap_or(&Value::Null)) {
                entry.insert(k, v);
            }
            ok(json!({}))
        })
    }

    fn untag_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&arn) {
                return Err(resource_not_found(format!(
                    "The specified resource ARN was not found: {arn}"
                )));
            }
            if let Some(entry) = d.tags.get_mut(&arn) {
                if let Some(keys) = body.get("TagKeys").and_then(Value::as_array) {
                    for k in keys.iter().filter_map(Value::as_str) {
                        entry.remove(k);
                    }
                }
            }
            ok(json!({}))
        })
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&arn) {
                return Err(resource_not_found(format!(
                    "The specified resource ARN was not found: {arn}"
                )));
            }
            let tags = d.tags.get(&arn).map(map_to_taglist).unwrap_or(json!([]));
            ok(json!({ "ResourceArn": arn, "Tags": tags }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn service() -> ComprehendService {
        ComprehendService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "comprehend".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: String::new(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn body_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[tokio::test]
    async fn detect_sentiment_is_neutral_default() {
        let svc = service();
        let resp = svc
            .handle(req(
                "DetectSentiment",
                json!({ "Text": "hello", "LanguageCode": "en" }),
            ))
            .await
            .unwrap();
        let v = body_of(resp).await;
        assert_eq!(v["Sentiment"], "NEUTRAL");
        assert_eq!(v["SentimentScore"]["Neutral"], 1.0);
    }

    #[tokio::test]
    async fn missing_required_text_is_invalid_request() {
        let svc = service();
        let err = svc
            .handle(req("DetectSentiment", json!({ "LanguageCode": "en" })))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidRequestException");
    }

    #[tokio::test]
    async fn sentiment_job_round_trip() {
        let svc = service();
        let start = svc
            .handle(req(
                "StartSentimentDetectionJob",
                json!({
                    "InputDataConfig": { "S3Uri": "s3://in/" },
                    "OutputDataConfig": { "S3Uri": "s3://out/" },
                    "DataAccessRoleArn": "arn:aws:iam::000000000000:role/r",
                    "LanguageCode": "en",
                    "JobName": "job1"
                }),
            ))
            .await
            .unwrap();
        let sv = body_of(start).await;
        let job_id = sv["JobId"].as_str().unwrap().to_string();
        assert_eq!(sv["JobStatus"], "SUBMITTED");

        // Describe settles it to COMPLETED.
        let desc = svc
            .handle(req(
                "DescribeSentimentDetectionJob",
                json!({ "JobId": job_id }),
            ))
            .await
            .unwrap();
        let dv = body_of(desc).await;
        assert_eq!(
            dv["SentimentDetectionJobProperties"]["JobStatus"],
            "COMPLETED"
        );

        // A job id belonging to another family is not found here.
        let cross = svc
            .handle(req(
                "DescribeEntitiesDetectionJob",
                json!({ "JobId": "00000000000000000000000000000000" }),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(cross.code(), "JobNotFoundException");

        // List shows it.
        let listed = svc
            .handle(req("ListSentimentDetectionJobs", json!({})))
            .await
            .unwrap();
        let lv = body_of(listed).await;
        assert_eq!(
            lv["SentimentDetectionJobPropertiesList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn flywheel_and_tagging_round_trip() {
        let svc = service();
        let create = svc
            .handle(req(
                "CreateFlywheel",
                json!({
                    "FlywheelName": "fw1",
                    "DataAccessRoleArn": "arn:aws:iam::000000000000:role/r",
                    "DataLakeS3Uri": "s3://lake/"
                }),
            ))
            .await
            .unwrap();
        let cv = body_of(create).await;
        let arn = cv["FlywheelArn"].as_str().unwrap().to_string();

        // Describe settles CREATING -> ACTIVE.
        let desc = svc
            .handle(req("DescribeFlywheel", json!({ "FlywheelArn": arn })))
            .await
            .unwrap();
        let dv = body_of(desc).await;
        assert_eq!(dv["FlywheelProperties"]["Status"], "ACTIVE");

        // Tag + list tags.
        svc.handle(req(
            "TagResource",
            json!({ "ResourceArn": arn, "Tags": [{ "Key": "team", "Value": "nlp" }] }),
        ))
        .await
        .unwrap();
        let tags = svc
            .handle(req("ListTagsForResource", json!({ "ResourceArn": arn })))
            .await
            .unwrap();
        let tv = body_of(tags).await;
        assert_eq!(tv["Tags"][0]["Key"], "team");

        // Unknown resource -> ResourceNotFoundException.
        let bad = svc
            .handle(req(
                "ListTagsForResource",
                json!({ "ResourceArn": "arn:aws:comprehend:us-east-1:000000000000:flywheel/nope" }),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(bad.code(), "ResourceNotFoundException");

        // Delete.
        svc.handle(req("DeleteFlywheel", json!({ "FlywheelArn": arn })))
            .await
            .unwrap();
        let after = svc
            .handle(req("DescribeFlywheel", json!({ "FlywheelArn": arn })))
            .await
            .err()
            .unwrap();
        assert_eq!(after.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn document_classifier_versions_summarized() {
        let svc = service();
        for v in ["v1", "v2"] {
            svc.handle(req(
                "CreateDocumentClassifier",
                json!({
                    "DocumentClassifierName": "clf",
                    "VersionName": v,
                    "DataAccessRoleArn": "arn:aws:iam::000000000000:role/r",
                    "LanguageCode": "en",
                    "InputDataConfig": { "DataFormat": "COMPREHEND_CSV", "S3Uri": "s3://in/" }
                }),
            ))
            .await
            .unwrap();
        }
        let sums = svc
            .handle(req("ListDocumentClassifierSummaries", json!({})))
            .await
            .unwrap();
        let sv = body_of(sums).await;
        let list = sv["DocumentClassifierSummariesList"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["DocumentClassifierName"], "clf");
        assert_eq!(list[0]["NumberOfVersions"], 2);
    }

    #[tokio::test]
    async fn batch_detect_indexes_each_document() {
        let svc = service();
        let resp = svc
            .handle(req(
                "BatchDetectSentiment",
                json!({ "TextList": ["a", "b", "c"], "LanguageCode": "en" }),
            ))
            .await
            .unwrap();
        let v = body_of(resp).await;
        let results = v["ResultList"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[2]["Index"], 2);
        assert_eq!(results[0]["Sentiment"], "NEUTRAL");
    }
}
