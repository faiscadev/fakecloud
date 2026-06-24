//! SES v1 Query protocol handlers for identity management, sending,
//! templates, configuration sets, receipt rules, receipt filters,
//! and inbound email processing.

use chrono::Utc;
use http::StatusCode;
use std::collections::HashMap;

use fakecloud_core::query::query_response_xml;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    ConfigurationSet, EmailIdentity, EmailTemplate, EventDestination, IpFilter, ReceiptAction,
    ReceiptFilter, ReceiptRule, ReceiptRuleSet, SentEmail, SesState, SharedSesState,
};

/// XML namespace for SES v1 responses.
const SES_NS: &str = "http://ses.amazonaws.com/doc/2010-12-01/";

/// List of v1 actions supported.
pub const V1_ACTIONS: &[&str] = &[
    // Identity management
    "VerifyEmailIdentity",
    "VerifyDomainIdentity",
    "VerifyDomainDkim",
    // Legacy v1 email-verification aliases. Real SES still accepts
    // these; the v2 surface uses the *Identity ops above.
    "VerifyEmailAddress",
    "ListVerifiedEmailAddresses",
    "DeleteVerifiedEmailAddress",
    "ListIdentities",
    "GetIdentityVerificationAttributes",
    "GetIdentityDkimAttributes",
    "DeleteIdentity",
    "SetIdentityDkimEnabled",
    // Identity notification/mail-from
    "SetIdentityNotificationTopic",
    "SetIdentityFeedbackForwardingEnabled",
    "GetIdentityNotificationAttributes",
    "GetIdentityMailFromDomainAttributes",
    "SetIdentityMailFromDomain",
    // Sending
    "SendEmail",
    "SendRawEmail",
    "SendTemplatedEmail",
    "SendBulkTemplatedEmail",
    "SendBounce",
    // Templates
    "CreateTemplate",
    "GetTemplate",
    "ListTemplates",
    "DeleteTemplate",
    "UpdateTemplate",
    // Configuration Sets
    "CreateConfigurationSet",
    "DeleteConfigurationSet",
    "DescribeConfigurationSet",
    "ListConfigurationSets",
    "CreateConfigurationSetEventDestination",
    "UpdateConfigurationSetEventDestination",
    "DeleteConfigurationSetEventDestination",
    // Account / Quota
    "GetSendQuota",
    "GetSendStatistics",
    "GetAccountSendingEnabled",
    // Receipt Rule Sets
    "CreateReceiptRuleSet",
    "DeleteReceiptRuleSet",
    "DescribeReceiptRuleSet",
    "ListReceiptRuleSets",
    "CloneReceiptRuleSet",
    "SetActiveReceiptRuleSet",
    "ReorderReceiptRuleSet",
    // Receipt Rules
    "CreateReceiptRule",
    "DeleteReceiptRule",
    "DescribeReceiptRule",
    "UpdateReceiptRule",
    // Receipt Filters
    "CreateReceiptFilter",
    "DeleteReceiptFilter",
    "ListReceiptFilters",
];

// ── Helpers ──

// ── Identity management operations ──

// ── Identity notification/mail-from attribute operations ──

// ── Sending operations ──

// ── Template operations ──

// ── Configuration Set operations ──

// ── Configuration Set Event Destination operations ──

// ── Account / Quota operations ──

// ── Simple random helpers (no external deps) ──

// ── Receipt Rule Set operations ──

// ── Receipt Rule operations ──

// ── Receipt Filter operations ──

// ── Inbound email processing ──

#[path = "v1_helpers.rs"]
mod v1_helpers;
pub use v1_helpers::*;

#[cfg(test)]
#[path = "v1_tests.rs"]
mod tests;
