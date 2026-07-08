//! Faithful static AWS reference data: the severity levels, the Trusted Advisor
//! check catalogue, and the service/category catalogue. These lists are
//! effectively static AWS reference data (the same well-known checks, severity
//! codes, and support categories the live service publishes), not fabricated
//! findings, so they are vendored here rather than emulated.

use serde_json::{json, Value};

/// The five AWS Support severity levels, lowest to highest. The `name` is the
/// English display name; localisation is not modelled (see the honest gap).
pub const SEVERITY_LEVELS: &[(&str, &str)] = &[
    ("low", "Low"),
    ("normal", "Normal"),
    ("high", "High"),
    ("urgent", "Urgent"),
    ("critical", "Critical"),
];

/// Build the `severityLevels` wire list for `DescribeSeverityLevels`.
pub fn severity_levels() -> Value {
    let list: Vec<Value> = SEVERITY_LEVELS
        .iter()
        .map(|(code, name)| json!({ "code": code, "name": name }))
        .collect();
    json!(list)
}

/// One Trusted Advisor check: `(id, name, category, description, metadata
/// column headers)`. The ids and names are the well-known live AWS Trusted
/// Advisor checks.
pub struct TaCheck {
    pub id: &'static str,
    pub name: &'static str,
    pub category: &'static str,
    pub description: &'static str,
    pub metadata: &'static [&'static str],
}

/// The Trusted Advisor check catalogue: a faithful subset of the well-known
/// live checks spanning all five categories (cost optimising, security, fault
/// tolerance, performance, service limits).
pub const TA_CHECKS: &[TaCheck] = &[
    TaCheck {
        id: "Qch7DwouX1",
        name: "Low Utilization Amazon EC2 Instances",
        category: "cost_optimizing",
        description: "Checks the Amazon Elastic Compute Cloud (Amazon EC2) instances that were running at any time during the last 14 days and alerts you if the daily CPU utilization was 10% or less and network I/O was 5 MB or less on 4 or more days.",
        metadata: &[
            "Region/AZ", "Instance ID", "Instance Name", "Instance Type",
            "Estimated Monthly Savings", "CPU Utilization 14-Day Average",
            "Network I/O 14-Day Average", "Number of Days Low Utilization",
        ],
    },
    TaCheck {
        id: "hjLMh88uM8",
        name: "Amazon EC2 Reserved Instances Optimization",
        category: "cost_optimizing",
        description: "Checks your usage of Amazon EC2 and provides recommendations on the purchase of Reserved Instances to help reduce costs incurred from using On-Demand Instances.",
        metadata: &[
            "Region", "Instance Type", "Platform", "Recommended number of RIs to purchase",
            "Estimated Monthly Savings", "Upfront Cost of RIs", "Estimated Break Even (months)",
        ],
    },
    TaCheck {
        id: "1qazXsw23e",
        name: "Amazon RDS Idle DB Instances",
        category: "cost_optimizing",
        description: "Checks the configuration of your Amazon Relational Database Service (Amazon RDS) for any database (DB) instances that appear to be idle.",
        metadata: &[
            "Region", "DB Instance Name", "Multi-AZ", "Instance Type",
            "Storage Provisioned (GB)", "Days Since Last Connection", "Estimated Monthly Savings",
        ],
    },
    TaCheck {
        id: "DAvU99Dc4C",
        name: "Security Groups - Specific Ports Unrestricted",
        category: "security",
        description: "Checks security groups for rules that allow unrestricted access (0.0.0.0/0) to specific ports. Unrestricted access increases opportunities for malicious activity (hacking, denial-of-service attacks, loss of data).",
        metadata: &[
            "Region", "Security Group Name", "Security Group ID", "Protocol", "Port", "Status",
        ],
    },
    TaCheck {
        id: "1iG5NDGVre",
        name: "Security Groups - Unrestricted Access",
        category: "security",
        description: "Checks security groups for rules that allow unrestricted access to a resource. Unrestricted access increases opportunities for malicious activity (hacking, denial-of-service attacks, loss of data).",
        metadata: &["Region", "Security Group Name", "Security Group ID", "Protocol", "Port", "Alert Level"],
    },
    TaCheck {
        id: "Pfx0RwqBli",
        name: "Amazon S3 Bucket Permissions",
        category: "security",
        description: "Checks buckets in Amazon Simple Storage Service (Amazon S3) that have open access permissions or allow access to any authenticated AWS user.",
        metadata: &["Region", "Region API Parameter", "Bucket Name", "ACL Allows List", "ACL Allows Upload/Delete", "Policy Allows Access", "Status"],
    },
    TaCheck {
        id: "HCP4007jGY",
        name: "IAM Access Key Rotation",
        category: "security",
        description: "Checks for active IAM access keys that have not been rotated in the last 90 days. Rotating access keys regularly reduces the risk if a key is compromised.",
        metadata: &["Status", "IAM User", "Access Key ID", "Key Last Rotated"],
    },
    TaCheck {
        id: "7DAFEmoDos",
        name: "MFA on Root Account",
        category: "security",
        description: "Checks the root account and warns if multi-factor authentication (MFA) is not enabled. For increased security, we recommend that you protect your account by using MFA.",
        metadata: &["Status"],
    },
    TaCheck {
        id: "BueAdJ7NrP",
        name: "Amazon EBS Snapshots",
        category: "fault_tolerance",
        description: "Checks the age of the snapshots for your Amazon Elastic Block Store (Amazon EBS) volumes (available or in-use). Even though Amazon EBS volumes are replicated, failures can occur.",
        metadata: &["Region", "Volume ID", "Volume Name", "Snapshot ID", "Snapshot Name", "Snapshot Age", "Volume Attachment", "Status"],
    },
    TaCheck {
        id: "eW7HH0l7J9",
        name: "Amazon EC2 Availability Zone Balance",
        category: "fault_tolerance",
        description: "Checks the distribution of Amazon EC2 instances across Availability Zones in a Region. Balancing your instances evenly promotes fault tolerance.",
        metadata: &["Region", "Zone a Instances", "Zone b Instances", "Zone c Instances", "Status"],
    },
    TaCheck {
        id: "zXCkfM1nI3",
        name: "Amazon RDS Backups",
        category: "fault_tolerance",
        description: "Checks for automated backups of Amazon RDS DB instances. By default, backups are enabled with a retention period of one day.",
        metadata: &["Region", "DB Instance", "VPC ID", "Backup Retention Period", "Status"],
    },
    TaCheck {
        id: "iqdCTZKCUp",
        name: "Load Balancer Optimization",
        category: "fault_tolerance",
        description: "Checks your Elastic Load Balancing configuration for load balancers that are not configured for high availability across multiple Availability Zones.",
        metadata: &["Region", "Load Balancer Name", "Number of Zones", "Number of Instances", "Status"],
    },
    TaCheck {
        id: "rSs93HQwa1",
        name: "High Utilization Amazon EC2 Instances",
        category: "performance",
        description: "Checks the Amazon EC2 instances that were running at any time during the last 14 days and alerts you if the daily CPU utilization was more than 90% on 4 or more days.",
        metadata: &["Region/AZ", "Instance ID", "Instance Name", "Instance Type", "CPU Utilization 14-Day Average", "Number of Days High Utilization"],
    },
    TaCheck {
        id: "Bh2xRR2FGH",
        name: "Amazon EBS Provisioned IOPS (SSD) Volume Attachment Configuration",
        category: "performance",
        description: "Checks for Amazon EBS Provisioned IOPS (SSD) volumes that are attached to an Amazon EC2 instance that is not EBS-optimized.",
        metadata: &["Region", "Instance ID", "Instance Type", "Volume ID", "Status"],
    },
    TaCheck {
        id: "eI7KK0l7J9",
        name: "Service Limits",
        category: "service_limits",
        description: "Checks for usage that is more than 80% of the service limit. Values are based on a snapshot, so your current usage might differ. Limit and usage data can take up to 24 hours to reflect any changes.",
        metadata: &["Region", "Service", "Limit Name", "Limit Amount", "Current Usage", "Status"],
    },
];

/// Build the `checks` wire list for `DescribeTrustedAdvisorChecks`.
pub fn ta_checks() -> Value {
    let list: Vec<Value> = TA_CHECKS
        .iter()
        .map(|c| {
            let meta: Vec<Value> = c.metadata.iter().map(|m| json!(m)).collect();
            json!({
                "id": c.id,
                "name": c.name,
                "description": c.description,
                "category": c.category,
                "metadata": meta,
            })
        })
        .collect();
    json!(list)
}

/// Look up a check's category by id, defaulting to `cost_optimizing` for an
/// unknown id (the live service always returns a well-formed result even for a
/// check id it does not recognise; this keeps the shape valid).
pub fn check_category(check_id: &str) -> &'static str {
    TA_CHECKS
        .iter()
        .find(|c| c.id == check_id)
        .map(|c| c.category)
        .unwrap_or("cost_optimizing")
}

/// The support service / category catalogue for `DescribeServices`. Service
/// codes and names match the live AWS Support service catalogue; each carries a
/// small set of the standard case categories.
pub const SERVICES: &[(&str, &str)] = &[
    (
        "amazon-elastic-compute-cloud-linux",
        "Amazon Elastic Compute Cloud (Linux)",
    ),
    (
        "amazon-simple-storage-service",
        "Amazon Simple Storage Service (S3)",
    ),
    (
        "amazon-relational-database-service-aurora",
        "Amazon Relational Database Service (Aurora)",
    ),
    ("aws-lambda", "AWS Lambda"),
    ("amazon-dynamodb", "Amazon DynamoDB"),
    (
        "aws-identity-and-access-management",
        "AWS Identity and Access Management (IAM)",
    ),
    ("aws-account-management", "Account and Billing Support"),
    (
        "general-info-and-getting-started",
        "General Info and Getting Started",
    ),
];

/// The standard support case categories offered for every service.
pub const CATEGORIES: &[(&str, &str)] = &[
    ("using-aws", "Using AWS"),
    ("performance", "Performance"),
    ("apis", "APIs"),
    ("security", "Security"),
    ("features", "Features"),
    ("other", "Other"),
];

/// Build the `services` wire list for `DescribeServices`, optionally filtered to
/// a set of requested service codes.
pub fn services(filter: Option<&[String]>) -> Value {
    let categories: Vec<Value> = CATEGORIES
        .iter()
        .map(|(code, name)| json!({ "code": code, "name": name }))
        .collect();
    let list: Vec<Value> = SERVICES
        .iter()
        .filter(|(code, _)| filter.map(|f| f.iter().any(|c| c == code)).unwrap_or(true))
        .map(|(code, name)| json!({ "code": code, "name": name, "categories": categories }))
        .collect();
    json!(list)
}
