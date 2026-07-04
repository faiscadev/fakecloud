//! Model-derived input validation for Transfer Family operations.
//!
//! Each operation's constrained top-level input members (string `@length`,
//! integer `@range`, and `enum` value sets) are validated against the AWS
//! Smithy model. A violation yields an `InvalidRequestException`, matching the
//! real service's client-side validation. Members that are absent from the
//! request are skipped (required-field presence is enforced by the handlers).

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::AwsServiceError;

enum RuleKind {
    Str {
        min: Option<usize>,
        max: Option<usize>,
    },
    Int {
        min: Option<i64>,
        max: Option<i64>,
    },
    Enum(&'static [&'static str]),
}

struct Rule {
    field: &'static str,
    kind: RuleKind,
}

impl Rule {
    const fn str_(field: &'static str, min: Option<usize>, max: Option<usize>) -> Rule {
        Rule {
            field,
            kind: RuleKind::Str { min, max },
        }
    }
    const fn int_(field: &'static str, min: Option<i64>, max: Option<i64>) -> Rule {
        Rule {
            field,
            kind: RuleKind::Int { min, max },
        }
    }
    const fn enum_(field: &'static str, values: &'static [&'static str]) -> Rule {
        Rule {
            field,
            kind: RuleKind::Enum(values),
        }
    }
}

fn invalid(msg: String) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", &msg)
}

/// Validate the constrained top-level members of `body` for `action`.
pub(crate) fn validate_input(action: &str, body: &Value) -> Result<(), AwsServiceError> {
    for rule in rules_for(action) {
        let Some(v) = body.get(rule.field) else {
            continue;
        };
        match &rule.kind {
            RuleKind::Str { min, max } => {
                if let Some(s) = v.as_str() {
                    let len = s.chars().count();
                    if let Some(min) = min {
                        if len < *min {
                            return Err(invalid(format!(
                                "{} must have length greater than or equal to {}.",
                                rule.field, min
                            )));
                        }
                    }
                    if let Some(max) = max {
                        if len > *max {
                            return Err(invalid(format!(
                                "{} must have length less than or equal to {}.",
                                rule.field, max
                            )));
                        }
                    }
                }
            }
            RuleKind::Int { min, max } => {
                if let Some(n) = v.as_i64() {
                    if let Some(min) = min {
                        if n < *min {
                            return Err(invalid(format!(
                                "{} must be greater than or equal to {}.",
                                rule.field, min
                            )));
                        }
                    }
                    if let Some(max) = max {
                        if n > *max {
                            return Err(invalid(format!(
                                "{} must be less than or equal to {}.",
                                rule.field, max
                            )));
                        }
                    }
                }
            }
            RuleKind::Enum(values) => {
                if let Some(s) = v.as_str() {
                    if !values.contains(&s) {
                        return Err(invalid(format!(
                            "{} '{}' is not a valid value. Valid values are: {}.",
                            rule.field,
                            s,
                            values.join(", ")
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Per-operation constrained-member rules, derived from the Smithy model.
fn rules_for(action: &str) -> &'static [Rule] {
    match action {
        "CreateAccess" => {
            const R: &[Rule] = &[
                Rule::str_("HomeDirectory", Some(0), Some(1024)),
                Rule::enum_("HomeDirectoryType", &["PATH", "LOGICAL"]),
                Rule::str_("Policy", Some(0), Some(2048)),
                Rule::str_("Role", Some(20), Some(2048)),
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("ExternalId", Some(1), Some(256)),
            ];
            R
        }
        "CreateAgreement" => {
            const R: &[Rule] = &[
                Rule::str_("Description", Some(1), Some(200)),
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("LocalProfileId", Some(19), Some(19)),
                Rule::str_("PartnerProfileId", Some(19), Some(19)),
                Rule::str_("BaseDirectory", Some(0), Some(1024)),
                Rule::str_("AccessRole", Some(20), Some(2048)),
                Rule::enum_("Status", &["ACTIVE", "INACTIVE"]),
                Rule::enum_("PreserveFilename", &["ENABLED", "DISABLED"]),
                Rule::enum_("EnforceMessageSigning", &["ENABLED", "DISABLED"]),
            ];
            R
        }
        "CreateConnector" => {
            const R: &[Rule] = &[
                Rule::str_("Url", Some(0), Some(255)),
                Rule::str_("AccessRole", Some(20), Some(2048)),
                Rule::str_("LoggingRole", Some(20), Some(2048)),
                Rule::str_("SecurityPolicyName", Some(0), Some(100)),
                Rule::enum_("IpAddressType", &["IPV4", "DUALSTACK"]),
            ];
            R
        }
        "CreateProfile" => {
            const R: &[Rule] = &[
                Rule::str_("As2Id", Some(1), Some(128)),
                Rule::enum_("ProfileType", &["LOCAL", "PARTNER"]),
            ];
            R
        }
        "CreateServer" => {
            const R: &[Rule] = &[
                Rule::str_("Certificate", Some(0), Some(1600)),
                Rule::enum_("Domain", &["S3", "EFS"]),
                Rule::enum_("EndpointType", &["PUBLIC", "VPC", "VPC_ENDPOINT"]),
                Rule::str_("HostKey", Some(0), Some(4096)),
                Rule::enum_(
                    "IdentityProviderType",
                    &[
                        "SERVICE_MANAGED",
                        "API_GATEWAY",
                        "AWS_DIRECTORY_SERVICE",
                        "AWS_LAMBDA",
                    ],
                ),
                Rule::str_("LoggingRole", Some(0), Some(2048)),
                Rule::str_("PostAuthenticationLoginBanner", Some(0), Some(4096)),
                Rule::str_("PreAuthenticationLoginBanner", Some(0), Some(4096)),
                Rule::str_("SecurityPolicyName", Some(0), Some(100)),
                Rule::enum_("IpAddressType", &["IPV4", "DUALSTACK"]),
            ];
            R
        }
        "CreateUser" => {
            const R: &[Rule] = &[
                Rule::str_("HomeDirectory", Some(0), Some(1024)),
                Rule::enum_("HomeDirectoryType", &["PATH", "LOGICAL"]),
                Rule::str_("Policy", Some(0), Some(2048)),
                Rule::str_("Role", Some(20), Some(2048)),
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("SshPublicKeyBody", Some(0), Some(2048)),
                Rule::str_("UserName", Some(3), Some(100)),
            ];
            R
        }
        "CreateWebApp" => {
            const R: &[Rule] = &[
                Rule::str_("AccessEndpoint", Some(1), Some(1024)),
                Rule::enum_("WebAppEndpointPolicy", &["FIPS", "STANDARD"]),
            ];
            R
        }
        "CreateWorkflow" => {
            const R: &[Rule] = &[Rule::str_("Description", Some(0), Some(256))];
            R
        }
        "DeleteAccess" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("ExternalId", Some(1), Some(256)),
            ];
            R
        }
        "DeleteAgreement" => {
            const R: &[Rule] = &[
                Rule::str_("AgreementId", Some(19), Some(19)),
                Rule::str_("ServerId", Some(19), Some(19)),
            ];
            R
        }
        "DeleteCertificate" => {
            const R: &[Rule] = &[Rule::str_("CertificateId", Some(22), Some(22))];
            R
        }
        "DeleteConnector" => {
            const R: &[Rule] = &[Rule::str_("ConnectorId", Some(19), Some(19))];
            R
        }
        "DeleteHostKey" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("HostKeyId", Some(25), Some(25)),
            ];
            R
        }
        "DeleteProfile" => {
            const R: &[Rule] = &[Rule::str_("ProfileId", Some(19), Some(19))];
            R
        }
        "DeleteServer" => {
            const R: &[Rule] = &[Rule::str_("ServerId", Some(19), Some(19))];
            R
        }
        "DeleteSshPublicKey" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("SshPublicKeyId", Some(21), Some(21)),
                Rule::str_("UserName", Some(3), Some(100)),
            ];
            R
        }
        "DeleteUser" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("UserName", Some(3), Some(100)),
            ];
            R
        }
        "DeleteWebApp" => {
            const R: &[Rule] = &[Rule::str_("WebAppId", Some(24), Some(24))];
            R
        }
        "DeleteWebAppCustomization" => {
            const R: &[Rule] = &[Rule::str_("WebAppId", Some(24), Some(24))];
            R
        }
        "DeleteWorkflow" => {
            const R: &[Rule] = &[Rule::str_("WorkflowId", Some(19), Some(19))];
            R
        }
        "DescribeAccess" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("ExternalId", Some(1), Some(256)),
            ];
            R
        }
        "DescribeAgreement" => {
            const R: &[Rule] = &[
                Rule::str_("AgreementId", Some(19), Some(19)),
                Rule::str_("ServerId", Some(19), Some(19)),
            ];
            R
        }
        "DescribeCertificate" => {
            const R: &[Rule] = &[Rule::str_("CertificateId", Some(22), Some(22))];
            R
        }
        "DescribeConnector" => {
            const R: &[Rule] = &[Rule::str_("ConnectorId", Some(19), Some(19))];
            R
        }
        "DescribeExecution" => {
            const R: &[Rule] = &[
                Rule::str_("ExecutionId", Some(36), Some(36)),
                Rule::str_("WorkflowId", Some(19), Some(19)),
            ];
            R
        }
        "DescribeHostKey" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("HostKeyId", Some(25), Some(25)),
            ];
            R
        }
        "DescribeProfile" => {
            const R: &[Rule] = &[Rule::str_("ProfileId", Some(19), Some(19))];
            R
        }
        "DescribeSecurityPolicy" => {
            const R: &[Rule] = &[Rule::str_("SecurityPolicyName", Some(0), Some(100))];
            R
        }
        "DescribeServer" => {
            const R: &[Rule] = &[Rule::str_("ServerId", Some(19), Some(19))];
            R
        }
        "DescribeUser" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("UserName", Some(3), Some(100)),
            ];
            R
        }
        "DescribeWebApp" => {
            const R: &[Rule] = &[Rule::str_("WebAppId", Some(24), Some(24))];
            R
        }
        "DescribeWebAppCustomization" => {
            const R: &[Rule] = &[Rule::str_("WebAppId", Some(24), Some(24))];
            R
        }
        "DescribeWorkflow" => {
            const R: &[Rule] = &[Rule::str_("WorkflowId", Some(19), Some(19))];
            R
        }
        "ImportCertificate" => {
            const R: &[Rule] = &[
                Rule::enum_("Usage", &["SIGNING", "ENCRYPTION", "TLS"]),
                Rule::str_("Certificate", Some(1), Some(16384)),
                Rule::str_("CertificateChain", Some(1), Some(2097152)),
                Rule::str_("PrivateKey", Some(1), Some(16384)),
                Rule::str_("Description", Some(1), Some(200)),
            ];
            R
        }
        "ImportHostKey" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("HostKeyBody", Some(0), Some(4096)),
                Rule::str_("Description", Some(0), Some(200)),
            ];
            R
        }
        "ImportSshPublicKey" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("SshPublicKeyBody", Some(0), Some(2048)),
                Rule::str_("UserName", Some(3), Some(100)),
            ];
            R
        }
        "ListAccesses" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
                Rule::str_("ServerId", Some(19), Some(19)),
            ];
            R
        }
        "ListAgreements" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
                Rule::str_("ServerId", Some(19), Some(19)),
            ];
            R
        }
        "ListCertificates" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
            ];
            R
        }
        "ListConnectors" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
            ];
            R
        }
        "ListExecutions" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
                Rule::str_("WorkflowId", Some(19), Some(19)),
            ];
            R
        }
        "ListFileTransferResults" => {
            const R: &[Rule] = &[
                Rule::str_("ConnectorId", Some(19), Some(19)),
                Rule::str_("TransferId", Some(1), Some(512)),
                Rule::str_("NextToken", Some(1), Some(6144)),
                Rule::int_("MaxResults", Some(1), Some(1000)),
            ];
            R
        }
        "ListHostKeys" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
                Rule::str_("ServerId", Some(19), Some(19)),
            ];
            R
        }
        "ListProfiles" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
                Rule::enum_("ProfileType", &["LOCAL", "PARTNER"]),
            ];
            R
        }
        "ListSecurityPolicies" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
            ];
            R
        }
        "ListServers" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
            ];
            R
        }
        "ListTagsForResource" => {
            const R: &[Rule] = &[
                Rule::str_("Arn", Some(20), Some(1600)),
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
            ];
            R
        }
        "ListUsers" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
                Rule::str_("ServerId", Some(19), Some(19)),
            ];
            R
        }
        "ListWebApps" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
            ];
            R
        }
        "ListWorkflows" => {
            const R: &[Rule] = &[
                Rule::int_("MaxResults", Some(1), Some(1000)),
                Rule::str_("NextToken", Some(1), Some(6144)),
            ];
            R
        }
        "SendWorkflowStepState" => {
            const R: &[Rule] = &[
                Rule::str_("WorkflowId", Some(19), Some(19)),
                Rule::str_("ExecutionId", Some(36), Some(36)),
                Rule::str_("Token", Some(1), Some(64)),
                Rule::enum_("Status", &["SUCCESS", "FAILURE"]),
            ];
            R
        }
        "StartDirectoryListing" => {
            const R: &[Rule] = &[
                Rule::str_("ConnectorId", Some(19), Some(19)),
                Rule::str_("RemoteDirectoryPath", Some(1), Some(1024)),
                Rule::int_("MaxItems", Some(1), None),
                Rule::str_("OutputDirectoryPath", Some(1), Some(1024)),
            ];
            R
        }
        "StartFileTransfer" => {
            const R: &[Rule] = &[
                Rule::str_("ConnectorId", Some(19), Some(19)),
                Rule::str_("LocalDirectoryPath", Some(1), Some(1024)),
                Rule::str_("RemoteDirectoryPath", Some(1), Some(1024)),
            ];
            R
        }
        "StartRemoteDelete" => {
            const R: &[Rule] = &[
                Rule::str_("ConnectorId", Some(19), Some(19)),
                Rule::str_("DeletePath", Some(1), Some(1024)),
            ];
            R
        }
        "StartRemoteMove" => {
            const R: &[Rule] = &[
                Rule::str_("ConnectorId", Some(19), Some(19)),
                Rule::str_("SourcePath", Some(1), Some(1024)),
                Rule::str_("TargetPath", Some(1), Some(1024)),
            ];
            R
        }
        "StartServer" => {
            const R: &[Rule] = &[Rule::str_("ServerId", Some(19), Some(19))];
            R
        }
        "StopServer" => {
            const R: &[Rule] = &[Rule::str_("ServerId", Some(19), Some(19))];
            R
        }
        "TagResource" => {
            const R: &[Rule] = &[Rule::str_("Arn", Some(20), Some(1600))];
            R
        }
        "TestConnection" => {
            const R: &[Rule] = &[Rule::str_("ConnectorId", Some(19), Some(19))];
            R
        }
        "TestIdentityProvider" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::enum_("ServerProtocol", &["SFTP", "FTP", "FTPS", "AS2"]),
                Rule::str_("SourceIp", Some(0), Some(32)),
                Rule::str_("UserName", Some(3), Some(100)),
                Rule::str_("UserPassword", Some(0), Some(1024)),
            ];
            R
        }
        "UntagResource" => {
            const R: &[Rule] = &[Rule::str_("Arn", Some(20), Some(1600))];
            R
        }
        "UpdateAccess" => {
            const R: &[Rule] = &[
                Rule::str_("HomeDirectory", Some(0), Some(1024)),
                Rule::enum_("HomeDirectoryType", &["PATH", "LOGICAL"]),
                Rule::str_("Policy", Some(0), Some(2048)),
                Rule::str_("Role", Some(20), Some(2048)),
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("ExternalId", Some(1), Some(256)),
            ];
            R
        }
        "UpdateAgreement" => {
            const R: &[Rule] = &[
                Rule::str_("AgreementId", Some(19), Some(19)),
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("Description", Some(1), Some(200)),
                Rule::enum_("Status", &["ACTIVE", "INACTIVE"]),
                Rule::str_("LocalProfileId", Some(19), Some(19)),
                Rule::str_("PartnerProfileId", Some(19), Some(19)),
                Rule::str_("BaseDirectory", Some(0), Some(1024)),
                Rule::str_("AccessRole", Some(20), Some(2048)),
                Rule::enum_("PreserveFilename", &["ENABLED", "DISABLED"]),
                Rule::enum_("EnforceMessageSigning", &["ENABLED", "DISABLED"]),
            ];
            R
        }
        "UpdateCertificate" => {
            const R: &[Rule] = &[
                Rule::str_("CertificateId", Some(22), Some(22)),
                Rule::str_("Description", Some(1), Some(200)),
            ];
            R
        }
        "UpdateConnector" => {
            const R: &[Rule] = &[
                Rule::str_("ConnectorId", Some(19), Some(19)),
                Rule::str_("Url", Some(0), Some(255)),
                Rule::str_("AccessRole", Some(20), Some(2048)),
                Rule::str_("LoggingRole", Some(20), Some(2048)),
                Rule::str_("SecurityPolicyName", Some(0), Some(100)),
                Rule::enum_("IpAddressType", &["IPV4", "DUALSTACK"]),
            ];
            R
        }
        "UpdateHostKey" => {
            const R: &[Rule] = &[
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("HostKeyId", Some(25), Some(25)),
                Rule::str_("Description", Some(0), Some(200)),
            ];
            R
        }
        "UpdateProfile" => {
            const R: &[Rule] = &[Rule::str_("ProfileId", Some(19), Some(19))];
            R
        }
        "UpdateServer" => {
            const R: &[Rule] = &[
                Rule::str_("Certificate", Some(0), Some(1600)),
                Rule::enum_("EndpointType", &["PUBLIC", "VPC", "VPC_ENDPOINT"]),
                Rule::str_("HostKey", Some(0), Some(4096)),
                Rule::str_("LoggingRole", Some(0), Some(2048)),
                Rule::str_("PostAuthenticationLoginBanner", Some(0), Some(4096)),
                Rule::str_("PreAuthenticationLoginBanner", Some(0), Some(4096)),
                Rule::str_("SecurityPolicyName", Some(0), Some(100)),
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::enum_("IpAddressType", &["IPV4", "DUALSTACK"]),
                Rule::enum_(
                    "IdentityProviderType",
                    &[
                        "SERVICE_MANAGED",
                        "API_GATEWAY",
                        "AWS_DIRECTORY_SERVICE",
                        "AWS_LAMBDA",
                    ],
                ),
            ];
            R
        }
        "UpdateUser" => {
            const R: &[Rule] = &[
                Rule::str_("HomeDirectory", Some(0), Some(1024)),
                Rule::enum_("HomeDirectoryType", &["PATH", "LOGICAL"]),
                Rule::str_("Policy", Some(0), Some(2048)),
                Rule::str_("Role", Some(20), Some(2048)),
                Rule::str_("ServerId", Some(19), Some(19)),
                Rule::str_("UserName", Some(3), Some(100)),
            ];
            R
        }
        "UpdateWebApp" => {
            const R: &[Rule] = &[
                Rule::str_("WebAppId", Some(24), Some(24)),
                Rule::str_("AccessEndpoint", Some(1), Some(1024)),
            ];
            R
        }
        "UpdateWebAppCustomization" => {
            const R: &[Rule] = &[
                Rule::str_("WebAppId", Some(24), Some(24)),
                Rule::str_("Title", Some(0), Some(100)),
            ];
            R
        }
        _ => &[],
    }
}
