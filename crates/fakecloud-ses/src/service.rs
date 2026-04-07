use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::collections::HashMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    AccountDetails, ConfigurationSet, Contact, ContactList, CustomVerificationEmailTemplate,
    DedicatedIp, DedicatedIpPool, EmailIdentity, EmailTemplate, EventDestination, ExportJob,
    ImportJob, MultiRegionEndpoint, ReputationEntityState, SentEmail, SharedSesState,
    SuppressedDestination, Tenant, TenantResourceAssociation, Topic, TopicPreference,
};

pub struct SesV2Service {
    state: SharedSesState,
}

impl SesV2Service {
    pub fn new(state: SharedSesState) -> Self {
        Self { state }
    }

    /// Determine the action from the HTTP method and path segments.
    /// SES v2 uses REST-style routing with base path /v2/email/:
    ///   GET    /v2/email/account                         -> GetAccount
    ///   POST   /v2/email/identities                      -> CreateEmailIdentity
    ///   GET    /v2/email/identities                      -> ListEmailIdentities
    ///   GET    /v2/email/identities/{id}                 -> GetEmailIdentity
    ///   DELETE /v2/email/identities/{id}                 -> DeleteEmailIdentity
    ///   POST   /v2/email/configuration-sets              -> CreateConfigurationSet
    ///   GET    /v2/email/configuration-sets              -> ListConfigurationSets
    ///   GET    /v2/email/configuration-sets/{name}       -> GetConfigurationSet
    ///   DELETE /v2/email/configuration-sets/{name}       -> DeleteConfigurationSet
    ///   POST   /v2/email/templates                       -> CreateEmailTemplate
    ///   GET    /v2/email/templates                       -> ListEmailTemplates
    ///   GET    /v2/email/templates/{name}                -> GetEmailTemplate
    ///   PUT    /v2/email/templates/{name}                -> UpdateEmailTemplate
    ///   DELETE /v2/email/templates/{name}                -> DeleteEmailTemplate
    ///   POST   /v2/email/outbound-emails                 -> SendEmail
    ///   POST   /v2/email/outbound-bulk-emails            -> SendBulkEmail
    ///   POST   /v2/email/tags                            -> TagResource
    ///   DELETE /v2/email/tags                            -> UntagResource
    ///   GET    /v2/email/tags                            -> ListTagsForResource
    ///   POST   /v2/email/contact-lists                   -> CreateContactList
    ///   GET    /v2/email/contact-lists                   -> ListContactLists
    ///   GET    /v2/email/contact-lists/{name}            -> GetContactList
    ///   PUT    /v2/email/contact-lists/{name}            -> UpdateContactList
    ///   DELETE /v2/email/contact-lists/{name}            -> DeleteContactList
    ///   POST   /v2/email/contact-lists/{name}/contacts   -> CreateContact
    ///   GET    /v2/email/contact-lists/{name}/contacts   -> ListContacts
    ///   GET    /v2/email/contact-lists/{name}/contacts/{email} -> GetContact
    ///   PUT    /v2/email/contact-lists/{name}/contacts/{email} -> UpdateContact
    ///   DELETE /v2/email/contact-lists/{name}/contacts/{email} -> DeleteContact
    ///   PUT    /v2/email/suppression/addresses            -> PutSuppressedDestination
    ///   GET    /v2/email/suppression/addresses            -> ListSuppressedDestinations
    ///   GET    /v2/email/suppression/addresses/{email}    -> GetSuppressedDestination
    ///   DELETE /v2/email/suppression/addresses/{email}    -> DeleteSuppressedDestination
    ///   POST   /v2/email/configuration-sets/{name}/event-destinations -> CreateConfigurationSetEventDestination
    ///   GET    /v2/email/configuration-sets/{name}/event-destinations -> GetConfigurationSetEventDestinations
    ///   PUT    /v2/email/configuration-sets/{name}/event-destinations/{dest} -> UpdateConfigurationSetEventDestination
    ///   DELETE /v2/email/configuration-sets/{name}/event-destinations/{dest} -> DeleteConfigurationSetEventDestination
    ///   POST   /v2/email/identities/{id}/policies/{policy} -> CreateEmailIdentityPolicy
    ///   GET    /v2/email/identities/{id}/policies         -> GetEmailIdentityPolicies
    ///   PUT    /v2/email/identities/{id}/policies/{policy} -> UpdateEmailIdentityPolicy
    ///   DELETE /v2/email/identities/{id}/policies/{policy} -> DeleteEmailIdentityPolicy
    ///   PUT    /v2/email/identities/{id}/dkim              -> PutEmailIdentityDkimAttributes
    ///   PUT    /v2/email/identities/{id}/dkim/signing      -> PutEmailIdentityDkimSigningAttributes
    ///   PUT    /v2/email/identities/{id}/feedback          -> PutEmailIdentityFeedbackAttributes
    ///   PUT    /v2/email/identities/{id}/mail-from         -> PutEmailIdentityMailFromAttributes
    ///   PUT    /v2/email/identities/{id}/configuration-set -> PutEmailIdentityConfigurationSetAttributes
    ///   PUT    /v2/email/configuration-sets/{name}/sending             -> PutConfigurationSetSendingOptions
    ///   PUT    /v2/email/configuration-sets/{name}/delivery-options    -> PutConfigurationSetDeliveryOptions
    ///   PUT    /v2/email/configuration-sets/{name}/tracking-options    -> PutConfigurationSetTrackingOptions
    ///   PUT    /v2/email/configuration-sets/{name}/suppression-options -> PutConfigurationSetSuppressionOptions
    ///   PUT    /v2/email/configuration-sets/{name}/reputation-options  -> PutConfigurationSetReputationOptions
    ///   PUT    /v2/email/configuration-sets/{name}/vdm-options         -> PutConfigurationSetVdmOptions
    ///   PUT    /v2/email/configuration-sets/{name}/archiving-options   -> PutConfigurationSetArchivingOptions
    ///   POST   /v2/email/custom-verification-email-templates           -> CreateCustomVerificationEmailTemplate
    ///   GET    /v2/email/custom-verification-email-templates            -> ListCustomVerificationEmailTemplates
    ///   GET    /v2/email/custom-verification-email-templates/{name}     -> GetCustomVerificationEmailTemplate
    ///   PUT    /v2/email/custom-verification-email-templates/{name}     -> UpdateCustomVerificationEmailTemplate
    ///   DELETE /v2/email/custom-verification-email-templates/{name}     -> DeleteCustomVerificationEmailTemplate
    ///   POST   /v2/email/outbound-custom-verification-emails            -> SendCustomVerificationEmail
    ///   POST   /v2/email/templates/{name}/render                        -> TestRenderEmailTemplate
    ///   POST   /v2/email/import-jobs                                     -> CreateImportJob
    ///   POST   /v2/email/import-jobs/list                                -> ListImportJobs
    ///   GET    /v2/email/import-jobs/{id}                                -> GetImportJob
    ///   POST   /v2/email/export-jobs                                     -> CreateExportJob
    ///   POST   /v2/email/list-export-jobs                                -> ListExportJobs
    ///   PUT    /v2/email/export-jobs/{id}/cancel                         -> CancelExportJob
    ///   GET    /v2/email/export-jobs/{id}                                -> GetExportJob
    ///   POST   /v2/email/tenants                                         -> CreateTenant
    ///   POST   /v2/email/tenants/list                                    -> ListTenants
    ///   POST   /v2/email/tenants/get                                     -> GetTenant
    ///   POST   /v2/email/tenants/delete                                  -> DeleteTenant
    ///   POST   /v2/email/tenants/resources                               -> CreateTenantResourceAssociation
    ///   POST   /v2/email/tenants/resources/delete                        -> DeleteTenantResourceAssociation
    ///   POST   /v2/email/tenants/resources/list                          -> ListTenantResources
    ///   POST   /v2/email/resources/tenants/list                          -> ListResourceTenants
    ///   POST   /v2/email/reputation/entities                             -> ListReputationEntities
    ///   PUT    /v2/email/reputation/entities/{type}/{ref}/customer-managed-status -> UpdateReputationEntityCustomerManagedStatus
    ///   PUT    /v2/email/reputation/entities/{type}/{ref}/policy          -> UpdateReputationEntityPolicy
    ///   GET    /v2/email/reputation/entities/{type}/{ref}                 -> GetReputationEntity
    ///   POST   /v2/email/metrics/batch                                   -> BatchGetMetricData
    fn resolve_action(req: &AwsRequest) -> Option<(&str, Option<String>, Option<String>)> {
        let segs = &req.path_segments;

        // Expect first two segments to be "v2" and "email"
        if segs.len() < 3 || segs[0] != "v2" || segs[1] != "email" {
            return None;
        }

        // URL-decode the resource name (e.g. test%40example.com -> test@example.com)
        let decode = |s: &str| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into_owned()
        };
        let resource = segs.get(3).map(|s| decode(s));

        match (req.method.clone(), segs.len()) {
            // /v2/email/account
            (Method::GET, 3) if segs[2] == "account" => Some(("GetAccount", None, None)),

            // /v2/email/identities
            (Method::POST, 3) if segs[2] == "identities" => {
                Some(("CreateEmailIdentity", None, None))
            }
            (Method::GET, 3) if segs[2] == "identities" => {
                Some(("ListEmailIdentities", None, None))
            }
            // /v2/email/identities/{id}
            (Method::GET, 4) if segs[2] == "identities" => {
                Some(("GetEmailIdentity", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "identities" => {
                Some(("DeleteEmailIdentity", resource, None))
            }

            // /v2/email/configuration-sets
            (Method::POST, 3) if segs[2] == "configuration-sets" => {
                Some(("CreateConfigurationSet", None, None))
            }
            (Method::GET, 3) if segs[2] == "configuration-sets" => {
                Some(("ListConfigurationSets", None, None))
            }
            // /v2/email/configuration-sets/{name}
            (Method::GET, 4) if segs[2] == "configuration-sets" => {
                Some(("GetConfigurationSet", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "configuration-sets" => {
                Some(("DeleteConfigurationSet", resource, None))
            }

            // /v2/email/templates
            (Method::POST, 3) if segs[2] == "templates" => {
                Some(("CreateEmailTemplate", None, None))
            }
            (Method::GET, 3) if segs[2] == "templates" => Some(("ListEmailTemplates", None, None)),
            // /v2/email/templates/{name}
            (Method::GET, 4) if segs[2] == "templates" => {
                Some(("GetEmailTemplate", resource, None))
            }
            (Method::PUT, 4) if segs[2] == "templates" => {
                Some(("UpdateEmailTemplate", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "templates" => {
                Some(("DeleteEmailTemplate", resource, None))
            }

            // /v2/email/outbound-emails
            (Method::POST, 3) if segs[2] == "outbound-emails" => Some(("SendEmail", None, None)),

            // /v2/email/outbound-bulk-emails
            (Method::POST, 3) if segs[2] == "outbound-bulk-emails" => {
                Some(("SendBulkEmail", None, None))
            }

            // /v2/email/contact-lists
            (Method::POST, 3) if segs[2] == "contact-lists" => {
                Some(("CreateContactList", None, None))
            }
            (Method::GET, 3) if segs[2] == "contact-lists" => {
                Some(("ListContactLists", None, None))
            }
            // /v2/email/contact-lists/{name}
            (Method::GET, 4) if segs[2] == "contact-lists" => {
                Some(("GetContactList", resource, None))
            }
            (Method::PUT, 4) if segs[2] == "contact-lists" => {
                Some(("UpdateContactList", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "contact-lists" => {
                Some(("DeleteContactList", resource, None))
            }
            // /v2/email/tags
            (Method::POST, 3) if segs[2] == "tags" => Some(("TagResource", None, None)),
            (Method::DELETE, 3) if segs[2] == "tags" => Some(("UntagResource", None, None)),
            (Method::GET, 3) if segs[2] == "tags" => Some(("ListTagsForResource", None, None)),

            // /v2/email/contact-lists/{name}/contacts
            (Method::POST, 5) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("CreateContact", resource, None))
            }
            (Method::GET, 5) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("ListContacts", resource, None))
            }
            // /v2/email/contact-lists/{name}/contacts/list (SDK sends POST for ListContacts)
            (Method::POST, 6)
                if segs[2] == "contact-lists" && segs[4] == "contacts" && segs[5] == "list" =>
            {
                Some(("ListContacts", resource, None))
            }
            // /v2/email/contact-lists/{name}/contacts/{email}
            (Method::GET, 6) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("GetContact", resource, Some(decode(&segs[5]))))
            }
            (Method::PUT, 6) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("UpdateContact", resource, Some(decode(&segs[5]))))
            }
            (Method::DELETE, 6) if segs[2] == "contact-lists" && segs[4] == "contacts" => {
                Some(("DeleteContact", resource, Some(decode(&segs[5]))))
            }

            // /v2/email/suppression/addresses
            (Method::PUT, 4) if segs[2] == "suppression" && segs[3] == "addresses" => {
                Some(("PutSuppressedDestination", None, None))
            }
            (Method::GET, 4) if segs[2] == "suppression" && segs[3] == "addresses" => {
                Some(("ListSuppressedDestinations", None, None))
            }
            // /v2/email/suppression/addresses/{email}
            (Method::GET, 5) if segs[2] == "suppression" && segs[3] == "addresses" => {
                Some(("GetSuppressedDestination", Some(decode(&segs[4])), None))
            }
            (Method::DELETE, 5) if segs[2] == "suppression" && segs[3] == "addresses" => {
                Some(("DeleteSuppressedDestination", Some(decode(&segs[4])), None))
            }

            // /v2/email/configuration-sets/{name}/event-destinations
            (Method::POST, 5)
                if segs[2] == "configuration-sets" && segs[4] == "event-destinations" =>
            {
                Some(("CreateConfigurationSetEventDestination", resource, None))
            }
            (Method::GET, 5)
                if segs[2] == "configuration-sets" && segs[4] == "event-destinations" =>
            {
                Some(("GetConfigurationSetEventDestinations", resource, None))
            }
            // /v2/email/configuration-sets/{name}/event-destinations/{dest-name}
            (Method::PUT, 6)
                if segs[2] == "configuration-sets" && segs[4] == "event-destinations" =>
            {
                Some((
                    "UpdateConfigurationSetEventDestination",
                    resource,
                    Some(decode(&segs[5])),
                ))
            }
            (Method::DELETE, 6)
                if segs[2] == "configuration-sets" && segs[4] == "event-destinations" =>
            {
                Some((
                    "DeleteConfigurationSetEventDestination",
                    resource,
                    Some(decode(&segs[5])),
                ))
            }

            // /v2/email/identities/{id}/policies
            (Method::GET, 5) if segs[2] == "identities" && segs[4] == "policies" => {
                Some(("GetEmailIdentityPolicies", resource, None))
            }
            // /v2/email/identities/{id}/policies/{policy-name}
            (Method::POST, 6) if segs[2] == "identities" && segs[4] == "policies" => Some((
                "CreateEmailIdentityPolicy",
                resource,
                Some(decode(&segs[5])),
            )),
            (Method::PUT, 6) if segs[2] == "identities" && segs[4] == "policies" => Some((
                "UpdateEmailIdentityPolicy",
                resource,
                Some(decode(&segs[5])),
            )),
            (Method::DELETE, 6) if segs[2] == "identities" && segs[4] == "policies" => Some((
                "DeleteEmailIdentityPolicy",
                resource,
                Some(decode(&segs[5])),
            )),

            // /v2/email/identities/{id}/dkim/signing (6 segments, must come before dkim at 5)
            (Method::PUT, 6)
                if segs[2] == "identities" && segs[4] == "dkim" && segs[5] == "signing" =>
            {
                Some(("PutEmailIdentityDkimSigningAttributes", resource, None))
            }

            // /v2/email/identities/{id}/dkim
            (Method::PUT, 5) if segs[2] == "identities" && segs[4] == "dkim" => {
                Some(("PutEmailIdentityDkimAttributes", resource, None))
            }
            // /v2/email/identities/{id}/feedback
            (Method::PUT, 5) if segs[2] == "identities" && segs[4] == "feedback" => {
                Some(("PutEmailIdentityFeedbackAttributes", resource, None))
            }
            // /v2/email/identities/{id}/mail-from
            (Method::PUT, 5) if segs[2] == "identities" && segs[4] == "mail-from" => {
                Some(("PutEmailIdentityMailFromAttributes", resource, None))
            }
            // /v2/email/identities/{id}/configuration-set
            (Method::PUT, 5) if segs[2] == "identities" && segs[4] == "configuration-set" => {
                Some(("PutEmailIdentityConfigurationSetAttributes", resource, None))
            }

            // /v2/email/configuration-sets/{name}/sending
            (Method::PUT, 5) if segs[2] == "configuration-sets" && segs[4] == "sending" => {
                Some(("PutConfigurationSetSendingOptions", resource, None))
            }
            // /v2/email/configuration-sets/{name}/delivery-options
            (Method::PUT, 5)
                if segs[2] == "configuration-sets" && segs[4] == "delivery-options" =>
            {
                Some(("PutConfigurationSetDeliveryOptions", resource, None))
            }
            // /v2/email/configuration-sets/{name}/tracking-options
            (Method::PUT, 5)
                if segs[2] == "configuration-sets" && segs[4] == "tracking-options" =>
            {
                Some(("PutConfigurationSetTrackingOptions", resource, None))
            }
            // /v2/email/configuration-sets/{name}/suppression-options
            (Method::PUT, 5)
                if segs[2] == "configuration-sets" && segs[4] == "suppression-options" =>
            {
                Some(("PutConfigurationSetSuppressionOptions", resource, None))
            }
            // /v2/email/configuration-sets/{name}/reputation-options
            (Method::PUT, 5)
                if segs[2] == "configuration-sets" && segs[4] == "reputation-options" =>
            {
                Some(("PutConfigurationSetReputationOptions", resource, None))
            }
            // /v2/email/configuration-sets/{name}/vdm-options
            (Method::PUT, 5) if segs[2] == "configuration-sets" && segs[4] == "vdm-options" => {
                Some(("PutConfigurationSetVdmOptions", resource, None))
            }
            // /v2/email/configuration-sets/{name}/archiving-options
            (Method::PUT, 5)
                if segs[2] == "configuration-sets" && segs[4] == "archiving-options" =>
            {
                Some(("PutConfigurationSetArchivingOptions", resource, None))
            }

            // /v2/email/custom-verification-email-templates
            (Method::POST, 3) if segs[2] == "custom-verification-email-templates" => {
                Some(("CreateCustomVerificationEmailTemplate", None, None))
            }
            (Method::GET, 3) if segs[2] == "custom-verification-email-templates" => {
                Some(("ListCustomVerificationEmailTemplates", None, None))
            }
            // /v2/email/custom-verification-email-templates/{name}
            (Method::GET, 4) if segs[2] == "custom-verification-email-templates" => {
                Some(("GetCustomVerificationEmailTemplate", resource, None))
            }
            (Method::PUT, 4) if segs[2] == "custom-verification-email-templates" => {
                Some(("UpdateCustomVerificationEmailTemplate", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "custom-verification-email-templates" => {
                Some(("DeleteCustomVerificationEmailTemplate", resource, None))
            }

            // /v2/email/outbound-custom-verification-emails
            (Method::POST, 3) if segs[2] == "outbound-custom-verification-emails" => {
                Some(("SendCustomVerificationEmail", None, None))
            }

            // /v2/email/templates/{name}/render
            (Method::POST, 5) if segs[2] == "templates" && segs[4] == "render" => {
                Some(("TestRenderEmailTemplate", resource, None))
            }

            // /v2/email/dedicated-ip-pools
            (Method::POST, 3) if segs[2] == "dedicated-ip-pools" => {
                Some(("CreateDedicatedIpPool", None, None))
            }
            (Method::GET, 3) if segs[2] == "dedicated-ip-pools" => {
                Some(("ListDedicatedIpPools", None, None))
            }
            // /v2/email/dedicated-ip-pools/{name}
            (Method::DELETE, 4) if segs[2] == "dedicated-ip-pools" => {
                Some(("DeleteDedicatedIpPool", resource, None))
            }
            // Note: GetDedicatedIpPool is not in scope but the SDK may hit it via
            // the dedicated-ip-pools/{name} GET path — we route to a pool-level getter.

            // /v2/email/dedicated-ip-pools/{name}/scaling
            (Method::PUT, 5) if segs[2] == "dedicated-ip-pools" && segs[4] == "scaling" => {
                Some(("PutDedicatedIpPoolScalingAttributes", resource, None))
            }

            // /v2/email/dedicated-ips
            (Method::GET, 3) if segs[2] == "dedicated-ips" => Some(("GetDedicatedIps", None, None)),
            // /v2/email/dedicated-ips/{ip}/pool (5 segments, must come before 4-segment match)
            (Method::PUT, 5) if segs[2] == "dedicated-ips" && segs[4] == "pool" => {
                Some(("PutDedicatedIpInPool", resource, None))
            }
            // /v2/email/dedicated-ips/{ip}/warmup
            (Method::PUT, 5) if segs[2] == "dedicated-ips" && segs[4] == "warmup" => {
                Some(("PutDedicatedIpWarmupAttributes", resource, None))
            }
            // /v2/email/dedicated-ips/{ip}
            (Method::GET, 4) if segs[2] == "dedicated-ips" => {
                Some(("GetDedicatedIp", resource, None))
            }

            // /v2/email/account/dedicated-ips/warmup
            (Method::PUT, 5)
                if segs[2] == "account" && segs[3] == "dedicated-ips" && segs[4] == "warmup" =>
            {
                Some(("PutAccountDedicatedIpWarmupAttributes", None, None))
            }

            // /v2/email/account/details
            (Method::POST, 4) if segs[2] == "account" && segs[3] == "details" => {
                Some(("PutAccountDetails", None, None))
            }
            // /v2/email/account/sending
            (Method::PUT, 4) if segs[2] == "account" && segs[3] == "sending" => {
                Some(("PutAccountSendingAttributes", None, None))
            }
            // /v2/email/account/suppression
            (Method::PUT, 4) if segs[2] == "account" && segs[3] == "suppression" => {
                Some(("PutAccountSuppressionAttributes", None, None))
            }
            // /v2/email/account/vdm
            (Method::PUT, 4) if segs[2] == "account" && segs[3] == "vdm" => {
                Some(("PutAccountVdmAttributes", None, None))
            }

            // /v2/email/multi-region-endpoints
            (Method::POST, 3) if segs[2] == "multi-region-endpoints" => {
                Some(("CreateMultiRegionEndpoint", None, None))
            }
            (Method::GET, 3) if segs[2] == "multi-region-endpoints" => {
                Some(("ListMultiRegionEndpoints", None, None))
            }
            // /v2/email/multi-region-endpoints/{name}
            (Method::GET, 4) if segs[2] == "multi-region-endpoints" => {
                Some(("GetMultiRegionEndpoint", resource, None))
            }
            (Method::DELETE, 4) if segs[2] == "multi-region-endpoints" => {
                Some(("DeleteMultiRegionEndpoint", resource, None))
            }

            // /v2/email/import-jobs
            (Method::POST, 3) if segs[2] == "import-jobs" => Some(("CreateImportJob", None, None)),
            // /v2/email/import-jobs/list (SDK sends POST for ListImportJobs)
            (Method::POST, 4) if segs[2] == "import-jobs" && segs[3] == "list" => {
                Some(("ListImportJobs", None, None))
            }
            // /v2/email/import-jobs/{id}
            (Method::GET, 4) if segs[2] == "import-jobs" => Some(("GetImportJob", resource, None)),

            // /v2/email/export-jobs
            (Method::POST, 3) if segs[2] == "export-jobs" => Some(("CreateExportJob", None, None)),
            // /v2/email/list-export-jobs (SDK sends POST for ListExportJobs)
            (Method::POST, 3) if segs[2] == "list-export-jobs" => {
                Some(("ListExportJobs", None, None))
            }
            // /v2/email/export-jobs/{id}/cancel
            (Method::PUT, 5) if segs[2] == "export-jobs" && segs[4] == "cancel" => {
                Some(("CancelExportJob", resource, None))
            }
            // /v2/email/export-jobs/{id}
            (Method::GET, 4) if segs[2] == "export-jobs" => Some(("GetExportJob", resource, None)),

            // /v2/email/tenants
            (Method::POST, 3) if segs[2] == "tenants" => Some(("CreateTenant", None, None)),
            // /v2/email/tenants/list
            (Method::POST, 4) if segs[2] == "tenants" && segs[3] == "list" => {
                Some(("ListTenants", None, None))
            }
            // /v2/email/tenants/get
            (Method::POST, 4) if segs[2] == "tenants" && segs[3] == "get" => {
                Some(("GetTenant", None, None))
            }
            // /v2/email/tenants/delete
            (Method::POST, 4) if segs[2] == "tenants" && segs[3] == "delete" => {
                Some(("DeleteTenant", None, None))
            }
            // /v2/email/tenants/resources (CreateTenantResourceAssociation)
            (Method::POST, 4) if segs[2] == "tenants" && segs[3] == "resources" => {
                Some(("CreateTenantResourceAssociation", None, None))
            }
            // /v2/email/tenants/resources/delete (DeleteTenantResourceAssociation)
            (Method::POST, 5)
                if segs[2] == "tenants" && segs[3] == "resources" && segs[4] == "delete" =>
            {
                Some(("DeleteTenantResourceAssociation", None, None))
            }
            // /v2/email/tenants/resources/list (ListTenantResources)
            (Method::POST, 5)
                if segs[2] == "tenants" && segs[3] == "resources" && segs[4] == "list" =>
            {
                Some(("ListTenantResources", None, None))
            }
            // /v2/email/resources/tenants/list (ListResourceTenants)
            (Method::POST, 5)
                if segs[2] == "resources" && segs[3] == "tenants" && segs[4] == "list" =>
            {
                Some(("ListResourceTenants", None, None))
            }

            // /v2/email/reputation/entities (ListReputationEntities)
            (Method::POST, 4) if segs[2] == "reputation" && segs[3] == "entities" => {
                Some(("ListReputationEntities", None, None))
            }
            // /v2/email/reputation/entities/{type}/{ref}/customer-managed-status
            (Method::PUT, 7)
                if segs[2] == "reputation"
                    && segs[3] == "entities"
                    && segs[6] == "customer-managed-status" =>
            {
                Some((
                    "UpdateReputationEntityCustomerManagedStatus",
                    Some(decode(&segs[4])),
                    Some(decode(&segs[5])),
                ))
            }
            // /v2/email/reputation/entities/{type}/{ref}/policy
            (Method::PUT, 7)
                if segs[2] == "reputation" && segs[3] == "entities" && segs[6] == "policy" =>
            {
                Some((
                    "UpdateReputationEntityPolicy",
                    Some(decode(&segs[4])),
                    Some(decode(&segs[5])),
                ))
            }
            // /v2/email/reputation/entities/{type}/{ref}
            (Method::GET, 6) if segs[2] == "reputation" && segs[3] == "entities" => Some((
                "GetReputationEntity",
                Some(decode(&segs[4])),
                Some(decode(&segs[5])),
            )),

            // /v2/email/metrics/batch
            (Method::POST, 4) if segs[2] == "metrics" && segs[3] == "batch" => {
                Some(("BatchGetMetricData", None, None))
            }

            _ => None,
        }
    }

    fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
        serde_json::from_slice(&req.body).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Invalid JSON in request body",
            )
        })
    }

    fn json_error(status: StatusCode, code: &str, message: &str) -> AwsResponse {
        let body = json!({
            "__type": code,
            "message": message,
        });
        AwsResponse::json(status, body.to_string())
    }

    fn get_account(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let acct = &state.account_settings;
        let production_access = acct
            .details
            .as_ref()
            .and_then(|d| d.production_access_enabled)
            .unwrap_or(true);
        let mut response = json!({
            "DedicatedIpAutoWarmupEnabled": acct.dedicated_ip_auto_warmup_enabled,
            "EnforcementStatus": "HEALTHY",
            "ProductionAccessEnabled": production_access,
            "SendQuota": {
                "Max24HourSend": 50000.0,
                "MaxSendRate": 14.0,
                "SentLast24Hours": state.sent_emails.iter()
                    .filter(|e| e.timestamp > Utc::now() - chrono::Duration::hours(24))
                    .count() as f64,
            },
            "SendingEnabled": acct.sending_enabled,
            "SuppressionAttributes": {
                "SuppressedReasons": acct.suppressed_reasons,
            },
        });
        if let Some(ref details) = acct.details {
            let mut d = json!({});
            if let Some(ref mt) = details.mail_type {
                d["MailType"] = json!(mt);
            }
            if let Some(ref url) = details.website_url {
                d["WebsiteURL"] = json!(url);
            }
            if let Some(ref lang) = details.contact_language {
                d["ContactLanguage"] = json!(lang);
            }
            if let Some(ref desc) = details.use_case_description {
                d["UseCaseDescription"] = json!(desc);
            }
            if !details.additional_contact_email_addresses.is_empty() {
                d["AdditionalContactEmailAddresses"] =
                    json!(details.additional_contact_email_addresses);
            }
            d["ReviewDetails"] = json!({
                "Status": "GRANTED",
                "CaseId": "fakecloud-case-001",
            });
            response["Details"] = d;
        }
        if let Some(ref vdm) = acct.vdm_attributes {
            response["VdmAttributes"] = vdm.clone();
        }
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn create_email_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let identity_name = match body["EmailIdentity"].as_str() {
            Some(name) => name.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailIdentity is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.identities.contains_key(&identity_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Identity {} already exists", identity_name),
            ));
        }

        let identity_type = if identity_name.contains('@') {
            "EMAIL_ADDRESS"
        } else {
            "DOMAIN"
        };

        let identity = EmailIdentity {
            identity_name: identity_name.clone(),
            identity_type: identity_type.to_string(),
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled: true,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            configuration_set_name: None,
        };

        state.identities.insert(identity_name, identity);

        let response = json!({
            "IdentityType": identity_type,
            "VerifiedForSendingStatus": true,
            "DkimAttributes": {
                "SigningEnabled": true,
                "Status": "SUCCESS",
                "Tokens": [
                    "token1",
                    "token2",
                    "token3",
                ],
            },
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_email_identities(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let identities: Vec<Value> = state
            .identities
            .values()
            .map(|id| {
                json!({
                    "IdentityType": id.identity_type,
                    "IdentityName": id.identity_name,
                    "SendingEnabled": true,
                })
            })
            .collect();

        let response = json!({
            "EmailIdentities": identities,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_email_identity(&self, identity_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let identity = match state.identities.get(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        let mail_from_domain = identity.mail_from_domain.as_deref().unwrap_or("");
        let mail_from_status = if mail_from_domain.is_empty() {
            "FAILED"
        } else {
            "SUCCESS"
        };

        let mut response = json!({
            "IdentityType": identity.identity_type,
            "VerifiedForSendingStatus": true,
            "FeedbackForwardingStatus": identity.email_forwarding_enabled,
            "DkimAttributes": {
                "SigningEnabled": identity.dkim_signing_enabled,
                "Status": "SUCCESS",
                "SigningAttributesOrigin": identity.dkim_signing_attributes_origin,
                "Tokens": [
                    "token1",
                    "token2",
                    "token3",
                ],
            },
            "MailFromAttributes": {
                "MailFromDomain": mail_from_domain,
                "MailFromDomainStatus": mail_from_status,
                "BehaviorOnMxFailure": identity.mail_from_behavior_on_mx_failure,
            },
            "Tags": [],
        });

        if let Some(ref cs) = identity.configuration_set_name {
            response["ConfigurationSetName"] = json!(cs);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_email_identity(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.identities.remove(identity_name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        // Remove tags for this identity
        let arn = format!(
            "arn:aws:ses:{}:{}:identity/{}",
            req.region, req.account_id, identity_name
        );
        state.tags.remove(&arn);

        // Remove policies for this identity
        state.identity_policies.remove(identity_name);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn create_configuration_set(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let name = match body["ConfigurationSetName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ConfigurationSetName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.configuration_sets.contains_key(&name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Configuration set {} already exists", name),
            ));
        }

        state.configuration_sets.insert(
            name.clone(),
            ConfigurationSet {
                name,
                sending_enabled: true,
                tls_policy: "OPTIONAL".to_string(),
                sending_pool_name: None,
                custom_redirect_domain: None,
                https_policy: None,
                suppressed_reasons: Vec::new(),
                reputation_metrics_enabled: false,
                vdm_options: None,
                archive_arn: None,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_configuration_sets(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let sets: Vec<Value> = state
            .configuration_sets
            .keys()
            .map(|name| json!(name))
            .collect();

        let response = json!({
            "ConfigurationSets": sets,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_configuration_set(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let cs = match state.configuration_sets.get(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        let mut delivery_options = json!({
            "TlsPolicy": cs.tls_policy,
        });
        if let Some(ref pool) = cs.sending_pool_name {
            delivery_options["SendingPoolName"] = json!(pool);
        }

        let mut tracking_options = json!({});
        if let Some(ref domain) = cs.custom_redirect_domain {
            tracking_options["CustomRedirectDomain"] = json!(domain);
        }
        if let Some(ref policy) = cs.https_policy {
            tracking_options["HttpsPolicy"] = json!(policy);
        }

        let mut response = json!({
            "ConfigurationSetName": name,
            "DeliveryOptions": delivery_options,
            "ReputationOptions": {
                "ReputationMetricsEnabled": cs.reputation_metrics_enabled,
            },
            "SendingOptions": {
                "SendingEnabled": cs.sending_enabled,
            },
            "Tags": [],
            "TrackingOptions": tracking_options,
        });

        if !cs.suppressed_reasons.is_empty() {
            response["SuppressionOptions"] = json!({
                "SuppressedReasons": cs.suppressed_reasons,
            });
        }

        if let Some(ref vdm) = cs.vdm_options {
            response["VdmOptions"] = vdm.clone();
        }

        if let Some(ref arn) = cs.archive_arn {
            response["ArchivingOptions"] = json!({
                "ArchiveArn": arn,
            });
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_configuration_set(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.configuration_sets.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", name),
            ));
        }

        // Remove tags for this configuration set
        let arn = format!(
            "arn:aws:ses:{}:{}:configuration-set/{}",
            req.region, req.account_id, name
        );
        state.tags.remove(&arn);

        // Remove event destinations for this configuration set
        state.event_destinations.remove(name);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn create_email_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let template_name = match body["TemplateName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TemplateName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.templates.contains_key(&template_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Template {} already exists", template_name),
            ));
        }

        let template = EmailTemplate {
            template_name: template_name.clone(),
            subject: body["TemplateContent"]["Subject"]
                .as_str()
                .map(|s| s.to_string()),
            html_body: body["TemplateContent"]["Html"]
                .as_str()
                .map(|s| s.to_string()),
            text_body: body["TemplateContent"]["Text"]
                .as_str()
                .map(|s| s.to_string()),
            created_at: Utc::now(),
        };

        state.templates.insert(template_name, template);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_email_templates(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let templates: Vec<Value> = state
            .templates
            .values()
            .map(|t| {
                json!({
                    "TemplateName": t.template_name,
                    "CreatedTimestamp": t.created_at.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({
            "TemplatesMetadata": templates,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_email_template(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let template = match state.templates.get(name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Template {} does not exist", name),
                ));
            }
        };

        let response = json!({
            "TemplateName": template.template_name,
            "TemplateContent": {
                "Subject": template.subject,
                "Html": template.html_body,
                "Text": template.text_body,
            },
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let template = match state.templates.get_mut(name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Template {} does not exist", name),
                ));
            }
        };

        if let Some(subject) = body["TemplateContent"]["Subject"].as_str() {
            template.subject = Some(subject.to_string());
        }
        if let Some(html) = body["TemplateContent"]["Html"].as_str() {
            template.html_body = Some(html.to_string());
        }
        if let Some(text) = body["TemplateContent"]["Text"].as_str() {
            template.text_body = Some(text.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_email_template(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.templates.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Template {} does not exist", name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn send_email(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        if !body["Content"].is_object()
            || (!body["Content"]["Simple"].is_object()
                && !body["Content"]["Raw"].is_object()
                && !body["Content"]["Template"].is_object())
        {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Content is required and must contain Simple, Raw, or Template",
            ));
        }

        let from = body["FromEmailAddress"].as_str().unwrap_or("").to_string();

        let to = extract_string_array(&body["Destination"]["ToAddresses"]);
        let cc = extract_string_array(&body["Destination"]["CcAddresses"]);
        let bcc = extract_string_array(&body["Destination"]["BccAddresses"]);

        let (subject, html_body, text_body, raw_data, template_name, template_data) =
            if body["Content"]["Simple"].is_object() {
                let simple = &body["Content"]["Simple"];
                let subject = simple["Subject"]["Data"].as_str().map(|s| s.to_string());
                let html = simple["Body"]["Html"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                let text = simple["Body"]["Text"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                (subject, html, text, None, None, None)
            } else if body["Content"]["Raw"].is_object() {
                let raw = body["Content"]["Raw"]["Data"]
                    .as_str()
                    .map(|s| s.to_string());
                (None, None, None, raw, None, None)
            } else if body["Content"]["Template"].is_object() {
                let tmpl = &body["Content"]["Template"];
                let tmpl_name = tmpl["TemplateName"].as_str().map(|s| s.to_string());
                let tmpl_data = tmpl["TemplateData"].as_str().map(|s| s.to_string());
                (None, None, None, None, tmpl_name, tmpl_data)
            } else {
                (None, None, None, None, None, None)
            };

        let message_id = uuid::Uuid::new_v4().to_string();

        let sent = SentEmail {
            message_id: message_id.clone(),
            from,
            to,
            cc,
            bcc,
            subject,
            html_body,
            text_body,
            raw_data,
            template_name,
            template_data,
            timestamp: Utc::now(),
        };

        self.state.write().sent_emails.push(sent);

        let response = json!({
            "MessageId": message_id,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Contact List operations ---

    fn create_contact_list(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let name = match body["ContactListName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ContactListName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.contact_lists.contains_key(&name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("List with name {} already exists.", name),
            ));
        }

        let topics = parse_topics(&body["Topics"]);
        let description = body["Description"].as_str().map(|s| s.to_string());
        let now = Utc::now();

        state.contact_lists.insert(
            name.clone(),
            ContactList {
                contact_list_name: name.clone(),
                description,
                topics,
                created_at: now,
                last_updated_at: now,
            },
        );
        state.contacts.insert(name, HashMap::new());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_contact_list(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let list = match state.contact_lists.get(name) {
            Some(l) => l,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("List with name {} does not exist.", name),
                ));
            }
        };

        let topics: Vec<Value> = list
            .topics
            .iter()
            .map(|t| {
                json!({
                    "TopicName": t.topic_name,
                    "DisplayName": t.display_name,
                    "Description": t.description,
                    "DefaultSubscriptionStatus": t.default_subscription_status,
                })
            })
            .collect();

        let response = json!({
            "ContactListName": list.contact_list_name,
            "Description": list.description,
            "Topics": topics,
            "CreatedTimestamp": list.created_at.timestamp() as f64,
            "LastUpdatedTimestamp": list.last_updated_at.timestamp() as f64,
            "Tags": [],
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_contact_lists(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let lists: Vec<Value> = state
            .contact_lists
            .values()
            .map(|l| {
                json!({
                    "ContactListName": l.contact_list_name,
                    "LastUpdatedTimestamp": l.last_updated_at.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({
            "ContactLists": lists,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_contact_list(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let list = match state.contact_lists.get_mut(name) {
            Some(l) => l,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("List with name {} does not exist.", name),
                ));
            }
        };

        if let Some(desc) = body.get("Description") {
            list.description = desc.as_str().map(|s| s.to_string());
        }
        if body.get("Topics").is_some() {
            list.topics = parse_topics(&body["Topics"]);
        }
        list.last_updated_at = Utc::now();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_contact_list(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.contact_lists.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", name),
            ));
        }

        // Also delete all contacts in this list
        state.contacts.remove(name);

        // Remove tags for this contact list
        let arn = format!(
            "arn:aws:ses:{}:{}:contact-list/{}",
            req.region, req.account_id, name
        );
        state.tags.remove(&arn);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Contact operations ---

    fn create_contact(
        &self,
        list_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let email = match body["EmailAddress"].as_str() {
            Some(e) => e.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailAddress is required",
                ));
            }
        };

        let mut state = self.state.write();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let contacts = state.contacts.entry(list_name.to_string()).or_default();

        if contacts.contains_key(&email) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Contact already exists in list {}", list_name),
            ));
        }

        let topic_preferences = parse_topic_preferences(&body["TopicPreferences"]);
        let unsubscribe_all = body["UnsubscribeAll"].as_bool().unwrap_or(false);
        let attributes_data = body["AttributesData"].as_str().map(|s| s.to_string());
        let now = Utc::now();

        contacts.insert(
            email.clone(),
            Contact {
                email_address: email,
                topic_preferences,
                unsubscribe_all,
                attributes_data,
                created_at: now,
                last_updated_at: now,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_contact(&self, list_name: &str, email: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let contact = state.contacts.get(list_name).and_then(|m| m.get(email));

        let contact = match contact {
            Some(c) => c,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Contact {} does not exist in list {}", email, list_name),
                ));
            }
        };

        // Build TopicDefaultPreferences from the contact list's topics
        let list = state.contact_lists.get(list_name).unwrap();
        let topic_default_preferences: Vec<Value> = list
            .topics
            .iter()
            .map(|t| {
                json!({
                    "TopicName": t.topic_name,
                    "SubscriptionStatus": t.default_subscription_status,
                })
            })
            .collect();

        let topic_preferences: Vec<Value> = contact
            .topic_preferences
            .iter()
            .map(|tp| {
                json!({
                    "TopicName": tp.topic_name,
                    "SubscriptionStatus": tp.subscription_status,
                })
            })
            .collect();

        let mut response = json!({
            "ContactListName": list_name,
            "EmailAddress": contact.email_address,
            "TopicPreferences": topic_preferences,
            "TopicDefaultPreferences": topic_default_preferences,
            "UnsubscribeAll": contact.unsubscribe_all,
            "CreatedTimestamp": contact.created_at.timestamp() as f64,
            "LastUpdatedTimestamp": contact.last_updated_at.timestamp() as f64,
        });

        if let Some(ref attrs) = contact.attributes_data {
            response["AttributesData"] = json!(attrs);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_contacts(&self, list_name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let contacts: Vec<Value> = state
            .contacts
            .get(list_name)
            .map(|m| {
                m.values()
                    .map(|c| {
                        let topic_prefs: Vec<Value> = c
                            .topic_preferences
                            .iter()
                            .map(|tp| {
                                json!({
                                    "TopicName": tp.topic_name,
                                    "SubscriptionStatus": tp.subscription_status,
                                })
                            })
                            .collect();

                        // Build TopicDefaultPreferences from the list's topics
                        let list = state.contact_lists.get(list_name).unwrap();
                        let topic_defaults: Vec<Value> = list
                            .topics
                            .iter()
                            .map(|t| {
                                json!({
                                    "TopicName": t.topic_name,
                                    "SubscriptionStatus": t.default_subscription_status,
                                })
                            })
                            .collect();

                        json!({
                            "EmailAddress": c.email_address,
                            "TopicPreferences": topic_prefs,
                            "TopicDefaultPreferences": topic_defaults,
                            "UnsubscribeAll": c.unsubscribe_all,
                            "LastUpdatedTimestamp": c.last_updated_at.timestamp() as f64,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let response = json!({
            "Contacts": contacts,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_contact(
        &self,
        list_name: &str,
        email: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let contact = state
            .contacts
            .get_mut(list_name)
            .and_then(|m| m.get_mut(email));

        let contact = match contact {
            Some(c) => c,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Contact {} does not exist in list {}", email, list_name),
                ));
            }
        };

        if body.get("TopicPreferences").is_some() {
            contact.topic_preferences = parse_topic_preferences(&body["TopicPreferences"]);
        }
        if let Some(unsub) = body["UnsubscribeAll"].as_bool() {
            contact.unsubscribe_all = unsub;
        }
        if let Some(attrs) = body.get("AttributesData") {
            contact.attributes_data = attrs.as_str().map(|s| s.to_string());
        }
        contact.last_updated_at = Utc::now();

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_contact(&self, list_name: &str, email: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if !state.contact_lists.contains_key(list_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("List with name {} does not exist.", list_name),
            ));
        }

        let removed = state
            .contacts
            .get_mut(list_name)
            .and_then(|m| m.remove(email));

        if removed.is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Contact {} does not exist in list {}", email, list_name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Tag operations ---

    /// Validate that a resource ARN refers to an existing resource.
    /// Returns `None` if the resource exists, or `Some(error_response)` if not.
    fn validate_resource_arn(&self, arn: &str) -> Option<AwsResponse> {
        let state = self.state.read();

        // Parse ARN: arn:aws:ses:{region}:{account}:{resource-type}/{name}
        let parts: Vec<&str> = arn.split(':').collect();
        if parts.len() < 6 {
            return Some(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Resource not found: {arn}"),
            ));
        }

        let resource = parts[5..].join(":");
        let found = if let Some(name) = resource.strip_prefix("identity/") {
            state.identities.contains_key(name)
        } else if let Some(name) = resource.strip_prefix("configuration-set/") {
            state.configuration_sets.contains_key(name)
        } else if let Some(name) = resource.strip_prefix("contact-list/") {
            state.contact_lists.contains_key(name)
        } else {
            false
        };

        if found {
            None
        } else {
            Some(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Resource not found: {arn}"),
            ))
        }
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let arn = match body["ResourceArn"].as_str() {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        let tags_arr = match body["Tags"].as_array() {
            Some(arr) => arr,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Tags is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        let mut state = self.state.write();
        let tag_map = state.tags.entry(arn).or_default();
        for tag in tags_arr {
            if let (Some(k), Some(v)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                tag_map.insert(k.to_string(), v.to_string());
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ResourceArn and TagKeys come as query params
        let arn = match req.query_params.get("ResourceArn") {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        // Parse TagKeys from raw query string (supports repeated params)
        let tag_keys: Vec<String> = form_urlencoded::parse(req.raw_query.as_bytes())
            .filter(|(k, _)| k == "TagKeys")
            .map(|(_, v)| v.into_owned())
            .collect();

        let mut state = self.state.write();
        if let Some(tag_map) = state.tags.get_mut(&arn) {
            for key in &tag_keys {
                tag_map.remove(key);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = match req.query_params.get("ResourceArn") {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        let state = self.state.read();
        let tags = state.tags.get(&arn);
        let tags_json = match tags {
            Some(t) => fakecloud_core::tags::tags_to_json(t, "Key", "Value"),
            None => vec![],
        };

        let response = json!({
            "Tags": tags_json,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Suppression List operations ---

    fn put_suppressed_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let email = match body["EmailAddress"].as_str() {
            Some(e) => e.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailAddress is required",
                ));
            }
        };
        let reason = match body["Reason"].as_str() {
            Some(r) if r == "BOUNCE" || r == "COMPLAINT" => r.to_string(),
            Some(_) => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Reason must be BOUNCE or COMPLAINT",
                ));
            }
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Reason is required",
                ));
            }
        };

        let mut state = self.state.write();
        state.suppressed_destinations.insert(
            email.clone(),
            SuppressedDestination {
                email_address: email,
                reason,
                last_update_time: Utc::now(),
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_suppressed_destination(&self, email: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let dest = match state.suppressed_destinations.get(email) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("{} is not on the suppression list", email),
                ));
            }
        };

        let response = json!({
            "SuppressedDestination": {
                "EmailAddress": dest.email_address,
                "Reason": dest.reason,
                "LastUpdateTime": dest.last_update_time.timestamp() as f64,
            }
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_suppressed_destination(&self, email: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        if state.suppressed_destinations.remove(email).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("{} is not on the suppression list", email),
            ));
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_suppressed_destinations(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let summaries: Vec<Value> = state
            .suppressed_destinations
            .values()
            .map(|d| {
                json!({
                    "EmailAddress": d.email_address,
                    "Reason": d.reason,
                    "LastUpdateTime": d.last_update_time.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({
            "SuppressedDestinationSummaries": summaries,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Event Destination operations ---

    fn create_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let state_read = self.state.read();
        if !state_read.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }
        drop(state_read);

        let dest_name = match body["EventDestinationName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EventDestinationName is required",
                ));
            }
        };

        let event_dest = parse_event_destination_definition(&dest_name, &body["EventDestination"]);

        let mut state = self.state.write();
        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        if dests.iter().any(|d| d.name == dest_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Event destination {} already exists", dest_name),
            ));
        }

        dests.push(event_dest);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_configuration_set_event_destinations(
        &self,
        config_set_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .get(config_set_name)
            .cloned()
            .unwrap_or_default();

        let dests_json: Vec<Value> = dests.iter().map(event_destination_to_json).collect();

        let response = json!({
            "EventDestinations": dests_json,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        dest_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let mut state = self.state.write();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        let existing = match dests.iter_mut().find(|d| d.name == dest_name) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Event destination {} does not exist", dest_name),
                ));
            }
        };

        let updated = parse_event_destination_definition(dest_name, &body["EventDestination"]);
        *existing = updated;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_configuration_set_event_destination(
        &self,
        config_set_name: &str,
        dest_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if !state.configuration_sets.contains_key(config_set_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Configuration set {} does not exist", config_set_name),
            ));
        }

        let dests = state
            .event_destinations
            .entry(config_set_name.to_string())
            .or_default();

        let len_before = dests.len();
        dests.retain(|d| d.name != dest_name);

        if dests.len() == len_before {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Event destination {} does not exist", dest_name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Email Identity Policy operations ---

    fn create_email_identity_policy(
        &self,
        identity_name: &str,
        policy_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let policy = match body["Policy"].as_str() {
            Some(p) => p.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Policy is required",
                ));
            }
        };

        let mut state = self.state.write();

        if !state.identities.contains_key(identity_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        let policies = state
            .identity_policies
            .entry(identity_name.to_string())
            .or_default();

        if policies.contains_key(policy_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Policy {} already exists", policy_name),
            ));
        }

        policies.insert(policy_name.to_string(), policy);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_email_identity_policies(
        &self,
        identity_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        if !state.identities.contains_key(identity_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        let policies = state
            .identity_policies
            .get(identity_name)
            .cloned()
            .unwrap_or_default();

        let policies_json: Value = policies
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect::<serde_json::Map<String, Value>>()
            .into();

        let response = json!({
            "Policies": policies_json,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_email_identity_policy(
        &self,
        identity_name: &str,
        policy_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let policy = match body["Policy"].as_str() {
            Some(p) => p.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Policy is required",
                ));
            }
        };

        let mut state = self.state.write();

        if !state.identities.contains_key(identity_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        let policies = state
            .identity_policies
            .entry(identity_name.to_string())
            .or_default();

        if !policies.contains_key(policy_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Policy {} does not exist", policy_name),
            ));
        }

        policies.insert(policy_name.to_string(), policy);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_email_identity_policy(
        &self,
        identity_name: &str,
        policy_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if !state.identities.contains_key(identity_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Identity {} does not exist", identity_name),
            ));
        }

        let policies = state
            .identity_policies
            .entry(identity_name.to_string())
            .or_default();

        if policies.remove(policy_name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Policy {} does not exist", policy_name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Identity Attribute operations ---

    fn put_email_identity_dkim_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        if let Some(enabled) = body["SigningEnabled"].as_bool() {
            identity.dkim_signing_enabled = enabled;
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_email_identity_dkim_signing_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        if let Some(origin) = body["SigningAttributesOrigin"].as_str() {
            identity.dkim_signing_attributes_origin = origin.to_string();
        }

        if let Some(attrs) = body.get("SigningAttributes") {
            if let Some(key) = attrs["DomainSigningPrivateKey"].as_str() {
                identity.dkim_domain_signing_private_key = Some(key.to_string());
            }
            if let Some(selector) = attrs["DomainSigningSelector"].as_str() {
                identity.dkim_domain_signing_selector = Some(selector.to_string());
            }
            if let Some(length) = attrs["NextSigningKeyLength"].as_str() {
                identity.dkim_next_signing_key_length = Some(length.to_string());
            }
        }

        let response = json!({
            "DkimStatus": "SUCCESS",
            "DkimTokens": ["token1", "token2", "token3"],
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn put_email_identity_feedback_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        if let Some(enabled) = body["EmailForwardingEnabled"].as_bool() {
            identity.email_forwarding_enabled = enabled;
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_email_identity_mail_from_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        if let Some(domain) = body["MailFromDomain"].as_str() {
            identity.mail_from_domain = Some(domain.to_string());
        }
        if let Some(behavior) = body["BehaviorOnMxFailure"].as_str() {
            identity.mail_from_behavior_on_mx_failure = behavior.to_string();
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_email_identity_configuration_set_attributes(
        &self,
        identity_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let identity = match state.identities.get_mut(identity_name) {
            Some(id) => id,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Identity {} does not exist", identity_name),
                ));
            }
        };

        identity.configuration_set_name =
            body["ConfigurationSetName"].as_str().map(|s| s.to_string());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Configuration Set Options ---

    fn put_configuration_set_sending_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        if let Some(enabled) = body["SendingEnabled"].as_bool() {
            cs.sending_enabled = enabled;
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_configuration_set_delivery_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        if let Some(policy) = body["TlsPolicy"].as_str() {
            cs.tls_policy = policy.to_string();
        }
        if let Some(pool) = body["SendingPoolName"].as_str() {
            cs.sending_pool_name = Some(pool.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_configuration_set_tracking_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        if let Some(domain) = body["CustomRedirectDomain"].as_str() {
            cs.custom_redirect_domain = Some(domain.to_string());
        }
        if let Some(policy) = body["HttpsPolicy"].as_str() {
            cs.https_policy = Some(policy.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_configuration_set_suppression_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        cs.suppressed_reasons = extract_string_array(&body["SuppressedReasons"]);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_configuration_set_reputation_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        if let Some(enabled) = body["ReputationMetricsEnabled"].as_bool() {
            cs.reputation_metrics_enabled = enabled;
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_configuration_set_vdm_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        cs.vdm_options = Some(body);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_configuration_set_archiving_options(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let cs = match state.configuration_sets.get_mut(name) {
            Some(cs) => cs,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Configuration set {} does not exist", name),
                ));
            }
        };

        cs.archive_arn = body["ArchiveArn"].as_str().map(|s| s.to_string());

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Custom Verification Email Template operations ---

    fn create_custom_verification_email_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let template_name = match body["TemplateName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TemplateName is required",
                ));
            }
        };

        let from_email = body["FromEmailAddress"].as_str().unwrap_or("").to_string();
        let subject = body["TemplateSubject"].as_str().unwrap_or("").to_string();
        let content = body["TemplateContent"].as_str().unwrap_or("").to_string();
        let success_url = body["SuccessRedirectionURL"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let failure_url = body["FailureRedirectionURL"]
            .as_str()
            .unwrap_or("")
            .to_string();

        let mut state = self.state.write();

        if state
            .custom_verification_email_templates
            .contains_key(&template_name)
        {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!(
                    "Custom verification email template {} already exists",
                    template_name
                ),
            ));
        }

        state.custom_verification_email_templates.insert(
            template_name.clone(),
            CustomVerificationEmailTemplate {
                template_name,
                from_email_address: from_email,
                template_subject: subject,
                template_content: content,
                success_redirection_url: success_url,
                failure_redirection_url: failure_url,
                created_at: Utc::now(),
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn get_custom_verification_email_template(
        &self,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let tmpl = match state.custom_verification_email_templates.get(name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Custom verification email template {} does not exist", name),
                ));
            }
        };

        let response = json!({
            "TemplateName": tmpl.template_name,
            "FromEmailAddress": tmpl.from_email_address,
            "TemplateSubject": tmpl.template_subject,
            "TemplateContent": tmpl.template_content,
            "SuccessRedirectionURL": tmpl.success_redirection_url,
            "FailureRedirectionURL": tmpl.failure_redirection_url,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_custom_verification_email_templates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let page_size: usize = req
            .query_params
            .get("PageSize")
            .and_then(|s| s.parse().ok())
            .unwrap_or(20);

        let mut templates: Vec<&CustomVerificationEmailTemplate> =
            state.custom_verification_email_templates.values().collect();
        templates.sort_by(|a, b| a.template_name.cmp(&b.template_name));

        let next_token = req.query_params.get("NextToken");
        let start_idx = if let Some(token) = next_token {
            templates
                .iter()
                .position(|t| t.template_name == *token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = templates
            .iter()
            .skip(start_idx)
            .take(page_size)
            .map(|t| {
                json!({
                    "TemplateName": t.template_name,
                    "FromEmailAddress": t.from_email_address,
                    "TemplateSubject": t.template_subject,
                    "SuccessRedirectionURL": t.success_redirection_url,
                    "FailureRedirectionURL": t.failure_redirection_url,
                })
            })
            .collect();

        let mut response = json!({
            "CustomVerificationEmailTemplates": page,
        });

        // Set NextToken if there are more results
        if start_idx + page_size < templates.len() {
            if let Some(next) = templates.get(start_idx + page_size) {
                response["NextToken"] = json!(next.template_name);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_custom_verification_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut state = self.state.write();

        let tmpl = match state.custom_verification_email_templates.get_mut(name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Custom verification email template {} does not exist", name),
                ));
            }
        };

        if let Some(from) = body["FromEmailAddress"].as_str() {
            tmpl.from_email_address = from.to_string();
        }
        if let Some(subject) = body["TemplateSubject"].as_str() {
            tmpl.template_subject = subject.to_string();
        }
        if let Some(content) = body["TemplateContent"].as_str() {
            tmpl.template_content = content.to_string();
        }
        if let Some(url) = body["SuccessRedirectionURL"].as_str() {
            tmpl.success_redirection_url = url.to_string();
        }
        if let Some(url) = body["FailureRedirectionURL"].as_str() {
            tmpl.failure_redirection_url = url.to_string();
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_custom_verification_email_template(
        &self,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state
            .custom_verification_email_templates
            .remove(name)
            .is_none()
        {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Custom verification email template {} does not exist", name),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn send_custom_verification_email(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let email_address = match body["EmailAddress"].as_str() {
            Some(e) => e.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailAddress is required",
                ));
            }
        };

        let template_name = match body["TemplateName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TemplateName is required",
                ));
            }
        };

        // Verify template exists
        {
            let state = self.state.read();
            if !state
                .custom_verification_email_templates
                .contains_key(&template_name)
            {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!(
                        "Custom verification email template {} does not exist",
                        template_name
                    ),
                ));
            }
        }

        let message_id = uuid::Uuid::new_v4().to_string();

        // Store as a sent email for introspection
        let sent = SentEmail {
            message_id: message_id.clone(),
            from: String::new(),
            to: vec![email_address],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: Some(format!("Custom verification: {}", template_name)),
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: Some(template_name),
            template_data: None,
            timestamp: Utc::now(),
        };

        self.state.write().sent_emails.push(sent);

        let response = json!({
            "MessageId": message_id,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- TestRenderEmailTemplate ---

    fn test_render_email_template(
        &self,
        template_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let template_data_str = match body["TemplateData"].as_str() {
            Some(d) => d.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TemplateData is required",
                ));
            }
        };

        let state = self.state.read();
        let template = match state.templates.get(template_name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Template {} does not exist", template_name),
                ));
            }
        };

        // Parse template data JSON
        let data: HashMap<String, Value> =
            serde_json::from_str(&template_data_str).unwrap_or_default();

        let substitute = |text: &str| -> String {
            let mut result = text.to_string();
            for (key, value) in &data {
                let placeholder = format!("{{{{{}}}}}", key);
                let replacement = match value {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                result = result.replace(&placeholder, &replacement);
            }
            result
        };

        let rendered_subject = template
            .subject
            .as_deref()
            .map(&substitute)
            .unwrap_or_default();
        let rendered_html = template.html_body.as_deref().map(&substitute);
        let rendered_text = template.text_body.as_deref().map(&substitute);

        // Build a simplified MIME message
        let mut mime = format!("Subject: {}\r\n", rendered_subject);
        mime.push_str("MIME-Version: 1.0\r\n");
        mime.push_str("Content-Type: text/html; charset=UTF-8\r\n");
        mime.push_str("\r\n");
        if let Some(ref html) = rendered_html {
            mime.push_str(html);
        } else if let Some(ref text) = rendered_text {
            mime.push_str(text);
        }

        let response = json!({
            "RenderedTemplate": mime,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn send_bulk_email(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let from = body["FromEmailAddress"].as_str().unwrap_or("").to_string();

        let entries = match body["BulkEmailEntries"].as_array() {
            Some(arr) if !arr.is_empty() => arr.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "BulkEmailEntries is required and must not be empty",
                ));
            }
        };

        let mut results = Vec::new();

        for entry in &entries {
            let to = extract_string_array(&entry["Destination"]["ToAddresses"]);
            let cc = extract_string_array(&entry["Destination"]["CcAddresses"]);
            let bcc = extract_string_array(&entry["Destination"]["BccAddresses"]);

            let message_id = uuid::Uuid::new_v4().to_string();

            let template_name = body["DefaultContent"]["Template"]["TemplateName"]
                .as_str()
                .map(|s| s.to_string());
            let template_data = entry["ReplacementEmailContent"]["ReplacementTemplate"]
                ["ReplacementTemplateData"]
                .as_str()
                .or_else(|| body["DefaultContent"]["Template"]["TemplateData"].as_str())
                .map(|s| s.to_string());

            let sent = SentEmail {
                message_id: message_id.clone(),
                from: from.clone(),
                to,
                cc,
                bcc,
                subject: None,
                html_body: None,
                text_body: None,
                raw_data: None,
                template_name,
                template_data,
                timestamp: Utc::now(),
            };

            self.state.write().sent_emails.push(sent);

            results.push(json!({
                "Status": "SUCCESS",
                "MessageId": message_id,
            }));
        }

        let response = json!({
            "BulkEmailEntryResults": results,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // ── Dedicated IP Pools ──────────────────────────────────────────────

    fn create_dedicated_ip_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let pool_name = match body["PoolName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "PoolName is required",
                ));
            }
        };
        let scaling_mode = body["ScalingMode"]
            .as_str()
            .unwrap_or("STANDARD")
            .to_string();

        let mut state = self.state.write();

        if state.dedicated_ip_pools.contains_key(&pool_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Pool {} already exists", pool_name),
            ));
        }

        // For MANAGED pools, generate some fake IPs
        if scaling_mode == "MANAGED" {
            let pool_idx = state.dedicated_ip_pools.len() as u8;
            for i in 1..=3 {
                let ip_addr = format!("198.51.100.{}", pool_idx * 10 + i);
                state.dedicated_ips.insert(
                    ip_addr.clone(),
                    DedicatedIp {
                        ip: ip_addr,
                        warmup_status: "NOT_APPLICABLE".to_string(),
                        warmup_percentage: -1,
                        pool_name: pool_name.clone(),
                    },
                );
            }
        }

        state.dedicated_ip_pools.insert(
            pool_name.clone(),
            DedicatedIpPool {
                pool_name,
                scaling_mode,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_dedicated_ip_pools(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let pools: Vec<&str> = state
            .dedicated_ip_pools
            .keys()
            .map(|k| k.as_str())
            .collect();
        let response = json!({ "DedicatedIpPools": pools });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_dedicated_ip_pool(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        if state.dedicated_ip_pools.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Pool {} does not exist", name),
            ));
        }
        // Remove IPs associated with this pool
        state.dedicated_ips.retain(|_, ip| ip.pool_name != name);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_dedicated_ip_pool_scaling_attributes(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let scaling_mode = match body["ScalingMode"].as_str() {
            Some(m) => m.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ScalingMode is required",
                ));
            }
        };

        let mut state = self.state.write();
        let pool = match state.dedicated_ip_pools.get_mut(name) {
            Some(p) => p,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Pool {} does not exist", name),
                ));
            }
        };

        if pool.scaling_mode == "MANAGED" && scaling_mode == "STANDARD" {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Cannot change scaling mode from MANAGED to STANDARD",
            ));
        }

        let old_mode = pool.scaling_mode.clone();
        pool.scaling_mode = scaling_mode.clone();

        // If changing from STANDARD to MANAGED, generate IPs
        if old_mode == "STANDARD" && scaling_mode == "MANAGED" {
            let pool_idx = state.dedicated_ip_pools.len() as u8;
            for i in 1..=3u8 {
                let ip_addr = format!("198.51.100.{}", pool_idx * 10 + i);
                state.dedicated_ips.insert(
                    ip_addr.clone(),
                    DedicatedIp {
                        ip: ip_addr,
                        warmup_status: "NOT_APPLICABLE".to_string(),
                        warmup_percentage: -1,
                        pool_name: name.to_string(),
                    },
                );
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ── Dedicated IPs ───────────────────────────────────────────────────

    fn get_dedicated_ip(&self, ip: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let dip = match state.dedicated_ips.get(ip) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Dedicated IP {} does not exist", ip),
                ));
            }
        };
        let response = json!({
            "DedicatedIp": {
                "Ip": dip.ip,
                "WarmupStatus": dip.warmup_status,
                "WarmupPercentage": dip.warmup_percentage,
                "PoolName": dip.pool_name,
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_dedicated_ips(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let pool_filter = req.query_params.get("PoolName").map(|s| s.as_str());
        let ips: Vec<Value> = state
            .dedicated_ips
            .values()
            .filter(|ip| match pool_filter {
                Some(pool) => ip.pool_name == pool,
                None => true,
            })
            .map(|ip| {
                json!({
                    "Ip": ip.ip,
                    "WarmupStatus": ip.warmup_status,
                    "WarmupPercentage": ip.warmup_percentage,
                    "PoolName": ip.pool_name,
                })
            })
            .collect();
        let response = json!({ "DedicatedIps": ips });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn put_dedicated_ip_in_pool(
        &self,
        ip: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let dest_pool = match body["DestinationPoolName"].as_str() {
            Some(p) => p.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "DestinationPoolName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if !state.dedicated_ip_pools.contains_key(&dest_pool) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Pool {} does not exist", dest_pool),
            ));
        }

        let dip = match state.dedicated_ips.get_mut(ip) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Dedicated IP {} does not exist", ip),
                ));
            }
        };
        dip.pool_name = dest_pool;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_dedicated_ip_warmup_attributes(
        &self,
        ip: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let warmup_pct = match body["WarmupPercentage"].as_i64() {
            Some(p) => p as i32,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "WarmupPercentage is required",
                ));
            }
        };

        let mut state = self.state.write();
        let dip = match state.dedicated_ips.get_mut(ip) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Dedicated IP {} does not exist", ip),
                ));
            }
        };
        dip.warmup_percentage = warmup_pct;
        dip.warmup_status = if warmup_pct >= 100 {
            "DONE".to_string()
        } else {
            "IN_PROGRESS".to_string()
        };
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_account_dedicated_ip_warmup_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let enabled = body["AutoWarmupEnabled"].as_bool().unwrap_or(false);
        self.state
            .write()
            .account_settings
            .dedicated_ip_auto_warmup_enabled = enabled;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ── Multi-region Endpoints ──────────────────────────────────────────

    fn create_multi_region_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let endpoint_name = match body["EndpointName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EndpointName is required",
                ));
            }
        };

        let mut state = self.state.write();
        if state.multi_region_endpoints.contains_key(&endpoint_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Endpoint {} already exists", endpoint_name),
            ));
        }

        // Extract regions from Details.RoutesDetails[].Region
        let mut regions = Vec::new();
        if let Some(details) = body.get("Details") {
            if let Some(routes) = details["RoutesDetails"].as_array() {
                for r in routes {
                    if let Some(region) = r["Region"].as_str() {
                        regions.push(region.to_string());
                    }
                }
            }
        }
        // The primary region is always the current region
        if !regions.contains(&state.region) {
            regions.insert(0, state.region.clone());
        }

        let endpoint_id = format!(
            "ses-{}-{}",
            state.region,
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
        );
        let now = Utc::now();

        state.multi_region_endpoints.insert(
            endpoint_name.clone(),
            MultiRegionEndpoint {
                endpoint_name,
                endpoint_id: endpoint_id.clone(),
                status: "READY".to_string(),
                regions,
                created_at: now,
                last_updated_at: now,
            },
        );

        let response = json!({
            "Status": "READY",
            "EndpointId": endpoint_id,
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_multi_region_endpoint(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let ep = match state.multi_region_endpoints.get(name) {
            Some(e) => e,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Endpoint {} does not exist", name),
                ));
            }
        };

        let routes: Vec<Value> = ep.regions.iter().map(|r| json!({ "Region": r })).collect();

        let response = json!({
            "EndpointName": ep.endpoint_name,
            "EndpointId": ep.endpoint_id,
            "Status": ep.status,
            "Routes": routes,
            "CreatedTimestamp": ep.created_at.timestamp() as f64,
            "LastUpdatedTimestamp": ep.last_updated_at.timestamp() as f64,
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_multi_region_endpoints(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let endpoints: Vec<Value> = state
            .multi_region_endpoints
            .values()
            .map(|ep| {
                json!({
                    "EndpointName": ep.endpoint_name,
                    "EndpointId": ep.endpoint_id,
                    "Status": ep.status,
                    "Regions": ep.regions,
                    "CreatedTimestamp": ep.created_at.timestamp() as f64,
                    "LastUpdatedTimestamp": ep.last_updated_at.timestamp() as f64,
                })
            })
            .collect();
        let response = json!({ "MultiRegionEndpoints": endpoints });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_multi_region_endpoint(&self, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        if state.multi_region_endpoints.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Endpoint {} does not exist", name),
            ));
        }
        let response = json!({ "Status": "DELETING" });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // ── Account Settings ────────────────────────────────────────────────

    fn put_account_details(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mail_type = match body["MailType"].as_str() {
            Some(m) => m.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "MailType is required",
                ));
            }
        };
        let website_url = match body["WebsiteURL"].as_str() {
            Some(u) => u.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "WebsiteURL is required",
                ));
            }
        };
        let contact_language = body["ContactLanguage"].as_str().map(|s| s.to_string());
        let use_case_description = body["UseCaseDescription"].as_str().map(|s| s.to_string());
        let additional = body["AdditionalContactEmailAddresses"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let production_access = body["ProductionAccessEnabled"].as_bool();

        let mut state = self.state.write();
        state.account_settings.details = Some(AccountDetails {
            mail_type: Some(mail_type),
            website_url: Some(website_url),
            contact_language,
            use_case_description,
            additional_contact_email_addresses: additional,
            production_access_enabled: production_access,
        });
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_account_sending_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let enabled = body["SendingEnabled"].as_bool().unwrap_or(false);
        self.state.write().account_settings.sending_enabled = enabled;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_account_suppression_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let reasons = body["SuppressedReasons"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        self.state.write().account_settings.suppressed_reasons = reasons;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn put_account_vdm_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let vdm = match body.get("VdmAttributes") {
            Some(v) => v.clone(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "VdmAttributes is required",
                ));
            }
        };
        self.state.write().account_settings.vdm_attributes = Some(vdm);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Import Job operations ---

    fn create_import_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let import_destination = match body.get("ImportDestination") {
            Some(v) if v.is_object() => v.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ImportDestination is required",
                ));
            }
        };

        let import_data_source = match body.get("ImportDataSource") {
            Some(v) if v.is_object() => v.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ImportDataSource is required",
                ));
            }
        };

        let job_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let job = ImportJob {
            job_id: job_id.clone(),
            import_destination,
            import_data_source,
            job_status: "COMPLETED".to_string(),
            created_timestamp: now,
            completed_timestamp: Some(now),
            processed_records_count: 0,
            failed_records_count: 0,
        };

        self.state.write().import_jobs.insert(job_id.clone(), job);

        let response = json!({ "JobId": job_id });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_import_job(&self, job_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let job = match state.import_jobs.get(job_id) {
            Some(j) => j,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Import job {} does not exist", job_id),
                ));
            }
        };

        let mut response = json!({
            "JobId": job.job_id,
            "ImportDestination": job.import_destination,
            "ImportDataSource": job.import_data_source,
            "JobStatus": job.job_status,
            "CreatedTimestamp": job.created_timestamp.timestamp() as f64,
            "ProcessedRecordsCount": job.processed_records_count,
            "FailedRecordsCount": job.failed_records_count,
        });
        if let Some(ref ts) = job.completed_timestamp {
            response["CompletedTimestamp"] = json!(ts.timestamp() as f64);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_import_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
        let filter_type = body["ImportDestinationType"].as_str();

        let state = self.state.read();
        let jobs: Vec<Value> = state
            .import_jobs
            .values()
            .filter(|j| {
                if let Some(ft) = filter_type {
                    // Check if import destination matches
                    if j.import_destination
                        .get("SuppressionListDestination")
                        .is_some()
                        && ft == "SUPPRESSION_LIST"
                    {
                        return true;
                    }
                    if j.import_destination.get("ContactListDestination").is_some()
                        && ft == "CONTACT_LIST"
                    {
                        return true;
                    }
                    return false;
                }
                true
            })
            .map(|j| {
                let mut obj = json!({
                    "JobId": j.job_id,
                    "ImportDestination": j.import_destination,
                    "JobStatus": j.job_status,
                    "CreatedTimestamp": j.created_timestamp.timestamp() as f64,
                });
                if j.processed_records_count > 0 {
                    obj["ProcessedRecordsCount"] = json!(j.processed_records_count);
                }
                if j.failed_records_count > 0 {
                    obj["FailedRecordsCount"] = json!(j.failed_records_count);
                }
                obj
            })
            .collect();

        let response = json!({ "ImportJobs": jobs });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Export Job operations ---

    fn create_export_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let export_data_source = match body.get("ExportDataSource") {
            Some(v) if v.is_object() => v.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ExportDataSource is required",
                ));
            }
        };

        let export_destination = match body.get("ExportDestination") {
            Some(v) if v.is_object() => v.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ExportDestination is required",
                ));
            }
        };

        // Determine export source type from the data source
        let export_source_type = if export_data_source.get("MetricsDataSource").is_some() {
            "METRICS_DATA"
        } else {
            "MESSAGE_INSIGHTS"
        };

        let job_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let job = ExportJob {
            job_id: job_id.clone(),
            export_source_type: export_source_type.to_string(),
            export_destination,
            export_data_source,
            job_status: "COMPLETED".to_string(),
            created_timestamp: now,
            completed_timestamp: Some(now),
        };

        self.state.write().export_jobs.insert(job_id.clone(), job);

        let response = json!({ "JobId": job_id });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_export_job(&self, job_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let job = match state.export_jobs.get(job_id) {
            Some(j) => j,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Export job {} does not exist", job_id),
                ));
            }
        };

        let mut response = json!({
            "JobId": job.job_id,
            "ExportSourceType": job.export_source_type,
            "JobStatus": job.job_status,
            "ExportDestination": job.export_destination,
            "ExportDataSource": job.export_data_source,
            "CreatedTimestamp": job.created_timestamp.timestamp() as f64,
            "Statistics": {
                "ProcessedRecordsCount": 0,
                "ExportedRecordsCount": 0,
            },
        });
        if let Some(ref ts) = job.completed_timestamp {
            response["CompletedTimestamp"] = json!(ts.timestamp() as f64);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_export_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
        let filter_status = body["JobStatus"].as_str();
        let filter_type = body["ExportSourceType"].as_str();

        let state = self.state.read();
        let jobs: Vec<Value> = state
            .export_jobs
            .values()
            .filter(|j| {
                if let Some(s) = filter_status {
                    if j.job_status != s {
                        return false;
                    }
                }
                if let Some(t) = filter_type {
                    if j.export_source_type != t {
                        return false;
                    }
                }
                true
            })
            .map(|j| {
                let mut obj = json!({
                    "JobId": j.job_id,
                    "ExportSourceType": j.export_source_type,
                    "JobStatus": j.job_status,
                    "CreatedTimestamp": j.created_timestamp.timestamp() as f64,
                });
                if let Some(ref ts) = j.completed_timestamp {
                    obj["CompletedTimestamp"] = json!(ts.timestamp() as f64);
                }
                obj
            })
            .collect();

        let response = json!({ "ExportJobs": jobs });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn cancel_export_job(&self, job_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let job = match state.export_jobs.get_mut(job_id) {
            Some(j) => j,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Export job {} does not exist", job_id),
                ));
            }
        };

        if job.job_status == "COMPLETED" || job.job_status == "CANCELLED" {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "ConflictException",
                &format!("Export job {} is already {}", job_id, job.job_status),
            ));
        }

        job.job_status = "CANCELLED".to_string();
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Tenant operations ---

    fn create_tenant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.tenants.contains_key(&tenant_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Tenant {} already exists", tenant_name),
            ));
        }

        let tenant_id = uuid::Uuid::new_v4().to_string();
        let tenant_arn = format!(
            "arn:aws:ses:{}:{}:tenant/{}",
            req.region, req.account_id, tenant_id
        );
        let now = Utc::now();

        let tags = body
            .get("Tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let tenant = Tenant {
            tenant_name: tenant_name.clone(),
            tenant_id: tenant_id.clone(),
            tenant_arn: tenant_arn.clone(),
            created_timestamp: now,
            sending_status: "ENABLED".to_string(),
            tags: tags.clone(),
        };

        state.tenants.insert(tenant_name.clone(), tenant);

        let response = json!({
            "TenantName": tenant_name,
            "TenantId": tenant_id,
            "TenantArn": tenant_arn,
            "CreatedTimestamp": now.timestamp() as f64,
            "SendingStatus": "ENABLED",
            "Tags": tags,
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_tenant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };

        let state = self.state.read();
        let tenant = match state.tenants.get(tenant_name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Tenant {} does not exist", tenant_name),
                ));
            }
        };

        let response = json!({
            "Tenant": {
                "TenantName": tenant.tenant_name,
                "TenantId": tenant.tenant_id,
                "TenantArn": tenant.tenant_arn,
                "CreatedTimestamp": tenant.created_timestamp.timestamp() as f64,
                "SendingStatus": tenant.sending_status,
                "Tags": tenant.tags,
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_tenants(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let tenants: Vec<Value> = state
            .tenants
            .values()
            .map(|t| {
                json!({
                    "TenantName": t.tenant_name,
                    "TenantId": t.tenant_id,
                    "TenantArn": t.tenant_arn,
                    "CreatedTimestamp": t.created_timestamp.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({ "Tenants": tenants });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_tenant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };

        let mut state = self.state.write();

        if state.tenants.remove(tenant_name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Tenant {} does not exist", tenant_name),
            ));
        }

        state.tenant_resource_associations.remove(tenant_name);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn create_tenant_resource_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };
        let resource_arn = match body["ResourceArn"].as_str() {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        let mut state = self.state.write();

        if !state.tenants.contains_key(&tenant_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Tenant {} does not exist", tenant_name),
            ));
        }

        let assoc = TenantResourceAssociation {
            resource_arn,
            associated_timestamp: Utc::now(),
        };

        state
            .tenant_resource_associations
            .entry(tenant_name)
            .or_default()
            .push(assoc);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn delete_tenant_resource_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };
        let resource_arn = match body["ResourceArn"].as_str() {
            Some(a) => a,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        let mut state = self.state.write();

        if let Some(assocs) = state.tenant_resource_associations.get_mut(tenant_name) {
            let before = assocs.len();
            assocs.retain(|a| a.resource_arn != resource_arn);
            if assocs.len() == before {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    "Resource association not found",
                ));
            }
        } else {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "Resource association not found",
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_tenant_resources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };

        let state = self.state.read();

        if !state.tenants.contains_key(tenant_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Tenant {} does not exist", tenant_name),
            ));
        }

        let resources: Vec<Value> = state
            .tenant_resource_associations
            .get(tenant_name)
            .map(|assocs| {
                assocs
                    .iter()
                    .map(|a| {
                        json!({
                            "ResourceType": "RESOURCE",
                            "ResourceArn": a.resource_arn,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let response = json!({ "TenantResources": resources });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_resource_tenants(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let resource_arn = match body["ResourceArn"].as_str() {
            Some(a) => a,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        let state = self.state.read();
        let mut resource_tenants: Vec<Value> = Vec::new();

        for (tenant_name, assocs) in &state.tenant_resource_associations {
            for assoc in assocs {
                if assoc.resource_arn == resource_arn {
                    if let Some(tenant) = state.tenants.get(tenant_name) {
                        resource_tenants.push(json!({
                            "TenantName": tenant.tenant_name,
                            "TenantId": tenant.tenant_id,
                            "ResourceArn": assoc.resource_arn,
                            "AssociatedTimestamp": assoc.associated_timestamp.timestamp() as f64,
                        }));
                    }
                }
            }
        }

        let response = json!({ "ResourceTenants": resource_tenants });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Reputation Entity operations ---

    fn get_reputation_entity(
        &self,
        entity_type: &str,
        entity_ref: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = format!("{}/{}", entity_type, entity_ref);
        let state = self.state.read();

        let entity = match state.reputation_entities.get(&key) {
            Some(e) => e,
            None => {
                // Return a default entity for any reference
                let response = json!({
                    "ReputationEntity": {
                        "ReputationEntityReference": entity_ref,
                        "ReputationEntityType": entity_type,
                        "SendingStatusAggregate": "ENABLED",
                        "CustomerManagedStatus": {
                            "SendingStatus": "ENABLED",
                        },
                        "AwsSesManagedStatus": {
                            "SendingStatus": "ENABLED",
                        },
                    }
                });
                return Ok(AwsResponse::json(StatusCode::OK, response.to_string()));
            }
        };

        let response = json!({
            "ReputationEntity": {
                "ReputationEntityReference": entity.reputation_entity_reference,
                "ReputationEntityType": entity.reputation_entity_type,
                "ReputationManagementPolicy": entity.reputation_management_policy,
                "SendingStatusAggregate": entity.sending_status_aggregate,
                "CustomerManagedStatus": {
                    "SendingStatus": entity.customer_managed_status,
                },
                "AwsSesManagedStatus": {
                    "SendingStatus": "ENABLED",
                },
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn list_reputation_entities(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let entities: Vec<Value> = state
            .reputation_entities
            .values()
            .map(|e| {
                json!({
                    "ReputationEntityReference": e.reputation_entity_reference,
                    "ReputationEntityType": e.reputation_entity_type,
                    "SendingStatusAggregate": e.sending_status_aggregate,
                })
            })
            .collect();

        let response = json!({ "ReputationEntities": entities });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn update_reputation_entity_customer_managed_status(
        &self,
        entity_type: &str,
        entity_ref: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let sending_status = body["SendingStatus"]
            .as_str()
            .unwrap_or("ENABLED")
            .to_string();

        let key = format!("{}/{}", entity_type, entity_ref);
        let mut state = self.state.write();

        let entity =
            state
                .reputation_entities
                .entry(key)
                .or_insert_with(|| ReputationEntityState {
                    reputation_entity_reference: entity_ref.to_string(),
                    reputation_entity_type: entity_type.to_string(),
                    reputation_management_policy: None,
                    customer_managed_status: "ENABLED".to_string(),
                    sending_status_aggregate: "ENABLED".to_string(),
                });

        entity.customer_managed_status = sending_status;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn update_reputation_entity_policy(
        &self,
        entity_type: &str,
        entity_ref: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let policy = body["ReputationEntityPolicy"]
            .as_str()
            .map(|s| s.to_string());

        let key = format!("{}/{}", entity_type, entity_ref);
        let mut state = self.state.write();

        let entity =
            state
                .reputation_entities
                .entry(key)
                .or_insert_with(|| ReputationEntityState {
                    reputation_entity_reference: entity_ref.to_string(),
                    reputation_entity_type: entity_type.to_string(),
                    reputation_management_policy: None,
                    customer_managed_status: "ENABLED".to_string(),
                    sending_status_aggregate: "ENABLED".to_string(),
                });

        entity.reputation_management_policy = policy;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Metrics ---

    fn batch_get_metric_data(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let queries = body["Queries"].as_array().cloned().unwrap_or_default();

        let results: Vec<Value> = queries
            .iter()
            .filter_map(|q| {
                let id = q["Id"].as_str()?;
                Some(json!({
                    "Id": id,
                    "Timestamps": [],
                    "Values": [],
                }))
            })
            .collect();

        let response = json!({
            "Results": results,
            "Errors": [],
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }
}

fn parse_topics(value: &Value) -> Vec<Topic> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let topic_name = v["TopicName"].as_str()?.to_string();
                    let display_name = v["DisplayName"].as_str().unwrap_or("").to_string();
                    let description = v["Description"].as_str().unwrap_or("").to_string();
                    let default_subscription_status = v["DefaultSubscriptionStatus"]
                        .as_str()
                        .unwrap_or("OPT_OUT")
                        .to_string();
                    Some(Topic {
                        topic_name,
                        display_name,
                        description,
                        default_subscription_status,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_topic_preferences(value: &Value) -> Vec<TopicPreference> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let topic_name = v["TopicName"].as_str()?.to_string();
                    let subscription_status = v["SubscriptionStatus"]
                        .as_str()
                        .unwrap_or("OPT_OUT")
                        .to_string();
                    Some(TopicPreference {
                        topic_name,
                        subscription_status,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn extract_string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_event_destination_definition(name: &str, def: &Value) -> EventDestination {
    let enabled = def["Enabled"].as_bool().unwrap_or(false);
    let matching_event_types = extract_string_array(&def["MatchingEventTypes"]);
    let kinesis_firehose_destination = def
        .get("KinesisFirehoseDestination")
        .filter(|v| v.is_object())
        .cloned();
    let cloud_watch_destination = def
        .get("CloudWatchDestination")
        .filter(|v| v.is_object())
        .cloned();
    let sns_destination = def.get("SnsDestination").filter(|v| v.is_object()).cloned();
    let event_bridge_destination = def
        .get("EventBridgeDestination")
        .filter(|v| v.is_object())
        .cloned();
    let pinpoint_destination = def
        .get("PinpointDestination")
        .filter(|v| v.is_object())
        .cloned();

    EventDestination {
        name: name.to_string(),
        enabled,
        matching_event_types,
        kinesis_firehose_destination,
        cloud_watch_destination,
        sns_destination,
        event_bridge_destination,
        pinpoint_destination,
    }
}

fn event_destination_to_json(dest: &EventDestination) -> Value {
    let mut obj = json!({
        "Name": dest.name,
        "Enabled": dest.enabled,
        "MatchingEventTypes": dest.matching_event_types,
    });
    if let Some(ref v) = dest.kinesis_firehose_destination {
        obj["KinesisFirehoseDestination"] = v.clone();
    }
    if let Some(ref v) = dest.cloud_watch_destination {
        obj["CloudWatchDestination"] = v.clone();
    }
    if let Some(ref v) = dest.sns_destination {
        obj["SnsDestination"] = v.clone();
    }
    if let Some(ref v) = dest.event_bridge_destination {
        obj["EventBridgeDestination"] = v.clone();
    }
    if let Some(ref v) = dest.pinpoint_destination {
        obj["PinpointDestination"] = v.clone();
    }
    obj
}

#[async_trait]
impl fakecloud_core::service::AwsService for SesV2Service {
    fn service_name(&self) -> &str {
        "ses"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, resource_name, sub_resource) =
            Self::resolve_action(&req).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "UnknownOperationException",
                    format!("Unknown operation: {} {}", req.method, req.raw_path),
                )
            })?;

        let res = resource_name.as_deref().unwrap_or("");
        let sub = sub_resource.as_deref().unwrap_or("");

        match action {
            "GetAccount" => self.get_account(),
            "CreateEmailIdentity" => self.create_email_identity(&req),
            "ListEmailIdentities" => self.list_email_identities(),
            "GetEmailIdentity" => self.get_email_identity(res),
            "DeleteEmailIdentity" => self.delete_email_identity(res, &req),
            "CreateConfigurationSet" => self.create_configuration_set(&req),
            "ListConfigurationSets" => self.list_configuration_sets(),
            "GetConfigurationSet" => self.get_configuration_set(res),
            "DeleteConfigurationSet" => self.delete_configuration_set(res, &req),
            "CreateEmailTemplate" => self.create_email_template(&req),
            "ListEmailTemplates" => self.list_email_templates(),
            "GetEmailTemplate" => self.get_email_template(res),
            "UpdateEmailTemplate" => self.update_email_template(res, &req),
            "DeleteEmailTemplate" => self.delete_email_template(res),
            "SendEmail" => self.send_email(&req),
            "SendBulkEmail" => self.send_bulk_email(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "CreateContactList" => self.create_contact_list(&req),
            "GetContactList" => self.get_contact_list(res),
            "ListContactLists" => self.list_contact_lists(),
            "UpdateContactList" => self.update_contact_list(res, &req),
            "DeleteContactList" => self.delete_contact_list(res, &req),
            "CreateContact" => self.create_contact(res, &req),
            "GetContact" => self.get_contact(res, sub),
            "ListContacts" => self.list_contacts(res),
            "UpdateContact" => self.update_contact(res, sub, &req),
            "DeleteContact" => self.delete_contact(res, sub),
            "PutSuppressedDestination" => self.put_suppressed_destination(&req),
            "GetSuppressedDestination" => self.get_suppressed_destination(res),
            "DeleteSuppressedDestination" => self.delete_suppressed_destination(res),
            "ListSuppressedDestinations" => self.list_suppressed_destinations(),
            "CreateConfigurationSetEventDestination" => {
                self.create_configuration_set_event_destination(res, &req)
            }
            "GetConfigurationSetEventDestinations" => {
                self.get_configuration_set_event_destinations(res)
            }
            "UpdateConfigurationSetEventDestination" => {
                self.update_configuration_set_event_destination(res, sub, &req)
            }
            "DeleteConfigurationSetEventDestination" => {
                self.delete_configuration_set_event_destination(res, sub)
            }
            "CreateEmailIdentityPolicy" => self.create_email_identity_policy(res, sub, &req),
            "GetEmailIdentityPolicies" => self.get_email_identity_policies(res),
            "UpdateEmailIdentityPolicy" => self.update_email_identity_policy(res, sub, &req),
            "DeleteEmailIdentityPolicy" => self.delete_email_identity_policy(res, sub),
            "PutEmailIdentityDkimAttributes" => self.put_email_identity_dkim_attributes(res, &req),
            "PutEmailIdentityDkimSigningAttributes" => {
                self.put_email_identity_dkim_signing_attributes(res, &req)
            }
            "PutEmailIdentityFeedbackAttributes" => {
                self.put_email_identity_feedback_attributes(res, &req)
            }
            "PutEmailIdentityMailFromAttributes" => {
                self.put_email_identity_mail_from_attributes(res, &req)
            }
            "PutEmailIdentityConfigurationSetAttributes" => {
                self.put_email_identity_configuration_set_attributes(res, &req)
            }
            "PutConfigurationSetSendingOptions" => {
                self.put_configuration_set_sending_options(res, &req)
            }
            "PutConfigurationSetDeliveryOptions" => {
                self.put_configuration_set_delivery_options(res, &req)
            }
            "PutConfigurationSetTrackingOptions" => {
                self.put_configuration_set_tracking_options(res, &req)
            }
            "PutConfigurationSetSuppressionOptions" => {
                self.put_configuration_set_suppression_options(res, &req)
            }
            "PutConfigurationSetReputationOptions" => {
                self.put_configuration_set_reputation_options(res, &req)
            }
            "PutConfigurationSetVdmOptions" => self.put_configuration_set_vdm_options(res, &req),
            "PutConfigurationSetArchivingOptions" => {
                self.put_configuration_set_archiving_options(res, &req)
            }
            "CreateCustomVerificationEmailTemplate" => {
                self.create_custom_verification_email_template(&req)
            }
            "GetCustomVerificationEmailTemplate" => {
                self.get_custom_verification_email_template(res)
            }
            "ListCustomVerificationEmailTemplates" => {
                self.list_custom_verification_email_templates(&req)
            }
            "UpdateCustomVerificationEmailTemplate" => {
                self.update_custom_verification_email_template(res, &req)
            }
            "DeleteCustomVerificationEmailTemplate" => {
                self.delete_custom_verification_email_template(res)
            }
            "SendCustomVerificationEmail" => self.send_custom_verification_email(&req),
            "TestRenderEmailTemplate" => self.test_render_email_template(res, &req),
            "CreateDedicatedIpPool" => self.create_dedicated_ip_pool(&req),
            "ListDedicatedIpPools" => self.list_dedicated_ip_pools(),
            "DeleteDedicatedIpPool" => self.delete_dedicated_ip_pool(res),
            "GetDedicatedIp" => self.get_dedicated_ip(res),
            "GetDedicatedIps" => self.get_dedicated_ips(&req),
            "PutDedicatedIpInPool" => self.put_dedicated_ip_in_pool(res, &req),
            "PutDedicatedIpPoolScalingAttributes" => {
                self.put_dedicated_ip_pool_scaling_attributes(res, &req)
            }
            "PutDedicatedIpWarmupAttributes" => self.put_dedicated_ip_warmup_attributes(res, &req),
            "PutAccountDedicatedIpWarmupAttributes" => {
                self.put_account_dedicated_ip_warmup_attributes(&req)
            }
            "CreateMultiRegionEndpoint" => self.create_multi_region_endpoint(&req),
            "GetMultiRegionEndpoint" => self.get_multi_region_endpoint(res),
            "ListMultiRegionEndpoints" => self.list_multi_region_endpoints(),
            "DeleteMultiRegionEndpoint" => self.delete_multi_region_endpoint(res),
            "PutAccountDetails" => self.put_account_details(&req),
            "PutAccountSendingAttributes" => self.put_account_sending_attributes(&req),
            "PutAccountSuppressionAttributes" => self.put_account_suppression_attributes(&req),
            "PutAccountVdmAttributes" => self.put_account_vdm_attributes(&req),
            "CreateImportJob" => self.create_import_job(&req),
            "GetImportJob" => self.get_import_job(res),
            "ListImportJobs" => self.list_import_jobs(&req),
            "CreateExportJob" => self.create_export_job(&req),
            "GetExportJob" => self.get_export_job(res),
            "ListExportJobs" => self.list_export_jobs(&req),
            "CancelExportJob" => self.cancel_export_job(res),
            "CreateTenant" => self.create_tenant(&req),
            "GetTenant" => self.get_tenant(&req),
            "ListTenants" => self.list_tenants(&req),
            "DeleteTenant" => self.delete_tenant(&req),
            "CreateTenantResourceAssociation" => self.create_tenant_resource_association(&req),
            "DeleteTenantResourceAssociation" => self.delete_tenant_resource_association(&req),
            "ListTenantResources" => self.list_tenant_resources(&req),
            "ListResourceTenants" => self.list_resource_tenants(&req),
            "GetReputationEntity" => self.get_reputation_entity(res, sub),
            "ListReputationEntities" => self.list_reputation_entities(&req),
            "UpdateReputationEntityCustomerManagedStatus" => {
                self.update_reputation_entity_customer_managed_status(res, sub, &req)
            }
            "UpdateReputationEntityPolicy" => self.update_reputation_entity_policy(res, sub, &req),
            "BatchGetMetricData" => self.batch_get_metric_data(&req),
            _ => Err(AwsServiceError::action_not_implemented("ses", action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "GetAccount",
            "CreateEmailIdentity",
            "ListEmailIdentities",
            "GetEmailIdentity",
            "DeleteEmailIdentity",
            "CreateConfigurationSet",
            "ListConfigurationSets",
            "GetConfigurationSet",
            "DeleteConfigurationSet",
            "CreateEmailTemplate",
            "ListEmailTemplates",
            "GetEmailTemplate",
            "UpdateEmailTemplate",
            "DeleteEmailTemplate",
            "SendEmail",
            "SendBulkEmail",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "CreateContactList",
            "GetContactList",
            "ListContactLists",
            "UpdateContactList",
            "DeleteContactList",
            "CreateContact",
            "GetContact",
            "ListContacts",
            "UpdateContact",
            "DeleteContact",
            "PutSuppressedDestination",
            "GetSuppressedDestination",
            "DeleteSuppressedDestination",
            "ListSuppressedDestinations",
            "CreateConfigurationSetEventDestination",
            "GetConfigurationSetEventDestinations",
            "UpdateConfigurationSetEventDestination",
            "DeleteConfigurationSetEventDestination",
            "CreateEmailIdentityPolicy",
            "GetEmailIdentityPolicies",
            "UpdateEmailIdentityPolicy",
            "DeleteEmailIdentityPolicy",
            "PutEmailIdentityDkimAttributes",
            "PutEmailIdentityDkimSigningAttributes",
            "PutEmailIdentityFeedbackAttributes",
            "PutEmailIdentityMailFromAttributes",
            "PutEmailIdentityConfigurationSetAttributes",
            "PutConfigurationSetSendingOptions",
            "PutConfigurationSetDeliveryOptions",
            "PutConfigurationSetTrackingOptions",
            "PutConfigurationSetSuppressionOptions",
            "PutConfigurationSetReputationOptions",
            "PutConfigurationSetVdmOptions",
            "PutConfigurationSetArchivingOptions",
            "CreateCustomVerificationEmailTemplate",
            "GetCustomVerificationEmailTemplate",
            "ListCustomVerificationEmailTemplates",
            "UpdateCustomVerificationEmailTemplate",
            "DeleteCustomVerificationEmailTemplate",
            "SendCustomVerificationEmail",
            "TestRenderEmailTemplate",
            "CreateDedicatedIpPool",
            "ListDedicatedIpPools",
            "DeleteDedicatedIpPool",
            "GetDedicatedIp",
            "GetDedicatedIps",
            "PutDedicatedIpInPool",
            "PutDedicatedIpPoolScalingAttributes",
            "PutDedicatedIpWarmupAttributes",
            "PutAccountDedicatedIpWarmupAttributes",
            "CreateMultiRegionEndpoint",
            "GetMultiRegionEndpoint",
            "ListMultiRegionEndpoints",
            "DeleteMultiRegionEndpoint",
            "PutAccountDetails",
            "PutAccountSendingAttributes",
            "PutAccountSuppressionAttributes",
            "PutAccountVdmAttributes",
            "CreateImportJob",
            "GetImportJob",
            "ListImportJobs",
            "CreateExportJob",
            "GetExportJob",
            "ListExportJobs",
            "CancelExportJob",
            "CreateTenant",
            "GetTenant",
            "ListTenants",
            "DeleteTenant",
            "CreateTenantResourceAssociation",
            "DeleteTenantResourceAssociation",
            "ListTenantResources",
            "ListResourceTenants",
            "GetReputationEntity",
            "ListReputationEntities",
            "UpdateReputationEntityCustomerManagedStatus",
            "UpdateReputationEntityPolicy",
            "BatchGetMetricData",
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SesState;
    use bytes::Bytes;
    use fakecloud_core::service::AwsService;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedSesState {
        Arc::new(RwLock::new(SesState::new("123456789012", "us-east-1")))
    }

    fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
        make_request_with_query(method, path, body, "", HashMap::new())
    }

    fn make_request_with_query(
        method: Method,
        path: &str,
        body: &str,
        raw_query: &str,
        query_params: HashMap<String, String>,
    ) -> AwsRequest {
        let path_segments: Vec<String> = path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        AwsRequest {
            service: "ses".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::from(body.to_string()),
            path_segments,
            raw_path: path.to_string(),
            raw_query: raw_query.to_string(),
            method,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    #[tokio::test]
    async fn test_identity_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create identity
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["VerifiedForSendingStatus"], true);
        assert_eq!(body["IdentityType"], "EMAIL_ADDRESS");

        // List identities
        let req = make_request(Method::GET, "/v2/email/identities", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EmailIdentities"].as_array().unwrap().len(), 1);

        // Get identity
        let req = make_request(Method::GET, "/v2/email/identities/test%40example.com", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["VerifiedForSendingStatus"], true);
        assert_eq!(body["DkimAttributes"]["Status"], "SUCCESS");

        // Delete identity
        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(Method::GET, "/v2/email/identities/test%40example.com", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_domain_identity() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["IdentityType"], "DOMAIN");
    }

    #[tokio::test]
    async fn test_duplicate_identity() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_template_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create template
        let req = make_request(
            Method::POST,
            "/v2/email/templates",
            r#"{"TemplateName": "welcome", "TemplateContent": {"Subject": "Welcome", "Html": "<h1>Hi</h1>", "Text": "Hi"}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get template
        let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TemplateName"], "welcome");
        assert_eq!(body["TemplateContent"]["Subject"], "Welcome");

        // Update template
        let req = make_request(
            Method::PUT,
            "/v2/email/templates/welcome",
            r#"{"TemplateContent": {"Subject": "Updated Welcome"}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TemplateContent"]["Subject"], "Updated Welcome");

        // List templates
        let req = make_request(Method::GET, "/v2/email/templates", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TemplatesMetadata"].as_array().unwrap().len(), 1);

        // Delete template
        let req = make_request(Method::DELETE, "/v2/email/templates/welcome", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_send_email() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "Destination": {
                    "ToAddresses": ["recipient@example.com"]
                },
                "Content": {
                    "Simple": {
                        "Subject": {"Data": "Test Subject"},
                        "Body": {
                            "Text": {"Data": "Hello world"},
                            "Html": {"Data": "<p>Hello world</p>"}
                        }
                    }
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["MessageId"].as_str().is_some());

        // Verify stored
        let s = state.read();
        assert_eq!(s.sent_emails.len(), 1);
        assert_eq!(s.sent_emails[0].from, "sender@example.com");
        assert_eq!(s.sent_emails[0].to, vec!["recipient@example.com"]);
        assert_eq!(s.sent_emails[0].subject.as_deref(), Some("Test Subject"));
    }

    #[tokio::test]
    async fn test_get_account() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::GET, "/v2/email/account", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SendingEnabled"], true);
        assert!(body["SendQuota"]["Max24HourSend"].as_f64().unwrap() > 0.0);
    }

    #[tokio::test]
    async fn test_configuration_set_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create
        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get
        let req = make_request(Method::GET, "/v2/email/configuration-sets/my-config", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ConfigurationSetName"], "my-config");

        // List
        let req = make_request(Method::GET, "/v2/email/configuration-sets", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ConfigurationSets"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(Method::GET, "/v2/email/configuration-sets/my-config", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_send_email_raw_content() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "Destination": {
                    "ToAddresses": ["to@example.com"]
                },
                "Content": {
                    "Raw": {
                        "Data": "From: sender@example.com\r\nTo: to@example.com\r\nSubject: Raw\r\n\r\nBody"
                    }
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["MessageId"].as_str().is_some());

        let s = state.read();
        assert_eq!(s.sent_emails.len(), 1);
        assert!(s.sent_emails[0].raw_data.is_some());
        assert!(
            s.sent_emails[0].subject.is_none(),
            "Raw emails should not have parsed subject"
        );
    }

    #[tokio::test]
    async fn test_send_email_template_content() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "Destination": {
                    "ToAddresses": ["to@example.com"]
                },
                "Content": {
                    "Template": {
                        "TemplateName": "welcome",
                        "TemplateData": "{\"name\": \"Alice\"}"
                    }
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let s = state.read();
        assert_eq!(s.sent_emails.len(), 1);
        assert_eq!(s.sent_emails[0].template_name.as_deref(), Some("welcome"));
        assert_eq!(
            s.sent_emails[0].template_data.as_deref(),
            Some("{\"name\": \"Alice\"}")
        );
    }

    #[tokio::test]
    async fn test_send_email_missing_content() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{"FromEmailAddress": "sender@example.com", "Destination": {"ToAddresses": ["to@example.com"]}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_send_email_with_cc_and_bcc() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "Destination": {
                    "ToAddresses": ["to@example.com"],
                    "CcAddresses": ["cc@example.com"],
                    "BccAddresses": ["bcc@example.com"]
                },
                "Content": {
                    "Simple": {
                        "Subject": {"Data": "Test"},
                        "Body": {"Text": {"Data": "Hello"}}
                    }
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let s = state.read();
        assert_eq!(s.sent_emails[0].cc, vec!["cc@example.com"]);
        assert_eq!(s.sent_emails[0].bcc, vec!["bcc@example.com"]);
    }

    #[tokio::test]
    async fn test_send_bulk_email() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-bulk-emails",
            r#"{
                "FromEmailAddress": "sender@example.com",
                "DefaultContent": {
                    "Template": {
                        "TemplateName": "bulk-template",
                        "TemplateData": "{\"default\": true}"
                    }
                },
                "BulkEmailEntries": [
                    {"Destination": {"ToAddresses": ["a@example.com"]}},
                    {"Destination": {"ToAddresses": ["b@example.com"]}}
                ]
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let results = body["BulkEmailEntryResults"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["Status"], "SUCCESS");
        assert_eq!(results[1]["Status"], "SUCCESS");

        let s = state.read();
        assert_eq!(s.sent_emails.len(), 2);
        assert_eq!(s.sent_emails[0].to, vec!["a@example.com"]);
        assert_eq!(s.sent_emails[1].to, vec!["b@example.com"]);
    }

    #[tokio::test]
    async fn test_send_bulk_email_empty_entries() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-bulk-emails",
            r#"{"FromEmailAddress": "s@example.com", "BulkEmailEntries": []}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_identity() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/nobody%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_configuration_set() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "dup-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "dup-config"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_duplicate_template() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/templates",
            r#"{"TemplateName": "dup-tmpl", "TemplateContent": {}}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/templates",
            r#"{"TemplateName": "dup-tmpl", "TemplateContent": {}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_template() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::DELETE, "/v2/email/templates/nope", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_configuration_set() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::DELETE, "/v2/email/configuration-sets/nope", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_unknown_route() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::GET, "/v2/email/unknown-resource", "");
        let result = svc.handle(req).await;
        assert!(result.is_err(), "Unknown route should return error");
    }

    #[tokio::test]
    async fn test_update_nonexistent_template() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/templates/nonexistent",
            r#"{"TemplateContent": {"Subject": "Updated"}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_invalid_json_body() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::POST, "/v2/email/identities", "not valid json {{{");
        let result = svc.handle(req).await;
        assert!(result.is_err(), "Invalid JSON body should return error");
    }

    #[tokio::test]
    async fn test_create_identity_missing_name() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::POST, "/v2/email/identities", r#"{}"#);
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    // --- Contact List tests ---

    #[tokio::test]
    async fn test_contact_list_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create contact list with topics
        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{
                "ContactListName": "my-list",
                "Description": "Test list",
                "Topics": [
                    {
                        "TopicName": "newsletters",
                        "DisplayName": "Newsletters",
                        "Description": "Weekly newsletters",
                        "DefaultSubscriptionStatus": "OPT_IN"
                    }
                ]
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get contact list
        let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ContactListName"], "my-list");
        assert_eq!(body["Description"], "Test list");
        assert_eq!(body["Topics"][0]["TopicName"], "newsletters");
        assert_eq!(body["Topics"][0]["DefaultSubscriptionStatus"], "OPT_IN");
        assert!(body["CreatedTimestamp"].as_f64().is_some());
        assert!(body["LastUpdatedTimestamp"].as_f64().is_some());

        // List contact lists
        let req = make_request(Method::GET, "/v2/email/contact-lists", "{}");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ContactLists"].as_array().unwrap().len(), 1);
        assert_eq!(body["ContactLists"][0]["ContactListName"], "my-list");

        // Update contact list
        let req = make_request(
            Method::PUT,
            "/v2/email/contact-lists/my-list",
            r#"{
                "Description": "Updated description",
                "Topics": [
                    {
                        "TopicName": "newsletters",
                        "DisplayName": "Updated Newsletters",
                        "Description": "Updated desc",
                        "DefaultSubscriptionStatus": "OPT_OUT"
                    },
                    {
                        "TopicName": "promotions",
                        "DisplayName": "Promotions",
                        "Description": "Promo emails",
                        "DefaultSubscriptionStatus": "OPT_OUT"
                    }
                ]
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Description"], "Updated description");
        assert_eq!(body["Topics"].as_array().unwrap().len(), 2);

        // Delete contact list
        let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_contact_list() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "dup-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "dup-list"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_contact_list_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(Method::GET, "/v2/email/contact-lists/nonexistent", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    // --- Contact tests ---

    #[tokio::test]
    async fn test_contact_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create contact list first
        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{
                "ContactListName": "my-list",
                "Topics": [
                    {
                        "TopicName": "newsletters",
                        "DisplayName": "Newsletters",
                        "Description": "Weekly newsletters",
                        "DefaultSubscriptionStatus": "OPT_OUT"
                    }
                ]
            }"#,
        );
        svc.handle(req).await.unwrap();

        // Create contact
        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/my-list/contacts",
            r#"{
                "EmailAddress": "user@example.com",
                "TopicPreferences": [
                    {"TopicName": "newsletters", "SubscriptionStatus": "OPT_IN"}
                ],
                "UnsubscribeAll": false
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get contact
        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EmailAddress"], "user@example.com");
        assert_eq!(body["ContactListName"], "my-list");
        assert_eq!(body["UnsubscribeAll"], false);
        assert_eq!(body["TopicPreferences"][0]["TopicName"], "newsletters");
        assert_eq!(body["TopicPreferences"][0]["SubscriptionStatus"], "OPT_IN");
        assert_eq!(
            body["TopicDefaultPreferences"][0]["SubscriptionStatus"],
            "OPT_OUT"
        );
        assert!(body["CreatedTimestamp"].as_f64().is_some());

        // List contacts
        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Contacts"].as_array().unwrap().len(), 1);
        assert_eq!(body["Contacts"][0]["EmailAddress"], "user@example.com");

        // Update contact
        let req = make_request(
            Method::PUT,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            r#"{
                "TopicPreferences": [
                    {"TopicName": "newsletters", "SubscriptionStatus": "OPT_OUT"}
                ],
                "UnsubscribeAll": true
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["UnsubscribeAll"], true);
        assert_eq!(body["TopicPreferences"][0]["SubscriptionStatus"], "OPT_OUT");

        // Delete contact
        let req = make_request(
            Method::DELETE,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts/user%40example.com",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_contact() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "my-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/my-list/contacts",
            r#"{"EmailAddress": "dup@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/my-list/contacts",
            r#"{"EmailAddress": "dup@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_contact_in_nonexistent_list() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/no-such-list/contacts",
            r#"{"EmailAddress": "user@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_nonexistent_contact() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "my-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::GET,
            "/v2/email/contact-lists/my-list/contacts/nobody%40example.com",
            "{}",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_contact_list_cascades_contacts() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        // Create list and contact
        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "my-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists/my-list/contacts",
            r#"{"EmailAddress": "user@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        // Delete the contact list
        let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
        svc.handle(req).await.unwrap();

        // Verify contacts map is cleaned up
        let s = state.read();
        assert!(!s.contacts.contains_key("my-list"));
    }

    #[tokio::test]
    async fn test_tag_resource() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        // Create an identity
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        // Tag it
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            r#"{"ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com", "Tags": [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}]}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // List tags
        let mut qp = HashMap::new();
        qp.insert(
            "ResourceArn".to_string(),
            "arn:aws:ses:us-east-1:123456789012:identity/test@example.com".to_string(),
        );
        let req = make_request_with_query(
            Method::GET,
            "/v2/email/tags",
            "",
            "ResourceArn=arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com",
            qp,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
    }

    #[tokio::test]
    async fn test_untag_resource() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        // Create an identity
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let arn = "arn:aws:ses:us-east-1:123456789012:identity/test@example.com";

        // Tag it
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            &format!(
                r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "env", "Value": "prod"}}, {{"Key": "team", "Value": "backend"}}]}}"#
            ),
        );
        svc.handle(req).await.unwrap();

        // Untag - remove "env"
        let mut qp = HashMap::new();
        qp.insert("ResourceArn".to_string(), arn.to_string());
        qp.insert("TagKeys".to_string(), "env".to_string());
        let raw_query = format!("ResourceArn={}&TagKeys=env", urlencoded(arn));
        let req = make_request_with_query(Method::DELETE, "/v2/email/tags", "", &raw_query, qp);
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify only "team" remains
        let s = state.read();
        let tags = s.tags.get(arn).unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags.get("team").unwrap(), "backend");
    }

    #[tokio::test]
    async fn test_tag_nonexistent_resource() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            r#"{"ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/nope", "Tags": [{"Key": "k", "Value": "v"}]}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_identity_removes_tags() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let arn = "arn:aws:ses:us-east-1:123456789012:identity/test@example.com";
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
        );
        svc.handle(req).await.unwrap();

        // Delete identity
        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com",
            "",
        );
        svc.handle(req).await.unwrap();

        // Tags should be gone
        let s = state.read();
        assert!(!s.tags.contains_key(arn));
    }

    #[tokio::test]
    async fn test_delete_config_set_removes_tags() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let arn = "arn:aws:ses:us-east-1:123456789012:configuration-set/my-config";
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
        );
        svc.handle(req).await.unwrap();

        // Delete config set
        let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
        svc.handle(req).await.unwrap();

        let s = state.read();
        assert!(!s.tags.contains_key(arn));
    }

    #[tokio::test]
    async fn test_delete_contact_list_removes_tags() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName": "my-list"}"#,
        );
        svc.handle(req).await.unwrap();

        let arn = "arn:aws:ses:us-east-1:123456789012:contact-list/my-list";
        let req = make_request(
            Method::POST,
            "/v2/email/tags",
            &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
        );
        svc.handle(req).await.unwrap();

        // Delete contact list
        let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
        svc.handle(req).await.unwrap();

        let s = state.read();
        assert!(!s.tags.contains_key(arn));
    }

    fn urlencoded(s: &str) -> String {
        form_urlencoded::byte_serialize(s.as_bytes()).collect()
    }

    // --- Suppression List tests ---

    #[tokio::test]
    async fn test_suppressed_destination_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Put suppressed destination
        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "bounce@example.com", "Reason": "BOUNCE"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get suppressed destination
        let req = make_request(
            Method::GET,
            "/v2/email/suppression/addresses/bounce%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["SuppressedDestination"]["EmailAddress"],
            "bounce@example.com"
        );
        assert_eq!(body["SuppressedDestination"]["Reason"], "BOUNCE");
        assert!(body["SuppressedDestination"]["LastUpdateTime"]
            .as_f64()
            .is_some());

        // List suppressed destinations
        let req = make_request(Method::GET, "/v2/email/suppression/addresses", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["SuppressedDestinationSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // Delete suppressed destination
        let req = make_request(
            Method::DELETE,
            "/v2/email/suppression/addresses/bounce%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/suppression/addresses/bounce%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_suppressed_destination_complaint() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "complaint@example.com", "Reason": "COMPLAINT"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(
            Method::GET,
            "/v2/email/suppression/addresses/complaint%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SuppressedDestination"]["Reason"], "COMPLAINT");
    }

    #[tokio::test]
    async fn test_suppressed_destination_invalid_reason() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "bad@example.com", "Reason": "INVALID"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_suppressed_destination_upsert() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Put with BOUNCE
        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "user@example.com", "Reason": "BOUNCE"}"#,
        );
        svc.handle(req).await.unwrap();

        // Put again with COMPLAINT (upsert)
        let req = make_request(
            Method::PUT,
            "/v2/email/suppression/addresses",
            r#"{"EmailAddress": "user@example.com", "Reason": "COMPLAINT"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::GET,
            "/v2/email/suppression/addresses/user%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SuppressedDestination"]["Reason"], "COMPLAINT");
    }

    #[tokio::test]
    async fn test_delete_nonexistent_suppressed_destination() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::DELETE,
            "/v2/email/suppression/addresses/nobody%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    // --- Event Destination tests ---

    #[tokio::test]
    async fn test_event_destination_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create config set first
        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        // Create event destination
        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/my-config/event-destinations",
            r#"{
                "EventDestinationName": "my-dest",
                "EventDestination": {
                    "Enabled": true,
                    "MatchingEventTypes": ["SEND", "BOUNCE"],
                    "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:my-topic"}
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get event destinations
        let req = make_request(
            Method::GET,
            "/v2/email/configuration-sets/my-config/event-destinations",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let dests = body["EventDestinations"].as_array().unwrap();
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0]["Name"], "my-dest");
        assert_eq!(dests[0]["Enabled"], true);
        assert_eq!(dests[0]["MatchingEventTypes"], json!(["SEND", "BOUNCE"]));
        assert_eq!(
            dests[0]["SnsDestination"]["TopicArn"],
            "arn:aws:sns:us-east-1:123456789012:my-topic"
        );

        // Update event destination
        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/my-config/event-destinations/my-dest",
            r#"{
                "EventDestination": {
                    "Enabled": false,
                    "MatchingEventTypes": ["DELIVERY"],
                    "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:updated-topic"}
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(
            Method::GET,
            "/v2/email/configuration-sets/my-config/event-destinations",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let dests = body["EventDestinations"].as_array().unwrap();
        assert_eq!(dests[0]["Enabled"], false);
        assert_eq!(dests[0]["MatchingEventTypes"], json!(["DELIVERY"]));

        // Delete event destination
        let req = make_request(
            Method::DELETE,
            "/v2/email/configuration-sets/my-config/event-destinations/my-dest",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/configuration-sets/my-config/event-destinations",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["EventDestinations"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_event_destination_config_set_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/nonexistent/event-destinations",
            r#"{
                "EventDestinationName": "dest",
                "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_event_destination_duplicate() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let body = r#"{
            "EventDestinationName": "dup-dest",
            "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
        }"#;

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/my-config/event-destinations",
            body,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/my-config/event-destinations",
            body,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_update_nonexistent_event_destination() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/my-config/event-destinations/nonexistent",
            r#"{"EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_event_destination() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::DELETE,
            "/v2/email/configuration-sets/my-config/event-destinations/nonexistent",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_event_destinations_cleaned_on_config_set_delete() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets/my-config/event-destinations",
            r#"{
                "EventDestinationName": "dest1",
                "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
            }"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
        svc.handle(req).await.unwrap();

        let s = state.read();
        assert!(!s.event_destinations.contains_key("my-config"));
    }

    // --- Email Identity Policy tests ---

    #[tokio::test]
    async fn test_identity_policy_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create identity first
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        // Create policy
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"ses:SendEmail","Resource":"*"}]}"#;
        let req = make_request(
            Method::POST,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            &format!(
                r#"{{"Policy": {}}}"#,
                serde_json::to_string(policy_doc).unwrap()
            ),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get policies
        let req = make_request(
            Method::GET,
            "/v2/email/identities/test%40example.com/policies",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Policies"]["my-policy"].is_string());
        assert_eq!(body["Policies"]["my-policy"].as_str().unwrap(), policy_doc);

        // Update policy
        let updated_doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let req = make_request(
            Method::PUT,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            &format!(
                r#"{{"Policy": {}}}"#,
                serde_json::to_string(updated_doc).unwrap()
            ),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(
            Method::GET,
            "/v2/email/identities/test%40example.com/policies",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Policies"]["my-policy"].as_str().unwrap(), updated_doc);

        // Delete policy
        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/identities/test%40example.com/policies",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Policies"].as_object().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_identity_policy_identity_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities/nonexistent%40example.com/policies/my-policy",
            r#"{"Policy": "{}"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_identity_policy_duplicate() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            r#"{"Policy": "{}"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            r#"{"Policy": "{}"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_update_nonexistent_policy() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/identities/test%40example.com/policies/nonexistent",
            r#"{"Policy": "{}"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_nonexistent_policy() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com/policies/nonexistent",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_policies_cleaned_on_identity_delete() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/identities/test%40example.com/policies/my-policy",
            r#"{"Policy": "{}"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::DELETE,
            "/v2/email/identities/test%40example.com",
            "",
        );
        svc.handle(req).await.unwrap();

        let s = state.read();
        assert!(!s.identity_policies.contains_key("test@example.com"));
    }

    // --- Identity Attribute tests ---

    #[tokio::test]
    async fn test_put_email_identity_dkim_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        // Create identity first
        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        // Disable DKIM signing
        let req = make_request(
            Method::PUT,
            "/v2/email/identities/example.com/dkim",
            r#"{"SigningEnabled": false}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify via GetEmailIdentity
        let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DkimAttributes"]["SigningEnabled"], false);
    }

    #[tokio::test]
    async fn test_put_email_identity_dkim_attributes_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/identities/nonexistent.com/dkim",
            r#"{"SigningEnabled": false}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_put_email_identity_dkim_signing_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/identities/example.com/dkim/signing",
            r#"{"SigningAttributesOrigin": "EXTERNAL", "SigningAttributes": {"DomainSigningPrivateKey": "key123", "DomainSigningSelector": "sel1"}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DkimStatus"], "SUCCESS");
        assert!(!body["DkimTokens"].as_array().unwrap().is_empty());

        // Verify stored
        let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["DkimAttributes"]["SigningAttributesOrigin"],
            "EXTERNAL"
        );
    }

    #[tokio::test]
    async fn test_put_email_identity_feedback_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "test@example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/identities/test%40example.com/feedback",
            r#"{"EmailForwardingEnabled": false}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/identities/test%40example.com", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["FeedbackForwardingStatus"], false);
    }

    #[tokio::test]
    async fn test_put_email_identity_mail_from_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/identities/example.com/mail-from",
            r#"{"MailFromDomain": "mail.example.com", "BehaviorOnMxFailure": "REJECT_MESSAGE"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["MailFromAttributes"]["MailFromDomain"],
            "mail.example.com"
        );
        assert_eq!(
            body["MailFromAttributes"]["BehaviorOnMxFailure"],
            "REJECT_MESSAGE"
        );
    }

    #[tokio::test]
    async fn test_put_email_identity_configuration_set_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        let req = make_request(
            Method::POST,
            "/v2/email/identities",
            r#"{"EmailIdentity": "example.com"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/identities/example.com/configuration-set",
            r#"{"ConfigurationSetName": "my-config"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ConfigurationSetName"], "my-config");
    }

    // --- Configuration Set Options tests ---

    #[tokio::test]
    async fn test_put_configuration_set_sending_options() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create config set
        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "test-config"}"#,
        );
        svc.handle(req).await.unwrap();

        // Disable sending
        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/test-config/sending",
            r#"{"SendingEnabled": false}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify
        let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SendingOptions"]["SendingEnabled"], false);
    }

    #[tokio::test]
    async fn test_put_configuration_set_sending_options_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/nonexistent/sending",
            r#"{"SendingEnabled": false}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_put_configuration_set_delivery_options() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "test-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/test-config/delivery-options",
            r#"{"TlsPolicy": "REQUIRE", "SendingPoolName": "my-pool"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DeliveryOptions"]["TlsPolicy"], "REQUIRE");
        assert_eq!(body["DeliveryOptions"]["SendingPoolName"], "my-pool");
    }

    #[tokio::test]
    async fn test_put_configuration_set_tracking_options() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "test-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/test-config/tracking-options",
            r#"{"CustomRedirectDomain": "track.example.com", "HttpsPolicy": "REQUIRE"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["TrackingOptions"]["CustomRedirectDomain"],
            "track.example.com"
        );
        assert_eq!(body["TrackingOptions"]["HttpsPolicy"], "REQUIRE");
    }

    #[tokio::test]
    async fn test_put_configuration_set_suppression_options() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "test-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/test-config/suppression-options",
            r#"{"SuppressedReasons": ["BOUNCE", "COMPLAINT"]}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let reasons = body["SuppressionOptions"]["SuppressedReasons"]
            .as_array()
            .unwrap();
        assert_eq!(reasons.len(), 2);
    }

    #[tokio::test]
    async fn test_put_configuration_set_reputation_options() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "test-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/test-config/reputation-options",
            r#"{"ReputationMetricsEnabled": true}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ReputationOptions"]["ReputationMetricsEnabled"], true);
    }

    #[tokio::test]
    async fn test_put_configuration_set_vdm_options() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "test-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/test-config/vdm-options",
            r#"{"DashboardOptions": {"EngagementMetrics": "ENABLED"}, "GuardianOptions": {"OptimizedSharedDelivery": "ENABLED"}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["VdmOptions"]["DashboardOptions"]["EngagementMetrics"],
            "ENABLED"
        );
    }

    #[tokio::test]
    async fn test_put_configuration_set_archiving_options() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/configuration-sets",
            r#"{"ConfigurationSetName": "test-config"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::PUT,
            "/v2/email/configuration-sets/test-config/archiving-options",
            r#"{"ArchiveArn": "arn:aws:ses:us-east-1:123456789012:mailmanager-archive/my-archive"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["ArchivingOptions"]["ArchiveArn"]
            .as_str()
            .unwrap()
            .contains("my-archive"));
    }

    // --- Custom Verification Email Template tests ---

    #[tokio::test]
    async fn test_custom_verification_email_template_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create
        let req = make_request(
            Method::POST,
            "/v2/email/custom-verification-email-templates",
            r#"{
                "TemplateName": "my-verification",
                "FromEmailAddress": "noreply@example.com",
                "TemplateSubject": "Verify your email",
                "TemplateContent": "<h1>Please verify</h1>",
                "SuccessRedirectionURL": "https://example.com/success",
                "FailureRedirectionURL": "https://example.com/failure"
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Get
        let req = make_request(
            Method::GET,
            "/v2/email/custom-verification-email-templates/my-verification",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TemplateName"], "my-verification");
        assert_eq!(body["FromEmailAddress"], "noreply@example.com");
        assert_eq!(body["TemplateSubject"], "Verify your email");
        assert_eq!(body["TemplateContent"], "<h1>Please verify</h1>");
        assert_eq!(body["SuccessRedirectionURL"], "https://example.com/success");
        assert_eq!(body["FailureRedirectionURL"], "https://example.com/failure");

        // List
        let req = make_request(
            Method::GET,
            "/v2/email/custom-verification-email-templates",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["CustomVerificationEmailTemplates"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // Update
        let req = make_request(
            Method::PUT,
            "/v2/email/custom-verification-email-templates/my-verification",
            r#"{"TemplateSubject": "Updated subject"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify update
        let req = make_request(
            Method::GET,
            "/v2/email/custom-verification-email-templates/my-verification",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TemplateSubject"], "Updated subject");

        // Delete
        let req = make_request(
            Method::DELETE,
            "/v2/email/custom-verification-email-templates/my-verification",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::GET,
            "/v2/email/custom-verification-email-templates/my-verification",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_duplicate_custom_verification_template() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let body = r#"{
            "TemplateName": "dup-tmpl",
            "FromEmailAddress": "a@b.com",
            "TemplateSubject": "s",
            "TemplateContent": "c",
            "SuccessRedirectionURL": "https://ok",
            "FailureRedirectionURL": "https://fail"
        }"#;

        let req = make_request(
            Method::POST,
            "/v2/email/custom-verification-email-templates",
            body,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/custom-verification-email-templates",
            body,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_send_custom_verification_email() {
        let state = make_state();
        let svc = SesV2Service::new(state.clone());

        // Create template first
        let req = make_request(
            Method::POST,
            "/v2/email/custom-verification-email-templates",
            r#"{
                "TemplateName": "verify",
                "FromEmailAddress": "a@b.com",
                "TemplateSubject": "Verify",
                "TemplateContent": "content",
                "SuccessRedirectionURL": "https://ok",
                "FailureRedirectionURL": "https://fail"
            }"#,
        );
        svc.handle(req).await.unwrap();

        // Send
        let req = make_request(
            Method::POST,
            "/v2/email/outbound-custom-verification-emails",
            r#"{"EmailAddress": "user@example.com", "TemplateName": "verify"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["MessageId"].as_str().is_some());

        // Verify stored in sent_emails
        let s = state.read();
        assert_eq!(s.sent_emails.len(), 1);
        assert_eq!(s.sent_emails[0].to, vec!["user@example.com"]);
    }

    #[tokio::test]
    async fn test_send_custom_verification_email_template_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/outbound-custom-verification-emails",
            r#"{"EmailAddress": "user@example.com", "TemplateName": "nonexistent"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    // --- TestRenderEmailTemplate tests ---

    #[tokio::test]
    async fn test_render_email_template() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create template
        let req = make_request(
            Method::POST,
            "/v2/email/templates",
            r#"{
                "TemplateName": "greet",
                "TemplateContent": {
                    "Subject": "Hello {{name}}",
                    "Html": "<h1>Welcome, {{name}}!</h1><p>Your code is {{code}}.</p>",
                    "Text": "Welcome, {{name}}! Your code is {{code}}."
                }
            }"#,
        );
        svc.handle(req).await.unwrap();

        // Render
        let req = make_request(
            Method::POST,
            "/v2/email/templates/greet/render",
            r#"{"TemplateData": "{\"name\": \"Alice\", \"code\": \"1234\"}"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let rendered = body["RenderedTemplate"].as_str().unwrap();
        assert!(rendered.contains("Subject: Hello Alice"));
        assert!(rendered.contains("Welcome, Alice!"));
        assert!(rendered.contains("Your code is 1234."));
    }

    #[tokio::test]
    async fn test_render_email_template_not_found() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/templates/nonexistent/render",
            r#"{"TemplateData": "{}"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_render_email_template_missing_data() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create template
        let req = make_request(
            Method::POST,
            "/v2/email/templates",
            r#"{"TemplateName": "t1", "TemplateContent": {"Subject": "Hi"}}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(Method::POST, "/v2/email/templates/t1/render", r#"{}"#);
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    // ── Dedicated IP Pool tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_dedicated_ip_pool_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create pool
        let req = make_request(
            Method::POST,
            "/v2/email/dedicated-ip-pools",
            r#"{"PoolName": "my-pool", "ScalingMode": "STANDARD"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // List pools
        let req = make_request(Method::GET, "/v2/email/dedicated-ip-pools", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DedicatedIpPools"].as_array().unwrap().len(), 1);

        // Duplicate
        let req = make_request(
            Method::POST,
            "/v2/email/dedicated-ip-pools",
            r#"{"PoolName": "my-pool"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);

        // Delete pool
        let req = make_request(Method::DELETE, "/v2/email/dedicated-ip-pools/my-pool", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Delete non-existent
        let req = make_request(Method::DELETE, "/v2/email/dedicated-ip-pools/my-pool", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_managed_pool_generates_ips() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create managed pool
        let req = make_request(
            Method::POST,
            "/v2/email/dedicated-ip-pools",
            r#"{"PoolName": "managed-pool", "ScalingMode": "MANAGED"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // List dedicated IPs filtered by pool
        let req = make_request_with_query(
            Method::GET,
            "/v2/email/dedicated-ips",
            "",
            "PoolName=managed-pool",
            {
                let mut m = HashMap::new();
                m.insert("PoolName".to_string(), "managed-pool".to_string());
                m
            },
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ips = body["DedicatedIps"].as_array().unwrap();
        assert_eq!(ips.len(), 3);
        assert_eq!(ips[0]["WarmupStatus"], "NOT_APPLICABLE");
        assert_eq!(ips[0]["WarmupPercentage"], -1);
    }

    #[tokio::test]
    async fn test_dedicated_ip_operations() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create two pools
        let req = make_request(
            Method::POST,
            "/v2/email/dedicated-ip-pools",
            r#"{"PoolName": "pool-a", "ScalingMode": "MANAGED"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/dedicated-ip-pools",
            r#"{"PoolName": "pool-b", "ScalingMode": "STANDARD"}"#,
        );
        svc.handle(req).await.unwrap();

        // Get a specific IP
        let req = make_request(Method::GET, "/v2/email/dedicated-ips/198.51.100.1", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DedicatedIp"]["PoolName"], "pool-a");

        // Move IP to pool-b
        let req = make_request(
            Method::PUT,
            "/v2/email/dedicated-ips/198.51.100.1/pool",
            r#"{"DestinationPoolName": "pool-b"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify it moved
        let req = make_request(Method::GET, "/v2/email/dedicated-ips/198.51.100.1", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DedicatedIp"]["PoolName"], "pool-b");

        // Set warmup
        let req = make_request(
            Method::PUT,
            "/v2/email/dedicated-ips/198.51.100.1/warmup",
            r#"{"WarmupPercentage": 50}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/dedicated-ips/198.51.100.1", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DedicatedIp"]["WarmupPercentage"], 50);
        assert_eq!(body["DedicatedIp"]["WarmupStatus"], "IN_PROGRESS");

        // Non-existent IP
        let req = make_request(Method::GET, "/v2/email/dedicated-ips/1.2.3.4", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_pool_scaling_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/dedicated-ip-pools",
            r#"{"PoolName": "scalable", "ScalingMode": "STANDARD"}"#,
        );
        svc.handle(req).await.unwrap();

        // Change to MANAGED
        let req = make_request(
            Method::PUT,
            "/v2/email/dedicated-ip-pools/scalable/scaling",
            r#"{"ScalingMode": "MANAGED"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Cannot change from MANAGED to STANDARD
        let req = make_request(
            Method::PUT,
            "/v2/email/dedicated-ip-pools/scalable/scaling",
            r#"{"ScalingMode": "STANDARD"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_account_dedicated_ip_warmup() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/account/dedicated-ips/warmup",
            r#"{"AutoWarmupEnabled": true}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/account", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DedicatedIpAutoWarmupEnabled"], true);
    }

    // ── Multi-region Endpoint tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_multi_region_endpoint_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create
        let req = make_request(
            Method::POST,
            "/v2/email/multi-region-endpoints",
            r#"{"EndpointName": "global-ep", "Details": {"RoutesDetails": [{"Region": "us-west-2"}]}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Status"], "READY");
        assert!(body["EndpointId"].as_str().is_some());

        // Get
        let req = make_request(
            Method::GET,
            "/v2/email/multi-region-endpoints/global-ep",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EndpointName"], "global-ep");
        assert_eq!(body["Status"], "READY");
        let routes = body["Routes"].as_array().unwrap();
        assert!(!routes.is_empty());

        // List
        let req = make_request(Method::GET, "/v2/email/multi-region-endpoints", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["MultiRegionEndpoints"].as_array().unwrap().len(), 1);

        // Duplicate
        let req = make_request(
            Method::POST,
            "/v2/email/multi-region-endpoints",
            r#"{"EndpointName": "global-ep", "Details": {"RoutesDetails": [{"Region": "eu-west-1"}]}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);

        // Delete
        let req = make_request(
            Method::DELETE,
            "/v2/email/multi-region-endpoints/global-ep",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Status"], "DELETING");

        // Get after delete
        let req = make_request(
            Method::GET,
            "/v2/email/multi-region-endpoints/global-ep",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    // ── Account Settings tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_account_details() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/account/details",
            r#"{"MailType": "TRANSACTIONAL", "WebsiteURL": "https://example.com", "UseCaseDescription": "Testing"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/account", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Details"]["MailType"], "TRANSACTIONAL");
        assert_eq!(body["Details"]["WebsiteURL"], "https://example.com");
        assert_eq!(body["Details"]["UseCaseDescription"], "Testing");
    }

    #[tokio::test]
    async fn test_account_sending_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Disable sending
        let req = make_request(
            Method::PUT,
            "/v2/email/account/sending",
            r#"{"SendingEnabled": false}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/account", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SendingEnabled"], false);

        // Re-enable
        let req = make_request(
            Method::PUT,
            "/v2/email/account/sending",
            r#"{"SendingEnabled": true}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(Method::GET, "/v2/email/account", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SendingEnabled"], true);
    }

    #[tokio::test]
    async fn test_account_suppression_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/account/suppression",
            r#"{"SuppressedReasons": ["BOUNCE", "COMPLAINT"]}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/account", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let reasons = body["SuppressionAttributes"]["SuppressedReasons"]
            .as_array()
            .unwrap();
        assert_eq!(reasons.len(), 2);
    }

    #[tokio::test]
    async fn test_account_vdm_attributes() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::PUT,
            "/v2/email/account/vdm",
            r#"{"VdmAttributes": {"VdmEnabled": "ENABLED", "DashboardAttributes": {"EngagementMetrics": "ENABLED"}}}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let req = make_request(Method::GET, "/v2/email/account", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["VdmAttributes"]["VdmEnabled"], "ENABLED");
    }

    #[tokio::test]
    async fn test_import_job_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create import job
        let req = make_request(
            Method::POST,
            "/v2/email/import-jobs",
            r#"{
                "ImportDestination": {
                    "SuppressionListDestination": {"SuppressionListImportAction": "PUT"}
                },
                "ImportDataSource": {
                    "S3Url": "s3://bucket/file.csv",
                    "DataFormat": "CSV"
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let job_id = body["JobId"].as_str().unwrap().to_string();

        // Get import job
        let req = make_request(
            Method::GET,
            &format!("/v2/email/import-jobs/{}", job_id),
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["JobId"], job_id);
        assert_eq!(body["JobStatus"], "COMPLETED");

        // List import jobs
        let req = make_request(Method::POST, "/v2/email/import-jobs/list", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ImportJobs"].as_array().unwrap().len(), 1);

        // Get non-existent job
        let req = make_request(Method::GET, "/v2/email/import-jobs/nonexistent", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_export_job_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create export job
        let req = make_request(
            Method::POST,
            "/v2/email/export-jobs",
            r#"{
                "ExportDataSource": {
                    "MetricsDataSource": {
                        "Dimensions": {},
                        "Namespace": "VDM",
                        "Metrics": []
                    }
                },
                "ExportDestination": {
                    "DataFormat": "CSV",
                    "S3Url": "s3://bucket/export"
                }
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let job_id = body["JobId"].as_str().unwrap().to_string();

        // Get export job
        let req = make_request(
            Method::GET,
            &format!("/v2/email/export-jobs/{}", job_id),
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["JobId"], job_id);
        assert_eq!(body["JobStatus"], "COMPLETED");
        assert_eq!(body["ExportSourceType"], "METRICS_DATA");

        // List export jobs
        let req = make_request(Method::POST, "/v2/email/list-export-jobs", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ExportJobs"].as_array().unwrap().len(), 1);

        // Cancel — should fail since already COMPLETED
        let req = make_request(
            Method::PUT,
            &format!("/v2/email/export-jobs/{}/cancel", job_id),
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_tenant_lifecycle() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Create tenant
        let req = make_request(
            Method::POST,
            "/v2/email/tenants",
            r#"{"TenantName": "my-tenant"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TenantName"], "my-tenant");
        assert!(body["TenantId"].as_str().is_some());
        assert_eq!(body["SendingStatus"], "ENABLED");

        // Get tenant
        let req = make_request(
            Method::POST,
            "/v2/email/tenants/get",
            r#"{"TenantName": "my-tenant"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Tenant"]["TenantName"], "my-tenant");

        // List tenants
        let req = make_request(Method::POST, "/v2/email/tenants/list", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Tenants"].as_array().unwrap().len(), 1);

        // Create resource association
        let req = make_request(
            Method::POST,
            "/v2/email/tenants/resources",
            r#"{"TenantName": "my-tenant", "ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // List tenant resources
        let req = make_request(
            Method::POST,
            "/v2/email/tenants/resources/list",
            r#"{"TenantName": "my-tenant"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TenantResources"].as_array().unwrap().len(), 1);

        // List resource tenants
        let req = make_request(
            Method::POST,
            "/v2/email/resources/tenants/list",
            r#"{"ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ResourceTenants"].as_array().unwrap().len(), 1);

        // Delete resource association
        let req = make_request(
            Method::POST,
            "/v2/email/tenants/resources/delete",
            r#"{"TenantName": "my-tenant", "ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify association is gone
        let req = make_request(
            Method::POST,
            "/v2/email/tenants/resources/list",
            r#"{"TenantName": "my-tenant"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["TenantResources"].as_array().unwrap().is_empty());

        // Delete tenant
        let req = make_request(
            Method::POST,
            "/v2/email/tenants/delete",
            r#"{"TenantName": "my-tenant"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify deleted
        let req = make_request(
            Method::POST,
            "/v2/email/tenants/get",
            r#"{"TenantName": "my-tenant"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_reputation_entity() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        // Get default reputation entity (auto-created)
        let req = make_request(
            Method::GET,
            "/v2/email/reputation/entities/RESOURCE/arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ReputationEntity"]["SendingStatusAggregate"],
            "ENABLED"
        );

        // Update customer managed status
        let req = make_request(
            Method::PUT,
            "/v2/email/reputation/entities/RESOURCE/arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com/customer-managed-status",
            r#"{"SendingStatus": "DISABLED"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Update policy
        let req = make_request(
            Method::PUT,
            "/v2/email/reputation/entities/RESOURCE/arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com/policy",
            r#"{"ReputationEntityPolicy": "arn:aws:ses:us-east-1:123456789012:policy/my-policy"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify via get
        let req = make_request(
            Method::GET,
            "/v2/email/reputation/entities/RESOURCE/arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ReputationEntity"]["CustomerManagedStatus"]["SendingStatus"],
            "DISABLED"
        );

        // List reputation entities
        let req = make_request(Method::POST, "/v2/email/reputation/entities", "{}");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ReputationEntities"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_batch_get_metric_data() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/metrics/batch",
            r#"{
                "Queries": [
                    {
                        "Id": "q1",
                        "Namespace": "VDM",
                        "Metric": "SEND",
                        "StartDate": "2024-01-01T00:00:00Z",
                        "EndDate": "2024-01-02T00:00:00Z"
                    }
                ]
            }"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Results"].as_array().unwrap().len(), 1);
        assert_eq!(body["Results"][0]["Id"], "q1");
        assert!(body["Errors"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_duplicate_tenant() {
        let state = make_state();
        let svc = SesV2Service::new(state);

        let req = make_request(
            Method::POST,
            "/v2/email/tenants",
            r#"{"TenantName": "dup"}"#,
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/v2/email/tenants",
            r#"{"TenantName": "dup"}"#,
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CONFLICT);
    }
}
