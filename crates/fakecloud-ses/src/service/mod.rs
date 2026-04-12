mod account;
mod configuration_sets;
mod contact_lists;
mod identities;
mod misc;
mod sending;
mod suppression;
mod templates;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::fanout::SesDeliveryContext;
use crate::state::{EventDestination, SharedSesState, Topic, TopicPreference};

pub struct SesV2Service {
    state: SharedSesState,
    delivery_ctx: Option<SesDeliveryContext>,
}

impl SesV2Service {
    pub fn new(state: SharedSesState) -> Self {
        Self {
            state,
            delivery_ctx: None,
        }
    }

    /// Attach a delivery context for cross-service event fanout.
    pub fn with_delivery(mut self, ctx: SesDeliveryContext) -> Self {
        self.delivery_ctx = Some(ctx);
        self
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
        // Route v1 Query protocol requests to the v1 module.
        if req.is_query_protocol {
            return crate::v1::handle_v1_action(&self.state, &req);
        }

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
            // NOTE: SES v1 receipt rule/filter actions are implemented (see v1.rs)
            // but excluded from the conformance audit because there is no SES v1
            // Smithy model (only sesv2.json exists) to generate checksums from.
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
