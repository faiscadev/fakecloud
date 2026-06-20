//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `ses`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn get_att_ses_configuration_set(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cs = state.configuration_sets.get(physical_id)?;
        match attribute {
            "Name" => Some(cs.name.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_ses_email_identity(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let id = state.identities.get(physical_id)?;
        match attribute {
            "IdentityName" => Some(id.identity_name.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_ses_template(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let tpl = state.templates.get(physical_id)?;
        match attribute {
            "TemplateName" => Some(tpl.template_name.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_ses_contact_list(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cl = state.contact_lists.get(physical_id)?;
        match attribute {
            "ContactListName" => Some(cl.contact_list_name.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_ses_dedicated_ip_pool(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let pool = state.dedicated_ip_pools.get(physical_id)?;
        match attribute {
            "PoolName" => Some(pool.pool_name.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_ses_receipt_rule_set(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rs = state.receipt_rule_sets.get(physical_id)?;
        match attribute {
            "RuleSetName" => Some(rs.name.clone()),
            _ => None,
        }
    }

    // --- SES ---

    pub(super) fn create_ses_configuration_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-cs-{}", resource.logical_id));
        let sending_enabled = props
            .get("SendingOptions")
            .and_then(|v| v.get("SendingEnabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let tls_policy = props
            .get("DeliveryOptions")
            .and_then(|v| v.get("TlsPolicy"))
            .and_then(|v| v.as_str())
            .unwrap_or("OPTIONAL")
            .to_string();
        let sending_pool_name = props
            .get("DeliveryOptions")
            .and_then(|v| v.get("SendingPoolName"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let custom_redirect_domain = props
            .get("TrackingOptions")
            .and_then(|v| v.get("CustomRedirectDomain"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let suppressed_reasons: Vec<String> = props
            .get("SuppressionOptions")
            .and_then(|v| v.get("SuppressedReasons"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let reputation_metrics_enabled = props
            .get("ReputationOptions")
            .and_then(|v| v.get("ReputationMetricsEnabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let cs = SesConfigurationSet {
            name: name.clone(),
            sending_enabled,
            tls_policy,
            sending_pool_name,
            custom_redirect_domain,
            https_policy: props
                .get("TrackingOptions")
                .and_then(|v| v.get("HttpsPolicy"))
                .and_then(|v| v.as_str())
                .map(String::from),
            suppressed_reasons,
            reputation_metrics_enabled,
            vdm_options: props.get("VdmOptions").cloned(),
            archive_arn: props
                .get("ArchivingOptions")
                .and_then(|v| v.get("ArchiveArn"))
                .and_then(|v| v.as_str())
                .map(String::from),
            archiving_options_present: props.get("ArchivingOptions").is_some_and(|v| v.is_object()),
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.configuration_sets.insert(name.clone(), cs);
        Ok(ProvisionResult::new(name.clone()).with("Name", name))
    }

    pub(super) fn delete_ses_configuration_set(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.configuration_sets.remove(physical_id);
        state.event_destinations.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_ses_event_destination(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cs_name = props
            .get("ConfigurationSetName")
            .and_then(|v| v.as_str())
            .ok_or("ConfigurationSetName is required")?
            .to_string();
        let dest_props = props
            .get("EventDestination")
            .and_then(|v| v.as_object())
            .ok_or("EventDestination is required")?;
        let name = dest_props
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-ed-{}", resource.logical_id));
        let enabled = dest_props
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let matching_event_types: Vec<String> = dest_props
            .get("MatchingEventTypes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let dest = SesEventDestination {
            name: name.clone(),
            enabled,
            matching_event_types,
            kinesis_firehose_destination: dest_props.get("KinesisFirehoseDestination").cloned(),
            cloud_watch_destination: dest_props.get("CloudWatchDestination").cloned(),
            sns_destination: dest_props.get("SnsDestination").cloned(),
            event_bridge_destination: dest_props.get("EventBridgeDestination").cloned(),
            pinpoint_destination: dest_props.get("PinpointDestination").cloned(),
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.configuration_sets.contains_key(&cs_name) {
            return Err(format!("ConfigurationSet {cs_name} not yet provisioned"));
        }
        let dests = state.event_destinations.entry(cs_name.clone()).or_default();
        dests.retain(|d| d.name != name);
        dests.push(dest);
        let physical = format!("{cs_name}|{name}");
        Ok(ProvisionResult::new(physical)
            .with("Name", name)
            .with("ConfigurationSetName", cs_name))
    }

    pub(super) fn delete_ses_event_destination(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut parts = physical_id.splitn(2, '|');
        let Some(cs) = parts.next() else {
            return Ok(());
        };
        let Some(name) = parts.next() else {
            return Ok(());
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(dests) = state.event_destinations.get_mut(cs) {
            dests.retain(|d| d.name != name);
        }
        Ok(())
    }

    pub(super) fn create_ses_email_identity(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identity_name = props
            .get("EmailIdentity")
            .and_then(|v| v.as_str())
            .ok_or("EmailIdentity is required")?
            .to_string();
        let identity_type = if identity_name.contains('@') {
            "EMAIL_ADDRESS"
        } else {
            "DOMAIN"
        }
        .to_string();
        let dkim_signing_enabled = props
            .get("DkimAttributes")
            .and_then(|v| v.get("SigningEnabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let dkim_signing_attributes_origin = props
            .get("DkimSigningAttributes")
            .map(|_| "EXTERNAL")
            .unwrap_or("AWS_SES")
            .to_string();
        let mail_from_domain = props
            .get("MailFromAttributes")
            .and_then(|v| v.get("MailFromDomain"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let mail_from_behavior = props
            .get("MailFromAttributes")
            .and_then(|v| v.get("BehaviorOnMxFailure"))
            .and_then(|v| v.as_str())
            .unwrap_or("USE_DEFAULT_VALUE")
            .to_string();
        let configuration_set_name = props
            .get("ConfigurationSetAttributes")
            .and_then(|v| v.get("ConfigurationSetName"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let email_forwarding_enabled = props
            .get("FeedbackAttributes")
            .and_then(|v| v.get("EmailForwardingEnabled"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let identity = SesEmailIdentity {
            identity_name: identity_name.clone(),
            identity_type,
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled,
            dkim_signing_attributes_origin,
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            dkim_public_key_b64: None,
            email_forwarding_enabled,
            mail_from_domain,
            mail_from_behavior_on_mx_failure: mail_from_behavior,
            mail_from_domain_status: "Success".to_string(),
            configuration_set_name,
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identities.insert(identity_name.clone(), identity);
        Ok(ProvisionResult::new(identity_name.clone()).with("EmailIdentity", identity_name))
    }

    pub(super) fn delete_ses_email_identity(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.identities.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_ses_template(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let template_block = props
            .get("Template")
            .and_then(|v| v.as_object())
            .ok_or("Template is required")?;
        let template_name = template_block
            .get("TemplateName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-tpl-{}", resource.logical_id));
        let tpl = SesEmailTemplate {
            template_name: template_name.clone(),
            subject: template_block
                .get("SubjectPart")
                .and_then(|v| v.as_str())
                .map(String::from),
            html_body: template_block
                .get("HtmlPart")
                .and_then(|v| v.as_str())
                .map(String::from),
            text_body: template_block
                .get("TextPart")
                .and_then(|v| v.as_str())
                .map(String::from),
            created_at: Utc::now(),
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.templates.insert(template_name.clone(), tpl);
        Ok(ProvisionResult::new(template_name.clone()).with("TemplateName", template_name))
    }

    pub(super) fn delete_ses_template(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.templates.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_ses_contact_list(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("ContactListName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-cl-{}", resource.logical_id));
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let now = Utc::now();
        let cl = SesContactList {
            contact_list_name: name.clone(),
            description,
            topics: Vec::new(),
            created_at: now,
            last_updated_at: now,
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.contact_lists.insert(name.clone(), cl);
        Ok(ProvisionResult::new(name.clone()).with("ContactListName", name))
    }

    pub(super) fn delete_ses_contact_list(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.contact_lists.remove(physical_id);
        state.contacts.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_ses_dedicated_ip_pool(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("PoolName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-pool-{}", resource.logical_id));
        let scaling_mode = props
            .get("ScalingMode")
            .and_then(|v| v.as_str())
            .unwrap_or("STANDARD")
            .to_string();
        let pool = SesDedicatedIpPool {
            pool_name: name.clone(),
            scaling_mode,
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.dedicated_ip_pools.insert(name.clone(), pool);
        Ok(ProvisionResult::new(name.clone()).with("PoolName", name))
    }

    pub(super) fn delete_ses_dedicated_ip_pool(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.dedicated_ip_pools.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_ses_receipt_rule_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("RuleSetName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-rs-{}", resource.logical_id));
        let rs = SesReceiptRuleSet {
            name: name.clone(),
            rules: Vec::new(),
            created_at: Utc::now(),
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.receipt_rule_sets.insert(name.clone(), rs);
        Ok(ProvisionResult::new(name.clone()).with("RuleSetName", name))
    }

    pub(super) fn delete_ses_receipt_rule_set(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.receipt_rule_sets.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_ses_receipt_rule(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let rule_set_name = props
            .get("RuleSetName")
            .and_then(|v| v.as_str())
            .ok_or("RuleSetName is required")?
            .to_string();
        let rule_block = props
            .get("Rule")
            .and_then(|v| v.as_object())
            .ok_or("Rule is required")?;
        let name = rule_block
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-rule-{}", resource.logical_id));
        let enabled = rule_block
            .get("Enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let scan_enabled = rule_block
            .get("ScanEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let tls_policy = rule_block
            .get("TlsPolicy")
            .and_then(|v| v.as_str())
            .unwrap_or("Optional")
            .to_string();
        let recipients: Vec<String> = rule_block
            .get("Recipients")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let actions: Vec<SesReceiptAction> = rule_block
            .get("Actions")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(parse_ses_receipt_action).collect())
            .unwrap_or_default();

        let rule = SesReceiptRule {
            name: name.clone(),
            enabled,
            scan_enabled,
            tls_policy,
            recipients,
            actions,
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let rs = state
            .receipt_rule_sets
            .get_mut(&rule_set_name)
            .ok_or_else(|| format!("ReceiptRuleSet {rule_set_name} not yet provisioned"))?;
        rs.rules.retain(|r| r.name != name);
        rs.rules.push(rule);
        let physical = format!("{rule_set_name}|{name}");
        Ok(ProvisionResult::new(physical)
            .with("Name", name)
            .with("RuleSetName", rule_set_name))
    }

    pub(super) fn delete_ses_receipt_rule(
        &self,
        physical_id: &str,
        _attributes: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let mut parts = physical_id.splitn(2, '|');
        let Some(rs_name) = parts.next() else {
            return Ok(());
        };
        let Some(rule_name) = parts.next() else {
            return Ok(());
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(rs) = state.receipt_rule_sets.get_mut(rs_name) {
            rs.rules.retain(|r| r.name != rule_name);
        }
        Ok(())
    }

    pub(super) fn create_ses_receipt_filter(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let filter_block = props
            .get("Filter")
            .and_then(|v| v.as_object())
            .ok_or("Filter is required")?;
        let name = filter_block
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-filter-{}", resource.logical_id));
        let ip_block = filter_block
            .get("IpFilter")
            .and_then(|v| v.as_object())
            .ok_or("Filter.IpFilter is required")?;
        let cidr = ip_block
            .get("Cidr")
            .and_then(|v| v.as_str())
            .ok_or("Filter.IpFilter.Cidr is required")?
            .to_string();
        let policy = ip_block
            .get("Policy")
            .and_then(|v| v.as_str())
            .unwrap_or("Block")
            .to_string();
        let filter = SesReceiptFilter {
            name: name.clone(),
            ip_filter: SesIpFilter { cidr, policy },
        };
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.receipt_filters.insert(name.clone(), filter);
        Ok(ProvisionResult::new(name.clone()).with("Name", name))
    }

    pub(super) fn delete_ses_receipt_filter(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.receipt_filters.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_ses_vdm_attributes(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut accounts = self.ses_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.account_settings.vdm_attributes = Some(props.clone());
        Ok(ProvisionResult::new(format!("vdm-{}", resource.logical_id)))
    }
}
