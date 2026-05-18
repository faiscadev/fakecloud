// Auto-extracted from service.rs as part of carryover service.rs split.
// Methods on `SnsService` grouped by resource concern.

#![allow(clippy::too_many_arguments)]

use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl SnsService {
    pub(super) fn set_sms_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let attrs = parse_entries(req, "attributes");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for (k, v) in attrs {
            state.sms_attributes.insert(k, v);
        }

        Ok(xml_resp(
            &format!(
                r#"<SetSMSAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <SetSMSAttributesResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</SetSMSAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn get_sms_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Parse optional attribute name filter: attributes.member.N
        let mut filter_names = Vec::new();
        for n in 1..=50 {
            let key = format!("attributes.member.{n}");
            if let Some(name) = req.query_params.get(&key) {
                filter_names.push(name.clone());
            } else {
                break;
            }
        }

        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);

        let attrs: String = state
            .sms_attributes
            .iter()
            .filter(|(k, _)| filter_names.is_empty() || filter_names.contains(k))
            .map(|(k, v)| format_attr(k, v))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<GetSMSAttributesResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetSMSAttributesResult>
    <attributes>
{attrs}
    </attributes>
  </GetSMSAttributesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetSMSAttributesResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn check_if_phone_number_is_opted_out(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let phone_number = required(req, "phoneNumber")?;

        // Validate phone number format (E.164)
        let valid = phone_number.starts_with('+')
            && phone_number.len() >= 2
            && phone_number[1..].chars().all(|c| c.is_ascii_digit());
        if !valid {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!(
                    "Invalid parameter: PhoneNumber Reason: {phone_number} does not meet the E164 format"
                ),
            ));
        }

        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        // Numbers ending in 99 are considered opted out by convention
        let is_opted_out =
            state.opted_out_numbers.contains(&phone_number) || phone_number.ends_with("99");

        Ok(xml_resp(
            &format!(
                r#"<CheckIfPhoneNumberIsOptedOutResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CheckIfPhoneNumberIsOptedOutResult>
    <isOptedOut>{is_opted_out}</isOptedOut>
  </CheckIfPhoneNumberIsOptedOutResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CheckIfPhoneNumberIsOptedOutResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn list_phone_numbers_opted_out(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let members: String = state
            .opted_out_numbers
            .iter()
            .map(|n| format!("      <member>{n}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        Ok(xml_resp(
            &format!(
                r#"<ListPhoneNumbersOptedOutResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListPhoneNumbersOptedOutResult>
    <phoneNumbers>
{members}
    </phoneNumbers>
  </ListPhoneNumbersOptedOutResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListPhoneNumbersOptedOutResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn opt_in_phone_number(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let phone_number = required(req, "phoneNumber")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.opted_out_numbers.retain(|n| n != &phone_number);

        Ok(xml_resp(
            &format!(
                r#"<OptInPhoneNumberResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <OptInPhoneNumberResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</OptInPhoneNumberResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn create_sms_sandbox_phone_number(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let phone_number = required(req, "PhoneNumber")?;
        validate_e164(&phone_number)?;
        let language_code = param(req, "LanguageCode").unwrap_or_else(|| "en-US".to_string());
        if !is_valid_sandbox_language(&language_code) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameter",
                format!("Invalid parameter: LanguageCode {language_code} is not supported"),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.sms_sandbox_phone_numbers.contains_key(&phone_number) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OptedOutException",
                format!(
                    "Phone number {phone_number} is already registered as a sandbox destination."
                ),
            ));
        }
        let otp = format!("{:06}", rand_u32() % 1_000_000);
        state.sms_sandbox_phone_numbers.insert(
            phone_number.clone(),
            crate::state::SmsSandboxPhoneNumber {
                phone_number: phone_number.clone(),
                language_code,
                status: crate::state::SmsSandboxPhoneStatus::Pending,
                one_time_password: otp,
            },
        );
        Ok(xml_resp(
            &format!(
                r#"<CreateSMSSandboxPhoneNumberResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreateSMSSandboxPhoneNumberResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateSMSSandboxPhoneNumberResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn delete_sms_sandbox_phone_number(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let phone_number = required(req, "PhoneNumber")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state
            .sms_sandbox_phone_numbers
            .remove(&phone_number)
            .is_none()
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                format!("Sandbox phone number {phone_number} not found."),
            ));
        }
        Ok(xml_resp(
            &format!(
                r#"<DeleteSMSSandboxPhoneNumberResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <DeleteSMSSandboxPhoneNumberResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</DeleteSMSSandboxPhoneNumberResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn verify_sms_sandbox_phone_number(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let phone_number = required(req, "PhoneNumber")?;
        let one_time_password = required(req, "OneTimePassword")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let entry = state
            .sms_sandbox_phone_numbers
            .get_mut(&phone_number)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFound",
                    format!("Sandbox phone number {phone_number} not found."),
                )
            })?;
        if entry.one_time_password != one_time_password {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "VerificationException",
                "The verification code provided is incorrect.",
            ));
        }
        entry.status = crate::state::SmsSandboxPhoneStatus::Verified;
        Ok(xml_resp(
            &format!(
                r#"<VerifySMSSandboxPhoneNumberResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <VerifySMSSandboxPhoneNumberResult/>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</VerifySMSSandboxPhoneNumberResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn list_sms_sandbox_phone_numbers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_max_results(req, 1, 100)?;
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let members: String = state
            .sms_sandbox_phone_numbers
            .values()
            .map(|p| {
                format!(
                    "      <member>\n        <PhoneNumber>{}</PhoneNumber>\n        <Status>{}</Status>\n      </member>",
                    xml_escape(&p.phone_number),
                    p.status.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(xml_resp(
            &format!(
                r#"<ListSMSSandboxPhoneNumbersResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListSMSSandboxPhoneNumbersResult>
    <PhoneNumbers>
{members}
    </PhoneNumbers>
  </ListSMSSandboxPhoneNumbersResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSMSSandboxPhoneNumbersResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn get_sms_sandbox_account_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _accts = self.state.read();
        let _empty = crate::state::SnsState::new(&req.account_id, &req.region, "");
        let state = _accts.get(&req.account_id).unwrap_or(&_empty);
        let in_sandbox = state.is_sms_sandboxed();
        Ok(xml_resp(
            &format!(
                r#"<GetSMSSandboxAccountStatusResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <GetSMSSandboxAccountStatusResult>
    <IsInSandbox>{in_sandbox}</IsInSandbox>
  </GetSMSSandboxAccountStatusResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetSMSSandboxAccountStatusResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }

    pub(super) fn list_origination_numbers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_max_results(req, 1, 30)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.seed_default_origination_numbers();
        let members: String = state
            .origination_numbers
            .iter()
            .map(|n| {
                let caps: String = n
                    .number_capabilities
                    .iter()
                    .map(|c| format!("          <member>{}</member>", xml_escape(c)))
                    .collect::<Vec<_>>()
                    .join("\n");
                format!(
                    r#"      <member>
        <PhoneNumber>{phone}</PhoneNumber>
        <IsoCountryCode>{iso}</IsoCountryCode>
        <Status>{status}</Status>
        <RouteType>{rt}</RouteType>
        <NumberCapabilities>
{caps}
        </NumberCapabilities>
        <CreatedAt>{created}</CreatedAt>
      </member>"#,
                    phone = xml_escape(&n.phone_number),
                    iso = xml_escape(&n.iso_country_code),
                    status = xml_escape(&n.status),
                    rt = xml_escape(&n.route_type),
                    caps = caps,
                    created = n.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(xml_resp(
            &format!(
                r#"<ListOriginationNumbersResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListOriginationNumbersResult>
    <PhoneNumbers>
{members}
    </PhoneNumbers>
  </ListOriginationNumbersResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListOriginationNumbersResponse>"#,
                req.request_id
            ),
            &req.request_id,
        ))
    }
}
