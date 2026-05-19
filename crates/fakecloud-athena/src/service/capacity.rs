//! `AthenaService` `capacity` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn create_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name targets CapacityReservationName @length(min:1, max:128).
        let name = validate_required_string_len(&body, "Name", 1, 128)?;
        let target_dpus =
            body.get("TargetDpus")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid_request("TargetDpus is required"))? as i32;
        let tags = parse_tags(body.get("Tags"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.capacity_reservations.contains_key(&name) {
            return Err(invalid_request(format!(
                "CapacityReservation {name} already exists"
            )));
        }
        let cr = CapacityReservation {
            name: name.clone(),
            status: "ACTIVE".to_string(),
            target_dpus,
            allocated_dpus: target_dpus,
            creation_time: Utc::now(),
            last_allocation: Some(Utc::now()),
            last_successful_allocation_time: Some(Utc::now()),
        };
        let arn = capacity_reservation_arn(&req.account_id, &req.region, &name);
        account.capacity_reservations.insert(name, cr);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cr = account
            .capacity_reservations
            .get(&name)
            .ok_or_else(|| invalid_request(format!("CapacityReservation {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "CapacityReservation": capacity_reservation_json(cr),
        })))
    }

    pub(super) fn list_capacity_reservations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let max_results = validate_max_results(&body, 1, 50)?;
        // Smithy: NextToken targets Token @length(1,1024).
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<CapacityReservation> =
            account.capacity_reservations.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let crs: Vec<Value> = page.iter().map(capacity_reservation_json).collect();
        let mut response = json!({ "CapacityReservations": crs });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn update_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let target_dpus =
            body.get("TargetDpus")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid_request("TargetDpus is required"))? as i32;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cr = account
            .capacity_reservations
            .get_mut(&name)
            .ok_or_else(|| invalid_request(format!("CapacityReservation {name} not found")))?;
        cr.target_dpus = target_dpus;
        cr.allocated_dpus = target_dpus;
        cr.last_allocation = Some(Utc::now());
        cr.last_successful_allocation_time = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn cancel_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cr = account
            .capacity_reservations
            .get_mut(&name)
            .ok_or_else(|| invalid_request(format!("CapacityReservation {name} not found")))?;
        cr.status = "CANCELLING".to_string();
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_capacity_reservation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: Name targets CapacityReservationName @length(1,128).
        let name = validate_required_string_len(&body, "Name", 1, 128)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.capacity_reservations.remove(&name).is_none() {
            return Err(invalid_request(format!(
                "CapacityReservation {name} not found"
            )));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn put_capacity_assignment_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cr_name = require_str(&body, "CapacityReservationName")?;
        let assignments = body
            .get("CapacityAssignments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.capacity_reservations.contains_key(&cr_name) {
            return Err(invalid_request(format!(
                "CapacityReservation {cr_name} not found"
            )));
        }
        account.capacity_assignment_config = Some(CapacityAssignmentConfiguration {
            capacity_reservation_name: cr_name,
            capacity_assignments: assignments,
        });
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_capacity_assignment_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cr_name = require_str(&body, "CapacityReservationName")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let cfg = account
            .capacity_assignment_config
            .clone()
            .filter(|c| c.capacity_reservation_name == cr_name)
            .ok_or_else(|| {
                invalid_request(format!("No CapacityAssignmentConfiguration for {cr_name}"))
            })?;
        Ok(AwsResponse::ok_json(json!({
            "CapacityAssignmentConfiguration": {
                "CapacityReservationName": cfg.capacity_reservation_name,
                "CapacityAssignments": cfg.capacity_assignments,
            }
        })))
    }
}
