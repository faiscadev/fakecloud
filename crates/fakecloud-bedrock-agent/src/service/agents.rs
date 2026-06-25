//! `BedrockAgentService` `agents` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl BedrockAgentService {
    pub(super) fn create_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "agentName")?;
        let id = short_id();
        let now_dt = now();
        let agent = Agent {
            agent_id: id.clone(),
            agent_name: name,
            agent_arn: format!(
                "arn:aws:bedrock:{}:{}:agent/{}",
                req.region, req.account_id, id
            ),
            agent_version: "DRAFT".to_string(),
            agent_resource_role_arn: opt_str(&body, "agentResourceRoleArn").unwrap_or_else(|| {
                format!(
                    "arn:aws:iam::{}:role/fakecloud-bedrock-agent-role",
                    req.account_id
                )
            }),
            description: opt_str(&body, "description"),
            instruction: opt_str(&body, "instruction"),
            foundation_model: opt_str(&body, "foundationModel"),
            idle_session_ttl_in_seconds: opt_i64(&body, "idleSessionTTLInSeconds").unwrap_or(1800),
            customer_encryption_key_arn: opt_str(&body, "customerEncryptionKeyArn"),
            prompt_override_configuration: opt_json(&body, "promptOverrideConfiguration"),
            guardrail_configuration: opt_json(&body, "guardrailConfiguration"),
            agent_collaboration: opt_str(&body, "agentCollaboration")
                .unwrap_or_else(|| "DISABLED".to_string()),
            agent_status: "NOT_PREPARED".to_string(),
            prepared_at: None,
            created_at: now_dt,
            updated_at: now_dt,
            failure_reasons: Vec::new(),
            recommended_actions: Vec::new(),
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if state
            .agents
            .values()
            .any(|a| a.agent_name == agent.agent_name)
        {
            return Err(conflict(format!(
                "Agent with name {} already exists",
                agent.agent_name
            )));
        }
        state.agents.insert(id.clone(), agent);
        let a = state.agents.get(&id).unwrap();
        Ok(AwsResponse::ok_json(json!({ "agent": agent_json(a) })))
    }

    pub(super) fn get_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        let a = state
            .agents
            .get(&id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "agent": agent_json(a) })))
    }

    pub(super) fn list_agents(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| s.agents.values().map(agent_summary_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "agentSummaries": list })))
    }

    pub(super) fn update_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "agentId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .agents
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        a.updated_at = now();
        if let Some(n) = opt_str(&body, "agentName") {
            a.agent_name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if let Some(i) = opt_str(&body, "instruction") {
            a.instruction = Some(i);
        }
        if let Some(m) = opt_str(&body, "foundationModel") {
            a.foundation_model = Some(m);
        }
        if let Some(t) = opt_i64(&body, "idleSessionTTLInSeconds") {
            a.idle_session_ttl_in_seconds = t;
        }
        if let Some(k) = opt_str(&body, "customerEncryptionKeyArn") {
            a.customer_encryption_key_arn = Some(k);
        }
        if body.get("promptOverrideConfiguration").is_some() {
            a.prompt_override_configuration = opt_json(&body, "promptOverrideConfiguration");
        }
        if body.get("guardrailConfiguration").is_some() {
            a.guardrail_configuration = opt_json(&body, "guardrailConfiguration");
        }
        if let Some(c) = opt_str(&body, "agentCollaboration") {
            a.agent_collaboration = c;
        }
        Ok(AwsResponse::ok_json(json!({ "agent": agent_json(a) })))
    }

    pub(super) fn delete_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "agentId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state
            .agents
            .remove(&id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        state.agent_versions.remove(&id);
        state.agent_knowledge_bases.remove(&id);
        state.agent_collaborators.remove(&id);
        state.agent_action_groups.retain(|_, ag| ag.agent_id != id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn prepare_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "agentId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .agents
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        a.agent_status = "PREPARED".to_string();
        a.prepared_at = Some(now());
        a.updated_at = now();
        let snapshot = a.clone();
        // PrepareAgent cuts a new numbered agent version from the current DRAFT
        // (real ECS-style: version 1 on first prepare, then incrementing). The
        // versions are what ListAgentVersions / the versions data source read.
        let versions = state.agent_versions.entry(id.clone()).or_default();
        let next = versions
            .iter()
            .filter_map(|v| v.agent_version.parse::<u32>().ok())
            .max()
            .map(|n| n + 1)
            .unwrap_or(1);
        versions.push(AgentVersion {
            agent_version: next.to_string(),
            agent_id: id.clone(),
            agent_name: snapshot.agent_name.clone(),
            description: snapshot.description.clone(),
            created_at: now(),
            updated_at: now(),
            instruction: snapshot.instruction.clone(),
            foundation_model: snapshot.foundation_model.clone(),
            guardrail_configuration: snapshot.guardrail_configuration.clone(),
            prompt_override_configuration: snapshot.prompt_override_configuration.clone(),
        });
        Ok(AwsResponse::ok_json(json!({
            "agentId": id,
            "agentStatus": "PREPARED",
            "agentVersion": "DRAFT",
            "preparedAt": snapshot.prepared_at.as_ref().unwrap().to_rfc3339(),
        })))
    }

    pub(super) fn create_agent_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let name = req_str(&body, "agentAliasName")?;
        let alias_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        // Creating an alias without an explicit routingConfiguration makes AWS
        // cut a new agent version and route 100% of traffic to it, so the alias
        // always reads back exactly one routing-configuration entry. Mirror that
        // by defaulting to the latest prepared version (or "1").
        let mut routing_configuration = opt_array(&body, "routingConfiguration");
        if routing_configuration.is_empty() {
            let version = state
                .agent_versions
                .get(&agent_id)
                .and_then(|vs| {
                    vs.iter()
                        .filter_map(|v| v.agent_version.parse::<u32>().ok())
                        .max()
                })
                .unwrap_or(1);
            routing_configuration = vec![json!({ "agentVersion": version.to_string() })];
        }
        let alias = AgentAlias {
            alias_id: alias_id.clone(),
            alias_name: name.clone(),
            agent_id: agent_id.clone(),
            agent_version: opt_str(&body, "agentVersion").unwrap_or_else(|| "DRAFT".to_string()),
            routing_configuration,
            description: opt_str(&body, "description"),
            alias_arn: format!(
                "arn:aws:bedrock:{}:{}:agent-alias/{}/{}",
                req.region, req.account_id, agent_id, alias_id
            ),
            agent_alias_status: "PREPARED".to_string(),
            failure_reasons: Vec::new(),
            created_at: now_dt,
            updated_at: now_dt,
        };
        state.agent_aliases.insert(alias_id.clone(), alias);
        let a = state.agent_aliases.get(&alias_id).unwrap();
        Ok(AwsResponse::ok_json(json!({ "agentAlias": alias_json(a) })))
    }

    pub(super) fn get_agent_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let alias_id = req_str(&body, "agentAliasId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Alias {alias_id} not found")))?;
        let a = state
            .agent_aliases
            .get(&alias_id)
            .filter(|a| a.agent_id == agent_id)
            .ok_or_else(|| not_found(format!("Alias {alias_id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "agentAlias": alias_json(a) })))
    }

    pub(super) fn list_agent_aliases(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.agent_aliases
                    .values()
                    .filter(|a| a.agent_id == agent_id)
                    .map(alias_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "agentAliasSummaries": list })))
    }

    pub(super) fn update_agent_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let alias_id = req_str(&body, "agentAliasId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .agent_aliases
            .get_mut(&alias_id)
            .filter(|a| a.agent_id == agent_id)
            .ok_or_else(|| not_found(format!("Alias {alias_id} not found")))?;
        a.updated_at = now();
        if let Some(n) = opt_str(&body, "agentAliasName") {
            a.alias_name = n;
        }
        if let Some(v) = opt_str(&body, "agentVersion") {
            a.agent_version = v;
        }
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if body.get("routingConfiguration").is_some() {
            a.routing_configuration = opt_array(&body, "routingConfiguration");
        }
        Ok(AwsResponse::ok_json(json!({ "agentAlias": alias_json(a) })))
    }

    pub(super) fn delete_agent_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let alias_id = req_str(&body, "agentAliasId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        // Verify ownership BEFORE removal. The prior order popped the
        // alias and then filtered — a request with the wrong agent_id
        // could still delete a real alias and then surface 404.
        match state.agent_aliases.get(&alias_id) {
            Some(a) if a.agent_id == agent_id => {
                state.agent_aliases.remove(&alias_id);
            }
            _ => return Err(not_found(format!("Alias {alias_id} not found"))),
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_agent_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let version = req_str(&body, "agentVersion")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Version {version} not found")))?;
        let v = state
            .agent_versions
            .get(&agent_id)
            .and_then(|vec| vec.iter().find(|v| v.agent_version == version))
            .ok_or_else(|| not_found(format!("Version {version} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "agentVersion": agent_version_json(v) }),
        ))
    }

    pub(super) fn list_agent_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.agent_versions.get(&agent_id))
            .map(|vec| vec.iter().map(agent_version_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "agentVersionSummaries": list }),
        ))
    }

    pub(super) fn delete_agent_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let version = req_str(&body, "agentVersion")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let vec = state
            .agent_versions
            .get_mut(&agent_id)
            .ok_or_else(|| not_found(format!("Version {version} not found")))?;
        let pos = vec
            .iter()
            .position(|v| v.agent_version == version)
            .ok_or_else(|| not_found(format!("Version {version} not found")))?;
        vec.remove(pos);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn associate_agent_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        let list = state
            .agent_knowledge_bases
            .entry(agent_id.clone())
            .or_default();
        if list.iter().any(|a| a.knowledge_base_id == kb_id) {
            return Err(conflict(format!(
                "KnowledgeBase {kb_id} already associated with agent {agent_id}"
            )));
        }
        list.push(AgentKnowledgeBase {
            agent_id: agent_id.clone(),
            knowledge_base_id: kb_id.clone(),
            description: opt_str(&body, "description"),
            knowledge_base_state: opt_str(&body, "knowledgeBaseState")
                .unwrap_or_else(|| "ENABLED".to_string()),
            created_at: now_dt,
            updated_at: now_dt,
        });
        Ok(AwsResponse::ok_json(json!({
            "agentKnowledgeBase": {
                "agentId": agent_id,
                "knowledgeBaseId": kb_id,
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    pub(super) fn disassociate_agent_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let list = state
            .agent_knowledge_bases
            .get_mut(&agent_id)
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        let pos = list
            .iter()
            .position(|a| a.knowledge_base_id == kb_id)
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        list.remove(pos);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_agent_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        let a = state
            .agent_knowledge_bases
            .get(&agent_id)
            .and_then(|list| list.iter().find(|a| a.knowledge_base_id == kb_id))
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        Ok(AwsResponse::ok_json(
            json!({ "agentKnowledgeBase": agent_kb_json(a) }),
        ))
    }

    pub(super) fn list_agent_knowledge_bases(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.agent_knowledge_bases.get(&agent_id))
            .map(|list| list.iter().map(agent_kb_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "agentKnowledgeBaseSummaries": list }),
        ))
    }

    pub(super) fn update_agent_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .agent_knowledge_bases
            .get_mut(&agent_id)
            .and_then(|list| list.iter_mut().find(|a| a.knowledge_base_id == kb_id))
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        a.updated_at = now();
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if let Some(s) = opt_str(&body, "knowledgeBaseState") {
            a.knowledge_base_state = s;
        }
        Ok(AwsResponse::ok_json(
            json!({ "agentKnowledgeBase": agent_kb_json(a) }),
        ))
    }

    pub(super) fn associate_agent_collaborator(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let collaborator_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        let coll = AgentCollaborator {
            agent_id: agent_id.clone(),
            agent_version: opt_str(&body, "agentVersion").unwrap_or_else(|| "DRAFT".to_string()),
            collaborator_id: collaborator_id.clone(),
            collaborator_name: req_str(&body, "collaboratorName")?,
            agent_descriptor: opt_json(&body, "agentDescriptor"),
            collaboration_instruction: opt_str(&body, "collaborationInstruction")
                .unwrap_or_default(),
            relay_conversation_history: opt_str(&body, "relayConversationHistory")
                .unwrap_or_else(|| "DISABLED".to_string()),
            created_at: now_dt,
            updated_at: now_dt,
        };
        state
            .agent_collaborators
            .entry(agent_id.clone())
            .or_default()
            .push(coll);
        let coll = state.agent_collaborators[&agent_id].last().unwrap();
        Ok(AwsResponse::ok_json(
            json!({ "agentCollaborator": agent_collaborator_json(coll) }),
        ))
    }

    pub(super) fn disassociate_agent_collaborator(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let collaborator_id = req_str(&body, "collaboratorId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let list = state
            .agent_collaborators
            .get_mut(&agent_id)
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        let pos = list
            .iter()
            .position(|c| c.collaborator_id == collaborator_id)
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        list.remove(pos);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_agent_collaborator(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let collaborator_id = req_str(&body, "collaboratorId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        let c = state
            .agent_collaborators
            .get(&agent_id)
            .and_then(|list| list.iter().find(|c| c.collaborator_id == collaborator_id))
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        Ok(AwsResponse::ok_json(
            json!({ "agentCollaborator": agent_collaborator_json(c) }),
        ))
    }

    pub(super) fn list_agent_collaborators(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.agent_collaborators.get(&agent_id))
            .map(|list| list.iter().map(agent_collaborator_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "agentCollaboratorSummaries": list }),
        ))
    }

    pub(super) fn update_agent_collaborator(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let collaborator_id = req_str(&body, "collaboratorId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let c = state
            .agent_collaborators
            .get_mut(&agent_id)
            .and_then(|list| {
                list.iter_mut()
                    .find(|c| c.collaborator_id == collaborator_id)
            })
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        c.updated_at = now();
        if let Some(n) = opt_str(&body, "collaboratorName") {
            c.collaborator_name = n;
        }
        if body.get("agentDescriptor").is_some() {
            c.agent_descriptor = opt_json(&body, "agentDescriptor");
        }
        if let Some(i) = opt_str(&body, "collaborationInstruction") {
            c.collaboration_instruction = i;
        }
        if let Some(r) = opt_str(&body, "relayConversationHistory") {
            c.relay_conversation_history = r;
        }
        Ok(AwsResponse::ok_json(
            json!({ "agentCollaborator": agent_collaborator_json(c) }),
        ))
    }

    pub(super) fn create_agent_action_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let action_group_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        let ag = AgentActionGroup {
            action_group_id: action_group_id.clone(),
            agent_id: agent_id.clone(),
            agent_version: opt_str(&body, "agentVersion").unwrap_or_else(|| "DRAFT".to_string()),
            action_group_name: req_str(&body, "actionGroupName")?.to_string(),
            description: opt_str(&body, "description"),
            // AWS defaults a new action group to ENABLED when the caller omits
            // actionGroupState.
            action_group_state: opt_str(&body, "actionGroupState")
                .unwrap_or_else(|| "ENABLED".to_string()),
            action_group_executor: opt_json(&body, "actionGroupExecutor"),
            api_schema: opt_json(&body, "apiSchema"),
            function_schema: opt_json(&body, "functionSchema"),
            parent_action_group_signature: opt_str(&body, "parentActionGroupSignature"),
            created_at: now_dt,
            updated_at: now_dt,
        };
        state
            .agent_action_groups
            .insert(action_group_id.clone(), ag);
        let ag = state.agent_action_groups.get(&action_group_id).unwrap();
        Ok(AwsResponse::ok_json(
            json!({ "agentActionGroup": action_group_json(ag) }),
        ))
    }

    pub(super) fn get_agent_action_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let action_group_id = req_str(&body, "actionGroupId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Action group {action_group_id} not found")))?;
        let ag = state
            .agent_action_groups
            .get(&action_group_id)
            .filter(|ag| ag.agent_id == agent_id)
            .ok_or_else(|| not_found(format!("Action group {action_group_id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "agentActionGroup": action_group_json(ag) }),
        ))
    }

    pub(super) fn list_agent_action_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Agent {agent_id} not found")))?;
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        let list: Vec<Value> = state
            .agent_action_groups
            .values()
            .filter(|ag| ag.agent_id == agent_id)
            .map(action_group_summary_json)
            .collect();
        Ok(AwsResponse::ok_json(
            json!({ "actionGroupSummaries": list }),
        ))
    }

    pub(super) fn update_agent_action_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let action_group_id = req_str(&body, "actionGroupId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let ag = state
            .agent_action_groups
            .get_mut(&action_group_id)
            .filter(|ag| ag.agent_id == agent_id)
            .ok_or_else(|| not_found(format!("Action group {action_group_id} not found")))?;
        if let Some(n) = opt_str(&body, "actionGroupName") {
            ag.action_group_name = n;
        }
        if let Some(v) = opt_str(&body, "agentVersion") {
            ag.agent_version = v;
        }
        if let Some(d) = opt_str(&body, "description") {
            ag.description = Some(d);
        }
        if let Some(s) = opt_str(&body, "actionGroupState") {
            ag.action_group_state = s;
        }
        if body.get("actionGroupExecutor").is_some() {
            ag.action_group_executor = opt_json(&body, "actionGroupExecutor");
        }
        if body.get("apiSchema").is_some() {
            ag.api_schema = opt_json(&body, "apiSchema");
        }
        if body.get("functionSchema").is_some() {
            ag.function_schema = opt_json(&body, "functionSchema");
        }
        ag.updated_at = now();
        Ok(AwsResponse::ok_json(
            json!({ "agentActionGroup": action_group_json(ag) }),
        ))
    }

    pub(super) fn delete_agent_action_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let action_group_id = req_str(&body, "actionGroupId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        match state.agent_action_groups.get(&action_group_id) {
            Some(ag) if ag.agent_id == agent_id => {
                state.agent_action_groups.remove(&action_group_id);
            }
            _ => {
                return Err(not_found(format!(
                    "Action group {action_group_id} not found"
                )))
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
