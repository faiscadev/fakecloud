//! Network Insights: reachability paths + analyses and access scopes +
//! scope analyses.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, require, validate_enum, validate_int_range, validate_length, validate_max_results,
};
use crate::state::{
    Ec2State, NetworkInsightsAccessScope, NetworkInsightsAccessScopeAnalysis,
    NetworkInsightsAnalysis, NetworkInsightsPath, Tag,
};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 1, 100)
}

// ---- paths ----

fn path_xml(p: &NetworkInsightsPath, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}<createdDate>{}</createdDate>{}{}<protocol>{}</protocol>{}",
        ec2_elem("networkInsightsPathId", &p.id),
        ec2_elem(
            "networkInsightsPathArn",
            &format!(
                "arn:aws:ec2:us-east-1:{owner}:network-insights-path/{}",
                p.id
            )
        ),
        FIXED_TIME,
        ec2_elem("source", &p.source),
        ec2_elem("destination", &p.destination),
        p.protocol,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_network_insights_path(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let source = require(&req.query_params, "Source")?;
    require(&req.query_params, "Protocol")?;
    require(&req.query_params, "ClientToken")?;
    validate_enum(&req.query_params, "Protocol", &["tcp", "udp"])?;
    validate_length(&req.query_params, "SourceIp", 0, 15)?;
    validate_length(&req.query_params, "DestinationIp", 0, 15)?;
    validate_int_range(&req.query_params, "DestinationPort", 0, 65535)?;
    let id = gen_id("nip");
    let p = NetworkInsightsPath {
        id: id.clone(),
        source,
        destination: req
            .query_params
            .get("Destination")
            .cloned()
            .unwrap_or_default(),
        protocol: req
            .query_params
            .get("Protocol")
            .cloned()
            .unwrap_or_else(|| "tcp".to_string()),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "network-insights-path",
        );
        let t = state.tags_for(&id).to_vec();
        state.ni_paths.insert(id.clone(), p.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateNetworkInsightsPath",
        &req.request_id,
        &format!(
            "<networkInsightsPath>{}</networkInsightsPath>",
            path_xml(&p, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_network_insights_path(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInsightsPathId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.ni_paths.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteNetworkInsightsPath",
        &req.request_id,
        &ec2_elem("networkInsightsPathId", &id),
    ))
}

pub(crate) fn describe_network_insights_paths(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ni_paths
        .values()
        .map(|p| path_xml(p, state.tags_for(&p.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeNetworkInsightsPaths",
        &req.request_id,
        &ec2_list("networkInsightsPathSet", &items),
    ))
}

// ---- analyses ----

fn analysis_xml(a: &NetworkInsightsAnalysis, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}<startDate>{}</startDate><status>succeeded</status><networkPathFound>true</networkPathFound>{}",
        ec2_elem("networkInsightsAnalysisId", &a.id),
        ec2_elem("networkInsightsAnalysisArn", &format!("arn:aws:ec2:us-east-1:{owner}:network-insights-analysis/{}", a.id)),
        ec2_elem("networkInsightsPathId", &a.path_id),
        FIXED_TIME,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn start_network_insights_analysis(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let path = require(&req.query_params, "NetworkInsightsPathId")?;
    require(&req.query_params, "ClientToken")?;
    let id = gen_id("nia");
    let a = NetworkInsightsAnalysis {
        id: id.clone(),
        path_id: path,
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "network-insights-analysis",
        );
        let t = state.tags_for(&id).to_vec();
        state.ni_analyses.insert(id.clone(), a.clone());
        t
    };
    Ok(Ec2Service::respond(
        "StartNetworkInsightsAnalysis",
        &req.request_id,
        &format!(
            "<networkInsightsAnalysis>{}</networkInsightsAnalysis>",
            analysis_xml(&a, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_network_insights_analysis(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInsightsAnalysisId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.ni_analyses.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteNetworkInsightsAnalysis",
        &req.request_id,
        &ec2_elem("networkInsightsAnalysisId", &id),
    ))
}

pub(crate) fn describe_network_insights_analyses(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ni_analyses
        .values()
        .map(|a| analysis_xml(a, state.tags_for(&a.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeNetworkInsightsAnalyses",
        &req.request_id,
        &ec2_list("networkInsightsAnalysisSet", &items),
    ))
}

// ---- access scopes ----

fn scope_xml(sc: &NetworkInsightsAccessScope, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}<createdDate>{}</createdDate>{}",
        ec2_elem("networkInsightsAccessScopeId", &sc.id),
        ec2_elem(
            "networkInsightsAccessScopeArn",
            &format!(
                "arn:aws:ec2:us-east-1:{owner}:network-insights-access-scope/{}",
                sc.id
            )
        ),
        FIXED_TIME,
        super::tags::tag_set_xml(tags),
    )
}

fn scope_content_xml(scope_id: &str) -> String {
    format!(
        "<networkInsightsAccessScopeContent>{}</networkInsightsAccessScopeContent>",
        ec2_elem("networkInsightsAccessScopeId", scope_id)
    )
}

pub(crate) fn create_network_insights_access_scope(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "ClientToken")?;
    let id = gen_id("nis");
    let sc = NetworkInsightsAccessScope { id: id.clone() };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "network-insights-access-scope",
        );
        let t = state.tags_for(&id).to_vec();
        state.ni_access_scopes.insert(id.clone(), sc.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateNetworkInsightsAccessScope",
        &req.request_id,
        &format!(
            "<networkInsightsAccessScope>{}</networkInsightsAccessScope>{}",
            scope_xml(&sc, &tags, &owner),
            scope_content_xml(&id)
        ),
    ))
}

pub(crate) fn delete_network_insights_access_scope(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInsightsAccessScopeId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.ni_access_scopes.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteNetworkInsightsAccessScope",
        &req.request_id,
        &ec2_elem("networkInsightsAccessScopeId", &id),
    ))
}

pub(crate) fn describe_network_insights_access_scopes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ni_access_scopes
        .values()
        .map(|sc| scope_xml(sc, state.tags_for(&sc.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeNetworkInsightsAccessScopes",
        &req.request_id,
        &ec2_list("networkInsightsAccessScopeSet", &items),
    ))
}

pub(crate) fn get_network_insights_access_scope_content(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInsightsAccessScopeId")?;
    Ok(Ec2Service::respond(
        "GetNetworkInsightsAccessScopeContent",
        &req.request_id,
        &scope_content_xml(&id),
    ))
}

// ---- scope analyses ----

fn scope_analysis_xml(a: &NetworkInsightsAccessScopeAnalysis, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}<status>succeeded</status><startDate>{}</startDate><findingsFound>true</findingsFound><analyzedEniCount>0</analyzedEniCount>{}",
        ec2_elem("networkInsightsAccessScopeAnalysisId", &a.id),
        ec2_elem("networkInsightsAccessScopeAnalysisArn", &format!("arn:aws:ec2:us-east-1:{owner}:network-insights-access-scope-analysis/{}", a.id)),
        ec2_elem("networkInsightsAccessScopeId", &a.scope_id),
        FIXED_TIME,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn start_network_insights_access_scope_analysis(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let scope = require(&req.query_params, "NetworkInsightsAccessScopeId")?;
    require(&req.query_params, "ClientToken")?;
    let id = gen_id("nisa");
    let a = NetworkInsightsAccessScopeAnalysis {
        id: id.clone(),
        scope_id: scope,
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "network-insights-access-scope-analysis",
        );
        let t = state.tags_for(&id).to_vec();
        state.ni_scope_analyses.insert(id.clone(), a.clone());
        t
    };
    Ok(Ec2Service::respond(
        "StartNetworkInsightsAccessScopeAnalysis",
        &req.request_id,
        &format!(
            "<networkInsightsAccessScopeAnalysis>{}</networkInsightsAccessScopeAnalysis>",
            scope_analysis_xml(&a, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_network_insights_access_scope_analysis(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInsightsAccessScopeAnalysisId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.ni_scope_analyses.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteNetworkInsightsAccessScopeAnalysis",
        &req.request_id,
        &ec2_elem("networkInsightsAccessScopeAnalysisId", &id),
    ))
}

pub(crate) fn describe_network_insights_access_scope_analyses(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ni_scope_analyses
        .values()
        .map(|a| scope_analysis_xml(a, state.tags_for(&a.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeNetworkInsightsAccessScopeAnalyses",
        &req.request_id,
        &ec2_list("networkInsightsAccessScopeAnalysisSet", &items),
    ))
}

pub(crate) fn get_network_insights_access_scope_analysis_findings(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NetworkInsightsAccessScopeAnalysisId")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    let body = format!(
        "{}<analysisStatus>succeeded</analysisStatus>{}",
        ec2_elem("networkInsightsAccessScopeAnalysisId", &id),
        ec2_list("analysisFindingSet", &[]),
    );
    Ok(Ec2Service::respond(
        "GetNetworkInsightsAccessScopeAnalysisFindings",
        &req.request_id,
        &body,
    ))
}
