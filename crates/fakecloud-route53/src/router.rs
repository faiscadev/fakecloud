//! HTTP method + URI to action routing for Route 53's REST-XML API.

use http::Method;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub action: &'static str,
    pub id: Option<String>,
    pub second_id: Option<String>,
}

impl Route {
    fn just(action: &'static str) -> Self {
        Self {
            action,
            id: None,
            second_id: None,
        }
    }

    fn with_id(action: &'static str, id: &str) -> Self {
        Self {
            action,
            id: Some(id.to_string()),
            second_id: None,
        }
    }

    fn with_two(action: &'static str, id: &str, second: &str) -> Self {
        Self {
            action,
            id: Some(id.to_string()),
            second_id: Some(second.to_string()),
        }
    }
}

pub fn route(method: &Method, path: &str, _raw_query: &str) -> Option<Route> {
    // Real Route 53 only serves operations beneath `/2013-04-01/`. Refuse
    // anything else outright instead of permissively trimming a missing
    // prefix and possibly matching a malformed path against a route.
    let path = path.strip_prefix(crate::API_PREFIX)?;
    if !path.is_empty() && !path.starts_with('/') {
        return None;
    }
    let path = path.trim_start_matches('/');
    // Filter out empty segments so a trailing slash (`/hostedzone/`, which
    // botocore/AWS CLI < 1.40 and curl emit for a collection root) routes to the
    // List op, not GetHostedZone(id="") -> NoSuchHostedZone (the #1645 shape;
    // bug-audit 2026-06-20, 1.1).
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match (method, segs.as_slice()) {
        // ─── Hosted Zones ────────────────────────────────────────────
        (&Method::POST, ["hostedzone"]) => Some(Route::just("CreateHostedZone")),
        (&Method::GET, ["hostedzone"]) => Some(Route::just("ListHostedZones")),
        (&Method::GET, ["hostedzone", id]) => Some(Route::with_id("GetHostedZone", id)),
        (&Method::DELETE, ["hostedzone", id]) => Some(Route::with_id("DeleteHostedZone", id)),
        (&Method::POST, ["hostedzone", id]) => Some(Route::with_id("UpdateHostedZoneComment", id)),
        (&Method::POST, ["hostedzone", id, "features"]) => {
            Some(Route::with_id("UpdateHostedZoneFeatures", id))
        }
        (&Method::GET, ["hostedzonecount"]) => Some(Route::just("GetHostedZoneCount")),
        (&Method::GET, ["hostedzonesbyname"]) => Some(Route::just("ListHostedZonesByName")),
        (&Method::GET, ["hostedzonelimit", id, lim_type]) => {
            Some(Route::with_two("GetHostedZoneLimit", id, lim_type))
        }

        // ─── Resource Record Sets ────────────────────────────────────
        (&Method::POST, ["hostedzone", id, "rrset"]) => {
            Some(Route::with_id("ChangeResourceRecordSets", id))
        }
        (&Method::GET, ["hostedzone", id, "rrset"]) => {
            Some(Route::with_id("ListResourceRecordSets", id))
        }

        // ─── Change tracking ─────────────────────────────────────────
        (&Method::GET, ["change", id]) => Some(Route::with_id("GetChange", id)),

        // ─── DNS Test ────────────────────────────────────────────────
        (&Method::GET, ["testdnsanswer"]) => Some(Route::just("TestDNSAnswer")),

        // ─── Health Checks ───────────────────────────────────────────
        (&Method::POST, ["healthcheck"]) => Some(Route::just("CreateHealthCheck")),
        (&Method::GET, ["healthcheck"]) => Some(Route::just("ListHealthChecks")),
        (&Method::GET, ["healthcheckcount"]) => Some(Route::just("GetHealthCheckCount")),
        (&Method::GET, ["healthcheck", id]) => Some(Route::with_id("GetHealthCheck", id)),
        (&Method::POST, ["healthcheck", id]) => Some(Route::with_id("UpdateHealthCheck", id)),
        (&Method::DELETE, ["healthcheck", id]) => Some(Route::with_id("DeleteHealthCheck", id)),
        (&Method::GET, ["healthcheck", id, "status"]) => {
            Some(Route::with_id("GetHealthCheckStatus", id))
        }
        (&Method::GET, ["healthcheck", id, "lastfailurereason"]) => {
            Some(Route::with_id("GetHealthCheckLastFailureReason", id))
        }
        (&Method::GET, ["checkeripranges"]) => Some(Route::just("GetCheckerIpRanges")),

        // ─── Traffic Policies ────────────────────────────────────────
        (&Method::POST, ["trafficpolicy"]) => Some(Route::just("CreateTrafficPolicy")),
        (&Method::POST, ["trafficpolicy", id]) => {
            Some(Route::with_id("CreateTrafficPolicyVersion", id))
        }
        (&Method::GET, ["trafficpolicy", id, version]) => {
            Some(Route::with_two("GetTrafficPolicy", id, version))
        }
        (&Method::POST, ["trafficpolicy", id, version]) => {
            Some(Route::with_two("UpdateTrafficPolicyComment", id, version))
        }
        (&Method::DELETE, ["trafficpolicy", id, version]) => {
            Some(Route::with_two("DeleteTrafficPolicy", id, version))
        }
        (&Method::GET, ["trafficpolicies"]) => Some(Route::just("ListTrafficPolicies")),
        (&Method::GET, ["trafficpolicies", id, "versions"]) => {
            Some(Route::with_id("ListTrafficPolicyVersions", id))
        }

        // ─── Traffic Policy Instances ────────────────────────────────
        (&Method::POST, ["trafficpolicyinstance"]) => {
            Some(Route::just("CreateTrafficPolicyInstance"))
        }
        (&Method::GET, ["trafficpolicyinstance", id]) => {
            Some(Route::with_id("GetTrafficPolicyInstance", id))
        }
        (&Method::POST, ["trafficpolicyinstance", id]) => {
            Some(Route::with_id("UpdateTrafficPolicyInstance", id))
        }
        (&Method::DELETE, ["trafficpolicyinstance", id]) => {
            Some(Route::with_id("DeleteTrafficPolicyInstance", id))
        }
        (&Method::GET, ["trafficpolicyinstances"]) => {
            Some(Route::just("ListTrafficPolicyInstances"))
        }
        (&Method::GET, ["trafficpolicyinstances", "hostedzone"]) => {
            Some(Route::just("ListTrafficPolicyInstancesByHostedZone"))
        }
        (&Method::GET, ["trafficpolicyinstances", "trafficpolicy"]) => {
            Some(Route::just("ListTrafficPolicyInstancesByPolicy"))
        }
        (&Method::GET, ["trafficpolicyinstancecount"]) => {
            Some(Route::just("GetTrafficPolicyInstanceCount"))
        }

        // ─── DNSSEC ──────────────────────────────────────────────────
        (&Method::GET, ["hostedzone", id, "dnssec"]) => Some(Route::with_id("GetDNSSEC", id)),
        (&Method::POST, ["hostedzone", id, "enable-dnssec"]) => {
            Some(Route::with_id("EnableHostedZoneDNSSEC", id))
        }
        (&Method::POST, ["hostedzone", id, "disable-dnssec"]) => {
            Some(Route::with_id("DisableHostedZoneDNSSEC", id))
        }

        // ─── Key Signing Keys ────────────────────────────────────────
        (&Method::POST, ["keysigningkey"]) => Some(Route::just("CreateKeySigningKey")),
        (&Method::DELETE, ["keysigningkey", zone, name]) => {
            Some(Route::with_two("DeleteKeySigningKey", zone, name))
        }
        (&Method::POST, ["keysigningkey", zone, name, "activate"]) => {
            Some(Route::with_two("ActivateKeySigningKey", zone, name))
        }
        (&Method::POST, ["keysigningkey", zone, name, "deactivate"]) => {
            Some(Route::with_two("DeactivateKeySigningKey", zone, name))
        }

        // ─── Query Logging ───────────────────────────────────────────
        (&Method::POST, ["queryloggingconfig"]) => Some(Route::just("CreateQueryLoggingConfig")),
        (&Method::GET, ["queryloggingconfig"]) => Some(Route::just("ListQueryLoggingConfigs")),
        (&Method::GET, ["queryloggingconfig", id]) => {
            Some(Route::with_id("GetQueryLoggingConfig", id))
        }
        (&Method::DELETE, ["queryloggingconfig", id]) => {
            Some(Route::with_id("DeleteQueryLoggingConfig", id))
        }

        // ─── CIDR Collections ────────────────────────────────────────
        (&Method::POST, ["cidrcollection"]) => Some(Route::just("CreateCidrCollection")),
        (&Method::GET, ["cidrcollection"]) => Some(Route::just("ListCidrCollections")),
        (&Method::POST, ["cidrcollection", id]) => Some(Route::with_id("ChangeCidrCollection", id)),
        (&Method::DELETE, ["cidrcollection", id]) => {
            Some(Route::with_id("DeleteCidrCollection", id))
        }
        (&Method::GET, ["cidrcollection", id]) => Some(Route::with_id("ListCidrLocations", id)),
        (&Method::GET, ["cidrcollection", id, "cidrblocks"]) => {
            Some(Route::with_id("ListCidrBlocks", id))
        }

        // ─── VPC associations ────────────────────────────────────────
        (&Method::POST, ["hostedzone", id, "associatevpc"]) => {
            Some(Route::with_id("AssociateVPCWithHostedZone", id))
        }
        (&Method::POST, ["hostedzone", id, "disassociatevpc"]) => {
            Some(Route::with_id("DisassociateVPCFromHostedZone", id))
        }
        (&Method::POST, ["hostedzone", id, "authorizevpcassociation"]) => {
            Some(Route::with_id("CreateVPCAssociationAuthorization", id))
        }
        (&Method::POST, ["hostedzone", id, "deauthorizevpcassociation"]) => {
            Some(Route::with_id("DeleteVPCAssociationAuthorization", id))
        }
        (&Method::GET, ["hostedzone", id, "authorizevpcassociation"]) => {
            Some(Route::with_id("ListVPCAssociationAuthorizations", id))
        }
        (&Method::GET, ["hostedzonesbyvpc"]) => Some(Route::just("ListHostedZonesByVPC")),

        // ─── Reusable Delegation Sets ────────────────────────────────
        (&Method::POST, ["delegationset"]) => Some(Route::just("CreateReusableDelegationSet")),
        (&Method::GET, ["delegationset"]) => Some(Route::just("ListReusableDelegationSets")),
        (&Method::GET, ["delegationset", id]) => {
            Some(Route::with_id("GetReusableDelegationSet", id))
        }
        (&Method::DELETE, ["delegationset", id]) => {
            Some(Route::with_id("DeleteReusableDelegationSet", id))
        }
        (&Method::GET, ["reusabledelegationsetlimit", id, lim_type]) => Some(Route::with_two(
            "GetReusableDelegationSetLimit",
            id,
            lim_type,
        )),

        // ─── Geo Locations + Account Limits ──────────────────────────
        (&Method::GET, ["geolocations"]) => Some(Route::just("ListGeoLocations")),
        (&Method::GET, ["geolocation"]) => Some(Route::just("GetGeoLocation")),
        (&Method::GET, ["accountlimit", lim_type]) => {
            Some(Route::with_id("GetAccountLimit", lim_type))
        }

        // ─── Tags ────────────────────────────────────────────────────
        (&Method::POST, ["tags", res_type, res_id]) => {
            Some(Route::with_two("ChangeTagsForResource", res_type, res_id))
        }
        (&Method::GET, ["tags", res_type, res_id]) => {
            Some(Route::with_two("ListTagsForResource", res_type, res_id))
        }
        (&Method::POST, ["tags", res_type]) => {
            Some(Route::with_id("ListTagsForResources", res_type))
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_hosted_zone() {
        assert_eq!(
            route(&Method::POST, "/2013-04-01/hostedzone", ""),
            Some(Route::just("CreateHostedZone"))
        );
    }

    #[test]
    fn get_hosted_zone_strips_prefix() {
        assert_eq!(
            route(&Method::GET, "/2013-04-01/hostedzone/Z123", ""),
            Some(Route::with_id("GetHostedZone", "Z123"))
        );
    }

    #[test]
    fn change_rrsets() {
        assert_eq!(
            route(&Method::POST, "/2013-04-01/hostedzone/Z123/rrset", ""),
            Some(Route::with_id("ChangeResourceRecordSets", "Z123"))
        );
    }

    #[test]
    fn trailing_slash_collection_routes_to_list() {
        // botocore/AWS CLI < 1.40 and curl append `/` to a bare collection URI;
        // it must route to the List op, not GetHostedZone(id="") (1.1).
        assert_eq!(
            route(&Method::GET, "/2013-04-01/hostedzone/", ""),
            Some(Route::just("ListHostedZones"))
        );
        // Without a trailing slash still lists.
        assert_eq!(
            route(&Method::GET, "/2013-04-01/hostedzone", ""),
            Some(Route::just("ListHostedZones"))
        );
    }
}
