//! DNS resolution over the Route 53 records created in fakecloud.
//!
//! This is the pure lookup half of the "real resolver" feature (issue #2219):
//! given a query name + type, it finds the authoritative zone among all
//! accounts' hosted zones and returns the matching records (chasing CNAMEs into
//! local zones). It performs no I/O and no wire encoding; the server's `dns`
//! module drives it and encodes the answer. A query whose name falls in no local
//! zone yields [`ResolveStatus::NotAuthoritative`] so the caller can forward it
//! upstream.

use crate::state::Route53Accounts;

/// A single record to place in the DNS answer section. `value` is the textual
/// Route 53 `ResourceRecord` value (e.g. `"10.0.0.5"`, `"10 mail.example.com"`),
/// encoded to wire RDATA by the caller via `dnssec::encode_rdata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnswerRecord {
    pub name: String,
    pub rtype: String,
    pub ttl: u32,
    pub value: String,
}

/// Outcome of a lookup, mapping to the DNS response the caller builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveStatus {
    /// At least one answer record was found (in `answers`).
    Answered,
    /// The name exists in an authoritative zone but not for this type
    /// (a NODATA / NOERROR-with-no-answers response).
    NoData,
    /// The name is inside an authoritative zone but does not exist (NXDOMAIN).
    NxDomain,
    /// No local zone is authoritative for the name, so the caller should forward
    /// the query to an upstream resolver.
    NotAuthoritative,
}

/// The result of [`resolve`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    pub answers: Vec<AnswerRecord>,
    pub status: ResolveStatus,
    /// For an `A`/`AAAA` query whose CNAME chain exits all local zones at an
    /// external target, the target name the caller should forward-resolve
    /// upstream and append the address records for (so a stub client that does
    /// not itself chase CNAMEs still gets an address). `None` otherwise.
    pub external_cname: Option<String>,
}

/// Deduplicate answer records, preserving first-seen order. A merged same-name /
/// cross-account zone or a CNAME chase can surface the same record twice; `dig`
/// against the `--dns` resolver returns it once, so both the socket response
/// builder and the HTTP introspection endpoint dedup on the same key
/// (case-insensitive name + type, exact value). Returns references in order.
pub fn dedup_answers(answers: &[AnswerRecord]) -> Vec<&AnswerRecord> {
    let mut seen = std::collections::HashSet::new();
    answers
        .iter()
        .filter(|a| {
            seen.insert((
                a.name.to_ascii_lowercase(),
                a.rtype.to_ascii_uppercase(),
                a.value.clone(),
            ))
        })
        .collect()
}

/// Default TTL (seconds) applied when a record set omits `TTL`.
const DEFAULT_TTL: u32 = 300;
/// Cap on CNAME-chase hops, so a record pointing at itself (or a cycle) can't
/// loop forever.
const MAX_CNAME_HOPS: usize = 8;

/// Normalize a DNS name for comparison: lowercase, trimmed, with exactly one
/// trailing dot (the root). The empty/root name normalizes to `"."`.
fn normalize(name: &str) -> String {
    let trimmed = name.trim().trim_end_matches('.').to_ascii_lowercase();
    if trimmed.is_empty() {
        ".".to_string()
    } else {
        format!("{trimmed}.")
    }
}

/// True when `name` is equal to, or a subdomain of, zone `zone` (both already
/// normalized). Matches only at a label boundary, so `notexample.com.` is NOT a
/// subdomain of `example.com.`.
fn name_in_zone(name: &str, zone: &str) -> bool {
    if name == zone {
        return true;
    }
    // `zone` ends with '.', so `name` must end with `.<zone>`, i.e. the char
    // just before the matched suffix is a label separator.
    name.len() > zone.len()
        && name.ends_with(zone)
        && name.as_bytes()[name.len() - zone.len() - 1] == b'.'
}

/// All record sets (across every account) that live in the single most-specific
/// zone authoritative for `qname`, paired with that zone's normalized name.
/// Returns `None` when no local zone is authoritative.
fn authoritative_records<'a>(
    accounts: &'a Route53Accounts,
    qname_norm: &str,
) -> Option<Vec<&'a crate::model::ResourceRecordSet>> {
    // The most-specific zone-name length that is authoritative for the name.
    let mut best_len: Option<usize> = None;
    for account in accounts.accounts.values() {
        for zone in account.hosted_zones.values() {
            let zn = normalize(&zone.name);
            if name_in_zone(qname_norm, &zn) {
                best_len = Some(best_len.map_or(zn.len(), |b| b.max(zn.len())));
            }
        }
    }
    let best_len = best_len?;

    // Merge the record sets of every zone authoritative at that most-specific
    // level. A same-name zone can recur across accounts, and as a public/private
    // split-horizon pair; a DNS query carries no account or VPC context, so a
    // local (single-tenant) resolver serves the union rather than guessing a
    // view. Identical records are de-duplicated when the answer is built.
    let mut records: Vec<&crate::model::ResourceRecordSet> = Vec::new();
    for account in accounts.accounts.values() {
        for zone in account.hosted_zones.values() {
            let zn = normalize(&zone.name);
            if name_in_zone(qname_norm, &zn) && zn.len() == best_len {
                records.extend(zone.resource_record_sets.iter());
            }
        }
    }
    Some(records)
}

/// The single-level wildcard owner for `name_norm` (`foo.example.com.` ->
/// `*.example.com.`), used when no exact record set matches. `None` for the root
/// or a bare apex with no parent label.
fn wildcard_of(name_norm: &str) -> Option<String> {
    let rest = name_norm.split_once('.')?.1;
    if rest.is_empty() || rest == "." {
        return None;
    }
    Some(format!("*.{rest}"))
}

/// Collect the values of `rtype` records whose owner is exactly `owner`, labeled
/// with `answer_name` (which differs from `owner` for wildcard synthesis).
fn collect(
    records: &[&crate::model::ResourceRecordSet],
    owner: &str,
    answer_name: &str,
    rtype: &str,
) -> Vec<AnswerRecord> {
    let mut out = Vec::new();
    for rr in records {
        if normalize(&rr.name) != owner || !rr.record_type.eq_ignore_ascii_case(rtype) {
            continue;
        }
        let ttl = rr
            .ttl
            .and_then(|t| u32::try_from(t).ok())
            .unwrap_or(DEFAULT_TTL);
        if let Some(values) = &rr.resource_records {
            for v in &values.resource_record {
                out.push(AnswerRecord {
                    name: answer_name.to_string(),
                    rtype: rtype.to_ascii_uppercase(),
                    ttl,
                    value: v.value.clone(),
                });
            }
        }
    }
    out
}

/// True when `name_norm` exists as a node in the zone tree: either an exact
/// owner, or an empty non-terminal (an ancestor of some record, e.g.
/// `b.example.com` when `a.b.example.com` exists). Such a name blocks wildcard
/// synthesis (RFC 4592) and answers NODATA rather than NXDOMAIN.
fn name_is_node(records: &[&crate::model::ResourceRecordSet], name_norm: &str) -> bool {
    records
        .iter()
        .any(|rr| name_in_zone(&normalize(&rr.name), name_norm))
}

/// Values of `rtype` for `name_norm`: an exact match, else a single-level
/// wildcard (`*.<parent>`) synthesized under the queried name (RFC 4592, common
/// case). The record set is already scoped to the authoritative zone.
fn records_of_type(
    records: &[&crate::model::ResourceRecordSet],
    name_norm: &str,
    rtype: &str,
) -> Vec<AnswerRecord> {
    let exact = collect(records, name_norm, name_norm, rtype);
    if !exact.is_empty() {
        return exact;
    }
    // RFC 4592: a wildcard applies only if the queried name does not exist at
    // all (no exact record and not an empty non-terminal). If the name exists,
    // this is NODATA, not a synthesized wildcard record.
    if name_is_node(records, name_norm) {
        return Vec::new();
    }
    match wildcard_of(name_norm) {
        Some(wild) => collect(records, &wild, name_norm, rtype),
        None => Vec::new(),
    }
}

/// True when `name_norm` is covered (NODATA rather than NXDOMAIN): it is a node
/// in the tree (exact owner or empty non-terminal) or a single-level wildcard
/// applies to it.
fn name_exists(records: &[&crate::model::ResourceRecordSet], name_norm: &str) -> bool {
    if name_is_node(records, name_norm) {
        return true;
    }
    let wild = wildcard_of(name_norm);
    wild.is_some()
        && records
            .iter()
            .any(|rr| wild.as_deref() == Some(normalize(&rr.name).as_str()))
}

/// Resolve `qname`/`qtype` against the Route 53 records in `accounts`.
///
/// `qtype` is a textual record type (`"A"`, `"AAAA"`, `"CNAME"`, `"MX"`,
/// `"TXT"`, ...). For an address query (`A`/`AAAA`) that lands on a `CNAME`, the
/// CNAME is returned and, when its target is itself in a local zone, the chased
/// address records are appended.
pub fn resolve(accounts: &Route53Accounts, qname: &str, qtype: &str) -> Resolution {
    let qname_norm = normalize(qname);
    let qtype_uc = qtype.to_ascii_uppercase();

    let records = match authoritative_records(accounts, &qname_norm) {
        Some(r) => r,
        None => {
            return Resolution {
                answers: Vec::new(),
                status: ResolveStatus::NotAuthoritative,
                external_cname: None,
            }
        }
    };

    // Direct hits for the requested type.
    let direct = records_of_type(&records, &qname_norm, &qtype_uc);
    if !direct.is_empty() {
        return Resolution {
            answers: direct,
            status: ResolveStatus::Answered,
            external_cname: None,
        };
    }

    // A query for any type other than CNAME that lands on a CNAME: return the
    // CNAME and chase its target through local zones, appending the requested
    // records where they exist (a real recursive resolver follows the alias for
    // the client). A CNAME query itself was already handled by the direct match
    // above.
    if qtype_uc != "CNAME" {
        let mut answers = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut current = qname_norm.clone();
        let mut cur_records = records.clone();
        let mut external_cname = None;
        for _ in 0..MAX_CNAME_HOPS {
            // Stop on a cycle so a loop can't append the same CNAMEs repeatedly.
            if !visited.insert(current.clone()) {
                break;
            }
            let cnames = records_of_type(&cur_records, &current, "CNAME");
            if cnames.is_empty() {
                break;
            }
            let target = normalize(&cnames[0].value);
            answers.extend(cnames);
            match authoritative_records(accounts, &target) {
                Some(next_records) => {
                    let hits = records_of_type(&next_records, &target, &qtype_uc);
                    if !hits.is_empty() {
                        answers.extend(hits);
                        break;
                    }
                    current = target;
                    cur_records = next_records;
                }
                // The chain left all local zones. For an address query hand the
                // external target back so the caller forward-resolves it and
                // appends the address (a stub client won't chase it itself). For
                // other types the client re-queries the returned CNAME target.
                None => {
                    if qtype_uc == "A" || qtype_uc == "AAAA" {
                        external_cname = Some(target);
                    }
                    break;
                }
            }
        }
        if !answers.is_empty() {
            return Resolution {
                answers,
                status: ResolveStatus::Answered,
                external_cname,
            };
        }
    }

    // Name present but not for this type -> NODATA; absent -> NXDOMAIN.
    let status = if name_exists(&records, &qname_norm) {
        ResolveStatus::NoData
    } else {
        ResolveStatus::NxDomain
    };
    Resolution {
        answers: Vec::new(),
        status,
        external_cname: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ResourceRecord, ResourceRecordSet, ResourceRecords};
    use crate::state::{Route53Accounts, StoredHostedZone};
    use chrono::Utc;

    fn rrset(name: &str, rtype: &str, ttl: i64, values: &[&str]) -> ResourceRecordSet {
        ResourceRecordSet {
            name: name.to_string(),
            record_type: rtype.to_string(),
            ttl: Some(ttl),
            resource_records: Some(ResourceRecords {
                resource_record: values
                    .iter()
                    .map(|v| ResourceRecord {
                        value: v.to_string(),
                    })
                    .collect(),
            }),
            ..Default::default()
        }
    }

    fn zone(name: &str, records: Vec<ResourceRecordSet>) -> StoredHostedZone {
        StoredHostedZone {
            id: format!("/hostedzone/{name}"),
            name: name.to_string(),
            caller_reference: "ref".to_string(),
            comment: None,
            private_zone: false,
            features: None,
            vpcs: Vec::new(),
            delegation_set_id: None,
            name_servers: Vec::new(),
            created_time: Utc::now(),
            resource_record_sets: records,
        }
    }

    fn accounts_with(account: &str, zones: Vec<StoredHostedZone>) -> Route53Accounts {
        let mut a = Route53Accounts::new();
        let st = a.entry(account);
        for z in zones {
            st.hosted_zones.insert(z.id.clone(), z);
        }
        a
    }

    #[test]
    fn a_record_answered() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset("app.example.com.", "A", 60, &["10.0.0.5"])],
            )],
        );
        let r = resolve(&acc, "app.example.com", "A");
        assert_eq!(r.status, ResolveStatus::Answered);
        assert_eq!(r.answers.len(), 1);
        assert_eq!(r.answers[0].value, "10.0.0.5");
        assert_eq!(r.answers[0].ttl, 60);
        assert_eq!(r.answers[0].rtype, "A");
    }

    #[test]
    fn case_and_trailing_dot_insensitive() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "Example.COM.",
                vec![rrset("APP.example.com", "A", 30, &["1.2.3.4"])],
            )],
        );
        let r = resolve(&acc, "app.EXAMPLE.com.", "A");
        assert_eq!(r.status, ResolveStatus::Answered);
        assert_eq!(r.answers[0].value, "1.2.3.4");
    }

    #[test]
    fn missing_ttl_defaults_to_300() {
        let mut rr = rrset("app.example.com.", "A", 0, &["1.1.1.1"]);
        rr.ttl = None;
        let acc = accounts_with("000000000000", vec![zone("example.com.", vec![rr])]);
        assert_eq!(resolve(&acc, "app.example.com", "A").answers[0].ttl, 300);
    }

    #[test]
    fn mx_and_txt() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![
                    rrset("example.com.", "MX", 300, &["10 mail.example.com."]),
                    rrset("example.com.", "TXT", 300, &["\"v=spf1 -all\""]),
                ],
            )],
        );
        assert_eq!(
            resolve(&acc, "example.com", "MX").answers[0].value,
            "10 mail.example.com."
        );
        assert_eq!(
            resolve(&acc, "example.com", "TXT").answers[0].value,
            "\"v=spf1 -all\""
        );
    }

    #[test]
    fn cname_chased_to_address() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![
                    rrset("www.example.com.", "CNAME", 60, &["app.example.com."]),
                    rrset("app.example.com.", "A", 60, &["10.0.0.9"]),
                ],
            )],
        );
        let r = resolve(&acc, "www.example.com", "A");
        assert_eq!(r.status, ResolveStatus::Answered);
        assert_eq!(r.answers.len(), 2);
        assert_eq!(r.answers[0].rtype, "CNAME");
        assert_eq!(r.answers[1].value, "10.0.0.9");
    }

    #[test]
    fn cname_query_returns_cname_only() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset(
                    "www.example.com.",
                    "CNAME",
                    60,
                    &["app.example.com."],
                )],
            )],
        );
        let r = resolve(&acc, "www.example.com", "CNAME");
        assert_eq!(r.status, ResolveStatus::Answered);
        assert_eq!(r.answers.len(), 1);
        assert_eq!(r.answers[0].value, "app.example.com.");
    }

    #[test]
    fn cname_loop_terminates() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![
                    rrset("a.example.com.", "CNAME", 60, &["b.example.com."]),
                    rrset("b.example.com.", "CNAME", 60, &["a.example.com."]),
                ],
            )],
        );
        // Must terminate and dedup: exactly the two distinct CNAME hops, no
        // repeats up to the hop cap.
        let r = resolve(&acc, "a.example.com", "A");
        assert_eq!(r.status, ResolveStatus::Answered);
        assert!(r.answers.iter().all(|a| a.rtype == "CNAME"));
        assert_eq!(r.answers.len(), 2);
        assert_eq!(r.external_cname, None);
    }

    #[test]
    fn cname_returned_for_non_address_query() {
        // A CNAME-only name queried for MX returns the CNAME (a real resolver
        // follows the alias), not NODATA. External target -> no forward for MX.
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset(
                    "www.example.com.",
                    "CNAME",
                    60,
                    &["mail.provider.net."],
                )],
            )],
        );
        let r = resolve(&acc, "www.example.com", "MX");
        assert_eq!(r.status, ResolveStatus::Answered);
        assert_eq!(r.answers.len(), 1);
        assert_eq!(r.answers[0].rtype, "CNAME");
        assert_eq!(r.external_cname, None);
    }

    #[test]
    fn external_cname_target_is_exposed_for_forwarding() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset(
                    "www.example.com.",
                    "CNAME",
                    60,
                    &["cdn.cloudfront.net."],
                )],
            )],
        );
        let r = resolve(&acc, "www.example.com", "A");
        assert_eq!(r.status, ResolveStatus::Answered);
        assert_eq!(r.answers.len(), 1);
        assert_eq!(r.answers[0].rtype, "CNAME");
        // The external target is handed back so the caller forwards it upstream.
        assert_eq!(r.external_cname.as_deref(), Some("cdn.cloudfront.net."));
    }

    #[test]
    fn nodata_when_name_exists_wrong_type() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset("app.example.com.", "A", 60, &["10.0.0.5"])],
            )],
        );
        assert_eq!(
            resolve(&acc, "app.example.com", "AAAA").status,
            ResolveStatus::NoData
        );
    }

    #[test]
    fn nxdomain_when_name_absent_in_zone() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset("app.example.com.", "A", 60, &["10.0.0.5"])],
            )],
        );
        assert_eq!(
            resolve(&acc, "nope.example.com", "A").status,
            ResolveStatus::NxDomain
        );
    }

    #[test]
    fn not_authoritative_when_no_zone() {
        let acc = accounts_with("000000000000", vec![zone("example.com.", vec![])]);
        assert_eq!(
            resolve(&acc, "registry-1.docker.io", "A").status,
            ResolveStatus::NotAuthoritative
        );
    }

    #[test]
    fn longest_zone_wins() {
        let acc = accounts_with(
            "000000000000",
            vec![
                zone(
                    "example.com.",
                    vec![rrset("app.sub.example.com.", "A", 60, &["1.1.1.1"])],
                ),
                zone(
                    "sub.example.com.",
                    vec![rrset("app.sub.example.com.", "A", 60, &["2.2.2.2"])],
                ),
            ],
        );
        // The more-specific zone (sub.example.com) is authoritative.
        let r = resolve(&acc, "app.sub.example.com", "A");
        assert_eq!(r.answers[0].value, "2.2.2.2");
    }

    #[test]
    fn suffix_not_matched_across_label_boundary() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset("app.example.com.", "A", 60, &["1.1.1.1"])],
            )],
        );
        // notexample.com is NOT in zone example.com.
        assert_eq!(
            resolve(&acc, "app.notexample.com", "A").status,
            ResolveStatus::NotAuthoritative
        );
    }

    #[test]
    fn wildcard_matches_under_queried_name() {
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset("*.example.com.", "A", 60, &["10.9.9.9"])],
            )],
        );
        let r = resolve(&acc, "anything.example.com", "A");
        assert_eq!(r.status, ResolveStatus::Answered);
        assert_eq!(r.answers.len(), 1);
        assert_eq!(r.answers[0].value, "10.9.9.9");
        // The synthesized record is owned by the queried name, not `*`.
        assert_eq!(r.answers[0].name, "anything.example.com.");
        // A wildcard covers the name, so a wrong-type query is NODATA not NXDOMAIN.
        assert_eq!(
            resolve(&acc, "anything.example.com", "AAAA").status,
            ResolveStatus::NoData
        );
    }

    #[test]
    fn wildcard_not_applied_when_exact_name_exists() {
        // app.example.com exists (A), and there is a *.example.com AAAA wildcard.
        // Per RFC 4592 the wildcard must NOT synthesize an AAAA for app: NODATA.
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![
                    rrset("app.example.com.", "A", 60, &["10.0.0.5"]),
                    rrset("*.example.com.", "AAAA", 60, &["2001:db8::1"]),
                ],
            )],
        );
        assert_eq!(
            resolve(&acc, "app.example.com", "AAAA").status,
            ResolveStatus::NoData
        );
        // A name that does not exist still gets the wildcard.
        assert_eq!(
            resolve(&acc, "other.example.com", "AAAA").answers[0].value,
            "2001:db8::1"
        );
    }

    #[test]
    fn split_horizon_returns_union() {
        // Same account, same name: a public and a private example.com. A DNS
        // query carries no VPC/account context, so a single-tenant local resolver
        // returns the union rather than guessing a view (and never NXDOMAINs a
        // name that exists in either).
        let mut acc = Route53Accounts::new();
        let st = acc.entry("000000000000");
        let mut public = zone(
            "example.com.",
            vec![rrset("app.example.com.", "A", 60, &["203.0.113.5"])],
        );
        public.id = "/hostedzone/public".to_string();
        public.private_zone = false;
        let mut private = zone(
            "example.com.",
            vec![rrset("db.example.com.", "A", 60, &["10.0.0.9"])],
        );
        private.id = "/hostedzone/private".to_string();
        private.private_zone = true;
        st.hosted_zones.insert(public.id.clone(), public);
        st.hosted_zones.insert(private.id.clone(), private);

        // A name only in the public zone still resolves (no private-zone shadow).
        assert_eq!(
            resolve(&acc, "app.example.com", "A").answers[0].value,
            "203.0.113.5"
        );
        // A name only in the private zone resolves too.
        assert_eq!(
            resolve(&acc, "db.example.com", "A").answers[0].value,
            "10.0.0.9"
        );
    }

    #[test]
    fn empty_non_terminal_is_nodata_not_wildcard() {
        // b.example.com has no records of its own but a.b.example.com exists, so
        // b is an empty non-terminal: NODATA, and the wildcard must NOT apply.
        let acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![
                    rrset("a.b.example.com.", "A", 60, &["1.2.3.4"]),
                    rrset("*.example.com.", "A", 60, &["9.9.9.9"]),
                ],
            )],
        );
        assert_eq!(
            resolve(&acc, "b.example.com", "A").status,
            ResolveStatus::NoData
        );
        // A genuinely absent name still gets the wildcard.
        assert_eq!(
            resolve(&acc, "x.example.com", "A").answers[0].value,
            "9.9.9.9"
        );
    }

    #[test]
    fn same_name_zones_across_accounts_merge() {
        // Two accounts each host example.com with different records; a query has
        // no account context, so both zones' records must be reachable.
        let mut acc = accounts_with(
            "000000000000",
            vec![zone(
                "example.com.",
                vec![rrset("a.example.com.", "A", 60, &["1.1.1.1"])],
            )],
        );
        let st = acc.entry("111111111111");
        let z = zone(
            "example.com.",
            vec![rrset("b.example.com.", "A", 60, &["2.2.2.2"])],
        );
        st.hosted_zones.insert(z.id.clone(), z);
        assert_eq!(
            resolve(&acc, "a.example.com", "A").answers[0].value,
            "1.1.1.1"
        );
        assert_eq!(
            resolve(&acc, "b.example.com", "A").answers[0].value,
            "2.2.2.2"
        );
    }

    #[test]
    fn resolves_across_accounts() {
        let mut acc = accounts_with(
            "000000000000",
            vec![zone(
                "a.example.",
                vec![rrset("x.a.example.", "A", 60, &["1.1.1.1"])],
            )],
        );
        let st = acc.entry("111111111111");
        let z = zone(
            "b.example.",
            vec![rrset("y.b.example.", "A", 60, &["2.2.2.2"])],
        );
        st.hosted_zones.insert(z.id.clone(), z);
        assert_eq!(
            resolve(&acc, "y.b.example", "A").answers[0].value,
            "2.2.2.2"
        );
    }
}
