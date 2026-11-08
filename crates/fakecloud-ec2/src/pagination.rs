//! `MaxResults` / `NextToken` paging for every paginated EC2 operation.
//!
//! The model gives more than two hundred EC2 operations a `NextToken` input
//! (see [`PAGED_OPS`], generated from `aws-models/ec2.json`). Each one is paged
//! here, at the dispatch boundary, the same way: the request's `MaxResults`
//! and `NextToken` are validated before the handler runs, and the handler's
//! full, deterministically ordered result set (state lives in `BTreeMap`s) is
//! then sliced to the requested window, with an opaque `nextToken` emitted only
//! when items remain.
//!
//! A handful of handlers page themselves (their item order or cursor needs
//! more than an offset over the rendered set); they are listed in
//! [`HANDLER_PAGED`] and only get the request validation.

use bytes::Bytes;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError, ResponseBody};

use crate::pagination_table::PAGED_OPS;
use crate::service_helpers::{
    decode_page_token, encode_page_token, invalid_parameter_value, parse_page_size,
};

/// How a result with several list members is paged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetLayout {
    /// The lists form one sequence in the order given: a page fills from the
    /// first list, then the next.
    Concat,
    /// The lists are index-aligned views of the same items and are sliced
    /// with the same window.
    Parallel,
}

/// One paginated operation: its result set element(s) and the model's
/// `@range` on `MaxResults`, if any.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PagedOp {
    pub action: &'static str,
    pub sets: &'static [&'static str],
    pub layout: SetLayout,
    pub max_results: Option<(i64, i64)>,
}

/// Operations whose handlers apply `MaxResults` / `NextToken` themselves.
/// Paging their output again here would slice an already-sliced page.
pub(crate) const HANDLER_PAGED: &[&str] = &[
    "DescribeApplicationStatus",
    "DescribeApplicationStatusCheckAssociations",
    "DescribeApplicationStatusChecks",
    "DescribeCapacityReservationDateChangeQuotes",
    "DescribeImages",
    "DescribeInstanceStatus",
    "DescribeInstanceTypeOfferings",
    "DescribeInstanceTypes",
    "DescribeInstances",
    "DescribeIpamInternetRegistryAssociations",
    "DescribeNetworkInterfaces",
    "DescribeSecurityGroups",
    "DescribeSnapshots",
    "DescribeSubnets",
    "DescribeTags",
    "DescribeVolumes",
    "DescribeVpcs",
    "GetInstanceTypesFromInstanceRequirements",
    "GetIpamDiscoveredRoutes",
    "GetIpamInternetRegistryAssociationAsns",
    "GetIpamInternetRegistryAssociationCidrs",
    "GetIpamRouteOriginAuthorizations",
    "GetIpamRouteProtectionFindings",
    "GetIpamRoutingPolicyRegistrationDeltas",
    "GetIpamRoutingPolicyRegistrations",
];

/// Handler-paged operations whose `NextToken` is not the shared offset token
/// (a `reverse` registration-delta listing resumes at a delta id), so the
/// handler validates it.
const CUSTOM_TOKEN: &[&str] = &["GetIpamRoutingPolicyRegistrationDeltas"];

/// The paging metadata for `action`, if the model paginates it.
pub(crate) fn paged_op(action: &str) -> Option<&'static PagedOp> {
    PAGED_OPS
        .binary_search_by(|op| op.action.cmp(action))
        .ok()
        .map(|i| &PAGED_OPS[i])
}

/// The window a request asks for, once validated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PageRequest {
    start: usize,
    size: Option<usize>,
}

/// Validate a paginated request's `MaxResults` and `NextToken` before the
/// handler runs. Returns the window to slice the handler's result to, or
/// `None` when the operation is not paginated, its handler pages itself, or
/// the request asks for the whole set.
pub(crate) fn validate_request(
    req: &AwsRequest,
) -> Result<Option<(PageRequest, &'static PagedOp)>, AwsServiceError> {
    let Some(op) = paged_op(&req.action) else {
        return Ok(None);
    };
    // The model's `@range` bounds MaxResults; without one, a page must hold
    // at least one item. A range that admits 0 (DescribeFastSnapshotRestores,
    // DescribeFastLaunchImages, DescribeAwsNetworkPerformanceMetricSubscriptions)
    // takes it as "no limit": a zero-item page could never advance its token.
    let (min, max) = match op.max_results {
        Some((min, max)) => (min.max(0), max),
        None => (1, i64::MAX),
    };
    let size = match parse_page_size(&req.query_params)? {
        Some(n) if n < min || n > max => {
            return Err(invalid_parameter_value(match (min, max) {
                (_, i64::MAX) => format!("MaxResults must be at least {min}"),
                _ => format!("MaxResults must be between {min} and {max}"),
            }));
        }
        Some(0) | None => None,
        Some(n) => Some(n as usize),
    };
    let token = req.query_params.get("NextToken").filter(|t| !t.is_empty());
    if CUSTOM_TOKEN.contains(&op.action) {
        return Ok(None);
    }
    let start = token.map(|t| decode_page_token(t)).transpose()?;
    if HANDLER_PAGED.contains(&op.action) || (size.is_none() && start.is_none()) {
        return Ok(None);
    }
    Ok(Some((
        PageRequest {
            start: start.unwrap_or(0),
            size,
        },
        op,
    )))
}

/// Slice a successful response's result set(s) to `page` and add the
/// `nextToken` that fetches the rest.
pub(crate) fn apply(resp: AwsResponse, page: PageRequest, op: &PagedOp) -> AwsResponse {
    if !resp.status.is_success() {
        return resp;
    }
    let ResponseBody::Bytes(bytes) = &resp.body else {
        return resp;
    };
    let Ok(xml) = std::str::from_utf8(bytes) else {
        return resp;
    };
    let Some(paged) = page_xml(xml, page, op) else {
        return resp;
    };
    AwsResponse {
        body: ResponseBody::Bytes(Bytes::from(paged)),
        ..resp
    }
}

/// Re-render `xml` (an `ec2Query` response document) with its set elements
/// sliced to `page`. `None` when the document is not shaped as expected.
fn page_xml(xml: &str, page: PageRequest, op: &PagedOp) -> Option<String> {
    let root = elements(xml, 0, xml.len()).into_iter().next()?;
    let (inner_start, inner_end) = root.content?;
    let children = elements(xml, inner_start, inner_end);

    // Each paged set, with its `<item>` spans.
    let mut sets: Vec<(&Element, Vec<Element>)> = Vec::new();
    for name in op.sets {
        if let Some(el) = children.iter().find(|c| c.name == *name) {
            let items = match el.content {
                Some((s, e)) => elements(xml, s, e)
                    .into_iter()
                    .filter(|i| i.name == "item")
                    .collect(),
                None => Vec::new(),
            };
            sets.push((el, items));
        }
    }

    // Which items of each set the page keeps, and whether any remain.
    let mut keep: Vec<(usize, usize)> = Vec::with_capacity(sets.len());
    let total: usize;
    match op.layout {
        SetLayout::Parallel => {
            total = sets.iter().map(|(_, i)| i.len()).max().unwrap_or(0);
            let (s, e) = window(page, total);
            for (_, items) in &sets {
                keep.push((s.min(items.len()), e.min(items.len())));
            }
        }
        SetLayout::Concat => {
            total = sets.iter().map(|(_, i)| i.len()).sum();
            let (s, e) = window(page, total);
            let mut offset = 0;
            for (_, items) in &sets {
                let lo = s.saturating_sub(offset).min(items.len());
                let hi = e.saturating_sub(offset).min(items.len());
                keep.push((lo, hi.max(lo)));
                offset += items.len();
            }
        }
    }
    let end = window(page, total).1;
    let next = (end < total).then(|| encode_page_token(end));

    // Splice the document back together: sliced sets in place, any stale
    // top-level `nextToken` dropped, the new one before the root closes.
    let mut edits: Vec<(usize, usize, String)> = Vec::new();
    for ((el, items), (lo, hi)) in sets.iter().zip(&keep) {
        let body: String = items[*lo..*hi]
            .iter()
            .map(|i| &xml[i.start..i.end])
            .collect();
        let rendered = if body.is_empty() {
            format!("<{}/>", el.name)
        } else {
            format!("<{0}>{body}</{0}>", el.name)
        };
        edits.push((el.start, el.end, rendered));
    }
    for stale in children.iter().filter(|c| c.name == "nextToken") {
        edits.push((stale.start, stale.end, String::new()));
    }
    if let Some(t) = next {
        edits.push((
            inner_end,
            inner_end,
            fakecloud_aws::ec2query::ec2_elem("nextToken", &t),
        ));
    }
    edits.sort_by_key(|e| e.0);
    let mut out = String::with_capacity(xml.len());
    let mut cursor = 0;
    for (s, e, text) in edits {
        out.push_str(&xml[cursor..s]);
        out.push_str(&text);
        cursor = e;
    }
    out.push_str(&xml[cursor..]);
    Some(out)
}

/// The `[start, end)` slice of a `total`-item sequence a page covers.
fn window(page: PageRequest, total: usize) -> (usize, usize) {
    let start = page.start.min(total);
    let end = page.size.map_or(total, |n| (start + n).min(total));
    (start, end)
}

/// One element in a document: its name, its full span, and the span of its
/// content (`None` for a self-closing element).
#[derive(Debug)]
struct Element {
    name: String,
    start: usize,
    end: usize,
    content: Option<(usize, usize)>,
}

/// The elements directly inside `xml[from..to]`. Text content in these
/// documents is entity-escaped, so a bare `<` always opens a tag.
fn elements(xml: &str, from: usize, to: usize) -> Vec<Element> {
    let bytes = xml.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut open: Option<(String, usize, usize)> = None;
    let mut pos = from;
    while pos < to {
        let Some(lt) = xml[pos..to].find('<').map(|i| pos + i) else {
            break;
        };
        let Some(gt) = tag_end(bytes, lt, to) else {
            break;
        };
        let tag = &xml[lt + 1..gt];
        pos = gt + 1;
        if tag.starts_with('?') || tag.starts_with('!') {
            continue;
        }
        if let Some(name) = tag.strip_prefix('/') {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                if let Some((open_name, start, content_start)) = open.take() {
                    if open_name == name.trim() {
                        out.push(Element {
                            name: open_name,
                            start,
                            end: pos,
                            content: Some((content_start, lt)),
                        });
                    }
                }
            }
            continue;
        }
        let self_closing = tag.ends_with('/');
        let name = tag
            .trim_end_matches('/')
            .split(|c: char| c.is_whitespace())
            .next()
            .unwrap_or("")
            .to_string();
        if depth == 0 {
            if self_closing {
                out.push(Element {
                    name,
                    start: lt,
                    end: pos,
                    content: None,
                });
                continue;
            }
            open = Some((name, lt, pos));
        }
        if !self_closing {
            depth += 1;
        }
    }
    out
}

/// The index of the `>` closing the tag that opens at `lt`, skipping any `>`
/// inside a quoted attribute value.
fn tag_end(bytes: &[u8], lt: usize, to: usize) -> Option<usize> {
    let mut quote: Option<u8> = None;
    for (i, &b) in bytes.iter().enumerate().take(to).skip(lt + 1) {
        match (quote, b) {
            (Some(q), _) if b == q => quote = None,
            (Some(_), _) => {}
            (None, b'"' | b'\'') => quote = Some(b),
            (None, b'>') => return Some(i),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(sets: &'static [&'static str], layout: SetLayout) -> PagedOp {
        PagedOp {
            action: "DescribeThings",
            sets,
            layout,
            max_results: None,
        }
    }

    fn doc(body: &str) -> String {
        fakecloud_aws::ec2query::ec2_response("DescribeThings", "rid", body)
    }

    fn list(name: &str, n: std::ops::Range<usize>) -> String {
        let items: Vec<String> = n.map(|i| format!("<id>t-{i}</id>")).collect();
        fakecloud_aws::ec2query::ec2_list(name, &items)
    }

    fn token_of(xml: &str) -> Option<String> {
        xml.split("<nextToken>")
            .nth(1)
            .and_then(|s| s.split("</nextToken>").next())
            .map(str::to_string)
    }

    #[test]
    fn table_is_sorted_and_covers_the_handler_paged_ops() {
        assert!(
            PAGED_OPS.windows(2).all(|w| w[0].action < w[1].action),
            "PAGED_OPS must stay sorted for binary search"
        );
        for action in HANDLER_PAGED.iter().chain(CUSTOM_TOKEN) {
            assert!(paged_op(action).is_some(), "{action} is not paginated");
        }
    }

    /// The generated table still lists exactly the operations the vendored
    /// model paginates (rerun `scripts/generate-ec2-pagination-table.py` after
    /// a model refresh).
    #[test]
    fn table_matches_the_model() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../aws-models/ec2.json");
        let model: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).expect("read aws-models/ec2.json"))
                .unwrap();
        let shapes = model["shapes"].as_object().unwrap();
        let mut from_model: Vec<&str> = shapes
            .iter()
            .filter(|(_, s)| s["type"] == "operation")
            .filter(|(_, s)| {
                s["input"]["target"]
                    .as_str()
                    .and_then(|t| shapes.get(t))
                    .is_some_and(|i| i["members"].get("NextToken").is_some())
            })
            .map(|(id, _)| id.rsplit('#').next().unwrap())
            .collect();
        from_model.sort_unstable();
        let table: Vec<&str> = PAGED_OPS.iter().map(|op| op.action).collect();
        assert_eq!(
            table, from_model,
            "rerun scripts/generate-ec2-pagination-table.py"
        );
    }

    #[test]
    fn pages_a_single_set_and_round_trips_the_token() {
        let op = op(&["thingSet"], SetLayout::Concat);
        let xml = doc(&list("thingSet", 0..7));
        let page = PageRequest {
            start: 0,
            size: Some(5),
        };
        let first = page_xml(&xml, page, &op).unwrap();
        assert!(first.contains("t-4") && !first.contains("t-5"), "{first}");
        let token = token_of(&first).expect("partial page carries a token");
        let start = decode_page_token(&token).unwrap();
        let second = page_xml(
            &xml,
            PageRequest {
                start,
                size: Some(5),
            },
            &op,
        )
        .unwrap();
        assert!(second.contains("t-5") && second.contains("t-6"), "{second}");
        assert!(!second.contains("t-4"), "{second}");
        assert!(token_of(&second).is_none(), "last page has no token");
        assert!(second.ends_with("</DescribeThingsResponse>"), "{second}");
    }

    #[test]
    fn nested_sets_with_the_same_name_are_not_paged() {
        // Only the top-level set is the result; an inner `itemSet` belongs to
        // one item.
        let op = op(&["thingSet"], SetLayout::Concat);
        let inner = list("thingSet", 0..3);
        let body = format!("<thingSet><item>{inner}</item><item><id>x</id></item></thingSet>");
        let out = page_xml(
            &doc(&body),
            PageRequest {
                start: 0,
                size: Some(1),
            },
            &op,
        )
        .unwrap();
        assert!(out.contains("t-2"), "the first item stays whole: {out}");
        assert!(!out.contains("<id>x</id>"), "{out}");
        assert!(token_of(&out).is_some());
    }

    #[test]
    fn concatenated_sets_page_as_one_sequence() {
        let op = op(&["aSet", "bSet"], SetLayout::Concat);
        let xml = doc(&format!("{}{}", list("aSet", 0..3), list("bSet", 10..13)));
        let page = |start| {
            page_xml(
                &xml,
                PageRequest {
                    start,
                    size: Some(2),
                },
                &op,
            )
            .unwrap()
        };
        let p1 = page(0);
        assert!(p1.contains("t-0") && p1.contains("t-1") && p1.contains("<bSet/>"));
        let p2 = page(2);
        assert!(p2.contains("t-2") && p2.contains("t-10") && !p2.contains("t-11"));
        let p3 = page(4);
        assert!(p3.contains("<aSet/>") && p3.contains("t-11") && p3.contains("t-12"));
        assert!(token_of(&p3).is_none());
    }

    #[test]
    fn parallel_sets_share_one_window() {
        let op = op(&["aSet", "bSet"], SetLayout::Parallel);
        let xml = doc(&format!("{}{}", list("aSet", 0..4), list("bSet", 10..14)));
        let out = page_xml(
            &xml,
            PageRequest {
                start: 1,
                size: Some(2),
            },
            &op,
        )
        .unwrap();
        assert!(out.contains("t-1") && out.contains("t-2") && !out.contains("t-3"));
        assert!(out.contains("t-11") && out.contains("t-12") && !out.contains("t-13"));
        assert!(token_of(&out).is_some());
    }

    #[test]
    fn a_missing_or_empty_set_pages_to_nothing() {
        let op = op(&["thingSet"], SetLayout::Concat);
        let out = page_xml(
            &doc("<thingSet/>"),
            PageRequest {
                start: 0,
                size: Some(5),
            },
            &op,
        )
        .unwrap();
        assert!(
            out.contains("<thingSet/>") && token_of(&out).is_none(),
            "{out}"
        );
    }
}

/// Paging exercised through the service's dispatch, the way a client sees it.
#[cfg(test)]
mod dispatch_tests {
    use super::*;
    use crate::service::Ec2Service;
    use crate::test_support::ec2_request;
    use fakecloud_core::service::AwsService;

    async fn call(
        svc: &Ec2Service,
        action: &str,
        params: &[(&str, &str)],
    ) -> Result<String, AwsServiceError> {
        let resp = svc.handle(ec2_request(action, params)).await?;
        let ResponseBody::Bytes(b) = &resp.body else {
            panic!("{action}: unexpected streamed body");
        };
        Ok(String::from_utf8(b.to_vec()).unwrap())
    }

    async fn ok(svc: &Ec2Service, action: &str, params: &[(&str, &str)]) -> String {
        match call(svc, action, params).await {
            Ok(xml) => xml,
            Err(e) => panic!("{action} failed: {} {}", e.code(), e.message()),
        }
    }

    fn first(xml: &str, tag: &str) -> Option<String> {
        xml.split(&format!("<{tag}>"))
            .nth(1)
            .and_then(|s| s.split(&format!("</{tag}>")).next())
            .map(str::to_string)
    }

    /// The top-level `nextToken` of a response, if any.
    fn next_token(xml: &str) -> Option<String> {
        let root = elements(xml, 0, xml.len()).into_iter().next()?;
        let (s, e) = root.content?;
        let el = elements(xml, s, e)
            .into_iter()
            .find(|c| c.name == "nextToken")?;
        let (cs, ce) = el.content?;
        Some(xml[cs..ce].to_string())
    }

    /// The `<item>`s of each of `op`'s result sets, in document order.
    fn set_items(xml: &str, op: &PagedOp) -> Vec<Vec<String>> {
        let root = elements(xml, 0, xml.len()).into_iter().next().unwrap();
        let (s, e) = root.content.unwrap();
        let children = elements(xml, s, e);
        op.sets
            .iter()
            .map(|name| match children.iter().find(|c| c.name == *name) {
                Some(Element {
                    content: Some((cs, ce)),
                    ..
                }) => elements(xml, *cs, *ce)
                    .into_iter()
                    .filter(|i| i.name == "item")
                    .map(|i| xml[i.start..i.end].to_string())
                    .collect(),
                _ => Vec::new(),
            })
            .collect()
    }

    /// Seven of each resource the default listings below page over.
    async fn seeded() -> Ec2Service {
        let svc = Ec2Service::new();
        let mut vpcs = Vec::new();
        for i in 0..7 {
            let cidr = format!("10.{i}.0.0/16");
            let xml = ok(&svc, "CreateVpc", &[("CidrBlock", &cidr)]).await;
            vpcs.push(first(&xml, "vpcId").unwrap());
            ok(&svc, "CreateInternetGateway", &[]).await;
            ok(&svc, "CreateTransitGateway", &[]).await;
            let domain = format!("d{i}.example");
            ok(
                &svc,
                "CreateDhcpOptions",
                &[
                    ("DhcpConfiguration.1.Key", "domain-name"),
                    ("DhcpConfiguration.1.Value.1", &domain),
                ],
            )
            .await;
        }
        for vpc in &vpcs {
            ok(&svc, "CreateRouteTable", &[("VpcId", vpc)]).await;
            ok(&svc, "CreateNetworkAcl", &[("VpcId", vpc)]).await;
        }
        svc
    }

    /// Every paginated operation that answers without arguments pages its
    /// listing losslessly: walking the tokens at the smallest page size the
    /// model allows yields exactly the unpaged result, each page within the
    /// size, with a token on every page but the last.
    #[tokio::test]
    async fn every_paginated_default_listing_pages_losslessly() {
        let svc = seeded().await;
        let mut paged_over_several_pages = Vec::new();
        for op in PAGED_OPS {
            if HANDLER_PAGED.contains(&op.action) || op.sets.is_empty() {
                continue;
            }
            let Ok(full_xml) = call(&svc, op.action, &[]).await else {
                continue;
            };
            assert!(
                next_token(&full_xml).is_none(),
                "{}: unpaged result carries a token",
                op.action
            );
            let full = set_items(&full_xml, op);
            let size = op.max_results.map_or(1, |(min, _)| min.max(1) as usize);
            let size_s = size.to_string();
            let mut walked: Vec<Vec<String>> = vec![Vec::new(); op.sets.len()];
            let mut token: Option<String> = None;
            let mut pages = 0;
            loop {
                pages += 1;
                assert!(
                    pages <= 10_000,
                    "{}: pagination does not terminate",
                    op.action
                );
                let mut params = vec![("MaxResults", size_s.as_str())];
                if let Some(t) = &token {
                    params.push(("NextToken", t.as_str()));
                }
                let xml = ok(&svc, op.action, &params).await;
                let page = set_items(&xml, op);
                let count = match op.layout {
                    SetLayout::Concat => page.iter().map(Vec::len).sum(),
                    SetLayout::Parallel => page.iter().map(Vec::len).max().unwrap_or(0),
                };
                assert!(
                    count <= size,
                    "{}: page of {count} exceeds {size}",
                    op.action
                );
                for (acc, items) in walked.iter_mut().zip(page) {
                    acc.extend(items);
                }
                token = next_token(&xml);
                if token.is_none() {
                    break;
                }
                assert!(count > 0, "{}: empty page with a token", op.action);
            }
            assert_eq!(
                walked, full,
                "{}: paged walk differs from the full listing",
                op.action
            );
            if pages > 1 {
                paged_over_several_pages.push(op.action);
            }
        }
        for expected in [
            "DescribeDhcpOptions",
            "DescribeInternetGateways",
            "DescribeNetworkAcls",
            "DescribeRouteTables",
            "DescribeTransitGateways",
            "DescribeVpcEndpointServices",
        ] {
            assert!(
                paged_over_several_pages.contains(&expected),
                "{expected} should have spanned several pages; paged: {paged_over_several_pages:?}"
            );
        }
    }

    /// Every paginated operation reads `NextToken`: one this server never
    /// minted is rejected before the handler runs, not silently taken as
    /// "start from the top".
    #[tokio::test]
    async fn every_paginated_op_rejects_a_foreign_token() {
        let svc = Ec2Service::new();
        for op in PAGED_OPS {
            if CUSTOM_TOKEN.contains(&op.action) {
                continue;
            }
            let err = call(&svc, op.action, &[("NextToken", "not-a-token")])
                .await
                .err()
                .unwrap_or_else(|| panic!("{} accepted a foreign NextToken", op.action));
            assert_eq!(err.code(), "InvalidParameterValue", "{}", op.action);
            assert!(
                err.message().contains("nextToken"),
                "{}: {}",
                op.action,
                err.message()
            );
        }
    }

    /// `MaxResults` must be a positive integer inside the model's `@range`.
    #[tokio::test]
    async fn every_paginated_op_bounds_max_results() {
        let svc = Ec2Service::new();
        for op in PAGED_OPS {
            let zero_allowed = op.max_results.is_some_and(|(min, _)| min <= 0);
            let bad: &[&str] = if zero_allowed {
                &["-3", "many"]
            } else {
                &["0", "-3", "many"]
            };
            for &bad in bad {
                let err = call(&svc, op.action, &[("MaxResults", bad)])
                    .await
                    .err()
                    .unwrap_or_else(|| panic!("{} accepted MaxResults={bad}", op.action));
                assert_eq!(err.code(), "InvalidParameterValue", "{} {bad}", op.action);
            }
            if let Some((min, max)) = op.max_results {
                for bad in [min.checked_sub(1), max.checked_add(1)]
                    .into_iter()
                    .flatten()
                {
                    if bad < 1 {
                        continue;
                    }
                    let bad = bad.to_string();
                    let err = call(&svc, op.action, &[("MaxResults", &bad)])
                        .await
                        .err()
                        .unwrap_or_else(|| panic!("{} accepted MaxResults={bad}", op.action));
                    assert_eq!(err.code(), "InvalidParameterValue", "{} {bad}", op.action);
                }
            }
        }
    }

    /// Where the model admits `MaxResults=0` it means "no limit": the whole
    /// listing, and never a token that cannot advance.
    #[tokio::test]
    async fn zero_max_results_is_unpaged_where_the_model_allows_it() {
        let svc = Ec2Service::new();
        let zero_ops: Vec<&PagedOp> = PAGED_OPS
            .iter()
            .filter(|op| op.max_results.is_some_and(|(min, _)| min <= 0))
            .collect();
        assert!(zero_ops.len() >= 3, "{}", zero_ops.len());
        for op in zero_ops {
            let full = call(&svc, op.action, &[]).await;
            let zero = call(&svc, op.action, &[("MaxResults", "0")]).await;
            match (full, zero) {
                (Ok(full), Ok(zero)) => {
                    assert!(next_token(&zero).is_none(), "{}: {zero}", op.action);
                    assert_eq!(set_items(&zero, op), set_items(&full, op), "{}", op.action);
                }
                (Err(a), Err(b)) => assert_eq!(a.code(), b.code(), "{}", op.action),
                (_, Err(e)) => panic!("{} rejected MaxResults=0: {}", op.action, e.message()),
                (Err(e), _) => panic!("{} failed unpaged: {}", op.action, e.message()),
            }
        }
        let window = PageRequest {
            start: 0,
            size: None,
        };
        let op = PagedOp {
            action: "DescribeThings",
            sets: &["thingSet"],
            layout: SetLayout::Concat,
            max_results: Some((0, 100)),
        };
        let xml = fakecloud_aws::ec2query::ec2_response(
            "DescribeThings",
            "rid",
            &fakecloud_aws::ec2query::ec2_list("thingSet", &["<id>a</id>".to_string()]),
        );
        let out = page_xml(&xml, window, &op).unwrap();
        assert!(next_token(&out).is_none(), "{out}");
    }

    /// Handler-paged listings are not paged a second time at dispatch: the
    /// second page at five per page holds exactly what the first left over.
    #[tokio::test]
    async fn handler_paged_listings_are_not_double_paged() {
        let svc = seeded().await;
        // Seven created plus the default VPC.
        let total = ok(&svc, "DescribeVpcs", &[])
            .await
            .matches("<item><vpcId>")
            .count();
        assert!(total > 5, "{total}");
        let first_page = ok(&svc, "DescribeVpcs", &[("MaxResults", "5")]).await;
        assert_eq!(
            first_page.matches("<item><vpcId>").count(),
            5,
            "{first_page}"
        );
        let token = next_token(&first_page).expect("token on a partial page");
        let second = ok(
            &svc,
            "DescribeVpcs",
            &[("MaxResults", "5"), ("NextToken", &token)],
        )
        .await;
        assert_eq!(
            second.matches("<item><vpcId>").count(),
            total - 5,
            "{second}"
        );
        assert!(next_token(&second).is_none(), "{second}");
    }

    /// The instance type catalog renders only the page asked for, so a walk
    /// at the smallest page size stays cheap, and it loses nothing.
    #[tokio::test]
    async fn instance_type_catalog_listings_page_losslessly() {
        let svc = Ec2Service::new();
        let requirements = [
            ("ArchitectureType.1", "x86_64"),
            ("VirtualizationType.1", "hvm"),
            ("InstanceRequirements.VCpuCount.Min", "1"),
            ("InstanceRequirements.MemoryMiB.Min", "1"),
        ];
        for (action, params, size) in [
            ("DescribeInstanceTypes", &[][..], "5"),
            ("DescribeInstanceTypeOfferings", &[][..], "5"),
            (
                "GetInstanceTypesFromInstanceRequirements",
                &requirements[..],
                "1",
            ),
        ] {
            let names = |xml: &str| -> Vec<String> {
                xml.split("<instanceType>")
                    .skip(1)
                    .map(|s| s.split('<').next().unwrap().to_string())
                    .collect()
            };
            let full_xml = ok(&svc, action, params).await;
            assert!(next_token(&full_xml).is_none(), "{action}");
            let full = names(&full_xml);
            assert!(full.len() > 100, "{action}: {}", full.len());
            let mut walked = Vec::new();
            let mut token: Option<String> = None;
            loop {
                let mut request = params.to_vec();
                request.push(("MaxResults", size));
                if let Some(t) = &token {
                    request.push(("NextToken", t));
                }
                let xml = ok(&svc, action, &request).await;
                let page = names(&xml);
                assert!(page.len() <= size.parse().unwrap(), "{action}");
                walked.extend(page);
                token = next_token(&xml);
                if token.is_none() {
                    break;
                }
            }
            assert_eq!(walked, full, "{action}");
        }
    }

    /// The reservations of both families page as one sequence.
    #[tokio::test]
    async fn subnet_cidr_reservations_page_across_both_families() {
        let svc = Ec2Service::new();
        let vpc = first(
            &ok(&svc, "CreateVpc", &[("CidrBlock", "10.0.0.0/16")]).await,
            "vpcId",
        )
        .unwrap();
        let subnet = first(
            &ok(
                &svc,
                "CreateSubnet",
                &[("VpcId", &vpc), ("CidrBlock", "10.0.0.0/24")],
            )
            .await,
            "subnetId",
        )
        .unwrap();
        for i in 0..7 {
            let cidr = format!("10.0.0.{}/28", i * 16);
            ok(
                &svc,
                "CreateSubnetCidrReservation",
                &[
                    ("SubnetId", &subnet),
                    ("Cidr", &cidr),
                    ("ReservationType", "prefix"),
                ],
            )
            .await;
        }
        let op = paged_op("GetSubnetCidrReservations").unwrap();
        let full = set_items(&ok(&svc, op.action, &[("SubnetId", &subnet)]).await, op);
        assert_eq!(full[0].len(), 7);
        let p1 = ok(
            &svc,
            op.action,
            &[("SubnetId", &subnet), ("MaxResults", "5")],
        )
        .await;
        assert_eq!(set_items(&p1, op)[0].len(), 5);
        let token = next_token(&p1).unwrap();
        let p2 = ok(
            &svc,
            op.action,
            &[
                ("SubnetId", &subnet),
                ("MaxResults", "5"),
                ("NextToken", &token),
            ],
        )
        .await;
        assert_eq!(set_items(&p2, op)[0].len(), 2);
        assert!(next_token(&p2).is_none());
    }
}
