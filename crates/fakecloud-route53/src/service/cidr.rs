//! Route53 `cidr` family extracted from service.rs by audit-2026-05-19.

use super::*;

impl Route53Service {
    pub(super) fn create_cidr_collection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cfg: CreateCidrCollectionRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid CreateCidrCollectionRequest XML: {e}"))
        })?;
        if cfg.name.is_empty() || cfg.caller_reference.is_empty() {
            return Err(invalid_argument("Name and CallerReference are required"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .entry(DEFAULT_ACCOUNT.to_string())
            .or_default();
        if account
            .cidr_collections
            .values()
            .any(|c| c.name == cfg.name)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "CidrCollectionAlreadyExistsException",
                format!("A CIDR collection named '{}' already exists", cfg.name),
            ));
        }
        let id = Uuid::new_v4().to_string();
        // CIDR-collection ARNs omit the account id: arn:aws:route53:::cidrcollection/<id>.
        let arn = Arn::global("route53", "", &format!("cidrcollection/{id}")).to_string();
        let stored = StoredCidrCollection {
            id: id.clone(),
            name: cfg.name,
            arn: arn.clone(),
            version: 1,
            caller_reference: cfg.caller_reference,
            locations: BTreeMap::new(),
        };
        account.cidr_collections.insert(id.clone(), stored.clone());
        drop(state);
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<CreateCidrCollectionResponse xmlns=\"{NS}\">"));
        push_cidr_collection_full(&mut body, &stored);
        body.push_str("<Location>");
        body.push_str(&format!("<Arn>{}</Arn>", esc(&arn)));
        body.push_str("</Location>");
        body.push_str("</CreateCidrCollectionResponse>");
        let mut headers = HeaderMap::new();
        if let Ok(loc) =
            http::HeaderValue::from_str(&format!("/2013-04-01/cidrcollection/{}", stored.id))
        {
            headers.insert(http::header::LOCATION, loc);
        }
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    pub(super) fn change_cidr_collection(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let cfg: ChangeCidrCollectionRequest = xml_io::from_xml_root(&req.body).map_err(|e| {
            invalid_argument(format!("invalid ChangeCidrCollectionRequest XML: {e}"))
        })?;
        if cfg.changes.change.is_empty() {
            return Err(invalid_argument("Changes must contain at least one entry"));
        }
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        let coll = account
            .cidr_collections
            .get_mut(&id)
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        if let Some(client_v) = cfg.collection_version {
            if client_v != coll.version {
                return Err(aws_error(
                    StatusCode::CONFLICT,
                    "CidrCollectionVersionMismatchException",
                    format!(
                        "CollectionVersion ({}) does not match the current ({})",
                        client_v, coll.version
                    ),
                ));
            }
        }
        // Stage changes against a clone so a later invalid change rolls
        // everything back atomically.
        let mut working = coll.locations.clone();
        for ch in &cfg.changes.change {
            match ch.action.to_uppercase().as_str() {
                "PUT" => {
                    let entry = working.entry(ch.location_name.clone()).or_default();
                    for cidr in &ch.cidr_list.cidr {
                        if !entry.contains(cidr) {
                            entry.push(cidr.clone());
                        }
                    }
                    entry.sort();
                }
                "DELETE_IF_EXISTS" => {
                    if let Some(entry) = working.get_mut(&ch.location_name) {
                        entry.retain(|c| !ch.cidr_list.cidr.contains(c));
                        if entry.is_empty() {
                            working.remove(&ch.location_name);
                        }
                    }
                }
                other => {
                    return Err(invalid_argument(format!(
                        "Unknown CIDR change action: {other}"
                    )));
                }
            }
        }
        coll.locations = working;
        coll.version += 1;
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ChangeCidrCollectionResponse xmlns=\"{NS}\">"));
        body.push_str(&format!("<Id>{}</Id>", esc(&id)));
        body.push_str("</ChangeCidrCollectionResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn delete_cidr_collection(
        &self,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        let coll = account
            .cidr_collections
            .get(&id)
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        if !coll.locations.is_empty() {
            return Err(aws_error(
                StatusCode::BAD_REQUEST,
                "CidrCollectionInUseException",
                format!(
                    "CIDR collection {} still contains {} location(s)",
                    id,
                    coll.locations.len()
                ),
            ));
        }
        account.cidr_collections.remove(&id);
        drop(state);
        let mut body = String::with_capacity(128);
        body.push_str(XML_DECL);
        body.push_str(&format!("<DeleteCidrCollectionResponse xmlns=\"{NS}\"/>"));
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_cidr_collections(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(
            &req.query_params,
            &[
                QueryConstraint::StrLen {
                    key: "nexttoken",
                    min: 0,
                    max: 1024,
                },
                MAX_RESULTS_CONSTRAINT,
            ],
        )?;
        let max_items: usize = req
            .query_params
            .get("maxresults")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let mut colls: Vec<StoredCidrCollection> = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .map(|a| a.cidr_collections.values().cloned().collect())
            .unwrap_or_default();
        drop(state);
        colls.sort_by(|a, b| a.id.cmp(&b.id));
        let nexttoken = req
            .query_params
            .get("nexttoken")
            .cloned()
            .unwrap_or_default();
        // Collections are sorted by id and the token is the last id of the
        // previous page, so resume at the first collection strictly greater
        // than the token. A strict `>` keeps the pager moving forward even when
        // the token's collection was deleted between pages.
        let start = if nexttoken.is_empty() {
            0
        } else {
            colls
                .iter()
                .position(|c| c.id > nexttoken)
                .unwrap_or(colls.len())
        };
        let slice: Vec<&StoredCidrCollection> = colls.iter().skip(start).take(max_items).collect();
        let next = if start + slice.len() < colls.len() {
            slice.last().map(|c| c.id.clone())
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListCidrCollectionsResponse xmlns=\"{NS}\">"));
        body.push_str("<CidrCollections>");
        for c in &slice {
            // CollectionSummaries.member has no xmlName trait, so AWS
            // SDKs deserialize members from the default `<member>` element.
            body.push_str("<member>");
            body.push_str(&format!("<Arn>{}</Arn>", esc(&c.arn)));
            body.push_str(&format!("<Id>{}</Id>", esc(&c.id)));
            body.push_str(&format!("<Name>{}</Name>", esc(&c.name)));
            body.push_str(&format!("<Version>{}</Version>", c.version));
            body.push_str("</member>");
        }
        body.push_str("</CidrCollections>");
        if let Some(n) = &next {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(n)));
        }
        body.push_str("</ListCidrCollectionsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_cidr_locations(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let max_items: usize = req
            .query_params
            .get("maxresults")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let coll = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.cidr_collections.get(&id).cloned())
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        drop(state);
        let mut names: Vec<String> = coll.locations.keys().cloned().collect();
        names.sort();
        let nexttoken = req
            .query_params
            .get("nexttoken")
            .cloned()
            .unwrap_or_default();
        // Location names are sorted and the token is the last name of the
        // previous page, so resume at the first name strictly greater than the
        // token. A strict `>` keeps the pager moving forward even when the
        // token's location was deleted between pages.
        let start = if nexttoken.is_empty() {
            0
        } else {
            names
                .iter()
                .position(|n| n > &nexttoken)
                .unwrap_or(names.len())
        };
        let slice: Vec<&String> = names.iter().skip(start).take(max_items).collect();
        let next = if start + slice.len() < names.len() {
            slice.last().map(|n| (*n).clone())
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListCidrLocationsResponse xmlns=\"{NS}\">"));
        body.push_str("<CidrLocations>");
        for n in &slice {
            body.push_str("<member>");
            body.push_str(&format!("<LocationName>{}</LocationName>", esc(n)));
            body.push_str("</member>");
        }
        body.push_str("</CidrLocations>");
        if let Some(n) = &next {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(n)));
        }
        body.push_str("</ListCidrLocationsResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    pub(super) fn list_cidr_blocks(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = require_id(route)?;
        let location_name = req.query_params.get("location").cloned();
        let max_items: usize = req
            .query_params
            .get("maxresults")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let state = self.state.read();
        let coll = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.cidr_collections.get(&id).cloned())
            .ok_or_else(|| no_such_cidr_collection(&id))?;
        drop(state);
        let blocks: Vec<(String, String)> = coll
            .locations
            .iter()
            .filter(|(n, _)| location_name.as_ref().is_none_or(|name| name == *n))
            .flat_map(|(n, blocks)| blocks.iter().map(move |b| (n.clone(), b.clone())))
            .collect();
        let nexttoken = req
            .query_params
            .get("nexttoken")
            .cloned()
            .unwrap_or_default();
        // Token format: `<loc_len>:<location>:<cidr>`. Length-prefixing
        // the location name keeps the boundary unambiguous when the
        // location itself contains separators (`|`, `:`, etc).
        fn encode_token(loc: &str, cidr: &str) -> String {
            format!("{}:{loc}:{cidr}", loc.len())
        }
        fn decode_token(t: &str) -> Option<(&str, &str)> {
            let (len_str, rest) = t.split_once(':')?;
            let loc_len: usize = len_str.parse().ok()?;
            if rest.len() < loc_len + 1 || !rest.is_char_boundary(loc_len) {
                return None;
            }
            let (loc, after) = rest.split_at(loc_len);
            let cidr = after.strip_prefix(':')?;
            Some((loc, cidr))
        }
        // The cursor here is a compound (location, cidr) token, not a single
        // sortable key, so resume by finding the marker's position in the
        // current order and starting after it. A missing marker (its block was
        // deleted between pages) falls back to `blocks.len()` (terminate)
        // rather than 0, so the pager never restarts at page 1.
        let start = if nexttoken.is_empty() {
            0
        } else if let Some((loc, cidr)) = decode_token(&nexttoken) {
            blocks
                .iter()
                .position(|(n, b)| n == loc && b == cidr)
                .map(|p| p + 1)
                .unwrap_or(blocks.len())
        } else {
            blocks
                .iter()
                .position(|(_, b)| b == &nexttoken)
                .map(|p| p + 1)
                .unwrap_or(blocks.len())
        };
        let slice: Vec<&(String, String)> = blocks.iter().skip(start).take(max_items).collect();
        let next = if start + slice.len() < blocks.len() {
            slice.last().map(|(loc, cidr)| encode_token(loc, cidr))
        } else {
            None
        };
        let mut body = String::with_capacity(512);
        body.push_str(XML_DECL);
        body.push_str(&format!("<ListCidrBlocksResponse xmlns=\"{NS}\">"));
        body.push_str("<CidrBlocks>");
        for (loc, cidr) in &slice {
            body.push_str("<member>");
            body.push_str(&format!("<CidrBlock>{}</CidrBlock>", esc(cidr)));
            body.push_str(&format!("<LocationName>{}</LocationName>", esc(loc)));
            body.push_str("</member>");
        }
        body.push_str("</CidrBlocks>");
        if let Some(n) = &next {
            body.push_str(&format!("<NextToken>{}</NextToken>", esc(n)));
        }
        body.push_str("</ListCidrBlocksResponse>");
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}
