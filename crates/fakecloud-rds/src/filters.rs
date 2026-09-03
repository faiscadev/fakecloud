//! The RDS view of the shared `Filters` parsing in `fakecloud-core`.
//!
//! DocumentDB and Neptune take the same request member with the same
//! wire form and the same `arn:aws:rds:` identifiers, so the parsing and
//! the identifier helpers live in `fakecloud_core::query_filters` and are
//! re-exported here under the name RDS call sites already use.

pub(crate) use fakecloud_core::query_filters::{
    addresses_own_account, identifier_account, identifier_matches_type, normalized_identifier,
    optional_flag, parse_filters, requested_identifier, sibling_rds_arn, warn_unknown_filters,
    QueryFilter as RdsFilter,
};
