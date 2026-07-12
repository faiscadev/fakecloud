//! Primitives shared across the Amazon Managed Blockchain (`managedblockchain`)
//! handlers: resource-id minting, ARN synthesis, deterministic framework
//! endpoint derivation and timestamps. Kept in one place so the create / get
//! paths cannot diverge on wire format.

use rand::Rng;

/// Mint a fresh `26`-character uppercase-alphanumeric resource suffix, the
/// form Managed Blockchain uses for network / member / node / proposal ids.
fn suffix26() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    (0..26)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect()
}

/// A network id, `n-<26 uppercase alnum>`.
pub fn new_network_id() -> String {
    format!("n-{}", suffix26())
}

/// A member id, `m-<26 uppercase alnum>`.
pub fn new_member_id() -> String {
    format!("m-{}", suffix26())
}

/// A node id, `nd-<26 uppercase alnum>`.
pub fn new_node_id() -> String {
    format!("nd-{}", suffix26())
}

/// A proposal id, `p-<26 uppercase alnum>`.
pub fn new_proposal_id() -> String {
    format!("p-{}", suffix26())
}

/// An invitation id, `i-<26 uppercase alnum>`.
pub fn new_invitation_id() -> String {
    format!("i-{}", suffix26())
}

/// An accessor id (a UUID, matching the real accessor id form).
pub fn new_accessor_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// An accessor billing token (an opaque `40`-char base62 token).
pub fn new_billing_token() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..40)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect()
}

/// The network ARN. Managed Blockchain networks are global, so the ARN carries
/// no region and no account.
pub fn network_arn(id: &str) -> String {
    format!("arn:aws:managedblockchain:::networks/{id}")
}

/// The member ARN, `arn:aws:managedblockchain:{region}:{account}:members/{id}`.
pub fn member_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:managedblockchain:{region}:{account}:members/{id}")
}

/// The node ARN, `arn:aws:managedblockchain:{region}:{account}:nodes/{id}`.
pub fn node_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:managedblockchain:{region}:{account}:nodes/{id}")
}

/// The proposal ARN,
/// `arn:aws:managedblockchain:{region}:{account}:proposals/{id}`.
pub fn proposal_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:managedblockchain:{region}:{account}:proposals/{id}")
}

/// The invitation ARN,
/// `arn:aws:managedblockchain:{region}:{account}:invitations/{id}`.
pub fn invitation_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:managedblockchain:{region}:{account}:invitations/{id}")
}

/// The accessor ARN,
/// `arn:aws:managedblockchain:{region}:{account}:accessors/{id}`.
pub fn accessor_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:managedblockchain:{region}:{account}:accessors/{id}")
}

/// Current time as an ISO-8601 `date-time` string (the format every Managed
/// Blockchain timestamp member declares).
pub fn iso_now() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// The Hyperledger Fabric ordering-service endpoint a network advertises.
/// Deterministically derived from the network id; well-formed but pointing at
/// no live orderer (see the crate-level honest gap).
pub fn fabric_ordering_endpoint(network_id: &str, region: &str) -> String {
    let short = network_id.trim_start_matches("n-").to_lowercase();
    format!("orderer.{short}.managedblockchain.{region}.amazonaws.com:30001")
}

/// The Fabric certificate-authority endpoint a member advertises.
pub fn fabric_ca_endpoint(member_id: &str, region: &str) -> String {
    let short = member_id.trim_start_matches("m-").to_lowercase();
    format!("ca.{short}.managedblockchain.{region}.amazonaws.com:30002")
}

/// The Fabric peer endpoint a node advertises.
pub fn fabric_peer_endpoint(node_id: &str, region: &str) -> String {
    let short = node_id.trim_start_matches("nd-").to_lowercase();
    format!("{short}.managedblockchain.{region}.amazonaws.com:30003")
}

/// The Fabric peer-event endpoint a node advertises.
pub fn fabric_peer_event_endpoint(node_id: &str, region: &str) -> String {
    let short = node_id.trim_start_matches("nd-").to_lowercase();
    format!("{short}.managedblockchain.{region}.amazonaws.com:30004")
}

/// The Ethereum JSON-RPC HTTP endpoint a node advertises.
pub fn ethereum_http_endpoint(node_id: &str, region: &str) -> String {
    let short = node_id.trim_start_matches("nd-").to_lowercase();
    format!("https://{short}.ethereum.managedblockchain.{region}.amazonaws.com")
}

/// The Ethereum JSON-RPC WebSocket endpoint a node advertises.
pub fn ethereum_ws_endpoint(node_id: &str, region: &str) -> String {
    let short = node_id.trim_start_matches("nd-").to_lowercase();
    format!("wss://{short}.wss.ethereum.managedblockchain.{region}.amazonaws.com")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_have_expected_prefixes() {
        assert!(new_network_id().starts_with("n-"));
        assert_eq!(new_network_id().len(), 28);
        assert!(new_member_id().starts_with("m-"));
        assert!(new_node_id().starts_with("nd-"));
        assert!(new_proposal_id().starts_with("p-"));
        assert!(new_invitation_id().starts_with("i-"));
        // Accessor ids are UUIDs.
        assert_eq!(new_accessor_id().len(), 36);
        assert_eq!(new_billing_token().len(), 40);
    }
}
