//! Amazon Route 53 Resolver (`route53resolver`) implementation for FakeCloud.
//!
//! Real control plane for the whole Route 53 Resolver surface: resolver
//! endpoints (validated against real EC2 VPC subnets + security groups),
//! resolver rules and their VPC associations, query-log configurations and
//! associations, DNS Firewall rule groups / domain lists / rules / associations,
//! the per-VPC firewall / resolver / DNSSEC configuration singletons, Outpost
//! resolvers, resource-based policies and tags. State machines
//! (endpoint `CREATING`->`OPERATIONAL`, association settle, deletion guards)
//! mirror AWS.
//!
//! DNS query forwarding / filtering at endpoints (the Resolver "data plane") is
//! not implemented — see the service doc. Everything terraform-provider-aws and
//! typical users exercise is control-plane.

pub mod persistence;
pub mod state;
pub mod validate;

mod service;

pub use persistence::save_route53resolver_snapshot;
pub use service::Route53ResolverService;
pub use state::{
    AccountState, EndpointRecord, FirewallConfig, FirewallDomainList, FirewallRule,
    FirewallRuleGroup, FirewallRuleGroupAssociation, IpAddressResponse, OutpostResolver,
    ResolverConfig, ResolverDnssecConfig, ResolverEndpoint, ResolverQueryLogConfig,
    ResolverQueryLogConfigAssociation, ResolverRule, ResolverRuleAssociation,
    Route53ResolverAccounts, Route53ResolverSnapshot, SharedRoute53ResolverState, Tag,
    TargetAddress, R53R_SNAPSHOT_SCHEMA_VERSION,
};
