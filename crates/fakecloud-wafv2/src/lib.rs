pub mod evaluator;
pub mod inspection;
pub(crate) mod service;
pub(crate) mod state;

pub use evaluator::{
    evaluate, evaluate_detailed, evaluate_web_acl, RateLimiter, WafAction, WafEvaluation,
    WafRequest, WafVerdict, FAKECLOUD_GEO_COUNTRY_HEADER,
};
pub use inspection::{evaluate_request, Decision, RequestContext, DEFAULT_BODY_INSPECTION_LIMIT};
pub use service::Wafv2Service;
pub use state::{
    AccountState, IpSet, RegexPatternSet, RuleGroup, ScopedKey, SharedWafv2State, Wafv2Accounts,
    WebAcl,
};
