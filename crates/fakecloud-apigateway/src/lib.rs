//! API Gateway v1 (REST APIs) implementation.
//!
//! Distinct from `fakecloud-apigatewayv2` (HTTP APIs). The v1 surface
//! uses REST-style URLs (`POST /restapis`, `GET /restapis/{id}/...`)
//! and a different resource hierarchy: REST APIs own a tree of
//! resources, each with methods, integrations, method/integration
//! responses; deployments snapshot the API and stages point at them.
//!
//! Lambda integrations re-use the `DeliveryBus::invoke_lambda` path
//! already used by API Gateway v2 and EventBridge — same envelope,
//! different version field (`event.version = "1.0"`).

pub mod data_plane;
pub mod dispatch;
pub mod facade;
pub mod lambda_proxy;
pub mod model_validation;
pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod validation;
pub mod vtl;

pub use facade::ApiGatewayFacade;
pub use state::{
    make_id, ApiGatewaySnapshot, ApiGatewayState, ApiKey, AuthEffect, Authorizer,
    CachedAuthorizerResult, Deployment, Integration, Method, Model, Resource, RestApi,
    SharedApiGatewayState, Stage, UsagePlan, APIGATEWAY_SNAPSHOT_SCHEMA_VERSION,
};

pub use service::ApiGatewayService;
