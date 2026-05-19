+++
title = "Local ELB for integration tests"
description = "Run local Application/Network/Gateway Load Balancer (ELBv2) for tests with fakecloud. Free, AGPL-3.0, no account."
template = "page.html"
+++

Need a local Application Load Balancer, Network Load Balancer, or Gateway Load Balancer for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud).

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`. The `elbv2` client connects exactly like real AWS — endpoint resolution + SigV4 signing on `elasticloadbalancing` work unchanged.

## Why fakecloud for ELBv2

- **All three load balancer types.** `application`, `network`, and `gateway` — `Type` is honored, ARN prefix follows AWS conventions (`app/`, `net/`, `gwy/`), DNS names are deterministic per LB.
- **Real Smithy validation.** Name length (1-32), allowed characters (alphanumeric + hyphens), `internal-` prefix rejection, `Scheme` and `IpAddressType` enum constraints — all checked exactly as AWS does, returning `ValidationError` with the same shape.
- **Idempotent `CreateLoadBalancer`.** Re-creating with the same name returns the existing load balancer instead of throwing — matches AWS's documented idempotency.
- **Tags work across every taggable resource.** `AddTags`/`RemoveTags`/`DescribeTags` operate on load balancers, target groups, listeners, rules, and trust stores.
- **`DescribeAccountLimits`** returns the AWS-published default limits for application/network/gateway LBs, target groups, targets per LB/AZ/group, listeners, rules, certificates, trust stores, and revocation entries.
- **`DescribeSSLPolicies`** returns predefined ALB security policies (`ELBSecurityPolicy-TLS13-1-2-2021-06`, `ELBSecurityPolicy-FS-1-2-Res-2020-10`, etc.) with the exact protocol + cipher lists AWS publishes.

## Smoke test

```sh
fakecloud &

# Create an ALB.
aws --endpoint-url http://localhost:4566 elbv2 create-load-balancer \
  --name my-app \
  --type application \
  --subnets subnet-aaaa subnet-bbbb

# Tag it.
LB_ARN=$(aws --endpoint-url http://localhost:4566 elbv2 describe-load-balancers \
  --names my-app --query 'LoadBalancers[0].LoadBalancerArn' --output text)
aws --endpoint-url http://localhost:4566 elbv2 add-tags \
  --resource-arns "$LB_ARN" \
  --tags Key=env,Value=test

# Assert in tests via the introspection endpoint.
curl http://localhost:4566/_fakecloud/elbv2/load-balancers
```

## Introspection endpoints

For tests that need to assert on load balancer state without going through the AWS SDK:

- `GET /_fakecloud/elbv2/load-balancers` — list all load balancers
- `GET /_fakecloud/elbv2/target-groups` — list all target groups
- `GET /_fakecloud/elbv2/listeners` — list all listeners
- `GET /_fakecloud/elbv2/rules` — list all listener rules

## Status

ELBv2 control plane is fully implemented (ALB/NLB/GWLB CRUD, target groups + targets + health probes, listeners, rules, certificates, attributes, capacity reservations, mTLS trust stores + revocations, SSL policies, resource policies, tags). ALB has an in-process HTTP data plane for rule matching, target routing, fixed-response, redirect, and sticky sessions, with WAFv2 inspection wired in. NLB and GWLB are control-plane only.

[Open an issue](https://github.com/faiscadev/fakecloud/issues) if you have a specific ELBv2 use case to prioritize.
