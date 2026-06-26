+++
title = "EC2 Auto Scaling"
description = "Amazon EC2 Auto Scaling — Auto Scaling Groups, Launch Configurations, desired-capacity reconciliation, and scaling activities. Query protocol."
weight = 28
+++

EC2 Auto Scaling (the `autoscaling` service) manages EC2 fleets — distinct from
[Application Auto Scaling](/docs/services/application-autoscaling/) (the
`application-autoscaling` service), which scales DynamoDB / ECS / etc. targets.

The wedge: against every other free local emulator an Auto Scaling Group scales
to a *mock* instance (LocalStack #8367 "ASG launches a mock EC2 instead of a
Docker instance"; MiniStack's ASG is a Terraform-timeout stub). fakecloud runs
EC2 instances as real containers, so an ASG reconciled to its desired capacity
launches *real* instances.

## Supported today

- **Launch Configurations** — `CreateLaunchConfiguration`, `DescribeLaunchConfigurations`, `DeleteLaunchConfiguration`.
- **Auto Scaling Groups** — `CreateAutoScalingGroup`, `DescribeAutoScalingGroups`, `UpdateAutoScalingGroup`, `DeleteAutoScalingGroup` (rejects delete with instances unless `ForceDelete`). Launch source is a Launch Configuration or an EC2 Launch Template.
- **Capacity** — `SetDesiredCapacity` and the `DesiredCapacity` on create/update reconcile the group's instance set: launching/terminating instances to match, recording a `Successful` scaling activity for each.
- **Activities** — `DescribeScalingActivities` returns the launch/terminate activities (this is the op Terraform's `aws_autoscaling_group` create blocks on, and the one MiniStack lacks — #331).
- **Instances** — `DescribeAutoScalingInstances` reports each group's instances with `LifecycleState` / `HealthStatus`.
- **Tags** — `CreateOrUpdateTags`, `DeleteTags`, `DescribeTags` (with `PropagateAtLaunch`).

## Protocol

AWS Query protocol (form-encoded request, `<ActionResponse>...<ResponseMetadata>`
XML response), endpoint `autoscaling.<region>.amazonaws.com`.

## Roadmap

- **Real instances**: desired-capacity reconciliation launches real container-backed EC2 instances through the EC2 runtime (resolving the launch template/config AMI from the seeded catalogue), so `DescribeInstances` shows the ASG's fleet. *(in progress)*
- Scaling policies (target-tracking / step), lifecycle hooks, instance refresh, and conformance-harness coverage.
