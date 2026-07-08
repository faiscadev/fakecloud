+++
title = "Migration from LocalStack"
description = "Technical path for migrating from LocalStack to fakecloud, targeting users frustrated with LocalStack's 2026 account requirements and proprietary tier shifts."
weight = 2
+++

If your local development workflow is blocked by LocalStack's account requirements or proprietary tier shifts, fakecloud offers a drop-in replacement for core services with zero internet dependency. This page is the technical reference card; for the full step-by-step walkthrough with docker-compose, CI, Terraform, CDK, and Serverless snippets, see [Migrating from LocalStack to fakecloud in 10 minutes](@/blog/migrate-from-localstack.md).

## Key Differences
- **No API Key**: fakecloud is fully functional offline. No `ACTIVATE_PRO` or account login required.
- **Single Binary**: Replace heavy Docker-in-Docker setups with a ~19MB binary that starts in <300ms.
- **Parity**: 100% API conformance across 6,246 operations, including features LocalStack gates behind Pro (like ECR and Bedrock).

## Service Mapping
| Feature | LocalStack | fakecloud |
| :--- | :--- | :--- |
| **ECR** | Paid (Pro) | Included (Full OCI Support) |
| **Bedrock** | Limited/Paid | Included (214 ops across 4 APIs) |
| **IAM** | Partial (Community) | 100% Conformance |
| **S3/Lambda/SQS** | Included | Included (High Fidelity) |

## Step-by-Step Migration

### 1. Swap the Runtime
Stop your LocalStack container and run the fakecloud binary. fakecloud defaults to port `4566` for maximum compatibility.
```bash
# Stop LocalStack
docker-compose down

# Start fakecloud
./fakecloud
```

### 2. Update Environment Variables
Ensure your environment points to the local endpoint. fakecloud respects standard AWS SDK environment variables:
```bash
export AWS_ENDPOINT_URL="http://localhost:4566"
```

### 3. Tooling Compatibility
Existing wrappers like `tflocal` or `cdklocal` will continue to work as they target `localhost:4566` by default. No changes to your Infrastructure as Code (IaC) logic are required.
