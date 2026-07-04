+++
title = "Transfer Family"
description = "AWS Transfer Family (transfer) control plane on fakecloud: SFTP/FTPS/FTP/AS2 servers, users, SSH and host keys, accesses, workflows, agreements, connectors, profiles, certificates, security policies, and web apps — real account-partitioned, persisted state."
weight = 53
+++

fakecloud implements the **AWS Transfer Family** (`transfer`) control plane. The
full **71-operation awsJson1.1 API** ships now: SFTP/FTPS/FTP/AS2 servers, users
and their SSH public keys, host keys, service-managed accesses, workflows and
executions, AS2 agreements, connectors, profiles, certificates, the managed
security-policy catalogue, web apps, identity-provider testing, and tagging.
Every resource is real, account-partitioned state that persists across restarts
in persistent mode, so what one session creates the next session still sees.

Transfer Family on fakecloud is a control-plane emulator: there is no real SFTP
daemon and no AS2 message-transport engine — the actual movement of files
between clients and storage is out of scope, the same way LocalStack Community
mocks Transfer. Everything up to and including the control plane that configures
a transfer (creating and configuring servers, users, connectors, workflows, and
agreements, testing connections and identity providers, and transitioning server
state) is real.

## Supported features

- **Servers** (`CreateServer`, `DescribeServer`, `UpdateServer`, `DeleteServer`,
  `StartServer`, `StopServer`, `ListServers`). New servers get an
  `arn:aws:transfer:<region>:<acct>:server/s-<id>` ARN and settle straight to
  `ONLINE`; `StartServer` / `StopServer` transition `State` to `ONLINE` /
  `OFFLINE` synchronously so `server-online` / `server-offline` waiters
  complete. `DescribeServer` reflects the live `UserCount`.
- **Users + SSH public keys** (`CreateUser`, `DescribeUser`, `UpdateUser`,
  `DeleteUser`, `ListUsers`, `ImportSshPublicKey`, `DeleteSshPublicKey`). Keys
  attach to the user and surface in `DescribeUser`; `ListUsers` reports the
  per-user key count.
- **Host keys** (`ImportHostKey`, `UpdateHostKey`, `DescribeHostKey`,
  `DeleteHostKey`, `ListHostKeys`) with synthesized fingerprints.
- **Accesses** (`CreateAccess`, `UpdateAccess`, `DescribeAccess`,
  `DeleteAccess`, `ListAccesses`) keyed by the directory group `ExternalId`.
- **Workflows + executions** (`CreateWorkflow`, `DescribeWorkflow`,
  `DeleteWorkflow`, `ListWorkflows`, `SendWorkflowStepState`,
  `DescribeExecution`, `ListExecutions`). Workflow steps round-trip verbatim.
- **AS2 agreements** (`CreateAgreement`, `UpdateAgreement`, `DescribeAgreement`,
  `DeleteAgreement`, `ListAgreements`).
- **Connectors** (`CreateConnector`, `UpdateConnector`, `DescribeConnector`,
  `DeleteConnector`, `ListConnectors`, `TestConnection`) for SFTP and AS2, plus
  the connector transfer actions `StartFileTransfer`, `StartDirectoryListing`,
  `StartRemoteDelete`, `StartRemoteMove`, and `ListFileTransferResults`.
- **AS2 profiles** (`CreateProfile`, `UpdateProfile`, `DescribeProfile`,
  `DeleteProfile`, `ListProfiles`) and **certificates** (`ImportCertificate`,
  `UpdateCertificate`, `DescribeCertificate`, `DeleteCertificate`,
  `ListCertificates`).
- **Security policies** (`DescribeSecurityPolicy`, `ListSecurityPolicies`)
  return the real AWS-managed Transfer security-policy catalogue
  (`TransferSecurityPolicy-2018-11` through the FIPS and PQ-SSH variants).
- **Web apps** (`CreateWebApp`, `UpdateWebApp`, `DescribeWebApp`,
  `DeleteWebApp`, `ListWebApps`) with customization
  (`UpdateWebAppCustomization`, `DescribeWebAppCustomization`,
  `DeleteWebAppCustomization`).
- **Identity provider testing** (`TestIdentityProvider`) and **tagging**
  (`TagResource`, `UntagResource`, `ListTagsForResource`) keyed by resource ARN.

All `List` operations honor `MaxResults` / `NextToken` pagination, and
`@length`, `@range`, and enum input constraints are enforced with the model's
declared error codes.

## Not implemented

- **The SFTP / FTPS / FTP daemon and AS2 transport engine.** Transfer Family on
  fakecloud does not open a listening SFTP endpoint or move AS2 messages — no
  files are transferred over the wire. This mirrors LocalStack Community, which
  mocks Transfer as a control plane only.

100% conformance: all 2,756 generated Smithy probe variants pass.
