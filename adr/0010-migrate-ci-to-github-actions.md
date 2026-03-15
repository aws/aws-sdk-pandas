# 10. Migrate CI orchestration to GitHub Actions

Date: 2026-03-15

## Status

Proposed

## Context

Integration tests are currently orchestrated by AWS CodeBuild, configured via a separate private CDK repository. This repository defines five CodeBuild projects (unit tests, distributed tests, batch unit tests, load tests, benchmark tests), a custom Docker image, an S3 artifacts bucket with KMS encryption, a GitHub OAuth token, webhook filters, batch build configurations, and a Slack notification Lambda.

This setup has several operational downsides:

1. **Two repositories to maintain** — test infrastructure lives outside the main project, making changes harder to review and coordinate.
2. **Complex trigger logic** — each CodeBuild project has its own webhook filters, batch build configuration, and environment overrides.
3. **Custom Docker image** — an Amazon Linux 2 image with pyenv and multiple Python versions must be maintained, rebuilt, and pushed to ECR.
4. **Limited visibility** — CodeBuild logs are not natively surfaced in GitHub PR checks. A separate SAR application is deployed to post log links.
5. **AL2 end-of-life** — Amazon Linux 2 reached EOL in June 2025. The custom Docker image and some CodeBuild projects still depend on it, causing issues with newer package builds (e.g. numpy >= 2.3 requires GCC >= 9.3, unavailable on AL2).

Meanwhile, the project already runs linting, static analysis, and minimal unit tests on GitHub Actions. GitHub Actions natively provides Python version matrices, OIDC federation for AWS credentials, PR check integration, and artifact uploads — all without custom infrastructure.

## Decision

Migrate CI orchestration from standalone CodeBuild webhooks to GitHub Actions workflows that trigger a single CodeBuild project via `aws-actions/aws-codebuild-run-build`.

### Architecture

```
GitHub Actions (orchestration)          AWS CodeBuild (execution)
┌─────────────────────────┐             ┌──────────────────────┐
│ .github/workflows/      │  OIDC +     │ Single project:      │
│                         │  StartBuild │ "integration-tests"  │
│ integration-tests.yml   │────────────>│                      │
│   matrix:               │             │ - VPC + DB sec group │
│     python: [3.10..14]  │             │ - IAM test role      │
│     type: [unit, dist]  │             │ - AL2023 image       │
│                         │             │ - Buildspec override  │
│ deploy-test-infra.yml   │             │   per invocation     │
│   on push to main       │             └──────────────────────┘
│   paths: test_infra/**  │
└─────────────────────────┘
```

- **GitHub Actions** handles triggers (PR, push, manual), the Python version matrix, concurrency, status reporting, and artifact collection.
- **CodeBuild** handles execution inside the VPC with access to private databases (RDS, Redshift, Neptune, OpenSearch). It runs a buildspec overridden per invocation — no webhooks, no batch builds.
- **CDK stacks** for the CodeBuild project, IAM roles, and Ray load test infrastructure move into `./test_infra/`, alongside the existing test resource stacks. The private repository is retired.

### What changes

| Before | After |
|--------|-------|
| 5 CodeBuild projects with webhooks | 1 CodeBuild project, no webhooks |
| Custom AL2 Docker image in ECR | Standard AL2023 CodeBuild image |
| Batch builds for Python matrix | GitHub Actions `strategy.matrix` |
| S3 artifacts bucket + KMS CMK | GitHub Actions artifacts |
| GitHub OAuth token in Secrets Manager | GitHub OIDC (no long-lived secrets) |
| SAR app for PR log links | Native GitHub PR checks |
| Slack notification Lambda | GitHub Actions Slack integration |
| Separate private repository | New stack in `test_infra/` in main repo |
| Internal CI deploys all infrastructure | GitHub Actions `deploy-test-infra.yml` workflow |
| Hardcoded AWS account ID in deploy pipeline | Account ID from GitHub Actions secrets / OIDC |
| Manual deploy buttons per stack | Manual approval via GitHub Environments, then `cdk deploy --all` |
| Ray cluster infra in private repository | Moves to `test_infra/` |

### Infrastructure deployment

An internal CI pipeline currently deploys **all** infrastructure — both its own stacks (CodeBuild projects, artifacts bucket, Docker images) and the `./test_infra` stacks (VPC, databases, OpenSearch, etc.) by cloning the public repo. It also bootstraps CDK, manages EMR service-linked roles, and stores the GitHub OAuth token in Secrets Manager.

This is replaced by a GitHub Actions workflow that runs `cdk deploy --all` on changes to `test_infra/`, gated by a GitHub Environment requiring manual approval. Key differences from the current pipeline:

- **No hardcoded account IDs** — credentials come from OIDC, not pipeline YAML.
- **No GitHub OAuth token management** — OIDC replaces the long-lived token entirely.
- **All stacks in one CDK app** — single `cdk deploy --all` replaces per-stack manual deploy buttons.
- **Manual approval via GitHub Environments** — replaces current manual deploy gates.

### Ray load test infrastructure

The load tests stack in the private repository provisions infrastructure for remote Ray cluster tests: an EC2 instance profile for Ray workers, security groups for cluster communication, and an auto-terminate Lambda that cleans up orphaned instances. All of this moves into `./test_infra/`. The execution model is unchanged.

### What does not change

- The test suite itself (no test code changes).
- The test execution flow (`tox` → `pytest` with `pytest-xdist`).
- The AWS resources tests run against (databases, S3 buckets, Glue catalogs, etc.) — managed by existing stacks in `./test_infra/`.
- The VPC, subnets, and security group configuration.

## Security analysis

This migration does not weaken the security posture. The threat model and trust boundaries remain identical.

### Test execution stays on CodeBuild

Integration tests continue to run inside CodeBuild containers, not on GitHub-hosted runners. CodeBuild provides:

- **Network isolation** — the project runs inside the VPC with a specific security group. No database ports are exposed to the internet.
- **Ephemeral containers** — each build runs in a fresh container that is destroyed after completion. No persistent state between builds.
- **Buildspec from repo** — the buildspec is read from the base branch of the repository, not from the PR branch. An attacker submitting a fork PR cannot modify the build commands executed by CodeBuild.

### GitHub Actions role is narrowly scoped

The OIDC role assumed by GitHub Actions only has permission to:

- `codebuild:StartBuild` and `codebuild:BatchGetBuilds` — scoped to the single integration-tests project.
- `logs:GetLogEvents` — scoped to the project's log group.

It **cannot** access databases, S3 test buckets, Secrets Manager, or any other test resource. It can only start a build and read its logs.

### Test role stays on CodeBuild

The broad test permissions (Glue, Athena, S3, DynamoDB, Redshift, etc.) are attached to an IAM role that only CodeBuild's service role can assume. GitHub Actions never holds these credentials.

### Fork PR protections

| Concern | Before (CodeBuild webhooks) | After (GitHub Actions + CodeBuild) |
|---------|----------------------------|-----------------------------------|
| Fork PR triggers build | Yes — webhook fires on `PULL_REQUEST_CREATED` / `PULL_REQUEST_UPDATED` | Controlled by workflow trigger — can use `pull_request_target` + environment approval |
| Attacker modifies build commands | Cannot — buildspec is in S3, not in PR | Cannot — `aws-codebuild-run-build` passes the buildspec path, CodeBuild reads it from the checked-out base branch |
| Attacker modifies workflow | N/A — no workflows involved | Cannot affect credentialed jobs if using `pull_request_target` (workflow definition comes from base branch) |
| Credentials exposed in logs | CodeBuild masks env vars | Same — CodeBuild still executes, plus GitHub Actions masks OIDC tokens |

### CDK stack moves from private to public repository

The CI CDK code is currently in a private repository. Moving it into the public `aws-sdk-pandas` repo exposes the infrastructure-as-code to anyone. This is acceptable because:

1. **The CDK code contains no secrets.** IAM roles are referenced by logical names, not ARNs. Database credentials are generated by Secrets Manager at deploy time. SSM parameter names (e.g. `/SDKPandas/EC2/DatabaseSecurityGroupId`) are lookup keys, not the values themselves — actual resource IDs are resolved at `cdk deploy` time from the target account.
2. **Security does not depend on the infrastructure code being hidden.** The protection comes from IAM policies, VPC security groups, and OIDC trust conditions — not from an attacker's inability to read the CDK stack. This follows the principle that security should not rely on obscurity.
3. **The existing test resource stacks are already public.** `./test_infra/stacks/` — which defines the VPC, databases, security groups, OpenSearch domains, and IAM roles — is already in the public repository. The CI stack follows the same pattern and exposes no additional sensitive information.
4. **Comparable AWS open-source projects do this.** AWS Powertools for Lambda (Python) publishes all CI and infrastructure configuration in their public repository, including IAM role definitions and OIDC trust policies.

**What to verify before merging:** audit the CDK stack to confirm no account IDs, resource ARNs, IP ranges, or other environment-specific values are hardcoded. All such values should come from SSM parameters, CDK context, or environment variables at deploy time.

### Comparison with prior art

AWS Powertools for Lambda (Python) — a comparable AWS open-source project — uses GitHub Actions with OIDC for all CI, including e2e tests that deploy to AWS. They additionally enforce SHA-pinned actions via a dedicated workflow. We adopt the same practices: OIDC federation, SHA-pinned actions, repo origin checks, and environment-gated approvals for sensitive jobs.

### Removed attack surface

The migration actually **removes** some attack surface present in the current setup:

- **GitHub OAuth token** — currently stored in Secrets Manager and used by CodeBuild's GitHub source credentials. Replaced by OIDC (no long-lived token).
- **S3 artifacts bucket + KMS CMK** — no longer needed. Fewer resources to manage and secure.
- **Multiple CodeBuild service roles** — consolidated into one, reducing IAM sprawl.

## Consequences

### Positive

- Single repository for all CI configuration and test infrastructure.
- Native PR check integration — logs, status, and artifacts visible directly in GitHub.
- Python version matrix managed declaratively in YAML instead of CodeBuild batch builds.
- AL2 dependency eliminated (standard AL2023 CodeBuild image).
- Simpler IAM: one CodeBuild project, one service role, one test role, one OIDC role.
- Infrastructure deployment via GitHub Actions workflow with environment approval gates.

### Negative

- CodeBuild is still required for VPC database tests — full elimination would require self-hosted runners inside the VPC, which introduces fleet management overhead and a wider security surface.
- ~1–2 minute overhead per job for the GitHub Actions → CodeBuild handoff (OIDC token exchange, StartBuild API call, log streaming).

### Future opportunities

- **Split tests by VPC requirement** — ~40 test files (~300+ test functions) only need public AWS API access (S3, Glue, Athena, DynamoDB, Timestream, etc.) and could run directly on GitHub Actions runners via OIDC, without CodeBuild. Only ~8 test files (~185 test functions) require VPC database connectivity. This split would further reduce CodeBuild usage and speed up the majority of the test suite.
- **Self-hosted runners** — if VPC database tests are eventually moved to self-hosted GitHub Actions runners, CodeBuild can be fully retired.
