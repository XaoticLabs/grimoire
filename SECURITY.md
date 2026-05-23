# Security Policy

Grimoire is an orchestration daemon that runs LLM agents under the invoking
user, brokers cross-daemon traffic over mTLS, and persists state to local
SQLite. Vulnerabilities in any of these surfaces — privilege boundaries,
peer/worker mTLS, namespace KV access control, scroll/agent dispatch, the
HTTP/UDS/gRPC ingress paths — are in scope.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for suspected security
vulnerabilities.

Use GitHub's private vulnerability reporting:

  https://github.com/XaoticLabs/grimoire/security/advisories/new

Include, where possible:

- Affected version or commit (`grim --version` or `git rev-parse HEAD`)
- A minimal reproduction or proof-of-concept
- Impact assessment (local user, cross-daemon, cross-peer)
- Any suggested remediation

You should expect an initial acknowledgement within **3 business days** and a
disposition (accepted / declined / needs-more-info) within **14 days**.

## Scope

In scope:

- The `grim` and `grimw` binaries built from this repository
- All transport surfaces: UDS, HTTP, peer gRPC, worker gRPC
- Persistence layer (SQLite schema, event log, namespace KV)
- Federation features (cross-daemon namespace KV, peer mTLS, cert pinning)
- Standing-agent providers shipped in-tree

Out of scope:

- Vulnerabilities in third-party LLM CLIs that Grimoire orchestrates
  (report those upstream)
- Resource exhaustion via legitimate workloads (file a normal issue)
- Issues that require an attacker who already has the daemon's identity
  cert and key on disk

## Supported versions

Grimoire is pre-1.0; only the latest commit on `main` receives security
fixes. There is no LTS branch.
