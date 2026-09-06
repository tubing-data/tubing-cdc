# Codex development environment

This repository keeps Codex bootstrap and verification commands under
`scripts/codex/`. The scripts are also useful locally and in other clean Linux
development environments.

## Codex cloud configuration

Create or edit the repository environment in Codex settings, then use:

- Runtime: Go 1.21 (the version declared by `go.mod` and used in CI).
- Setup script: `bash scripts/codex/setup.sh`
- Maintenance script: `bash scripts/codex/maintenance.sh`
- Environment variables: none required for unit tests.
- Secrets: none required.
- Agent internet access: leave disabled unless a task explicitly needs current
  external documentation or another network resource. Dependency downloads run
  during setup, when network access is available.

The setup script downloads and verifies Go modules, then compiles all packages
and tests to warm the build cache. The maintenance script repeats those safe,
idempotent steps whenever Codex resumes a cached container on a newer commit.

Docker is optional. Tests backed by Testcontainers skip when a Docker daemon is
not reachable, so the default Codex cloud environment can still run the unit
suite. Use a Docker-capable local environment for the MySQL integration tests.

## Verification

Run the same repository check Codex should use before handing off code:

```bash
bash scripts/codex/check.sh
```

It runs `go vet` and the full Go test command. Format Go files changed by a task
with `gofmt` before running it. For performance work, also run:

```bash
go test -bench=. -benchmem ./...
```

To run the optional local services used for manual CDC and Elasticsearch work:

```bash
docker compose up -d mysql elasticsearch
```

See [development.md](development.md) for service details and the broader test
matrix.
