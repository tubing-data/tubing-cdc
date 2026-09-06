#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if ! command -v go >/dev/null 2>&1; then
  echo "Go is required. Configure Go 1.21 or newer in the Codex environment." >&2
  exit 1
fi

echo "Using $(go version)"
go mod download
go mod verify

# Compile every package and test once so later Codex runs can reuse the build cache.
go test -run '^$' ./...
