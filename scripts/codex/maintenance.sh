#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

# A cached Codex container may have been prepared from an older commit.
# Refresh modules and the compile cache after the requested branch is checked out.
go mod download
go mod verify
go test -run '^$' ./...
