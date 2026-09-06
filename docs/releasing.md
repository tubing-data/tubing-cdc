# Releasing

The public module path is `github.com/tubing-data/tubing-cdc`. Releases use semantic versioning.
While the API is still evolving, publish `v0.x.y` tags; reserve `v1.0.0` for the first compatibility
commitment.

Before tagging:

1. Run `bash scripts/codex/check.sh` and `go test -race ./...`.
2. Document breaking API or envelope changes.
3. Confirm the repository owner has selected and added a `LICENSE` file.
4. Tag the reviewed commit, for example `git tag -a v0.1.0 -m "v0.1.0"`, and push the tag.

No license has been selected automatically: that is a legal policy decision for the repository owner.
