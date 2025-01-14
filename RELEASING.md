Releasing Rust Bindings
=======================

1. Bump version into the respective crate's Cargo.toml
2. Bump version in dependency references unless it's a minor version upgrade
3. Get that merged to master (via PR)
4. Tag release via `releases/<crate-name>/v<version_no>` tag

In case multiple crates are released here is the order:
* `gel-errors`
* `gel-derive` and `gel-protocol`
* `edgedb-client`

Notes on releasing multiple crates
1. All of the version bumps can to in a single PR
2. Tags should be pushed in order, waiting for the crate to be published
