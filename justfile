
test:
    # Test all features
    cargo test --workspace --all-features

    # Check no default features
    cargo check --no-default-features --workspace
    
    # Check `fs` feature (edgedb-tokio)
    cargo check --features=fs --package edgedb-tokio
    
    # Check with env feature, edgedb-tokio
    cargo check --features=env --package edgedb-tokio

    # Test edgedb-protocol without default features
    cargo test --package=edgedb-protocol --no-default-features

    # Test edgedb-protocol with "all-types" feature
    cargo test --package=edgedb-protocol --features=all-types

    cargo clippy --workspace --all-features --all-targets

    cargo fmt --check


test-fast:
    cargo fmt

    cargo test --workspace --features=unstable

    cargo clippy --workspace --all-features --all-targets