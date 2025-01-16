_default:
    just --list

test:
    # Test all features
    cargo test --workspace --all-features

    # Check no default features
    cargo check --no-default-features --workspace
    
    # Check `fs` feature (gel-tokio)
    cargo check --features=fs --package gel-tokio
    
    # Check with env feature, gel-tokio
    cargo check --features=env --package gel-tokio

    # Test gel-protocol without default features
    cargo test --package=gel-protocol --no-default-features

    # Test gel-protocol with "all-types" feature
    cargo test --package=gel-protocol --features=all-types

    cargo clippy --workspace --all-features --all-targets

    cargo fmt --check


test-fast:
    cargo fmt

    cargo test --workspace --features=unstable

    cargo clippy --workspace --all-features --all-targets