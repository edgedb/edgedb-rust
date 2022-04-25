EdgeDB Rust Binding
===================

This workspace is a collection of Rust crates for EdgeDB support. Individual
docs can currently be found on docs.rs:

* [edgedb-tokio](https://docs.rs/edgedb-tokio) -- client for Tokio
* [edgedb-derive](https://docs.rs/edgedb-derive) -- derive macro for data
  structures fetched from the database
* Async-std bindings [edgedb-client](https://docs.rs/edgedb-client) (currently
  deprecated)


Running Tests
=============

Due to cargo's limitation on propagation of "features", tests can only be
run as few separate command-lines:
```
cargo test --workspace --exclude edgedb-tokio
cargo test -p edgeql-tokio
cd edgedb-protocol; cargo test --no-default-features
```

License
=======


Licensed under either of

* Apache License, Version 2.0,
  (./LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license (./LICENSE-MIT or http://opensource.org/licenses/MIT)

at your option.
