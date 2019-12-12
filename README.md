EdgeDB Rust Binding
===================

Work in progress...


Running Tests
=============

Due to cargo's limitation on propagation of "features", tests can only be
run as two separate invocations:
```
cargo test --workspace --exclude edgeql-pytest
cargo build -p edgeql-python
cargo test -p edgeql-pytest
```
The `cargo build` is required to build the library used by `edgeql-pytest`.

License
=======


Licensed under either of

* Apache License, Version 2.0,
  (./LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license (./LICENSE-MIT or http://opensource.org/licenses/MIT)

at your option.
