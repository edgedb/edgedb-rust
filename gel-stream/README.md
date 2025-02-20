# gel-stream

This crate provides a runtime and TLS agnostic client and serverstream API for
the EdgeDB server.

## Features

- `full`: Enable all features.
- `openssl`: Enable OpenSSL support.
- `rustls`: Enable Rustls support.
- `tokio`: Enable Tokio support.
- `hickory`: Enable Hickory support.
- `keepalive`: Enable keepalive support.

## TLS

TLS is supported via the `openssl` or `rustls` features. Regardless of which TLS
library is used, the API is the same.

