# ingestix-derive

> ⚠️ **Internal Component:** This crate provides procedural macros for the Ingestix framework.

## Overview

This crate is a support library for `ingestix` and is not intended to be used as a standalone dependency. It provides the core procedural macros, such as `#[derive(FlowWorker)]`, which simplify the implementation of workers within the Ingestix ecosystem.

## Usage

To use these macros, simply add the main `ingestix` crate to your `Cargo.toml` with the `derive` feature enabled (included in the `full` feature):

```toml
[dependencies]
ingestix = { version = "0.1.0-alpha", features = ["derive"] }
```

## Documentation

For full documentation and examples, please visit the main [Ingestix repository](https://github.com/ingestix/ingestix).

## License
Licensed under Apache-2.0 or MIT.
