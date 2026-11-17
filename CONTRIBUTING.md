# Contributing

Issues and pull requests are welcome, in English or Vietnamese.

## Before you open a pull request

Run these from the repository root. CI runs the same checks on Linux and Windows.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Code rules

- Behaviour that a Redis or Valkey client can see should match Valkey 9.1.3. When it cannot, say so in the docs.
- New behaviour comes with tests. Integration tests live in `crates/hcache/tests/`.
- Avoid `unwrap` and `expect` outside tests; return an error instead.
- Every `unsafe` block has a `// SAFETY:` comment that says why it is sound.
- Match the style of the code around your change, including how much it comments.

## Licence

By contributing you agree that your contribution is licensed under the Apache License 2.0, as the rest of the project is.
