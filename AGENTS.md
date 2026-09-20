# Verification Steps for AI Agents

This document describes how to verify code changes in the `mtorrent` workspace. These are the exact commands CI runs in `.github/workflows/ci.yml` (jobs `prechecks` and `checks`), adapted for a local machine that does **not** have Rust installed.

## Prerequisites

- Docker must be installed and runnable by the current user.
- Use `./cargo.sh` in place of `cargo`. It builds the dev-container image from `.devcontainer/` and runs cargo inside it, mounting the workspace at `/usr/src/mtorrent`.
- First invocation will be slow (image build + dependency download). Subsequent invocations reuse the Docker image and the `target/` cache.

## Commands (run from the workspace root, in this order)

### 1. Formatting (nightly rustfmt)
```sh
./cargo.sh +nightly fmt --check --verbose
```

### 2. Clippy — debug profile
```sh
./cargo.sh clippy --all-targets --all-features -- -D warnings
```

### 3. Clippy — release profile
```sh
./cargo.sh clippy --release --all-targets --all-features -- -D warnings
```

### 4. Build docs (nightly, with docsrs cfg, warnings denied)
```sh
RUSTDOCFLAGS="--cfg docsrs -Dwarnings" RUSTFLAGS="-Dwarnings" \
  ./cargo.sh +nightly doc --lib --no-deps --all-features --document-private-items
```

### 5. Doctests
```sh
./cargo.sh test --doc
```

### 6. Run all tests
```sh
./cargo.sh nextest run --all-targets --all-features --no-fail-fast --failure-output=final
```

### 7. Release build
```sh
./cargo.sh build --release --all-targets --verbose
```

## Notes for agents

- All steps must exit with code 0. Any warning is treated as an error in clippy and doc builds because of `-D warnings` / `-Dwarnings`.
- Timeouts: builds and tests can take several minutes on a cold cache. Set generous shell timeouts (≥ 30 minutes) when invoking these through tools.
- Do not chain the steps with `&&` in a single command if you need to inspect output per step; run them sequentially and check exit codes individually.
- The integration tests in `mtorrent-cli/tests/` include network-like scenarios that can take 5–15 seconds each; expect a full nextest run to take ~15–30 seconds after compilation.
- `cargo.sh` passes all arguments through unchanged; toolchain overrides like `+nightly` work as with a native `cargo`.
- If you only want a fast sanity check, running steps 2 and 6 catches the vast majority of regressions.

## Mapping to CI

| Step | CI job    | CI step                |
|------|-----------|------------------------|
| 1    | prechecks | Check formatting       |
| 2    | prechecks | Lint debug             |
| 3    | prechecks | Lint release           |
| 4    | prechecks | Build docs             |
| 5    | prechecks | Run doctests           |
| 6    | checks    | Run tests              |
| 7    | checks    | Build release          |
