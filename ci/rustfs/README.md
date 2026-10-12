# RustFS

[`../README.md`](../README.md) has the convention this lane follows. This file
covers what is specific to RustFS.

| Lane | Line |
| --- | --- |
| `stable` | The newest RustFS release. **Primary**, and the only lane. |

The image is `rustfs/rustfs`, an S3-compatible object store. The spate-s3
container suites and the fault runs in `faults/` start it through
`spate_test_support::Rustfs`, which sets a fixed access key pair through
`RUSTFS_ACCESS_KEY` and `RUSTFS_SECRET_KEY` and reaches the S3 API on port
9000. The server refuses unsigned requests, so every client signs.

## Why one lane

RustFS is a test fixture with no support window and backs no support claim, so
there is no `DOCS` file. The one lane follows the newest release.

## What a bump runs

`cargo xtask ci-changes` maps this directory to the `spate-s3` and
`spate-test-support` container suites. The fault runs are not part of the
`containers` job, so before merging a bump pull request, run
`cargo xtask fault-test` on its branch.
