# spate xtask

Repository automation for this workspace, implemented with the
[`xtask` pattern](https://github.com/matklad/cargo-xtask).

## Usage

Run from the repository root for the available commands, their arguments and
examples:

```sh
cargo xtask --help
```

## CI selection

`cargo xtask ci-changes` decides which CI jobs a change needs and writes the
answers to `$GITHUB_OUTPUT`. The `changes` job in `.github/workflows/ci.yml`
runs it, and every other job gates on its outputs.

The crate graph it selects over comes from `cargo metadata`, so a new workspace
member, a new dependency edge or a new bench target is picked up with no table
to update.
