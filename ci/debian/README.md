# Debian

[`../README.md`](../README.md) has the convention this lane follows. This file
covers what is specific to Debian.

| Lane | Line |
| --- | --- |
| `trixie` | Debian 13. **Primary**, and the only lane. |

It is the client image for the vendored OpenSSL bundle test. `spate-kafka`'s
`tls_system_ca` copies its own test binary into it and writes a test CA to the
system bundle, so the clients read a trust store the test controls.

## Why this line

The image runs a binary built on the host, so its glibc bounds which hosts can
run the test: the binary must reference no glibc symbol version newer than the
image's. Debian 13 ships glibc 2.41, and a build on the CI runner, which has
glibc 2.39, references nothing newer. Moving to the next Debian release is a
maintainer edit.
