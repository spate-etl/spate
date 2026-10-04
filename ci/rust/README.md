# Rust

[`../README.md`](../README.md) has the convention this lane follows. This file
covers what is specific to the Rust image.

| Lane | Line |
| --- | --- |
| `stable` | The newest Rust release on Debian 12. **Primary**, and the only lane. |

It is a builder and client image. `spate-kafka`'s `tls_system_ca` builds its
own test binary in it with `OPENSSL_NO_VENDOR=1`, offline from the host's cargo
registry, then runs that binary in it with a test CA in the hashed certificate
directory `/etc/ssl/certs`. The image ships the system libssl the build links,
`ca-certificates`, and the `openssl` tool that hashes the directory.

## Why this line

The lane holds the Debian 12 variant, which ships OpenSSL 3.0. Dependabot moves
the Rust release and keeps the suffix. Moving to another Debian release is a
maintainer edit.

`examples/docker/Dockerfile` pins the same image under its own Dependabot entry.
The two pins move independently.
