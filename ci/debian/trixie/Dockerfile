# The client image `spate-kafka`'s `tls_system_ca` runs its clients in.
#
# It runs the test binary built on the host, so its glibc must be at least the
# newest version that binary references, and it must ship libz and libgcc_s.
# Dependabot takes Debian 13 point releases. ../README.md has the account.

FROM debian:13.7-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a
