# The client image `spate-kafka`'s `tls_system_ca` runs its clients in.
#
# It runs the test binary built on the host, so its glibc must be at least the
# newest version that binary references, and it must ship libz and libgcc_s.
# Dependabot takes Debian 13 point releases. ../README.md has the account.

FROM debian:13.7-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f
