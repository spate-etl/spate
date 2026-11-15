# The client image `spate-kafka`'s `tls_system_ca` runs its clients in.
#
# It runs the test binary built on the host, so its glibc must be at least the
# newest version that binary references, and it must ship libz and libgcc_s.
# Dependabot takes Debian 13 point releases. ../README.md has the account.

FROM debian:13.7-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2
