# The `rgw` lane: the newest Ceph release, running Ceph RGW. The store the
# spate-s3 container suites, spate-kafka's notification suite and
# spate-test-support's smoke test boot under `SPATE_S3_LANE=rgw` on the
# `s3-stores` job.
#
# Dependabot follows every stable release. Its `ignore` holds back the next
# major's development and release-candidate tags (x.0.z and x.1.z), one range
# per major.
# ../README.md has the account.

FROM quay.io/ceph/ceph:v21.2.0@sha256:c9e6ed4ac3fcf67b28f38358bb9619dee4d1dfb96ddd4b9b583eee316a15f04d
