# The `rgw` lane: the newest Ceph release, running Ceph RGW. The store the
# spate-s3 container suites, spate-kafka's notification suite and
# spate-test-support's smoke test boot under `SPATE_S3_LANE=rgw` on the
# `s3-stores` job.
#
# Dependabot follows every stable release. Its `ignore` holds back the next
# major's development and release-candidate tags (x.0.z and x.1.z), one range
# per major.
# ../README.md has the account.

FROM quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9
