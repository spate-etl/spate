**Breaking:** **Portable descriptor budget** (`spate-s3`)

The split planner rejects descriptors above 294,720 raw bytes so the complete
encoded spec fits the portable 384 KiB stored-value budget. Earlier versions
allowed raw descriptors up to 400 KiB. Jobs whose listed metadata produces
larger descriptors must reduce that metadata before upgrading.
