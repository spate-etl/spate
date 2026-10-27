#!/bin/bash
# Boots a one-node Ceph cluster and its RADOS Gateway in this container: one
# monitor and one memstore OSD on loopback, the fixed key pair, then radosgw
# on port 7480 in the foreground. Loopback serves every daemon, so the same
# script works standalone, on a user network and in another container's
# network namespace. Each wait polls the condition it needs.
set -eu

ACCESS_KEY="$1"
SECRET_KEY="$2"

fsid=$(cat /proc/sys/kernel/random/uuid)
mkdir -p /etc/ceph /var/lib/ceph/mon/ceph-a /var/lib/ceph/osd/ceph-0 /var/log/ceph /var/run/ceph
cat > /etc/ceph/ceph.conf <<EOF
[global]
fsid = $fsid
mon_host = 127.0.0.1:6789
ms_bind_msgr2 = false
auth_cluster_required = none
auth_service_required = none
auth_client_required = none
osd_objectstore = memstore
memstore_device_bytes = 2147483648
osd_pool_default_size = 1
osd_pool_default_min_size = 1
mon_allow_pool_size_one = true
mon_warn_on_pool_no_redundancy = false
osd_crush_chooseleaf_type = 0
osd_pool_default_pg_num = 8
osd_pool_default_pgp_num = 8
osd_pool_default_pg_autoscale_mode = off
mon_max_pg_per_osd = 1000
[mon.a]
mon_data = /var/lib/ceph/mon/ceph-a
[osd.0]
osd_data = /var/lib/ceph/osd/ceph-0
[client.rgw.a]
rgw_frontends = beast port=7480
EOF

monmaptool --create --add a v1:127.0.0.1:6789 --fsid "$fsid" /tmp/monmap >/dev/null
ceph-mon --mkfs -i a --monmap /tmp/monmap --setuser root --setgroup root
ceph-mon -i a --setuser root --setgroup root
until ceph mon stat >/dev/null 2>&1; do :; done

ceph osd create >/dev/null
ceph-osd -i 0 --mkfs --setuser root --setgroup root 2>/dev/null
ceph-osd -i 0 --setuser root --setgroup root
until ceph osd stat 2>/dev/null | grep -q '1 up'; do :; done

# The user exists before radosgw listens, so its first 200 means the key
# pair signs.
radosgw-admin user create --uid=spate-test --display-name=spate-test \
  --access-key="$ACCESS_KEY" --secret-key="$SECRET_KEY" >/dev/null

exec radosgw -f -n client.rgw.a --setuser root --setgroup root
