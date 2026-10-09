# Toxiproxy

[`../README.md`](../README.md) has the convention this lane follows. This file
covers what is specific to Toxiproxy.

| Lane | Line |
| --- | --- |
| `stable` | The newest Toxiproxy release. **Primary**, and the only lane. |

The image is `ghcr.io/shopify/toxiproxy`, the build Shopify publishes. The fault
runs in `faults/` start it through `spate_test_support::Toxiproxy` on the same
Docker network as the store, so its proxies reach the store by container name,
and drive it through its HTTP API on port 8474. Proxies listen on container
ports 21000 to 21015.

## Why one lane

Toxiproxy is a test fixture with no support window and backs no support claim,
so there is no `DOCS` file. The one lane follows the newest release.

## No suite on a bump

`cargo xtask ci-changes` maps this directory to no container suite. The fault
runs are not part of the `containers` job, so the weekly fault run is the first
to boot a bumped image.
