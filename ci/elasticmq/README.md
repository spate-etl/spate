# ElasticMQ

[`../README.md`](../README.md) has the convention this lane follows. This file
covers what is specific to ElasticMQ.

| Lane | Line |
| --- | --- |
| `stable` | The newest ElasticMQ release. **Primary**, and the only lane. |

The image is `softwaremill/elasticmq-native`, the GraalVM build of an
SQS-compatible queue. Tests start it through `spate_test_support::ElasticMq`,
which reaches the SQS query API on port 9324.

## Why one lane

ElasticMQ is a test fixture with no support window and backs no support claim,
so there is no `DOCS` file. The one lane follows the newest release.

## What a bump runs

`cargo xtask ci-changes` maps this directory to the `spate-test-support`
container suite.
