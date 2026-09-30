# DynamoDB Local

[`../README.md`](../README.md) has the convention this lane follows. This file
covers what is specific to DynamoDB Local.

| Lane | Line |
| --- | --- |
| `stable` | The newest DynamoDB Local release. **Primary**, and the only lane. |

The image is `amazon/dynamodb-local`, the emulator AWS publishes. The suite
starts it in memory (`-jar DynamoDBLocal.jar -inMemory`) and waits for its
"Initializing DynamoDB Local" line on stdout.

## Why one lane

The service has no versions a deployment chooses between, and the emulator has
no support window. The one lane follows the newest release, so an emulator
change reaches CI first. The lane backs no support claim, so there is no `DOCS`
file.

The emulator checks the API's semantics: conditions, the item returned on a
failed condition, update expressions, filters, ARN table names and the time to
live calls. It does not model throughput limits, eventually consistent reads or
1 MB query pages. The store's in-memory table covers those in the unit tests.
