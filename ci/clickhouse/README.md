# ClickHouse

[`../README.md`](../README.md) has the convention these lanes follow: the
directory layout, `scripts/container-image.sh`, what CI runs, how bumps arrive,
and what to do when a lane goes red. This file covers what is specific to
ClickHouse.

| Lane | Line |
| --- | --- |
| `lts-previous` | The older LTS still in support. |
| `lts` | The newest LTS. **Primary**: the lane CI runs for the whole container tier, and the default. |
| `stable` | The newest release, LTS or otherwise. Where a server change reaches CI first. |

`SPATE_CLICKHOUSE_LANE` selects one:

```sh
make test-docker SPATE_CLICKHOUSE_LANE=lts-previous
```

## Why these three

ClickHouse ships an LTS twice a year, as `YY.3` and `YY.8`, and supports each for
a year. Two LTS lanes span roughly eighteen months of releases, the window an
operator on a conservative upgrade policy sits inside.

`stable` earns its place separately. ClickHouse backports fixes to the three
newest stables, so a release between LTS lines is a version people run, and it
surfaces a behaviour change first. It carries no Dependabot `ignore`, so it
follows every release.

The suite has already found version-gated behaviour. `Time` and `Time64` need
`enable_time_time64_type=1` on 25.8 and create without it from 26.3, and under
`RowBinaryWithNamesAndTypes` that setting governs the type name in the insert
header as well as the DDL.

## Scope of the claim

The lanes say which servers CI proves the sink against. Support is a separate
question, and the connector page states the tested set for a reader.
