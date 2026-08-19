**`validate_schema: full` checks a decimal wrapper's scale** (`spate-clickhouse`)
— a `Decimal64<4>` field against a `Decimal(18, 2)` column was accepted, because
the check compared only the wrapper's integer width against the column's
precision. The widths agree, the insert succeeds, and every row lands 100× too
large. The decimal wrappers now serialize under a name carrying their scale, so
a disagreement fails fatally on the first record, before anything is inserted. A
plain integer field declares no scale and still passes any decimal column of the
matching width.
