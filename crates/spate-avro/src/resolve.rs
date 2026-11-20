//! A bound on what resolving one decoded value against a reader schema costs.
//!
//! apache-avro picks a union's branch by resolving the value against each
//! candidate branch, and then resolves it again against the branch it found.
//! Every union on a path therefore resolves the subtree below it at least
//! twice. Under a reader schema that recurses through a union the depth is the
//! datum's, so the cost of one datum is exponential in its nesting.
//!
//! [`check_resolution`] walks a decoded value against the reader schema by the
//! rules apache-avro picks a branch with, and weighs every value in it. A
//! value weighs its size times the current multiplier. The multiplier doubles
//! below a union that is already on the path, because the datum chooses how
//! often that happens. A union met for the first time on a path weighs its
//! costliest candidate and leaves the multiplier alone, so a reader schema
//! that does not recurse weighs every value once and is never refused.
//!
//! A value the reader schema has no place for is weighed too, at the
//! multiplier of the value that holds it: each trial copies it with the rest.

use apache_avro::Schema;
use apache_avro::schema::{NamesRef, NamespaceRef, RecordSchema, UnionSchema};
use apache_avro::types::Value;

/// How many times its own size, or its encoded length where that is less,
/// doubling may add to a value's weight.
const DOUBLING_PER_UNIT: u64 = 3;

/// The weight doubling may add to every value, however small it is.
const MIN_DOUBLING: u64 = 1024;

/// The bytes of text or binary content that weigh as much as one value.
const BYTES_PER_UNIT: usize = 64;

/// The reason a refused datum carries. It quotes nothing from the record.
pub(crate) const REFUSED: &str = "avro datum refused: resolving its nesting against the reader \
                                  schema's recursive unions exceeds the resolution limit";

/// A value whose resolution would weigh more than its limit.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ResolutionRefused;

/// Refuse `value`, decoded from `encoded` bytes, when resolving it against
/// `reader` would weigh more than [`weight_limit`] allows.
pub(crate) fn check_resolution(
    value: &Value,
    encoded: usize,
    reader: &Schema,
    names: &NamesRef<'_>,
) -> Result<(), ResolutionRefused> {
    let walk = Walk {
        names,
        limit: weight_limit(size(value), encoded as u64),
    };
    walk.weigh(value, reader, None, 1, None).map(drop)
}

/// The weight a value of this size may reach: its size, which it weighs with
/// no doubling, and `max(3 × min(size, encoded), 1,024)` more. The encoded
/// length caps the allowance because a value can take no bytes to encode, as
/// a null does, and such values would otherwise buy doubling for nothing.
fn weight_limit(size: u64, encoded: u64) -> u64 {
    let doubling = size
        .min(encoded)
        .saturating_mul(DOUBLING_PER_UNIT)
        .max(MIN_DOUBLING);
    size.saturating_add(doubling)
}

fn content(bytes: usize) -> u64 {
    (bytes / BYTES_PER_UNIT) as u64
}

/// What one copy of this value costs without the values inside it: one, and
/// one more for each 64 bytes of content, field names or map keys it holds.
fn own(value: &Value) -> u64 {
    let content = match value {
        Value::String(text) | Value::Enum(_, text) => content(text.len()),
        Value::Bytes(bytes) | Value::Fixed(_, bytes) => content(bytes.len()),
        // A decimal gives its length only as the bytes it encodes to.
        Value::Decimal(decimal) => Vec::<u8>::try_from(decimal).map_or(0, |b| content(b.len())),
        Value::BigDecimal(decimal) => {
            let bytes = decimal.as_bigint_and_scale().0.bits() / 8;
            content(usize::try_from(bytes).unwrap_or(usize::MAX))
        }
        Value::Record(fields) => content(fields.iter().map(|(name, _)| name.len()).sum()),
        Value::Map(items) => content(items.keys().map(String::len).sum()),
        _ => 0,
    };
    content.saturating_add(1)
}

/// What one copy of the whole value costs: every value in the tree, a
/// union's wrapper included.
fn size(value: &Value) -> u64 {
    let below = match value {
        Value::Union(_, inner) => size(inner),
        Value::Record(fields) => sum(fields.iter().map(|(_, field)| size(field))),
        Value::Array(items) => sum(items.iter().map(size)),
        Value::Map(items) => sum(items.values().map(size)),
        _ => 0,
    };
    below.saturating_add(own(value))
}

fn sum(weights: impl Iterator<Item = u64>) -> u64 {
    weights.fold(0, u64::saturating_add)
}

/// The unions between the root and the current visit, innermost first.
struct Path<'a> {
    union: &'a UnionSchema,
    outer: Option<&'a Path<'a>>,
}

fn on_path(mut path: Option<&Path<'_>>, union: &UnionSchema) -> bool {
    while let Some(frame) = path {
        if std::ptr::eq(frame.union, union) {
            return true;
        }
        path = frame.outer;
    }
    false
}

struct Walk<'a> {
    names: &'a NamesRef<'a>,
    limit: u64,
}

impl Walk<'_> {
    /// The weight of resolving `value` against `schema`, or the refusal once
    /// any subtree weighs more than the limit. It is never less than the
    /// value's size times `multiplier`.
    fn weigh(
        &self,
        value: &Value,
        schema: &Schema,
        namespace: NamespaceRef<'_>,
        multiplier: u64,
        path: Option<&Path<'_>>,
    ) -> Result<u64, ResolutionRefused> {
        let (wrapper, value) = match value {
            Value::Union(_, inner) => (multiplier, &**inner),
            other => (0, other),
        };
        let weight = match (self.named(schema, namespace), value) {
            (Some((Schema::Union(union), namespace)), _) => {
                self.weigh_union(value, union, namespace, multiplier, path)?
            }
            (Some((Schema::Record(record), namespace)), Value::Record(_) | Value::Map(_)) => {
                let namespace = record.name.namespace().or(namespace);
                self.weigh_record(value, record, namespace, multiplier, path)?
            }
            (Some((Schema::Array(array), namespace)), Value::Array(items)) => self
                .weigh_all(items.iter(), &array.items, namespace, multiplier, path)?
                .saturating_add(self.copied(value, multiplier)),
            (Some((Schema::Map(map), namespace)), Value::Map(items)) => self
                .weigh_all(items.values(), &map.types, namespace, multiplier, path)?
                .saturating_add(self.copied(value, multiplier)),
            // A leaf, a value the schema does not match, or a name the schema
            // does not define: the resolve settles it in one step, and every
            // trial above has copied all of it.
            _ => size(value).saturating_mul(multiplier),
        };
        self.within(weight.saturating_add(wrapper))
    }

    /// The value's own weight, without the values inside it.
    fn copied(&self, value: &Value, multiplier: u64) -> u64 {
        own(value).saturating_mul(multiplier)
    }

    /// Follow a `Ref` to its definition, as `Value::resolve_internal` does.
    fn named<'s>(
        &'s self,
        schema: &'s Schema,
        namespace: NamespaceRef<'s>,
    ) -> Option<(&'s Schema, NamespaceRef<'s>)> {
        match schema {
            Schema::Ref { name } => {
                let target = *self.names.get(&*name.fully_qualified_name(namespace))?;
                Some((target, target.namespace()))
            }
            other => Some((other, namespace)),
        }
    }

    fn within(&self, weight: u64) -> Result<u64, ResolutionRefused> {
        if weight > self.limit {
            return Err(ResolutionRefused);
        }
        Ok(weight)
    }

    fn weigh_all<'v>(
        &self,
        values: impl Iterator<Item = &'v Value>,
        schema: &Schema,
        namespace: NamespaceRef<'_>,
        multiplier: u64,
        path: Option<&Path<'_>>,
    ) -> Result<u64, ResolutionRefused> {
        let mut total = 0u64;
        for value in values {
            let weight = self.weigh(value, schema, namespace, multiplier, path)?;
            total = self.within(total.saturating_add(weight))?;
        }
        Ok(total)
    }

    /// A record or map value against a reader record: each of its fields
    /// against the reader field of that name. A field the reader does not
    /// name is dropped by the resolve, and weighs its size. A reader field
    /// the value lacks weighs nothing, because it adds nothing to the
    /// value's size.
    fn weigh_record(
        &self,
        value: &Value,
        record: &RecordSchema,
        namespace: NamespaceRef<'_>,
        multiplier: u64,
        path: Option<&Path<'_>>,
    ) -> Result<u64, ResolutionRefused> {
        let mut total = self.copied(value, multiplier);
        let mut field = |name: &str, found: &Value| {
            // The resolve matches by name alone. The lookup also holds
            // aliases, and an alias can take a name's entry.
            let reader = match record.lookup.get(name).map(|&at| &record.fields[at]) {
                Some(reader) if reader.name != name => {
                    record.fields.iter().find(|reader| reader.name == name)
                }
                found => found,
            };
            let weight = match reader {
                Some(reader) => self.weigh(found, &reader.schema, namespace, multiplier, path)?,
                None => size(found).saturating_mul(multiplier),
            };
            total = self.within(total.saturating_add(weight))?;
            Ok(())
        };
        match value {
            Value::Record(fields) => fields
                .iter()
                .try_for_each(|(name, found)| field(name, found))?,
            Value::Map(items) => items
                .iter()
                .try_for_each(|(name, found)| field(name, found))?,
            _ => {}
        }
        Ok(total)
    }

    /// The branches apache-avro resolves `value` against to pick one: the
    /// array branch for an array, the map branch for a map, and every record
    /// branch for a record or a map.
    fn weigh_union(
        &self,
        value: &Value,
        union: &UnionSchema,
        namespace: NamespaceRef<'_>,
        multiplier: u64,
        path: Option<&Path<'_>>,
    ) -> Result<u64, ResolutionRefused> {
        let again = on_path(path, union);
        let doubled = if again {
            multiplier.saturating_mul(2)
        } else {
            multiplier
        };
        let path = Path { union, outer: path };
        let (mut costliest, mut total) = (0u64, 0u64);
        for branch in union.variants() {
            let namespace = branch.namespace().or(namespace);
            let candidate = matches!(
                (self.named(branch, namespace), value),
                (
                    Some((Schema::Record(_), _)),
                    Value::Record(_) | Value::Map(_)
                ) | (Some((Schema::Array(_), _)), Value::Array(_))
                    | (Some((Schema::Map(_), _)), Value::Map(_))
            );
            if candidate {
                let weight = self.weigh(value, branch, namespace, doubled, Some(&path))?;
                costliest = costliest.max(weight);
                total = total.saturating_add(weight);
            }
        }
        if total == 0 {
            // No branch is tried, so the value is resolved once.
            return Ok(size(value).saturating_mul(multiplier));
        }
        if !again {
            return Ok(costliest);
        }
        // Each branch is tried and the one found is resolved again. The
        // doubled multiplier over the sum of the branches bounds that.
        self.within(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::schema::ResolvedSchema;

    const LIST: &str = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"]}]}"#;
    const TREE: &str = r#"{"type":"record","name":"Node","fields":[{"name":"l","type":["null","Node"]},{"name":"r","type":["null","Node"]}]}"#;

    fn null() -> Value {
        Value::Union(0, Box::new(Value::Null))
    }

    fn list(nodes: usize) -> Value {
        let next = match nodes {
            1 => null(),
            _ => Value::Union(1, Box::new(list(nodes - 1))),
        };
        Value::Record(vec![
            ("value".into(), Value::Long(0)),
            ("next".into(), next),
        ])
    }

    fn tree(depth: usize) -> Value {
        let child = || match depth {
            1 => null(),
            _ => Value::Union(1, Box::new(tree(depth - 1))),
        };
        Value::Record(vec![("l".into(), child()), ("r".into(), child())])
    }

    /// The weight of `value` under `schema` with no limit.
    fn weight(value: &Value, schema: &str) -> u64 {
        let schema = Schema::parse_str(schema).unwrap();
        let names = ResolvedSchema::new(&schema).unwrap();
        let walk = Walk {
            names: names.get_names(),
            limit: u64::MAX,
        };
        walk.weigh(value, &schema, None, 1, None).unwrap()
    }

    fn check(value: &Value, schema: &str) -> Result<(), ResolutionRefused> {
        let schema = Schema::parse_str(schema).unwrap();
        let names = ResolvedSchema::new(&schema).unwrap();
        check_resolution(value, usize::MAX, &schema, names.get_names())
    }

    /// The list as nested maps, the value a writer schema of maps decodes to.
    fn map_list(nodes: usize) -> Value {
        let next = match nodes {
            1 => null(),
            _ => Value::Union(1, Box::new(map_list(nodes - 1))),
        };
        Value::Map(
            [
                ("value".to_owned(), Value::Long(0)),
                ("next".to_owned(), next),
            ]
            .into(),
        )
    }

    #[test]
    fn a_list_weighs_double_for_each_node_past_the_second() {
        let weights: Vec<u64> = (1..=10).map(|nodes| weight(&list(nodes), LIST)).collect();
        assert_eq!(weights, [4, 7, 14, 28, 56, 112, 224, 448, 896, 1792]);
    }

    #[test]
    fn a_list_resolves_up_to_nine_nodes() {
        assert_eq!(check(&list(9), LIST), Ok(()));
        assert_eq!(check(&list(10), LIST), Err(ResolutionRefused));
    }

    #[test]
    fn a_binary_tree_resolves_up_to_depth_five() {
        let weights: Vec<u64> = (1..=6).map(|depth| weight(&tree(depth), TREE)).collect();
        assert_eq!(weights, [5, 13, 39, 127, 447, 1663]);
        assert_eq!(check(&tree(5), TREE), Ok(()));
        assert_eq!(check(&tree(6), TREE), Err(ResolutionRefused));
    }

    #[test]
    fn doubling_may_add_three_times_the_size_and_at_least_1024() {
        assert_eq!(weight_limit(0, 0), 1024);
        assert_eq!(weight_limit(341, 341), 1365);
        assert_eq!(weight_limit(342, 342), 1368);
        assert_eq!(weight_limit(342, u64::MAX), 1368);
        assert_eq!(weight_limit(u64::MAX, u64::MAX), u64::MAX);
    }

    /// Nulls take no bytes to encode, so an array of them adds its size to
    /// the limit and nothing to what doubling may add.
    #[test]
    fn values_that_take_no_bytes_buy_no_doubling() {
        assert_eq!(weight_limit(1_000_000, 50), 1_001_024);
        let padded = |nodes| {
            Value::Record(vec![
                ("value".into(), Value::Long(0)),
                ("pad".into(), Value::Array(vec![Value::Null; 100_000])),
                ("next".into(), Value::Union(1, Box::new(list(nodes)))),
            ])
        };
        let schema = Schema::parse_str(LIST).unwrap();
        let names = ResolvedSchema::new(&schema).unwrap();
        let check = |nodes| check_resolution(&padded(nodes), 50, &schema, names.get_names());
        assert_eq!(check(8), Ok(()));
        assert_eq!(check(9), Err(ResolutionRefused));
        // The same pad with a byte for each item buys the doubling it could
        // before.
        let wide = |nodes| check_resolution(&padded(nodes), 100_050, &schema, names.get_names());
        assert_eq!(wide(16), Ok(()));
        assert_eq!(wide(17), Err(ResolutionRefused));
        assert_eq!(size(&list(9)), 28);
    }

    #[test]
    fn a_map_counts_each_of_its_values() {
        assert_eq!(size(&map_list(2)), 7);
    }

    #[test]
    fn a_field_is_found_by_name_where_the_reader_reorders_it() {
        let reordered = r#"{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"value","type":"long"}]}"#;
        let weights: Vec<u64> = (1..=4).map(|n| weight(&list(n), reordered)).collect();
        assert_eq!(weights, [4, 7, 14, 28]);
    }

    #[test]
    fn a_weight_equal_to_the_limit_is_admitted() {
        let schema = Schema::parse_str(LIST).unwrap();
        let names = ResolvedSchema::new(&schema).unwrap();
        let weigh = |limit| {
            let walk = Walk {
                names: names.get_names(),
                limit,
            };
            walk.weigh(&list(9), &schema, None, 1, None)
        };
        assert_eq!(weigh(896), Ok(896));
        assert_eq!(weigh(895), Err(ResolutionRefused));
    }

    #[test]
    fn a_wide_value_is_held_to_four_times_its_size() {
        let lists = format!(r#"{{"type":"array","items":{LIST}}}"#);
        let of = |nodes| Value::Array(vec![list(nodes); 100]);
        assert_eq!(size(&of(5)), 1601);
        assert_eq!(weight(&of(5), &lists), 5601);
        assert_eq!(check(&of(5), &lists), Ok(()));
        assert_eq!(size(&of(6)), 1901);
        assert_eq!(weight(&of(6), &lists), 11201);
        assert_eq!(check(&of(6), &lists), Err(ResolutionRefused));
    }

    #[test]
    fn a_map_weighs_as_the_record_it_resolves_into() {
        assert_eq!(weight(&map_list(9), LIST), 896);
        assert_eq!(check(&map_list(10), LIST), Err(ResolutionRefused));
    }

    #[test]
    fn a_reference_under_a_namespace_is_followed() {
        let list_ns = LIST.replace(r#""name":"Node","#, r#""name":"Node","namespace":"a.b","#);
        assert_eq!(weight(&list(9), &list_ns), 896);
        let outer = format!(
            r#"{{"type":"record","name":"Outer","namespace":"a.b","fields":[{{"name":"head","type":{LIST}}}]}}"#
        );
        let value = Value::Record(vec![("head".into(), list(9))]);
        assert_eq!(weight(&value, &outer), 897);
    }

    #[test]
    fn recursion_through_an_array_or_a_map_branch_doubles() {
        let through = |container: &str| {
            format!(
                r#"{{"type":"record","name":"Node","fields":[{{"name":"kids","type":["null",{{"type":"{container}","{}":"Node"}}]}}]}}"#,
                if container == "array" {
                    "items"
                } else {
                    "values"
                }
            )
        };
        fn nest(depth: usize, wrap: fn(Value) -> Value) -> Value {
            let kids = match depth {
                1 => Value::Union(0, Box::new(Value::Null)),
                _ => Value::Union(1, Box::new(wrap(nest(depth - 1, wrap)))),
            };
            Value::Record(vec![("kids".into(), kids)])
        }
        let array = |inner| Value::Array(vec![inner]);
        let map = |inner| Value::Map([("k".to_owned(), inner)].into());
        let arrays: Vec<u64> = (1..=5)
            .map(|d| weight(&nest(d, array), &through("array")))
            .collect();
        let maps: Vec<u64> = (1..=5)
            .map(|d| weight(&nest(d, map), &through("map")))
            .collect();
        assert_eq!(arrays, [3, 6, 13, 27, 55]);
        assert_eq!(maps, arrays);
    }

    #[test]
    fn a_reader_field_the_value_lacks_weighs_nothing() {
        let wider = LIST.replace(
            r#"{"name":"value","type":"long"},"#,
            r#"{"name":"value","type":"long"},{"name":"added","type":"long","default":0},"#,
        );
        assert_eq!(weight(&list(1), LIST), 4);
        assert_eq!(weight(&list(1), &wider), 4);
    }

    /// A reader that adds defaulted fields to each item of an array does not
    /// recurse, so no datum under it weighs more than its node count.
    #[test]
    fn added_default_fields_under_an_array_are_never_refused() {
        let added: String = (1..=7)
            .map(|i| format!(r#",{{"name":"a{i}","type":"long","default":0}}"#))
            .collect();
        let reader = format!(
            r#"{{"type":"array","items":{{"type":"record","name":"E","fields":[{{"name":"id","type":"long"}}{added}]}}}}"#
        );
        let item = Value::Record(vec![("id".into(), Value::Long(1))]);
        let items = Value::Array(vec![item; 200]);
        assert_eq!(size(&items), 401);
        assert_eq!(weight(&items, &reader), 401);
        assert_eq!(check(&items, &reader), Ok(()));
    }

    /// The floor admits a list of narrow nodes to nine. Past the floor the
    /// limit is four per node, which admits five nodes of any width.
    #[test]
    fn a_list_of_wide_nodes_resolves_up_to_five_nodes() {
        let fields: String = (0..40)
            .map(|i| format!(r#"{{"name":"f{i}","type":"long"}},"#))
            .collect();
        let schema = format!(
            r#"{{"type":"record","name":"Node","fields":[{fields}{{"name":"next","type":["null","Node"]}}]}}"#
        );
        fn wide(nodes: usize) -> Value {
            let mut fields: Vec<(String, Value)> =
                (0..40).map(|i| (format!("f{i}"), Value::Long(0))).collect();
            let next = match nodes {
                1 => Value::Union(0, Box::new(Value::Null)),
                _ => Value::Union(1, Box::new(wide(nodes - 1))),
            };
            fields.push(("next".into(), next));
            Value::Record(fields)
        }
        assert_eq!(check(&wide(5), &schema), Ok(()));
        assert_eq!(check(&wide(6), &schema), Err(ResolutionRefused));
    }

    #[test]
    fn a_value_with_no_union_wrapper_is_weighed_under_a_reader_union() {
        let bare = Value::Record(vec![
            ("value".into(), Value::Long(0)),
            (
                "next".into(),
                Value::Record(vec![
                    ("value".into(), Value::Long(0)),
                    ("next".into(), Value::Null),
                ]),
            ),
        ]);
        assert_eq!(size(&bare), 5);
        assert_eq!(weight(&bare, LIST), 5);
    }

    /// `depth` records nested through nullable fields, each a type of its own.
    fn nullable_chain(depth: usize) -> (String, Value) {
        let mut schema =
            r#"{"type":"record","name":"R0","fields":[{"name":"n","type":"long"}]}"#.to_owned();
        let mut value = Value::Record(vec![("n".into(), Value::Long(0))]);
        for level in 1..depth {
            schema = format!(
                r#"{{"type":"record","name":"R{level}","fields":[{{"name":"n","type":"long"}},{{"name":"inner","type":["null",{schema}]}}]}}"#
            );
            value = Value::Record(vec![
                ("n".into(), Value::Long(0)),
                ("inner".into(), Value::Union(1, Box::new(value))),
            ]);
        }
        (schema, value)
    }

    #[test]
    fn a_schema_that_does_not_recurse_weighs_one_per_visit() {
        let (schema, value) = nullable_chain(12);
        assert_eq!(weight(&value, &schema), 35);
        assert_eq!(check(&value, &schema), Ok(()));

        let mut arrays = r#""long""#.to_owned();
        let mut wide = Value::Long(0);
        for _ in 0..12 {
            arrays = format!(r#"{{"type":"array","items":["null",{arrays}]}}"#);
            wide = Value::Array(vec![Value::Union(1, Box::new(wide)); 2]);
        }
        assert_eq!(size(&wide), 16381);
        assert_eq!(weight(&wide, &arrays), 16381);
        assert_eq!(check(&wide, &arrays), Ok(()));
    }

    #[test]
    fn like_shaped_record_branches_weigh_as_one_where_the_schema_does_not_recurse() {
        let branch = |name: &str, inner: &str| {
            format!(
                r#"{{"type":"record","name":"{name}","fields":[{{"name":"items","type":{{"type":"array","items":"long"}}}},{{"name":"inner","type":{inner}}}]}}"#
            )
        };
        let six = |prefix: &str, inner: &dyn Fn(usize) -> String| {
            let branches: Vec<String> = (0..6)
                .map(|i| branch(&format!("{prefix}{i}"), &inner(i)))
                .collect();
            format!(r#"["null",{}]"#, branches.join(","))
        };
        let null = |_| r#""null""#.to_owned();
        let record = |inner| {
            Value::Record(vec![
                ("items".into(), Value::Array(vec![Value::Long(0); 300])),
                ("inner".into(), inner),
            ])
        };
        let alone = six("A", &null);
        let one = Value::Union(1, Box::new(record(Value::Null)));
        assert_eq!(weight(&one, &alone), 304);
        assert_eq!(check(&one, &alone), Ok(()));

        let nested = branch("Root", &six("B", &|i| six(&format!("C{i}x"), &null)));
        let two = record(Value::Union(
            1,
            Box::new(record(Value::Union(1, Box::new(record(Value::Null))))),
        ));
        assert_eq!(weight(&two, &nested), 909);
        assert_eq!(check(&two, &nested), Ok(()));
    }

    #[test]
    fn several_record_branches_under_recursion_are_refused_without_walking_every_trial() {
        let twins = r#"{"type":"record","name":"A","fields":[{"name":"x","type":"long"},{"name":"next","type":["null","A",{"type":"record","name":"B","fields":[{"name":"x","type":"long"},{"name":"next","type":["null","A","B"]}]}]}]}"#;
        fn chain(depth: usize) -> Value {
            let next = match depth {
                1 => Value::Union(0, Box::new(Value::Null)),
                _ => Value::Union(2, Box::new(chain(depth - 1))),
            };
            Value::Record(vec![("x".into(), Value::Long(0)), ("next".into(), next)])
        }
        assert_eq!(weight(&chain(3), twins), 22);
        assert_eq!(check(&chain(64), twins), Err(ResolutionRefused));
    }

    /// A field the reader drops is copied by every trial of the union above
    /// it, so it weighs at that union's multiplier.
    #[test]
    fn a_field_the_reader_drops_weighs_its_size_at_the_multiplier_above_it() {
        fn padded(nodes: usize, pad: usize) -> Value {
            let next = match nodes {
                1 => Value::Union(0, Box::new(Value::Null)),
                _ => Value::Union(1, Box::new(padded(nodes - 1, pad))),
            };
            Value::Record(vec![
                ("value".into(), Value::Long(0)),
                ("pad".into(), Value::Array(vec![Value::Long(0); pad])),
                ("next".into(), next),
            ])
        }
        // One node: the record, `value`, `next` and its null, and the pad.
        assert_eq!(weight(&padded(1, 10), LIST), 15);
        assert_eq!(weight(&padded(3, 10), LIST), 58);
        assert_eq!(check(&padded(5, 100), LIST), Ok(()));
        assert_eq!(check(&padded(6, 100), LIST), Err(ResolutionRefused));
        assert_eq!(check(&padded(15, 800), LIST), Err(ResolutionRefused));
    }

    /// apache-avro does not apply a reader field's alias: the writer's field
    /// is dropped and the reader's takes its default.
    #[test]
    fn a_field_matched_only_by_an_alias_is_weighed_as_dropped() {
        let renamed = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next2","aliases":["next"],"type":["null","Node"],"default":null}]}"#;
        assert_eq!(size(&list(12)), 37);
        assert_eq!(weight(&list(12), renamed), 37);
        assert_eq!(check(&list(12), renamed), Ok(()));
    }

    /// apache-avro lets a later field's alias take `next`'s lookup entry, and
    /// still resolves the writer's `next` into the field of that name.
    #[test]
    fn an_alias_that_shadows_a_field_name_does_not_hide_the_field() {
        let shadowed = LIST.replace(
            "]}",
            r#",{"name":"x","aliases":["next"],"type":"long","default":0}]}"#,
        );
        assert_eq!(weight(&list(9), &shadowed), 896);
        assert_eq!(check(&list(9), &shadowed), Ok(()));
        assert_eq!(check(&list(10), &shadowed), Err(ResolutionRefused));
    }

    #[test]
    fn a_map_key_the_reader_record_lacks_weighs_its_value() {
        let Value::Map(mut items) = map_list(1) else {
            unreachable!()
        };
        items.insert("extra".to_owned(), Value::Array(vec![Value::Long(0); 5]));
        assert_eq!(weight(&Value::Map(items), LIST), 10);
    }

    #[test]
    fn content_weighs_one_more_for_each_64_bytes() {
        assert_eq!(own(&Value::String("x".repeat(63))), 1);
        assert_eq!(own(&Value::String("x".repeat(64))), 2);
        assert_eq!(own(&Value::Bytes(vec![0; 640])), 11);
        assert_eq!(own(&Value::Fixed(128, vec![0; 128])), 3);
        assert_eq!(own(&Value::Enum(0, "x".repeat(128))), 3);
        assert_eq!(own(&Value::Decimal(vec![1u8; 640].into())), 11);
        let big: apache_avro::BigDecimal = "1".repeat(1600).parse().unwrap();
        assert_eq!(own(&Value::BigDecimal(big)), 11);
        let named = Value::Record(vec![
            ("n".repeat(64), Value::Null),
            ("m".repeat(64), Value::Null),
        ]);
        assert_eq!(own(&named), 3);
        assert_eq!(size(&named), 5);
        let keyed = Value::Map([("k".repeat(128), Value::Null)].into());
        assert_eq!(own(&keyed), 3);
        assert_eq!(weight(&Value::Bytes(vec![0; 640]), r#""bytes""#), 11);
        assert_eq!(weight(&keyed, r#"{"type":"map","values":"null"}"#), 4);
        assert_eq!(
            weight(&named, r#"{"type":"record","name":"R","fields":[]}"#),
            5
        );
    }

    #[test]
    fn a_weight_past_u64_saturates() {
        let schema = Schema::parse_str(LIST).unwrap();
        let names = ResolvedSchema::new(&schema).unwrap();
        let walk = Walk {
            names: names.get_names(),
            limit: u64::MAX,
        };
        assert_eq!(
            walk.weigh(&list(3), &schema, None, u64::MAX, None),
            Ok(u64::MAX)
        );
        assert_eq!(size(&list(3)), 10);
    }
}
