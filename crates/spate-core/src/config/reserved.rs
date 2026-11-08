//! A load-time check that a passthrough map names no key the connector owns.
//!
//! A [`Denylist`] holds the owned keys, each with the reason it is owned, and
//! optionally the library's normalisation, so every spelling the library
//! accepts for an owned key is refused. [`Denylist::check`] returns a
//! [`ReservedKeyError`] whose message starts at the passthrough's path. The
//! caller puts the section in front of it in its own style, so every
//! connector refuses an owned key in one wording.

use std::borrow::Cow;

/// The keys a connector owns in one passthrough map, and how the library
/// spells them.
///
/// Built in a `const` item for a fixed table, or at run time from a slice the
/// connector derives from the settings it applies.
#[derive(Clone, Copy, Debug)]
pub struct Denylist<'a> {
    entries: &'a [(&'a str, &'a str)],
    normalize: fn(&str) -> Cow<'_, str>,
    setting: &'a str,
}

impl<'a> Denylist<'a> {
    /// Refuse each key in `entries`, a list of `(key, why)` pairs, under
    /// exactly that spelling.
    #[must_use]
    pub const fn new(entries: &'a [(&'a str, &'a str)]) -> Self {
        Self {
            entries,
            normalize: exact,
            setting: "",
        }
    }

    /// This list, comparing keys after `normalize`, the library's mapping of
    /// a key to one spelling per setting. Both the written key and the owned
    /// one are normalised. `setting` names what the library calls one of its
    /// keys, such as `librdkafka property`, for the message to say two
    /// spellings set the same one.
    #[must_use]
    pub const fn normalized_by(
        self,
        normalize: fn(&str) -> Cow<'_, str>,
        setting: &'a str,
    ) -> Self {
        Self {
            normalize,
            setting,
            ..self
        }
    }

    /// Checks the keys of the passthrough at `path`, such as `rdkafka`, in
    /// iteration order.
    ///
    /// # Errors
    ///
    /// [`ReservedKeyError`] for the first key that names an owned key, under
    /// any spelling the normalisation maps to it.
    ///
    /// # Examples
    ///
    /// ```
    /// use spate_core::config::Denylist;
    /// use std::borrow::Cow;
    /// use std::collections::BTreeMap;
    ///
    /// const STORE: Denylist<'static> =
    ///     Denylist::new(&[("bucket", "owned by the typed `url` field")])
    ///         .normalized_by(|k| Cow::Owned(k.to_ascii_lowercase()), "store option");
    ///
    /// let store = BTreeMap::from([("Bucket".to_owned(), "logs".to_owned())]);
    /// let err = STORE.check("store", store.keys()).unwrap_err();
    /// assert_eq!(
    ///     format!("source.s3.{err}"),
    ///     "source.s3.store.\"Bucket\" cannot be overridden: owned by the typed `url` field \
    ///      (`Bucket` sets the same store option as `bucket`)"
    /// );
    /// ```
    pub fn check<K: AsRef<str>>(
        &self,
        path: &str,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<(), ReservedKeyError> {
        for written in keys {
            let written = written.as_ref();
            let name = (self.normalize)(written);
            if let Some((owned, why)) = self
                .entries
                .iter()
                .find(|(owned, _)| (self.normalize)(owned) == name)
            {
                return Err(ReservedKeyError {
                    path: path.to_owned(),
                    written: written.to_owned(),
                    owned: (*owned).to_owned(),
                    why: (*why).to_owned(),
                    setting: self.setting.to_owned(),
                });
            }
        }
        Ok(())
    }
}

/// The key as written, for a list compared by exact spelling.
fn exact(key: &str) -> Cow<'_, str> {
    Cow::Borrowed(key)
}

/// A passthrough key that names a key the connector owns.
///
/// Its message is `<path>."<key>" cannot be overridden: <why>`, and names the
/// owned key when the written one is another spelling of it. It never holds
/// the value.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{path}.\"{written}\" cannot be overridden: {why}{}", Spelling(self))]
pub struct ReservedKeyError {
    path: String,
    written: String,
    owned: String,
    why: String,
    setting: String,
}

/// The clause naming the owned key, empty when the key was written as owned.
struct Spelling<'a>(&'a ReservedKeyError);

impl std::fmt::Display for Spelling<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ReservedKeyError {
            written,
            owned,
            setting,
            ..
        } = self.0;
        if written == owned {
            return Ok(());
        }
        write!(f, " (`{written}` sets the same {setting} as `{owned}`)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WHY: &str = "owned by the typed `brokers` field";

    fn alias(key: &str) -> Cow<'_, str> {
        Cow::Borrowed(match key {
            "metadata.broker.list" => "bootstrap.servers",
            other => other,
        })
    }

    const BROKERS: Denylist<'static> = Denylist::new(&[("bootstrap.servers", WHY)]);
    const ALIASED: Denylist<'static> = BROKERS.normalized_by(alias, "librdkafka property");

    fn refused(list: Denylist<'_>, keys: &[&str]) -> String {
        list.check("rdkafka", keys)
            .expect_err(&format!("{keys:?} should be refused"))
            .to_string()
    }

    #[test]
    fn a_key_off_the_list_passes() {
        assert_eq!(ALIASED.check("rdkafka", ["client.id", "linger.ms"]), Ok(()));
    }

    #[test]
    fn an_owned_key_is_refused_by_its_name() {
        assert_eq!(
            refused(BROKERS, &["bootstrap.servers"]),
            "rdkafka.\"bootstrap.servers\" cannot be overridden: owned by the typed `brokers` field"
        );
    }

    #[test]
    fn another_spelling_is_refused_naming_the_owned_key() {
        assert_eq!(
            refused(ALIASED, &["metadata.broker.list"]),
            "rdkafka.\"metadata.broker.list\" cannot be overridden: owned by the typed \
             `brokers` field (`metadata.broker.list` sets the same librdkafka property as \
             `bootstrap.servers`)"
        );
    }

    #[test]
    fn without_a_normaliser_only_the_exact_key_is_refused() {
        assert_eq!(
            BROKERS.check("rdkafka", ["metadata.broker.list", "Bootstrap.Servers"]),
            Ok(())
        );
    }

    #[test]
    fn the_first_refused_key_in_order_is_reported() {
        let list = Denylist::new(&[("acks", "forced"), ("bootstrap.servers", WHY)]);
        assert_eq!(
            refused(list, &["linger.ms", "bootstrap.servers", "acks"]),
            "rdkafka.\"bootstrap.servers\" cannot be overridden: owned by the typed `brokers` field"
        );
    }

    /// An entry written as an alias still catches the spelling it maps to,
    /// because both sides are normalised.
    #[test]
    fn the_owned_key_is_normalised_too() {
        let owned = [("metadata.broker.list", WHY)];
        let list = Denylist::new(&owned).normalized_by(alias, "librdkafka property");
        assert_eq!(
            refused(list, &["bootstrap.servers"]),
            "rdkafka.\"bootstrap.servers\" cannot be overridden: owned by the typed \
             `brokers` field (`bootstrap.servers` sets the same librdkafka property as \
             `metadata.broker.list`)"
        );
    }

    /// A run-time slice and a closure build a list, as a connector that derives
    /// its owned keys does.
    #[test]
    fn a_list_takes_a_runtime_slice_and_a_closure() {
        let owned: Vec<(&str, &str)> = vec![("bucket", "owned by the typed `url` field")];
        let list = Denylist::new(&owned)
            .normalized_by(|k| Cow::Owned(k.to_ascii_lowercase()), "store option");
        let keys = std::collections::BTreeMap::from([("BUCKET".to_owned(), String::new())]);
        let err = list
            .check("store", keys.keys())
            .expect_err("BUCKET is bucket");
        assert_eq!(
            err.to_string(),
            "store.\"BUCKET\" cannot be overridden: owned by the typed `url` field \
             (`BUCKET` sets the same store option as `bucket`)"
        );
    }
}
