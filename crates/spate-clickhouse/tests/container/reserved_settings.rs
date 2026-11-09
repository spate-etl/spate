// ---- the reserved settings under other spellings ------------------------------
//
// The sink refuses its reserved settings in `settings:` by exact name. That
// holds only while the server reads no other spelling as the same setting:
// no alias in `system.settings` and no case folding. These tests pin both on
// each ClickHouse line the suite runs against.

use super::*;

/// The settings and HTTP parameters the sink refuses in `settings`.
fn reserved() -> Vec<&'static str> {
    spate_clickhouse::testing::reserved_settings()
}

/// No setting is an alias of a reserved one, so an exact match catches every
/// name the server reads as it.
#[tokio::test]
#[ignore = "requires Docker"]
async fn no_setting_is_an_alias_of_a_reserved_one() {
    let srv = bare_server("alias-secret").await;
    let aliases: Vec<String> = srv
        .admin
        .query("SELECT name FROM system.settings WHERE has(?, alias_for)")
        .bind(reserved())
        .fetch_all()
        .await
        .expect("read system.settings");
    assert_eq!(aliases, Vec::<String>::new());
}

/// A reserved name in another case, or padded with a space, is an unknown
/// setting, so it cannot override the name the sink sets.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_reserved_name_spelled_otherwise_is_an_unknown_setting() {
    let srv = bare_server("case-secret").await;
    for name in reserved() {
        for spelling in [name.to_ascii_uppercase(), format!(" {name}")] {
            // A database that exists: 26.3 matches `database` in any case and
            // checks the database before the setting names.
            let err = srv
                .admin
                .query("SELECT 1")
                .with_setting(spelling.clone(), "default")
                .fetch_one::<u8>()
                .await
                .expect_err(&format!("{spelling:?} must not be read as {name}"));
            assert!(
                err.to_string().contains("UNKNOWN_SETTING"),
                "{spelling:?}: {err}"
            );
        }
    }
}
