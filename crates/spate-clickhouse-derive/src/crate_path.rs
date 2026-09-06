//! Resolving an absolute path to the `spate_clickhouse` module that carries
//! `ClickHouseRow`, from whichever crate the derive's invoker actually
//! depends on.
//!
//! `spate` re-exports `spate-clickhouse` as an item
//! (`pub use spate_clickhouse as clickhouse;`), which does not put
//! `spate_clickhouse` in a downstream crate's own extern prelude. A crate
//! depending only on `spate` — the documented, intended usage — needs
//! `::spate::clickhouse::ClickHouseRow` instead of `::spate_clickhouse::ClickHouseRow`.
//! `spate-clickhouse`'s own tests and benches see themselves as
//! [`FoundCrate::Itself`], which still resolves: Cargo links an integration
//! test or bench against its own package's library under that library's
//! crate name.

use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;

fn found_path(found: FoundCrate, suffix: Option<&str>) -> proc_macro2::TokenStream {
    let base = match found {
        FoundCrate::Itself => quote!(::spate_clickhouse),
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(&name, Span::call_site());
            quote!(::#ident)
        }
    };
    match suffix {
        Some(s) => {
            let ident = syn::Ident::new(s, Span::call_site());
            quote!(#base::#ident)
        }
        None => base,
    }
}

pub(crate) fn resolve() -> syn::Result<proc_macro2::TokenStream> {
    resolve_from(
        crate_name("spate-clickhouse").ok(),
        crate_name("spate").ok(),
    )
}

/// The decision, taken separately from the `proc_macro_crate` lookups so it
/// can be tested without a real multi-crate build.
fn resolve_from(
    clickhouse: Option<FoundCrate>,
    spate: Option<FoundCrate>,
) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(found) = clickhouse {
        return Ok(found_path(found, None));
    }
    if let Some(found) = spate {
        return Ok(found_path(found, Some("clickhouse")));
    }
    Err(syn::Error::new(
        Span::call_site(),
        "#[derive(ClickHouseRow)] could not find `spate-clickhouse` or `spate` among this \
         crate's dependencies; if you reach it through your own facade crate, add \
         `#[clickhouse(crate = \"path::to::spate_clickhouse\")]`",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn itself_with_no_suffix_is_the_bare_crate_path() {
        assert_eq!(
            found_path(FoundCrate::Itself, None).to_string(),
            quote!(::spate_clickhouse).to_string()
        );
    }

    #[test]
    fn itself_with_a_suffix_appends_it() {
        assert_eq!(
            found_path(FoundCrate::Itself, Some("clickhouse")).to_string(),
            quote!(::spate_clickhouse::clickhouse).to_string()
        );
    }

    #[test]
    fn a_named_crate_with_no_suffix_is_its_own_path() {
        assert_eq!(
            found_path(FoundCrate::Name("spate".to_string()), None).to_string(),
            quote!(::spate).to_string()
        );
    }

    #[test]
    fn a_named_crate_with_a_suffix_appends_it() {
        assert_eq!(
            found_path(FoundCrate::Name("spate".to_string()), Some("clickhouse")).to_string(),
            quote!(::spate::clickhouse).to_string()
        );
    }

    #[test]
    fn clickhouse_found_directly_wins_over_the_facade() {
        let path = resolve_from(
            Some(FoundCrate::Itself),
            Some(FoundCrate::Name("spate".to_string())),
        )
        .unwrap();
        assert_eq!(path.to_string(), quote!(::spate_clickhouse).to_string());
    }

    #[test]
    fn falls_back_to_the_spate_facade_when_clickhouse_is_not_a_direct_dependency() {
        let path = resolve_from(None, Some(FoundCrate::Name("spate".to_string()))).unwrap();
        assert_eq!(path.to_string(), quote!(::spate::clickhouse).to_string());
    }

    #[test]
    fn neither_dependency_is_a_clear_error_naming_the_escape_hatch() {
        let err = resolve_from(None, None).unwrap_err();
        assert!(err.to_string().contains("could not find"), "{err}");
        assert!(err.to_string().contains("#[clickhouse(crate"), "{err}");
    }
}
