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
    if let Ok(found) = crate_name("spate-clickhouse") {
        return Ok(found_path(found, None));
    }
    if let Ok(found) = crate_name("spate") {
        return Ok(found_path(found, Some("clickhouse")));
    }
    Err(syn::Error::new(
        Span::call_site(),
        "#[derive(ClickHouseRow)] could not find `spate-clickhouse` or `spate` among this \
         crate's dependencies; if you reach it through your own facade crate, add \
         `#[clickhouse(crate = \"path::to::spate_clickhouse\")]`",
    ))
}
