//! The proc-macro backing `#[derive(ClickHouseRow)]`.
//!
//! Kept separate from `spate-clickhouse` because a proc-macro crate cannot
//! also export ordinary items, and this crate carries no dependency on
//! `spate-clickhouse` or `spate-core` at all — the `crate_path` module
//! resolves the absolute path to their types in the code this macro emits.

mod attrs;
mod crate_path;
mod validate;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DataStruct, DeriveInput, Fields, FieldsNamed, parse_macro_input};

/// Implements `ClickHouseRow` for a row struct, generating `COLUMNS` from the
/// field list in declaration order.
///
/// Honors `#[serde(rename = "...")]` for a column name no Rust identifier can
/// spell, such as a flattened `Nested` table's dotted `outer.inner`, and
/// excludes a field marked `#[serde(skip)]`/`#[serde(skip_serializing)]`. A
/// duplicate name after rename and a name outside the identifier-or-dotted
/// shape are compile errors, as are `#[serde(flatten)]`, a struct-level
/// `#[serde(rename_all = "...")]`, and `#[serde(skip_serializing_if = "...")]`
/// — the macro cannot see a flattened type's own fields, a renaming
/// convention serde applies at its own derive time, or a skip decided
/// per record.
#[proc_macro_derive(ClickHouseRow, attributes(serde, clickhouse))]
pub fn derive_click_house_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn combine(errors: Vec<syn::Error>) -> Option<syn::Error> {
    errors.into_iter().reduce(|mut acc, next| {
        acc.combine(next);
        acc
    })
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let mut errors = Vec::new();

    if let Some(span) = attrs::container_rename_all(&input.attrs)? {
        errors.push(syn::Error::new(
            span,
            "#[derive(ClickHouseRow)] does not support a container-level `rename_all`; \
                 rename each column individually with `#[serde(rename = \"...\")]`",
        ));
    }

    let named_fields = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => Some(named),
        _ => None,
    };

    let Some(named_fields) = named_fields else {
        errors.push(syn::Error::new(
            input.ident.span(),
            "ClickHouseRow can only be derived for a struct with named fields",
        ));
        return Err(combine(errors).expect("at least one error was pushed"));
    };

    let mut columns: Vec<(Span, String)> = Vec::new();
    for field in named_fields {
        let field_attrs = match attrs::parse_field_attrs(&field.attrs) {
            Ok(a) => a,
            Err(e) => {
                errors.push(e);
                continue;
            }
        };

        if field_attrs.flatten {
            errors.push(syn::Error::new_spanned(
                field,
                "#[serde(flatten)] is not supported: the derive cannot see the flattened \
                     type's own columns",
            ));
            continue;
        }
        if field_attrs.skip_serializing_if {
            errors.push(syn::Error::new_spanned(
                field,
                "#[serde(skip_serializing_if = \"...\")] is not supported: a value-dependent \
                     skip shifts every later column on the rows it fires for, and RowBinary has \
                     no framing to recover from that. Use Option<T> against a Nullable column \
                     instead, which stays in COLUMNS unconditionally",
            ));
            continue;
        }
        if field_attrs.skip {
            continue;
        }

        let ident = field
            .ident
            .as_ref()
            .expect("Fields::Named field has an ident");
        let name = field_attrs
            .rename
            .map(|lit| lit.value())
            .unwrap_or_else(|| ident.to_string());

        if !validate::is_column_name(&name) {
            errors.push(syn::Error::new_spanned(
                field,
                format!("column `{name}` is not a valid identifier (or dotted `outer.inner` name)"),
            ));
            continue;
        }

        columns.push((field.span(), name));
    }

    for i in 0..columns.len() {
        for j in 0..i {
            if columns[i].1 == columns[j].1 {
                errors.push(syn::Error::new(
                    columns[i].0,
                    format!(
                        "column `{}` is listed more than once (after #[serde(rename)])",
                        columns[i].1
                    ),
                ));
            }
        }
    }

    if columns.is_empty() {
        errors.push(syn::Error::new(
            input.ident.span(),
            "#[derive(ClickHouseRow)] needs at least one column",
        ));
    }

    if let Some(combined) = combine(errors) {
        return Err(combined);
    }

    let crate_path = match attrs::crate_override(&input.attrs)? {
        Some(path) => quote!(#path),
        None => crate_path::resolve()?,
    };
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let column_names = columns.iter().map(|(_, name)| name.as_str());

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #crate_path::ClickHouseRow for #ident #ty_generics #where_clause {
            const COLUMNS: &'static [&'static str] = &[#(#column_names),*];
        }
    })
}
