//! Syntactic `#[serde(...)]` and `#[clickhouse(...)]` attribute parsing.
//!
//! This reads attribute syntax only, never serde's own attribute types (this
//! crate does not depend on `serde` or `serde_derive`), and tolerates any
//! `serde` key it has no opinion on (`with`, `default`, `deserialize_with`,
//! `bound`, ...): a row struct routinely carries those for its `Deserialize`
//! half or for a custom wire-type conversion, and this derive only cares
//! about naming and serialized presence.

use proc_macro2::Span;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Attribute, Expr, ExprLit, Lit, LitStr, Meta, Path, Token};

/// What this derive needs from a field's `#[serde(...)]` attributes.
#[derive(Default)]
pub(crate) struct FieldAttrs {
    pub(crate) rename: Option<LitStr>,
    pub(crate) skip: bool,
    pub(crate) skip_serializing_if: bool,
    pub(crate) flatten: bool,
}

fn serde_metas(attrs: &[Attribute]) -> syn::Result<Vec<Meta>> {
    let mut metas = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        metas.extend(attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?);
    }
    Ok(metas)
}

fn lit_str(expr: &Expr) -> syn::Result<LitStr> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(s), ..
        }) => Ok(s.clone()),
        other => Err(syn::Error::new_spanned(other, "expected a string literal")),
    }
}

pub(crate) fn parse_field_attrs(attrs: &[Attribute]) -> syn::Result<FieldAttrs> {
    let mut out = FieldAttrs::default();
    for meta in serde_metas(attrs)? {
        match &meta {
            Meta::Path(p) if p.is_ident("skip") || p.is_ident("skip_serializing") => {
                out.skip = true;
            }
            Meta::Path(p) if p.is_ident("flatten") => out.flatten = true,
            Meta::NameValue(nv) if nv.path.is_ident("skip_serializing_if") => {
                out.skip_serializing_if = true;
            }
            Meta::NameValue(nv) if nv.path.is_ident("rename") => {
                out.rename = Some(lit_str(&nv.value)?);
            }
            // `rename(serialize = "...", deserialize = "...")`: only the
            // serialize half names a wire column.
            Meta::List(list) if list.path.is_ident("rename") => {
                let inner =
                    list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
                for m in inner {
                    if let Meta::NameValue(nv) = &m
                        && nv.path.is_ident("serialize")
                    {
                        out.rename = Some(lit_str(&nv.value)?);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

/// The span of a struct-level `#[serde(rename_all = "...")]`, if present.
/// Rejected outright: honoring it means reimplementing serde's own
/// naming-convention table inside this macro.
pub(crate) fn container_rename_all(attrs: &[Attribute]) -> syn::Result<Option<Span>> {
    for meta in serde_metas(attrs)? {
        match &meta {
            Meta::NameValue(nv) if nv.path.is_ident("rename_all") => {
                return Ok(Some(nv.path.span()));
            }
            Meta::List(list) if list.path.is_ident("rename_all") => {
                return Ok(Some(list.path.span()));
            }
            _ => {}
        }
    }
    Ok(None)
}

/// An explicit `#[clickhouse(crate = "path::to::spate_clickhouse")]` escape
/// hatch, for a crate that reaches `spate-clickhouse` through a facade of its
/// own rather than depending on `spate-clickhouse` or `spate` directly.
pub(crate) fn crate_override(attrs: &[Attribute]) -> syn::Result<Option<Path>> {
    for attr in attrs {
        if !attr.path().is_ident("clickhouse") {
            continue;
        }
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            if let Meta::NameValue(nv) = &meta
                && nv.path.is_ident("crate")
            {
                let lit = lit_str(&nv.value)?;
                return Ok(Some(lit.parse()?));
            }
        }
    }
    Ok(None)
}
