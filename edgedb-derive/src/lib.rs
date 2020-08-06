extern crate proc_macro;

use proc_macro::TokenStream;
use syn::{self, parse_macro_input};

mod attrib;
mod json;
mod shape;


#[proc_macro_derive(Queryable, attributes(edgedb))]
pub fn edgedb_queryable(input: TokenStream) -> TokenStream {
    let s = parse_macro_input!(input as syn::Item);
    match derive(&s) {
        Ok(stream) => stream.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn derive(item: &syn::Item) -> syn::Result<proc_macro2::TokenStream> {
    let attrs = match item {
        syn::Item::Struct(s) => &s.attrs,
        syn::Item::Enum(e) => &e.attrs,
        _ => {
            return Err(syn::Error::new_spanned(item,
                "can only derive Queryable for structs and enums"
            ));
        }
    };
    let attrs = attrib::ContainerAttrs::from_syn(&attrs)?;
    if attrs.json {
        json::derive(item)
    } else {
        match item {
            syn::Item::Struct(s) => shape::derive_struct(s),
            _ => {
                return Err(syn::Error::new_spanned(item,
                    "can only derive Queryable for a struct in non-JSON mode"
                ));
            }
        }
    }
}
