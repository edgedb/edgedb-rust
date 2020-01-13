extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{self, parse_macro_input};


#[proc_macro_derive(Queryable, attributes(edgedb))]
pub fn decode(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let _s = parse_macro_input!(input as syn::ItemStruct);

    // Build the output, possibly using quasi-quotation
    let expanded = quote! {
    };

    // Hand the output tokens back to the compiler
    TokenStream::from(expanded)
}
