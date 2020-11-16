use proc_macro2::TokenStream;
use quote::quote;


pub fn derive(item: &syn::Item) -> syn::Result<TokenStream> {
    let (name, impl_generics, ty_generics) = match item {
        syn::Item::Struct(s) => {
            let (impl_generics, ty_generics, _) = s.generics.split_for_impl();
            (&s.ident, impl_generics, ty_generics)
        }
        syn::Item::Enum(e) => {
            let (impl_generics, ty_generics, _) = e.generics.split_for_impl();
            (&e.ident, impl_generics, ty_generics)
        }
        _ => {
            return Err(syn::Error::new_spanned(item,
                "can only derive Queryable for structs and enums in JSON mode"
            ));
        }
    };
    let expanded = quote! {
        impl #impl_generics ::edgedb_protocol::queryable::Queryable
            for #name #ty_generics {
            fn decode(decoder: &::edgedb_protocol::queryable::Decoder, buf: &[u8])
                -> Result<Self, ::edgedb_protocol::errors::DecodeError>
            {
                let json: ::edgedb_protocol::model::Json =
                    ::edgedb_protocol::queryable::Queryable::decode(decoder, buf)?;
                Ok(::serde_json::from_str(json.as_ref())
                    .map_err(::edgedb_protocol::errors::decode_error)?)
            }
            fn check_descriptor(
                ctx: &::edgedb_protocol::queryable::DescriptorContext,
                type_pos: ::edgedb_protocol::descriptors::TypePos)
                -> Result<(), ::edgedb_protocol::queryable::DescriptorMismatch>
            {
                <::edgedb_protocol::model::Json as
                    ::edgedb_protocol::queryable::Queryable>
                    ::check_descriptor(ctx, type_pos)
            }
        }
    };
    Ok(expanded)
}
