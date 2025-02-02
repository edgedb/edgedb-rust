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
            return Err(syn::Error::new_spanned(
                item,
                "can only derive Queryable for structs and enums in JSON mode",
            ));
        }
    };
    let expanded = quote! {
        impl #impl_generics ::gel_protocol::queryable::Queryable
            for #name #ty_generics {
            type Args = ();

            fn decode(decoder: &::gel_protocol::queryable::Decoder, buf: &[u8])
                -> Result<Self, ::gel_protocol::errors::DecodeError>
            {
                let json: ::gel_protocol::model::Json =
                    ::gel_protocol::queryable::Queryable::decode(decoder, buf)?;
                ::serde_json::from_str(json.as_ref())
                    .map_err(::gel_protocol::errors::decode_error)
            }
            fn check_descriptor(
                ctx: &::gel_protocol::queryable::DescriptorContext,
                type_pos: ::gel_protocol::descriptors::TypePos)
                -> Result<(), ::gel_protocol::queryable::DescriptorMismatch>
            {
                <::gel_protocol::model::Json as
                    ::gel_protocol::queryable::Queryable>
                    ::check_descriptor(ctx, type_pos)
            }
        }
    };
    Ok(expanded)
}
