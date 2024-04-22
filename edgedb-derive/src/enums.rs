use proc_macro2::TokenStream;
use quote::quote;

pub fn derive_enum(s: &syn::ItemEnum) -> syn::Result<TokenStream> {
    let type_name = &s.ident;
    let (impl_generics, ty_generics, _) = s.generics.split_for_impl();
    let branches = s
        .variants
        .iter()
        .map(|v| match v.fields {
            syn::Fields::Unit => {
                let name = &v.ident;
                let name_bstr = syn::LitByteStr::new(name.to_string().as_bytes(), name.span());
                Ok(quote!(#name_bstr => Ok(#type_name::#name)))
            }
            _ => Err(syn::Error::new_spanned(
                &v.fields,
                "fields are not allowed in enum variants",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expanded = quote! {
        impl #impl_generics ::edgedb_protocol::queryable::Queryable
            for #type_name #ty_generics {
            fn decode(decoder: &::edgedb_protocol::queryable::Decoder, buf: &[u8])
                -> Result<Self, ::edgedb_protocol::errors::DecodeError>
            {
                match buf {
                    #(#branches,)*
                    _ => Err(::edgedb_protocol::errors::ExtraEnumValue.build()),
                }
            }
            fn check_descriptor(
                ctx: &::edgedb_protocol::queryable::DescriptorContext,
                type_pos: ::edgedb_protocol::descriptors::TypePos)
                -> Result<(), ::edgedb_protocol::queryable::DescriptorMismatch>
            {
                use ::edgedb_protocol::descriptors::Descriptor::Enumeration;
                let desc = ctx.get(type_pos)?;
                match desc {
                    // There is no need to check the members of the enumeration
                    // because schema updates can't be perfectly synchronized
                    // to app updates. And that means that extra variants
                    // might be added and only when they are really present in
                    // data we should issue an error. Removed variants are not a
                    // problem here.
                    Enumeration(_) => Ok(()),
                    _ => Err(ctx.wrong_type(desc, "str")),
                }
            }
        }
    };
    Ok(expanded)
}
