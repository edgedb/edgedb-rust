use proc_macro2::{Span, TokenStream};
use quote::quote;

use crate::attrib::FieldAttrs;

struct Field {
    name: syn::Ident,
    str_name: syn::LitStr,
    ty: syn::Type,
    attrs: FieldAttrs,
}

pub fn derive_struct(s: &syn::ItemStruct) -> syn::Result<TokenStream> {
    let name = &s.ident;
    let decoder = syn::Ident::new("decoder", Span::mixed_site());
    let buf = syn::Ident::new("buf", Span::mixed_site());
    let nfields = syn::Ident::new("nfields", Span::mixed_site());
    let elements = syn::Ident::new("elements", Span::mixed_site());
    let order = syn::Ident::new("order", Span::mixed_site());
    let sub_args = syn::Ident::new("sub_args", Span::mixed_site());
    let (impl_generics, ty_generics, _) = s.generics.split_for_impl();
    let fields = match &s.fields {
        syn::Fields::Named(named) => {
            let mut fields = Vec::with_capacity(named.named.len());
            for field in &named.named {
                let attrs = FieldAttrs::from_syn(&field.attrs)?;
                let name = field.ident.clone().unwrap();
                fields.push(Field {
                    str_name: syn::LitStr::new(&name.to_string(), name.span()),
                    name,
                    ty: field.ty.clone(),
                    attrs,
                });
            }
            fields
        }
        _ => {
            return Err(syn::Error::new_spanned(
                &s.fields,
                "only named fields are supported",
            ));
        }
    };
    let fieldname = fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
    let base_fields = fields.len();
    let type_id_block = Some(quote! {
        if #decoder.has_implicit_tid {
            #elements.skip_element()?;
        }
    });
    let type_name_block = Some(quote! {
        if #decoder.has_implicit_tname {
            #elements.skip_element()?;
        }
    });
    let id_block = Some(quote! {
        if #decoder.has_implicit_id {
            #elements.skip_element()?;
        }
    });
    let type_id_check = Some(quote! {
        if ctx.has_implicit_tid {
            if(!shape.elements[idx].flag_implicit) {
                return ::std::result::Result::Err(ctx.expected("implicit __tid__"));
            }
            idx += 1;
        }
    });
    let type_name_check = Some(quote! {
        if ctx.has_implicit_tname {
            if(!shape.elements[idx].flag_implicit) {
                return ::std::result::Result::Err(ctx.expected("implicit __tname__"));
            }
            idx += 1;
        }
    });
    let id_check = Some(quote! {
        if ctx.has_implicit_id {
            if(!shape.elements[idx].flag_implicit) {
                return ::std::result::Result::Err(ctx.expected("implicit id"));
            }
            idx += 1;
        }
    });
    let field_decoders = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let fieldname = &field.name;

            let index_lit = syn::LitInt::new(&index.to_string(), Span::mixed_site());
            let sub_arg = quote! { &#sub_args.#index_lit };
            let buf = quote! { fields[#order[#index]].as_deref() };

            if field.attrs.json {
                quote! {
                    let #fieldname: ::gel_protocol::model::Json =
                        <::gel_protocol::model::Json as
                            ::gel_protocol::queryable::Queryable>
                        ::decode_optional(#decoder, #sub_arg, #buf)?;
                    let #fieldname = ::serde_json::from_str(#fieldname.as_ref())
                        .map_err(::gel_protocol::errors::decode_error)?;
                }
            } else {
                quote! {
                    let #fieldname =
                        ::gel_protocol::queryable::Queryable
                        ::decode_optional(#decoder, #sub_arg, #buf)?;
                }
            }
        })
        .collect::<TokenStream>();
    let field_checks = fields
        .iter()
        .enumerate()
        .map(|(field_index, field)| {
            let name_str = &field.str_name;
            let description_str = syn::LitStr::new(
                &format!("field {}", field.str_name.value()),
                field.str_name.span(),
            );
            let get_element = quote! {
                let ::std::option::Option::Some((position, el)) = elements.get(#name_str) else {
                    return ::std::result::Result::Err(ctx.expected(#description_str));
                };
                order.push(*position);
            };

            let fieldtype = &field.ty;
            let check_descriptor = if field.attrs.json {
                quote! {
                    <::gel_protocol::model::Json as
                        ::gel_protocol::queryable::Queryable>
                        ::check_descriptor(ctx, el.type_pos)?
                }
            } else {
                quote! {
                    <#fieldtype as ::gel_protocol::queryable::Queryable>
                        ::check_descriptor(ctx, el.type_pos)?
                }
            };

            let arg_ident = quote::format_ident!("arg_{field_index}");

            quote! {
                #get_element
                let #arg_ident = #check_descriptor;
            }
        })
        .collect::<TokenStream>();
    let construct_sub_args = fields
        .iter()
        .enumerate()
        .map(|(field_index, _)| {
            let arg_ident = quote::format_ident!("arg_{field_index}");
            quote! { #arg_ident,  }
        })
        .collect::<TokenStream>();
    let args_ty = fields
        .iter()
        .map(|field| {
            if field.attrs.json {
                quote! { (), }
            } else {
                let ty = &field.ty;
                quote! { <#ty as ::gel_protocol::queryable::Queryable>::Args, }
            }
        })
        .collect::<TokenStream>();

    let field_count = fields.len();

    let expanded = quote! {
        impl #impl_generics ::gel_protocol::queryable::Queryable
            for #name #ty_generics {
            type Args = (::std::vec::Vec<usize>, (#args_ty));

            fn decode(
                #decoder: &::gel_protocol::queryable::Decoder,
                (#order, #sub_args): &Self::Args,
                #buf: &[u8]
            ) -> ::std::result::Result<Self, ::gel_protocol::errors::DecodeError> {
                let #nfields = #base_fields
                    + if #decoder.has_implicit_id { 1 } else { 0 }
                    + if #decoder.has_implicit_tid { 1 } else { 0 }
                    + if #decoder.has_implicit_tname { 1 } else { 0 };
                let mut #elements =
                    ::gel_protocol::serialization::decode::DecodeTupleLike
                    ::new_object(#buf, #nfields)?;

                #type_id_block
                #type_name_block
                #id_block
                let fields = #elements.read_n(#field_count)?;
                #field_decoders
                ::std::result::Result::Ok(#name {
                    #(
                        #fieldname,
                    )*
                })
            }
            fn check_descriptor(
                ctx: &::gel_protocol::queryable::DescriptorContext,
                type_pos: ::gel_protocol::descriptors::TypePos
            ) -> ::std::result::Result<Self::Args, ::gel_protocol::queryable::DescriptorMismatch>
            {
                use ::gel_protocol::descriptors::Descriptor::ObjectShape;
                let desc = ctx.get(type_pos)?;
                let shape = match desc {
                    ObjectShape(shape) => shape,
                    _ => {
                        return ::std::result::Result::Err(ctx.wrong_type(desc, "str"))
                    }
                };

                // TODO(tailhook) cache shape.id somewhere
                let mut idx = 0;
                #type_id_check
                #type_name_check
                #id_check
                if(shape.elements.len() != #field_count) {
                    return ::std::result::Result::Err(ctx.field_number(
                        #field_count, shape.elements.len())
                    );
                }

                let mut elements = ::std::collections::HashMap::with_capacity(shape.elements.len());
                use ::std::iter::Iterator;
                for (position, element) in shape.elements.iter().enumerate() {
                    elements.insert(element.name.as_str(), (position, element));
                }
                let mut order = ::std::vec::Vec::with_capacity(shape.elements.len());
                #field_checks
                ::std::result::Result::Ok((order, (#construct_sub_args)))
            }
        }
    };
    Ok(expanded)
}
