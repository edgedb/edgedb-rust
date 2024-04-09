use proc_macro2::{TokenStream, Span};
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
                &s.fields, "only named fields are supported"));
        }
    };
    let fieldname = fields.iter()
        .map(|f| f.name.clone()).collect::<Vec<_>>();
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
                return Err(ctx.expected("implicit __tid__"));
            }
            idx += 1;
        }
    });
    let type_name_check = Some(quote! {
        if ctx.has_implicit_tname {
            if(!shape.elements[idx].flag_implicit) {
                return Err(ctx.expected("implicit __tname__"));
            }
            idx += 1;
        }
    });
    let id_check = Some(quote! {
        if ctx.has_implicit_id {
            if(!shape.elements[idx].flag_implicit) {
                return Err(ctx.expected("implicit id"));
            }
            idx += 1;
        }
    });
    let field_decoders = fields.iter().map(|field| {
        let fieldname = &field.name;
        if field.attrs.json {
            quote!{
                let #fieldname: ::edgedb_protocol::model::Json =
                    <::edgedb_protocol::model::Json as
                        ::edgedb_protocol::queryable::Queryable>
                    ::decode_optional(#decoder, #elements.read()?)?;
                let #fieldname = ::serde_json::from_str(#fieldname.as_ref())
                    .map_err(::edgedb_protocol::errors::decode_error)?;
            }
        } else {
            quote!{
                let #fieldname =
                    ::edgedb_protocol::queryable::Queryable
                    ::decode_optional(#decoder, #elements.read()?)?;
            }
        }
    }).collect::<TokenStream>();
    let field_checks = fields.iter().map(|field| {
        let name_str = &field.str_name;
        let mut result = quote!{
            let el = &shape.elements[idx];
            if(el.name != #name_str) {
                return Err(ctx.wrong_field(#name_str, &el.name));
            }
            idx += 1;
        };
        let fieldtype = &field.ty;
        if field.attrs.json {
            result.extend(quote!{
                <::edgedb_protocol::model::Json as
                    ::edgedb_protocol::queryable::Queryable>
                    ::check_descriptor(ctx, el.type_pos)?;
            });
        } else {
            result.extend(quote!{
                <#fieldtype as ::edgedb_protocol::queryable::Queryable>
                    ::check_descriptor(ctx, el.type_pos)?;
            });
        }
        result
    }).collect::<TokenStream>();

    let expanded = quote! {
        impl #impl_generics ::edgedb_protocol::queryable::Queryable
            for #name #ty_generics {
            fn decode(#decoder: &::edgedb_protocol::queryable::Decoder, #buf: &[u8])
                -> Result<Self, ::edgedb_protocol::errors::DecodeError>
            {
                let #nfields = #base_fields
                    + if #decoder.has_implicit_id { 1 } else { 0 }
                    + if #decoder.has_implicit_tid { 1 } else { 0 }
                    + if #decoder.has_implicit_tname { 1 } else { 0 };
                let mut #elements =
                    ::edgedb_protocol::serialization::decode::DecodeTupleLike
                    ::new_object(#buf, #nfields)?;

                #type_id_block
                #type_name_block
                #id_block
                #field_decoders
                Ok(#name {
                    #(
                        #fieldname,
                    )*
                })
            }
            fn check_descriptor(
                ctx: &::edgedb_protocol::queryable::DescriptorContext,
                type_pos: ::edgedb_protocol::descriptors::TypePos)
                -> Result<(), ::edgedb_protocol::queryable::DescriptorMismatch>
            {
                use ::edgedb_protocol::descriptors::Descriptor::ObjectShape;
                let desc = ctx.get(type_pos)?;
                let shape = match desc {
                    ObjectShape(shape) => shape,
                    _ => {
                        return Err(ctx.wrong_type(desc, "str"))
                    }
                };

                // TODO(tailhook) cache shape.id somewhere
                let mut idx = 0;

                #type_id_check
                #type_name_check
                #id_check
                #field_checks

                if(shape.elements.len() != idx) {
                    return Err(ctx.field_number(
                        shape.elements.len(), idx));
                }
                Ok(())
            }
        }
    };
    Ok(expanded)
}
