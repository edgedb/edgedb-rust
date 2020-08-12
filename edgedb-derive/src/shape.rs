use proc_macro2::TokenStream;
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
    let has_id = fieldname.iter().find(|x| x.to_string() == "id").is_some();
    let has_type_id = fieldname.iter().find(|x| x.to_string() == "__tid__").is_some();
    let implicit_fields =
        if has_id { 0 } else { 1 } +
        if has_type_id { 0 } else { 1 };
    let nfields = fields.len()+implicit_fields;
    let typeid_block = if has_type_id {
        None
    } else {
        Some(quote! {
            if ::bytes::buf::Buf::remaining(buf) < 8 {
                return ::edgedb_protocol::errors::Underflow.fail();
            }
            let _reserved = ::bytes::buf::Buf::get_i32(buf);
            let len = ::bytes::buf::Buf::get_u32(buf) as usize;
            if ::bytes::buf::Buf::remaining(buf) < len {
                return ::edgedb_protocol::errors::Underflow.fail();
            }
            ::bytes::buf::Buf::advance(buf, len);
        })
    };
    let id_block = if has_id {
        None
    } else {
        Some(quote! {
            if ::bytes::buf::Buf::remaining(buf) < 8 {
                return ::edgedb_protocol::errors::Underflow.fail();
            }
            let _reserved = ::bytes::buf::Buf::get_i32(buf);
            let len = ::bytes::buf::Buf::get_u32(buf) as usize;
            if ::bytes::buf::Buf::remaining(buf) < len {
                return ::edgedb_protocol::errors::Underflow.fail();
            }
            ::bytes::buf::Buf::advance(buf, len);
        })
    };
    let type_id_check = if has_type_id {
        None
    } else {
        Some(quote! {
            if(!shape.elements[0].flag_implicit) {
                return Err(ctx.expected("implicit __tid__"));
            }
        })
    };
    let id_check = if has_id {
        None
    } else {
        let n: usize = if has_type_id { 1 } else { 0 };
        Some(quote! {
            if(!shape.elements[#n].flag_implicit) {
                return Err(ctx.expected("implicit id"));
            }
        })
    };
    let field_decoders = fields.iter().map(|field| {
        let mut result = quote!{
            if ::bytes::buf::Buf::remaining(buf) < 8 {
                return ::edgedb_protocol::errors::Underflow.fail();
            }
            let _reserved = ::bytes::buf::Buf::get_i32(buf);
            let len = ::bytes::buf::Buf::get_u32(buf) as usize;
            if ::bytes::buf::Buf::remaining(buf) < len {
                return ::edgedb_protocol::errors::Underflow.fail();
            }
            let off = ::std::io::Cursor::position(buf) as usize;
            let mut chunk = ::std::io::Cursor::new(
                buf.get_ref().slice(off..off + len));
            ::bytes::buf::Buf::advance(buf, len);
        };
        let ref fieldname = field.name;
        if field.attrs.json {
            result.extend(quote!{
                let #fieldname: ::edgedb_protocol::json::Json =
                    ::edgedb_protocol::queryable::Queryable::decode(
                        &mut chunk)?;
                let #fieldname = ::serde_json::from_str(#fieldname.as_ref())
                    .map_err(::edgedb_protocol::errors::decode_error)?;
            });
        } else {
            result.extend(quote!{
                let #fieldname =
                    ::edgedb_protocol::queryable::Queryable::decode(
                        &mut chunk)?;
            });
        }
        result
    }).collect::<TokenStream>();
    let field_checks = fields.iter().enumerate().map(|(idx, field)| {
        let idx = idx + implicit_fields;
        let ref name_str = field.str_name;
        let mut result = quote!{
            let el = &shape.elements[#idx];
            if(el.name != #name_str) {
                return Err(ctx.wrong_field(&el.name, #name_str));
            }
        };
        let ref fieldtype = field.ty;
        if field.attrs.json {
            result.extend(quote!{
                <::edgedb_protocol::json::Json as
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
            fn decode_raw(buf: &mut ::std::io::Cursor<::bytes::Bytes>)
                -> Result<Self, ::edgedb_protocol::errors::DecodeError>
            {
                if ::bytes::buf::Buf::remaining(buf) < 4 {
                    return ::edgedb_protocol::errors::Underflow.fail();
                }
                let size = ::bytes::buf::Buf::get_u32(buf) as usize;
                if size != #nfields {
                    return ::edgedb_protocol::errors::ObjectSizeMismatch.fail()
                }

                #typeid_block
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

                if(shape.elements.len() != #nfields) {
                    return Err(ctx.field_number(
                        shape.elements.len(), #nfields));
                }
                #type_id_check
                #id_check
                #field_checks
                Ok(())
            }
        }
    };
    Ok(expanded)
}
