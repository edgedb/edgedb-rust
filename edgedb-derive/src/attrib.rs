use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;

#[derive(Debug)]
enum FieldAttr {
    Json,
}

#[derive(Debug)]
enum ContainerAttr {
    Json,
}

struct FieldAttrList(pub Punctuated<FieldAttr, syn::Token![,]>);
struct ContainerAttrList(pub Punctuated<ContainerAttr, syn::Token![,]>);

pub struct FieldAttrs {
    pub json: bool,
}

pub struct ContainerAttrs {
    pub json: bool,
}

mod kw {
    syn::custom_keyword!(json);
}

impl Parse for FieldAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::json) {
            let _ident: syn::Ident = input.parse()?;
            Ok(FieldAttr::Json)
        } else {
            Err(lookahead.error())
        }
    }
}

impl Parse for ContainerAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::json) {
            let _ident: syn::Ident = input.parse()?;
            Ok(ContainerAttr::Json)
        } else {
            Err(lookahead.error())
        }
    }
}

impl Parse for ContainerAttrList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Punctuated::parse_terminated(input).map(ContainerAttrList)
    }
}

impl Parse for FieldAttrList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Punctuated::parse_terminated(input).map(FieldAttrList)
    }
}

impl FieldAttrs {
    fn default() -> FieldAttrs {
        FieldAttrs { json: false }
    }
    pub fn from_syn(attrs: &[syn::Attribute]) -> syn::Result<FieldAttrs> {
        let mut res = FieldAttrs::default();
        for attr in attrs {
            if matches!(attr.style, syn::AttrStyle::Outer) && attr.path.is_ident("edgedb") {
                let chunk: FieldAttrList = attr.parse_args()?;
                for item in chunk.0 {
                    match item {
                        FieldAttr::Json => res.json = true,
                    }
                }
            }
        }
        Ok(res)
    }
}

impl ContainerAttrs {
    fn default() -> ContainerAttrs {
        ContainerAttrs { json: false }
    }
    pub fn from_syn(attrs: &[syn::Attribute]) -> syn::Result<ContainerAttrs> {
        let mut res = ContainerAttrs::default();
        for attr in attrs {
            if matches!(attr.style, syn::AttrStyle::Outer) && attr.path.is_ident("edgedb") {
                let chunk: ContainerAttrList = attr.parse_args()?;
                for item in chunk.0 {
                    match item {
                        ContainerAttr::Json => res.json = true,
                    }
                }
            }
        }
        Ok(res)
    }
}
