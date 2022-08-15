use quote::quote;
use proc_macro2::Span;


pub fn derive(item: &syn::ItemStruct) -> syn::Result<proc_macro2::TokenStream> {
    let man = syn::Ident::new("man", Span::mixed_site());

    let fields = match &item.fields {
        syn::Fields::Named(fields) => fields,
        _ => {
            return Err(syn::Error::new_spanned(&item.fields,
                "only named fields are supported"));
        }
    };
    let set_vars = fields.named.iter().map(|f| {
        let ident = f.ident.as_ref().expect("a named field");
        let name = ident.to_string();
        quote!{ #man.set(#name, self.#ident); }
    }).collect::<Vec<_>>();

    let name = &item.ident;
    let (impl_generics, ty_generics, where_c) = item.generics.split_for_impl();
    let expanded = quote! {
        impl #impl_generics ::edgedb_tokio::state::VariablesDelta
            for &'_ #name #ty_generics
            #where_c
        {
            fn apply(self, #man: &mut ::edgedb_tokio::state::VariablesModifier)
            {
                #(#set_vars)*
            }
        }
    };
    Ok(expanded)
}
