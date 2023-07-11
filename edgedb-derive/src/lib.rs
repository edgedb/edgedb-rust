/*!
Derive macro that allows structs and enums to be populated by database
queries.

This derive can be used on structures with named fields (which correspond
to "shapes" in EdgeDB). Note that field order matters, so the struct below
corresponds to an EdgeDB `User` query with `first_name` followed by `age`.
A `DescriptorMismatch` will be returned if the fields in the Rust struct
are not in the same order as those in the query shape.

```rust
# use edgedb_derive::Queryable;
#[derive(Queryable)]
struct User {
    first_name: String,
    age: i32,
}
```

This allows a query to directly unpack into the type instead
of working with the [Value](https://docs.rs/edgedb-protocol/latest/edgedb_protocol/value/enum.Value.html) enum.

```rust,ignore
let query = "select User { first_name, age };";
// With Queryable:
let query_res: Vec<User> = client.query(query, &()).await?;
// Without Queryable:
let query_res: Vec<Value> = client.query(query, &()).await?;
```

# Field attributes

## JSON

The `#[edgedb(json)]` attribute decodes a field using `serde_json` instead
of the EdgeDB binary protocol. This is useful if some data is stored in
the database as JSON, but you need to process it. The underlying type must
implement `serde::Deserialize`.

```rust
# use std::collections::HashMap;
# use edgedb_derive::Queryable;

#[derive(Queryable)]
struct User {
    #[edgedb(json)]
    user_notes: HashMap<String, String>,
}
```

# Container attributes

## JSON

The `#[edgedb(json)]` attribute can be used to unpack the structure from
the returned JSON.  The underlying type must implement
`serde::Deserialize`.

```rust
# use edgedb_derive::Queryable;
#[derive(Queryable, serde::Deserialize)]
#[edgedb(json)]
struct JsonData {
    field1: String,
    field2: u32,
}
```

This allows a query to directly unpack into the type without an intermediate
step using [serde_json::from_str](https://docs.rs/serde_json/latest/serde_json/fn.from_str.html):

```rust,ignore
let query = "select <json>JsonData { field1, field2 };";
let query_res: Vec<JsonData> = client.query(query, &()).await?;
```

*/
extern crate proc_macro;

use proc_macro::TokenStream;
use syn::{self, parse_macro_input};

mod attrib;
mod enums;
mod json;
mod shape;
mod variables;

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
            syn::Item::Enum(s) => enums::derive_enum(s),
            _ => {
                return Err(syn::Error::new_spanned(item,
                    "can only derive Queryable for a struct and enum \
                     in non-JSON mode"
                ));
            }
        }
    }
}

#[proc_macro_derive(GlobalsDelta, attributes(edgedb))]
pub fn globals_delta(input: TokenStream) -> TokenStream {
    let s = parse_macro_input!(input as syn::ItemStruct);
    match variables::derive_globals(&s) {
        Ok(stream) => stream.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(ConfigDelta, attributes(edgedb))]
pub fn config_delta(input: TokenStream) -> TokenStream {
    let s = parse_macro_input!(input as syn::ItemStruct);
    match variables::derive_config(&s) {
        Ok(stream) => stream.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
