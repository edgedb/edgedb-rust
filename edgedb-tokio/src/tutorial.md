# EdgeDB Rust client tutorial

## Getting started - quick start from repo

If you just want a working repo to get started, clone the [Rust client examples repo](https://github.com/Dhghomon/edgedb_rust_client_examples), type `edgedb project init` to start an EdgeDB project, and then `cargo run` to run the samples.

This tutorial contains a lot of similar examples to those found in the `main.rs` file inside that repo.

## Getting started - your own project

The minimum to add to your Cargo.toml to use the client is [edgedb-tokio](https://docs.rs/edgedb-tokio/latest/edgedb_tokio/):

    edgedb-tokio = "0.4.0"

The next most common dependency is [edgedb-protocol](https://docs.rs/edgedb-protocol/latest/edgedb_protocol/), which includes the EdgeDB types used for data modeling:

    edgedb-protocol = "0.4.0"

A third crate called [edgedb-derive](https://docs.rs/edgedb-derive/latest/edgedb_derive/) contains the `#[derive(Queryable)]` derive macro which is the main way to unpack EdgeDB output into Rust types:

    edgedb-derive = "0.5.0"
    
The Rust client uses tokio so add this to Cargo.toml as well:

    tokio = { version = "1.28.0", features = ["macros", "rt-multi-thread"] }`

If you are avoiding async code and want to emulate a blocking client, you will still need to use tokio as a dependency but can bridge with async using [one of the bridging methods recommended by tokio](https://tokio.rs/tokio/topics/bridging). This won't require any added features:

    tokio = "1.28.0"

And then you can start a runtime upon which you can use the `.block_on()` method to block and wait for futures to resolve:

```rust
let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;
let just_a_string: String =
    rt.block_on(client.query_required_single("select 'Just a string'", &()))?;
```

## Edgedb CLI

The `edgedb` CLI initializes EdgeDB projects in the same way cargo does, except it does not create a new directory. So to start a project, use `cargo new <your_crate_name>` as usual, then go into the directory and type `edgedb project init`. The CLI will prompt you for the instance name and version of EdgeDB to use. It will look something like this:

    PS C:\rust\my_db> edgedb project init
    No `edgedb.toml` found in `\\?\C:\rust\my_db` or above
    Do you want to initialize a new project? [Y/n]
    > Y
    Specify the name of EdgeDB instance to use with this project [default: my_db]:
    > my_db
    Checking EdgeDB versions...
    Specify the version of EdgeDB to use with this project [default: 2.15]:
    > 2.15
    ┌─────────────────────┬─────────────────────────────────┐
    │ Project directory   │ \\?\C:\rust\my_db               │
    │ Project config      │ \\?\C:\rust\my_db\edgedb.toml   │
    │ Schema dir (empty)  │ \\?\C:\rust\my_db\dbschema      │
    │ Installation method │ WSL                             │
    │ Version             │ 2.15+75c3494                    │
    │ Instance name       │ my_db                           │
    └─────────────────────┴─────────────────────────────────┘
    Version 2.15+75c3494 is already installed
    Initializing EdgeDB instance...
    Applying migrations...
    Everything is up to date. Revision initial
    Project initialized.
    To connect to my_db, run `edgedb`

Inside your cargo project you'll notice some new items:

* `edgedb.toml`, which is used to mark the directory as an EdgeDB project. The file itself doesn't contain much — just the version of EdgeDB being used — but is used by the CLI to run commands without connection flags. (E.g., `edgedb -I my_project migrate` becomes simply `edgedb migrate`). See more on edgedb.toml [in the blog post introducing the EdgeDB projects CLI](https://www.edgedb.com/blog/introducing-edgedb-projects).

* A `/dbschema` folder containing:
    * a `default.esdl` file which holds your schema. You can change the schema by directly modifying this file followed by `edgedb migration create` and `edgedb migrate`.
    * a `/migrations` folder with `.edgeql` files named starting at `00001`. These hold the [ddl](https://www.edgedb.com/docs/reference/ddl/index) commands that were used to migrate your schema. A new file will show up in this directory every time your schema is migrated.

If you are running EdgeDB 3.0 and above, you also have the option of using the [edgedb watch](https://www.edgedb.com/docs/cli/edgedb_watch) command. Doing so starts a long-running process that keeps an eye on changes in `/dbschema`, automatically applying these changes in real time.

Now that you have the right dependencies and an EdgeDB instance, you can create a client.

# Using the client

Creating a new EdgeDB client can be done in a single line:

```rust
let client = edgedb_tokio::create_client().await?;
```

Under the hood, this will create a [Builder](crate::Builder), look for environmental variables and/or an `edgedb.toml` file and return an `Ok(Self)` if successful. This `Builder` can be used on its own instead of `create_client()` if you need a more customized setup.

# Queries with the client

Here are the simplified signatures of the client methods involving queries:

(Note: `R` here means a type that implements [`QueryResult`](https://docs.rs/edgedb-protocol/0.4.0/edgedb_protocol/trait.QueryResult.html))

```rust
fn query -> Result<Vec<R>, Error>
fn query_json -> Result<Json, Error>

fn query_single -> Result<Option<R>, Error>
fn query_single_json -> Result<Option<Json>>

fn query_required_single -> Result<R, Error>
fn query_required_single_json -> Result<Json, Error>
```

Note the difference between the `_single` and the `_required_single` methods:

* The `_required_single` methods return empty results as a `NoDataError` which allows propagating errors normally through an application,
* The `_single` methods will simply give you an `Ok(None)` in this case.

These methods all take a *query* (a `&str`) and *arguments* (something that implements the [`QueryArgs`](https://docs.rs/edgedb-protocol/latest/edgedb_protocol/query_arg/trait.QueryArgs.html) trait).

The `()` unit type `QueryArgs` and is used when no arguments are present so `&()` is a pretty common sight when using the Rust client.

```rust
// Without arguments: just add &() after the query
let query_res: String = client.query_required_single("select 'Just a string'", &()).await?;

// With arguments, same output
let one = " a ";
let two = "string";
let query_res: String = client
    .query_required_single("select 'Just' ++ <str>$0 ++ <str>$1", &(first, second))
    .await?;
```

More information on passing in arguments can be found in [its own section](#passing-in-arguments) below.

These methods take two generic parameters which can be specified with the turbofish syntax:

```rust
let query_res = client
    .query_required_single::<String, ()>("select 'Just a string'", &())
    .await?;
// or
let query_res = client
    .query_required_single::<String, _>("select 'Just a string'", &())
    .await?;
```
    
But declaring the final expected type up front tends to look neater.

```rust
let query_res: String = client
    .query_required_single("select 'Just a string'", &())
    .await?;
```

# Sample queries

## When cardinality is guaranteed to be 1

Using the `.query()` method works fine for any cardinality, but returns a `Vec` of results. This query with a cardinality of 1 returns a `Result<Vec<String>>` which becomes a `Vec<String>` after the error is handled:

```rust
let query = "select 'Just a string'";
let query_res: Vec<String> = client.query(query, &()).await?;
```

But if you know that only a single result will be returned, using `.query_required_single()` or `.query_single()` will be more ergonomic:

```rust
let query = "select 'Just a string'";
let query_res: String = client.query_required_single(query, &()).await?;
let query_res_opt: Option<String> = client.query_single(query, &()).await?;
```

## Using the `Queryable` macro

The easiest way to unpack an EdgeDB query result is the built-in `Queryable` macro from the `edgedb-derive` crate. This turns queries directly into Rust types without having to match on a `Value` (more on the `Value` enum in [its own section](#the-value-enum)), cast to JSON, etc.

```rust
#[derive(Debug, Deserialize, Queryable)]
pub struct QueryableAccount {
    pub username: String,
    pub id: Uuid,
}

let query = "select account {
      username,
      id
    };";
let as_queryable_account: QueryableAccount = client
    .query_required_single(query, &())
    .await?;
```

Note: Field order within the shape of the query matters when using the `Queryable` macro. In the example before, a query is done in the order `id, username` instead of `username, id` as defined in the struct:

```rust
let query = "select account {
      id,
      username
    };";
let wrong_order: Result<QueryableAccount, _> = client
    .query_required_single(query, &())
    .await;
assert!(
    format!("{wrong_order:?}")
    .contains(r#"WrongField { unexpected: "id", expected: "username" }"#);
);
```

You can use [cargo expand](https://github.com/dtolnay/cargo-expand) with the nightly compiler to see the code generated by the Queryable macro, but the minimal example repo also contains a somewhat cleaned up version of the generated code [here](https://github.com/Dhghomon/edgedb_rust_client_examples/blob/master/src/lib.rs#L12).

## Passing in arguments

A regular EdgeQL query without arguments looks like this:

```
with 
    message1 := 'Hello there', 
    message2 := 'General Kenobi', 
select message1 ++ ' ' ++ message2;
```

And the same query with arguments:

```
with 
    message1 := <str>$0, 
    message2 := <str>$1, 
select message1 ++ ' ' ++ message2;
```

In the EdgeQL REPL you are prompted to enter arguments:

```
db> with
... message1 := <str>$0,
... message2 := <str>$1,
... select message1 ++ ' ' ++ message2;
Parameter <str>$0: Hello there
Parameter <str>$1: General Kenobi
{'Hello there General Kenobi'}
```

But when using the Rust client there is no prompt to do so. At present, arguments also have to be in the order `$0`, `$1`, and so on as opposed to in the REPL where they can be named (e.g. `$message` and `$person` instead of `$0` and `$1`). The arguments in the client are then passed in as a tuple:

```rust
let arguments = ("Nice movie", 2023);
let query = "with
movie := (insert Movie {
  title := <str>$0,
  release_year := <int32>$1
})
  select  {
    title,
    release_year,
    id
}";
let query_res: Value = client.query_required_single(query, &(arguments)).await?;
```

## The `Value` enum

The [Value](https://docs.rs/edgedb-protocol/latest/edgedb_protocol/value/enum.Value.html) enum can be found in the edgedb-protocol crate. A `Value` represents anything returned from EdgeDB so you can always return a `Value` from any of the query methods without needing to deserialize into a Rust type, and the enum can be instructive in getting to know the protocol. On the other hand, returning a `Value` leads to pattern matching to get to the inner value and is not the most ergonomic way to work with results from EdgeDB.

```rust
pub enum Value {
    Nothing,
    Uuid(Uuid),
    Str(String),
    Bytes(Vec<u8>),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    BigInt(BigInt),
    // ... and so on
}
```

Most variants of the `Value` enum correspond to a Rust type from the standard library, while some are from the `edgedb-protocol` crate and will have to be constructed. For example, this query expecting a `bigint` will return an error as it receives a `20` (an `i32`) but EdgeDB is expecting a `bigint`:

```rust
let query = "select <bigint>$0";
let argument = 20;
let query_res: Result<Value, _> = client.query_required_single(query, &(argument,)).await;
assert!(format!("{query_res:?}").contains("expected std::int32"));
```

Instead, first construct a `BigInt` from the `i32` and pass that in as an argument:

```rust
use edgedb_protocol::model::BigInt;

let query = "select <bigint>$0";
let bigint_arg = BigInt::from(20);
let query_res: Result<Value, _> = client.query_required_single(query, &(bigint_arg,)).await;
assert_eq!(
    format!("{query_res:?}"),
    "Ok(BigInt(BigInt { negative: false, weight: 0, digits: [20] }))"
);
```

## Casting inside the EdgeDB compiler

EdgeDB requires arguments to have a cast in the same way that Rust requires a type declaration in function signatures. As such, arguments in queries are used as type specification for the EdgeDB compiler, not to cast from queries from the Rust side. Take this query as an example:

```rust
let query = "select <int32>$0";
```

This simply means "select an argument that must be an `int32`", not "take the received argument and cast it into an `int32`".

As such, this will return an error:

```rust
let query = "select <int32>$0";
let argument = 9i16; // Rust client will expect an int16
let query_res: Result<Value, _> = client.query_required_single(query, &(argument,)).await;
assert!(query_res
    .unwrap_err()
    .to_string()
    .contains("expected std::int16"));
```

## Using JSON

EdgeDB can cast any type to JSON with `<json>`, but the `_json` methods don't require this cast in the query. This result can be turned into a String and used to respond to some JSON API request directly, unpacked into a struct using `serde` and `serde_json`, etc.

```rust
#[derive(Debug, Deserialize)]
pub struct Account {
    pub username: String,
    pub id: Uuid,
}

// No need for <json> cast here
let query = "with account := 
    (insert Account {
        username := <str>$0
    }),
    select account {
      username,
      id
    };";

// We know there will only be one result so use query_single_json;
// otherwise it will return a map of json
let json_res = client
    .query_single_json(query, &("SomeUserName",))
    .await?
    .unwrap();

// Format: {"username" : "SomeUser1", "id" : "7093944a-fd3a-11ed-a013-c7de12ffe7a9"}
let as_string = json_res.to_string();
let as_account: Account = serde_json::from_str(&json_res)?;
```

## Transactions

The client also has a `.transaction()` method that allows atomic [transactions](https://www.edgedb.com/docs/edgeql/transactions). Wikipedia has a good example of a transaction and why it would be best done atomically:

```
An example of an atomic transaction is a monetary transfer from bank account A
to account B. It consists of two operations, withdrawing the money from 
account A and saving it to account B. Performing these operations in an atomic
transaction ensures that the database remains in a consistent state, that is,
money is neither lost nor created if either of those two operations fails.
```

A transaction removing 10 cents from one customer's account and placing it in another's would look like this:

```rust
#[derive(Debug, Deserialize, Queryable)]
pub struct BankCustomer {
    pub name: String,
    pub bank_balance: i32,
}
// Customer1 has an account with 110 cents in it.
// Customer2 has an account with 90 cents in it.
// Customer1 is going to send 10 cents to Customer 2. This will be a transaction
// because we don't want the case to ever occur - even for a split second -
// where one account has sent money while the other has not received it yet.

// After the transaction is over, each customer should have 100 cents.

let query = "with customer := (
    update BankCustomer filter .name = <str>$0
    set { bank_balance -= 10 }
    ),
    select customer {
      name,
      bank_balance
    };";

cloned_client
    .transaction(|mut conn| async move {
        let _res_1: BankCustomer = conn.query_required_single(query, &(c1,)).await?;
        let _res_2: BankCustomer = conn.query_required_single(query, &(&c2,)).await?;
    })
    .await?;
```

Note that atomic transactions can often be achieved with links and [backlinks](https://www.edgedb.com/docs/edgeql/paths#backlinks) instead of transaction operations, which is both more idiomatic to EdgeDB and easier to use.

For example, if one object holds a `required link` to two other objects and each of these two objects has a single banklink to the first one, simply inserting the first object will effectively change the state of the other two instantaneously.

## Client configuration

The Client can still be configured after initialization via the `with_` methods ([`with_retry_options`](crate::Client::with_retry_options), [`with_transaction_options`](crate::Client::with_transaction_options), etc.) that create a shallow copy of the client with adjusted options.

```rust
    // Take a schema with matching Rust structs:
    //
    // module default {
    //   type User {
    //     required property name -> str;
    //   }
    // }

    // module test {
    //   type User {
    //     required property name -> str;
    //   }
    // };
    
    // The regular client will query from module 'default' by default
    let client = edgedb_tokio::create_client().await?;
    
    // This client will query from module 'test' by default
    // The original client is unaffected
    let test_client = client.with_default_module(Some("test"));
        
    // Each client queries separately with different behavior
    let query = "select User {name};";
    let users: Vec<User> = client.query(query, &()).await?;
    let test_users: Vec<TestUser> = test_client.query(query, &()).await?;

    // Many other clients can be created with different options,
    // all independent of the main client:
    let transaction_opts = TransactionOptions::default().read_only(true);
    let _read_only_client = client.with_transaction_options(transaction_opts);

    let retry_opts = RetryOptions::default().with_rule(
        RetryCondition::TransactionConflict,
        // No. of retries
        1,
        // Retry immediately instead of default with increasing backoff
        |_| std::time::Duration::from_millis(0),
    );
    let _immediate_retry_once_client = client.with_retry_options(retry_opts);
```