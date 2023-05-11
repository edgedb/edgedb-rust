# EdgeDB Rust client tutorial

# Getting started (project template repo)

If you just want a working repo to get started, clone the [Rust client examples repo](https://github.com/Dhghomon/edgedb_rust_client_examples), type `edgedb project init` to start an EdgeDB project, and then `cargo run` to run the samples.

This tutorial is essentially a more detailed version of the `main.rs` file inside that repo. It uses the same schema as [the EdgeDB tutorial](https://www.edgedb.com/tutorial), with a few extra types on top.

# Getting started (detailed)

## Cargo

The minimum to add to your Cargo.toml to use the client is [edgedb-tokio](https://docs.rs/edgedb-tokio/latest/edgedb_tokio/):

    edgedb-tokio = "0.3.0"

The next most common dependency is [edgedb-protocol](https://docs.rs/edgedb-protocol/latest/edgedb_protocol/), which includes the EdgeDB types used for data modeling:

    edgedb-protocol = "0.4.0"

A third crate called [edgedb-derive](https://docs.rs/edgedb-derive/latest/edgedb_derive/) contains a `#[derive(Queryable)]` derive macro:

    edgedb-derive = "0.4.0"
    
The Rust client uses tokio so add this to Cargo.toml as well:

    tokio = { version = "1.27.0", features = ["macros", "rt-multi-thread"] }`

If you are avoiding async code and want to emulate a blocking client, you will still need to use tokio as a dependency but can bridge with async using [one of the methods](https://tokio.rs/tokio/topics/bridging) recommended by tokio. This won't require any added features:

    tokio = "1.27.0"

And then you can start a runtime upon which you can use the `.block_on()` method to block and wait for futures to resolve. e.g.:

```rust
let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;
let just_a_string: String =
    rt.block_on(client.query_required_single("select 'This just returns a string'", &()))?;
```

## Edgedb CLI

The `edgedb` CLI initializes EdgeDB projects in the same way cargo does, except it does not create a new directory. So to start a project, use `cargo new (your crate name)` as usual, then go into the directory and type `edgedb project init`. The CLI will prompt you for the instance name and version of EdgeDB to use. It will look something like this:

    PS C:\rust\my_db> edgedb project init
    No `edgedb.toml` found in `\\?\C:\rust\my_db` or above
    Do you want to initialize a new project? [Y/n]
    > Y
    Specify the name of EdgeDB instance to use with this project [default: my_db]:
    > my_db
    Checking EdgeDB versions...
    Specify the version of EdgeDB to use with this project [default: 2.12]:
    > 2.12
    ┌─────────────────────┬─────────────────────────────────┐
    │ Project directory   │ \\?\C:\rust\my_db               │
    │ Project config      │ \\?\C:\rust\my_db\edgedb.toml   │
    │ Schema dir (empty)  │ \\?\C:\rust\my_db\dbschema      │
    │ Installation method │ WSL                             │
    │ Version             │ 2.12+5454e58                    │
    │ Instance name       │ my_db                           │
    └─────────────────────┴─────────────────────────────────┘
    Version 2.12+5454e58 is already installed
    Initializing EdgeDB instance...
    [edgedb] CRITICAL 12273 2023-04-03T08:07:06.626 postgres: the database system is starting up
    [edgedb] CRITICAL 12295 2023-04-03T08:07:25.455 postgres: the database system is starting up
    Applying migrations...
    Everything is up to date. Revision initial
    Project initialized.
    To connect to my_db, run `edgedb`

Inside your cargo project you'll notice some new items:

* edgedb.toml, which is used to mark the directory as an EdgeDB project. The file itself doesn't contain much (just the version of EdgeDB being used) but is used by the CLI to run commands without connection flags (e.g. `edgedb -I my_project migrate` becomes simply `edgedb migrate`). See more on edgedb.toml [here](https://www.edgedb.com/blog/introducing-edgedb-projects).

* A /dbschema folder, inside of which there is:
    * a default.esdl file. This holds your schema.
    * a /migrations folder with .edgeql files starting with 00001. These hold the [ddl](https://www.edgedb.com/docs/reference/ddl/index) commands that were used to migrate your schema. Every time you change your schema followed by `edgedb migration create` and `edgedb migrate`, a new file will be slipped into this directory.

Now that you have the right dependencies and an EdgeDB instance, you can create a client.

# Using the client

Creating a new EdgeDB client can be done in a single line:

```rust
let client = edgedb_tokio::create_client().await.unwrap();
```

Under the hood, this will create a Builder, look for environmental variables and/or an edgedb.toml file and return an Ok(Self) if successful.

If you need a more customized setup, you can use a Builder:

```rust
let mut builder = edgedb_tokio::Builder::uninitialized();
// Read from env vars
builder.read_env_vars().unwrap();
// Or read from an instance
builder.read_instance("my_project").await.unwrap();
// The .build() method returns a Config
let config = builder.build().unwrap();
let client = edgedb_tokio::Client::new(&config);
```

As the documentation notes, in most cases you can just read from the environment:

```
A builder used to create connection configuration
Note: in most cases you don't need to tweak connection configuration as
it's read from the environment. So using
[`create_client`][crate::create_client] in this case
is encouraged.
```

# Queries with the client

Here are the simplified signatures of the client methods involving queries:

(Note: R here means a type that implements [QueryResult](https://docs.rs/edgedb-protocol/0.4.0/edgedb_protocol/trait.QueryResult.html))

```
fn query -> Result<Vec<R>, Error>
fn query_json -> Result<Json, Error>

fn query_single -> Result<Option<R>, Error>
fn query_single_json -> Result<Option<Json>>

fn query_required_single -> Result<R, Error>
fn query_required_single_json -> Result<Json, Error>
```

By the way, the two `_required_single` methods just call the `_single` methods with an `.ok_or_else()` inside so structurally there is nothing different about them.

These methods all take a *query* and *arguments*.

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
edgedb> with
....... message1 := <str>$0,
....... message2 := <str>$1,
....... select message1 ++ ' ' ++ message2;
Parameter <str>$0: Hello there
Parameter <str>$1: General Kenobi
{'Hello there General Kenobi'}
```

But when using the Rust client there is no prompt to do so. Arguments also have to be in the order $0, $1, and so on as opposed to in the REPL where they can be named, as above.

The `()` unit type [implements QueryArgs](https://docs.rs/edgedb-protocol/latest/edgedb_protocol/query_arg/trait.QueryArgs.html#impl-QueryArgs-for-()) and is used when no arguments are present so `&()` is a pretty common sight when using the Rust client:

```rust
client.query("select 'This just returns a string'", &());
```

These methods take two generic parameters:


```rust
let query_res = client.query_required_single::<String, ()>
    ("select {'This just returns a string'}", &()).await?;
    // or
let query_res = client.query_required_single::<String, _>
    ("select {'This just returns a string'}", &()).await?;
```
    
Declaring the type up front tends to look neater than the turbofish syntax:

```rust
let query_res: String = client.query_required_single
    ("select {'This just returns a string'}", &()).await?;
```

# Sample queries

## When cardinality is guaranteed to be 1

Using the `.query()` method works fine in any case, but returns a Vec of results. In this case we return a `Result<Vec<String>>`:

```rust
let query = "select {'This just returns a string'}";
let query_res: Result<Vec<String>, Error> = client.query(query, &()).await;
```

If you know that only a single result will be returned, using .query_required_single() or .query_single() will be more ergonomic:

```rust
let query = "select {'This just returns a string'}";
let query_res: Result<String, Error> = client.query_required_single(query, &()).await;
let query_res_opt: Result<Option<String>, Error> = client.query_single(query, &()).await;
```

## Passing in arguments

Technically arguments can be avoided by passing in a correctly formatted string, but this can be fragile and quickly gets awkward. Imagine that we want to select a tuple and pass in two arguments such that the EdgeDB compiler sees this:

```
select {( 'Hi there', <int32>10 )};
```

Even in an example as simple as this, we need double `{{` curly braces as well as `' '` around the "Hello there" string to keep the EdgeDB compiler from seeing two separate `Hello` and `there` tokens:

```rust
let message = "Hi there";
let num = 10;
let query = format!("select {{
    ('{message}', {num})
    }};");
```

Instead of directly formatting a query, passing in arguments as a tuple allows the query to be a directly typed single &'static str and the cast notation (`<str>`, `<int32>`) makes the types more clearly visible:

```rust
let query = "select {
    (<str>$0, <int32>$1)
    };";
let arguments = ("Hi there", 10);
let query_res: Value = client.query_required_single(query, &arguments).await?;
assert_eq!(
    format!("{query_res:?}"),
    r#"Tuple([Str("Hi there"), Int32(10)])"#
);
```

## Casting inside the EdgeDB compiler

EdgeDB requires arguments to have a cast in the same way that Rust requires a type declaration in function signatures. As such, arguments in queries are used as type specification for the EdgeDB compiler, not to cast from queries from the Rust side. Take this query as an example:

```rust
    let query = "select <int32>$0";
```

This simply means "select an argument that must be an int32", not "take the received argument and cast it into an int32".

As such, this will return an error:

```rust
let query = "select <int32>$0";
let argument = 9i16; // Rust client will expect an int16
let query_res: Result<Value, _> = client.query_required_single(query, &(argument,)).await;
assert!(query_res.unwrap_err().to_string().contains("expected std::int16"));
```

## The Value enum

Thus far we have mostly just worked with Values from our queries in order to print them out and understand them. You can always return a Value from a query, as a Value represents anything returned from EdgeDB. On the other hand, returning a Value can lead to a lot of pattern matching to get to the inner value.

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
    ConfigMemory(ConfigMemory),
    Decimal(Decimal),
    Bool(bool),
    Datetime(Datetime),
    LocalDatetime(LocalDatetime),
    LocalDate(LocalDate),
    LocalTime(LocalTime),
    Duration(Duration),
    RelativeDuration(RelativeDuration),
    DateDuration(DateDuration),
    Json(String),
    Set(Vec<Value>),
    Object { shape: ObjectShape, fields: Vec<Option<Value>> },
    SparseObject(SparseObject),
    Tuple(Vec<Value>),
    NamedTuple { shape: NamedTupleShape, fields: Vec<Value> },
    Array(Vec<Value>),
    Enum(EnumValue),
    Range(Range<Box<Value>>),
}
```

One example of working with a Value:

```rust
    // Inserting an object will return the object's type and id (a Uuid):
    let query = "insert Account {
        username := <str>$0
        };";
    let query_res: Value = client
        .query_required_single(query, &("SomeUserName",))
        .await?;
    // So there is guaranteed to be a Uuid inside this Value.
    match query_res {
        // The fields property is a Vec<Option<Value>>. In this case we'll only have one:
        Value::Object { shape: _, fields } => {
            println!("Insert worked, Fields are: {fields:?}\n");
            for field in fields {
                match field {
                    Some(Value::Uuid(uuid)) => {
                        println!("Got a Uuid: {uuid}")
                    }
                    _other => println!("This shouldn't happen"),
                }
            }
        }
        _other => println!("This shouldn't happen"),
    };
```

## Value enum variants

Most variants of the Value enum correspond to a Rust type from the standard library, while some are from the edgedb-protocol crate and will have to be constructed. For example, this query expecting a `bigint` will return an error as it receives a `20` (an i32) but EdgeDB is expecting a bigint:

```rust
let query = "select <bigint>$0";
let argument = 20;
let query_res: Result<Value, _> = client.query_required_single(query, &(argument,)).await;
assert!(format!("{query_res:?}").contains("expected std::int32"));
```

Instead, first construct a BigInt from the i32 and pass that in as an argument:

```rust
let query = "select <bigint>$0";
let bigint_arg = edgedb_protocol::model::BigInt::from(20);
let query_res: Result<Value, _> = client.query_required_single(query, &(bigint_arg,)).await;
assert_eq!(
    format!("{query_res:?}"),
    "Ok(BigInt(BigInt { negative: false, weight: 0, digits: [20] }))"
);
```

Variants only needing standard library types to construct:

```rust
    Nothing,
    Str(String),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Json(Json), // Holds a String
    Enum(EnumValue), // Holds a str
```

Variants holding types from the edgedb-protocol:

```rust
    Datetime(Datetime),
    LocalDatetime(LocalDatetime),
    LocalDate(LocalDate),
    LocalTime(LocalTime),
    Duration(Duration),
    RelativeDuration(RelativeDuration),
    DateDuration(DateDuration),
    BigInt(BigInt),
    ConfigMemory(ConfigMemory),
    Decimal(Decimal),
```

Variants using a different external crate to construct:

```rust
    Uuid(Uuid), // from https://docs.rs/uuid/latest/uuid/
    Bytes(Bytes), // from https://docs.rs/bytes/latest/bytes/
```

Variants holding other Values:

```rust
    Set(Vec<Value>),
    Object { shape: ObjectShape, fields: Vec<Option<Value>> },
    SparseObject(SparseObject),
    Tuple(Vec<Value>),
    NamedTuple { shape: NamedTupleShape, fields: Vec<Value> },
    Array(Vec<Value>),
    Range(Range<Box<Value>>),
```

## Alternatives to working with Value

There are a lot of alternatives to Value when dealing with the output from EdgeDB on the Rust side.

### Using json

Using json is pretty comfortable for Rust user thanks to serde and serde_json. EdgeDB can cast any type to json with `<json>` so just sticking that in front of a query is enough to return the same object as json:

```rust
let query = "select <json>(
    insert Account {
    username := <str>$0
    }) {
    username, 
    id
    };";

// We know there will only be one result so use query_single_json;
// otherwise it will return a map of json
let json_res = client
    .query_single_json(query, &("SomeUserName",))
    .await?
    .unwrap();
```

You can turn this into a serde_json::Value and access using square brackets:

```rust
let as_value: serde_json::Value = serde_json::from_str(&json_res)?;
println!(
    "Username is {},\nId is {}.\n",
    as_value["username"], as_value["id"]
);
```

But deserializing into a Rust type is much more common (and rigorous). If you have an Account struct that implements Deserialize, you can use serde_json to deserialize the result into an Account.

```rust
#[derive(Debug, Deserialize)]
pub struct Account {
    pub username: String,
    pub id: Uuid,
}

let as_account: Account = serde_json::from_str(&json_res)?;
```

### Using the Queryable macro

The edgedb-derive crate has a built-in Queryable macro that lets us just query without having to cast to json. Same query as before:

```rust
#[derive(Debug, Deserialize, Queryable)]
pub struct QueryableAccount {
    pub username: String,
    pub id: Uuid,
}
let query = "select (
    insert Account {
    username := <str>$0
    }) {
    username, 
    id
    };";
let as_queryable_account: QueryableAccount = client
    .query_required_single(query, &("SomeUserName",))
    .await?;
```

Note: field order (in other words, the "shape" of the query) matters when using the Queryable macro. In the example before, a query is done in the order `id, username` instead of `username, id` as defined in the struct:

```rust
let query = "select (
    insert Account {
    username := <str>$0
    }) {
    id, 
    username
    };";
let cannot_make_into_queryable_account: Result<QueryableAccount, _> =
    client.query_required_single(query, &("SomeUserName",)).await;
assert!(
    format!("{cannot_make_into_queryable_account:?}")
    .contains(r#"error: Some(WrongField { unexpected: "id", expected: "username" })"#);
);
```

### Using json with the edgedb(json) attribute

Adding the edgedb(json) attribute on top of Queryable allows unpacking a struct from json returned from EdgeDB in a single call:

```rust
#[derive(Debug, Deserialize, Queryable)]
#[edgedb(json)]
pub struct JsonQueryableAccount {
    pub username: String,
    pub id: Uuid,
}

let json_queryable_accounts: Vec<JsonQueryableAccount> = client
    .query("select <json>Account { username, id }", &())
    .await
    .unwrap();
```

This attribute can also be used on an inner struct value that implements Queryable. Here, some random json is turned into a HashMap<String, String>:

```rust
#[derive(Debug, Deserialize, Queryable)]
pub struct InnerJsonQueryableAccount {
    pub username: String,
    pub id: Uuid,
    #[edgedb(json)]
    pub some_json: HashMap<String, String>,
}

let query = r#" with j := <json>(
    nice_user := "yes",
    bad_user := "no"
)
select Account {
    username,
    id,
    some_json := j
    };"#;
let query_res: Vec<InnerJsonQueryableAccount> = client.query(query, &()).await.unwrap();
``` 

## Transactions

The client also has a .transaction() method that allows atomic [transactions](https://www.edgedb.com/docs/edgeql/transactions). Wikipedia has a good example of a transaction and why it would be best done atomically:

```
An example of an atomic transaction is a monetary transfer from bank account A 
to account B. It consists of two operations, withdrawing the money from account A 
and saving it to account B. Performing these operations in an atomic transaction 
ensures that the database remains in a consistent state, that is, money is 
neither lost nor created if either of those two operations fails.
```

A transaction removing 10 cents from one customer's account and placing it in the other's account would look as follows:

```rust
#[derive(Debug, Deserialize, Queryable)]
#[edgedb(json)]
pub struct BankCustomer {
    pub name: String,
    pub bank_balance: i32,
}
    // Transactions
    // Customer1 has an account with 110 cents in it.
    // Customer2 has an account with 90 cents in it.
    // Customer1 is going to send 10 cents to Customer 2. This will be a transaction 
    // because we don't want the case to ever occur - even for a split second -  
    // where one account has sent money while the other has not received it yet.

    // After the transaction is over, each customer should have 100 cents.

    let customers_after_transaction = client.transaction(|mut conn| async move {
        let res_1 = conn.query_required_single_json
        ("select(update BankCustomer filter .name = <str>$0 set 
        { bank_balance := .bank_balance - 10 }){name, bank_balance};", &("Customer1",)).await?;
        let res_2 = conn.query_required_single_json
        ("select(update BankCustomer filter .name = <str>$0 set
        { bank_balance := .bank_balance + 10 }){name, bank_balance};", &("Customer2",)).await?;
        Ok(vec![res_1, res_2])
    }).await?;
```

## Links can work instead of transactions

Note that atomic transactions can often be achieved with links instead of transaction operations and is more idiomatic to EdgeDB. For example, a wedding ceremony can in theory be seen as an atomic operation as it involves two instantaneous changes in state (from single to married) and should not have a state in between where Person A is married to Person B while Person B is still not yet married to Person A.

However, in EdgeDB this is better accomplished through links instead: insert a WeddingCertificate with links to a Person type, with the Person type having a computed `spouse` that looks for a MarriageCertificate that includes the Person's government id.

Such a schema might look like this:

```
  type Citizen {
    required name: str;
    required gov_id: int32 {
      constraint exclusive;
    }
    single link spouse := assert_single((
      with 
      id := .gov_id,
      cert := (select MarriageCertificate filter id in {.spouse_1.gov_id, .spouse_2.gov_id}),
      select cert.spouse_2 if cert.spouse_1.gov_id = id else cert.spouse_1
    ));
  }

  type MarriageCertificate {
    required spouse_1: Citizen;
    required spouse_2: Citizen;
    property spouse_ids := { .spouse_1.gov_id, .spouse_2.gov_id };

    trigger prohibit_multi_marriage
            after update, insert 
            for each do (assert(
                not any(__new__.spouse_ids in (MarriageCertificate except __new__).spouse_ids), 
                message := 'Already married to someone else'));
  }
```

First insert two citizens:

```
edgedb> insert Citizen {
....... name := "Citizen1",
....... gov_id := 1
....... };
{default::Citizen {id: 612d4752-eec2-11ed-bd0c-eba0abe0ac68}}
edgedb> insert Citizen {
....... name := "Citizen2",
....... gov_id := 2
....... };
{default::Citizen {id: 6400483a-eec2-11ed-bd0c-572f7ab1b059}}
edgedb> select Citizen {**};
{
  default::Citizen {
    id: 612d4752-eec2-11ed-bd0c-eba0abe0ac68,
    name: 'Citizen1',
    gov_id: 1,
    spouse: {},
  },
  default::Citizen {
    id: 6400483a-eec2-11ed-bd0c-572f7ab1b059,
    name: 'Citizen2',
    gov_id: 2,
    spouse: {},
  },
}
```

The two citizens then get married via a MarriageCertificate that links to both of them, no need for a transaction:

```
edgedb> insert MarriageCertificate {
....... spouse_1 := (select Citizen filter .gov_id = 1),
....... spouse_2 := (select Citizen filter .gov_id = 2)
....... };
{default::MarriageCertificate {id: 82209cd4-eec2-11ed-bd0c-c727d9d35d73}}
edgedb> select MarriageCertificate {**};
{
  default::MarriageCertificate {
    id: 82209cd4-eec2-11ed-bd0c-c727d9d35d73,
    spouse_ids: {1, 2},
    spouse_1: default::Citizen {
      id: 612d4752-eec2-11ed-bd0c-eba0abe0ac68,
      name: 'Citizen1',
      gov_id: 1,
    },
    spouse_2: default::Citizen {
      id: 6400483a-eec2-11ed-bd0c-572f7ab1b059,
      name: 'Citizen2',
      gov_id: 2,
    },
  },
}
```

Selecting the citizens shows that one is now the spouse of the other:

```
edgedb> select Citizen {**};
{
  default::Citizen {
    id: 612d4752-eec2-11ed-bd0c-eba0abe0ac68,
    name: 'Citizen1',
    gov_id: 1,
    spouse: default::Citizen {
      id: 6400483a-eec2-11ed-bd0c-572f7ab1b059,
      name: 'Citizen2',
      gov_id: 2,
    },
  },
  default::Citizen {
    id: 6400483a-eec2-11ed-bd0c-572f7ab1b059,
    name: 'Citizen2',
    gov_id: 2,
    spouse: default::Citizen {
      id: 612d4752-eec2-11ed-bd0c-eba0abe0ac68,
      name: 'Citizen1',
      gov_id: 1,
    },
  },
}
```

Later the citizens decide to go their separate ways, so delete the certificate:

```
edgedb> delete MarriageCertificate filter 1 in .spouse_ids and 2 in .spouse_ids;
{default::MarriageCertificate {id: 82209cd4-eec2-11ed-bd0c-c727d9d35d73}}
```

And now the citizens are back to an empty set for their `spouse` link.

```
edgedb> select Citizen {**};
{
  default::Citizen {
    id: 612d4752-eec2-11ed-bd0c-eba0abe0ac68,
    name: 'Citizen1',
    gov_id: 1,
    spouse: {},
  },
  default::Citizen {
    id: 6400483a-eec2-11ed-bd0c-572f7ab1b059,
    name: 'Citizen2',
    gov_id: 2,
    spouse: {},
  },
}
```