//! Connection state modification utilities

use std::collections::{BTreeMap, HashMap};
use std::default::Default;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use edgedb_protocol::client_message::{State as EncodedState};
use edgedb_protocol::descriptors::{RawTypedesc,StateBorrow};
use edgedb_protocol::query_arg::QueryArg;
use edgedb_protocol::value::Value;
use edgedb_protocol::model::Uuid;

use crate::errors::{ClientError, ProtocolEncodingError, Error, ErrorKind};

/// Unset a set of global or config variables
///
/// Accepts an iterator of names. Used with globals lie this:
///
/// ```rust,no_run
/// # use edgedb_tokio::state::Unset;
/// # #[tokio::main]
/// # async fn main() {
/// # let conn = edgedb_tokio::create_client().await.unwrap();
/// let conn = conn.with_globals(Unset(["xxx", "yyy"]));
/// # }
/// ```
#[derive(Debug)]
pub struct Unset<I>(pub I);

/// Use a closure to set or unset global or config variables
///
/// ```rust,no_run
/// # use edgedb_tokio::state::{Fn, GlobalsModifier};
/// # #[tokio::main]
/// # async fn main() {
/// # let conn = edgedb_tokio::create_client().await.unwrap();
/// let conn = conn.with_globals(Fn(|m: &mut GlobalsModifier| {
///     m.set("x", "x_value");
///     m.unset("y");
/// }));
/// # }
/// ```
#[derive(Debug)]
pub struct Fn<F>(pub F);


#[derive(Debug)]
pub struct PoolState {
    raw_state: RawState,
    cache: ArcSwapOption<EncodedState>,
}

#[derive(Debug)]
struct RawState {
    // The idea behind the split between common and globals is that once
    // setting module/aliases/config becomes bottleneck it's possible to have
    // connection pool with those settings pre-set. But globals are supposed to
    // have per-request values more often (one example is having `user_id`
    // global variable).
    common: Arc<CommonState>,
    globals: BTreeMap<String, Value>,
}

#[derive(Debug)]
struct CommonState {
    module: Option<String>,
    aliases: BTreeMap<String, String>,
    config: BTreeMap<String, Value>,
}

/// Utility object used to modify globals
///
/// This object is passed to [`Fn`] closure and [`GlobalsDelta::apply`].
#[derive(Debug)]
pub struct GlobalsModifier<'a> {
    globals: &'a mut BTreeMap<String, Value>,
    module: &'a str,
    aliases: &'a BTreeMap<String, String>,
}

/// Utility object used to modify config
///
/// This object is passed to [`Fn`] closure and [`ConfigDelta::apply`].
#[derive(Debug)]
pub struct ConfigModifier<'a> {
    config: &'a mut BTreeMap<String, Value>,
}

/// Utility object used to modify aliases
///
/// This object is passed to [`AliasesDelta::apply`] to do the actual
/// modification
#[derive(Debug)]
pub struct AliasesModifier<'a> {
    data: &'a mut BTreeMap<String, String>,
}

/// Trait that modifies global variables
pub trait GlobalsDelta {
    /// Applies variables delta using specified modifier object
    fn apply(self, man: &mut GlobalsModifier<'_>);
}

/// Trait that modifies config variables
pub trait ConfigDelta {
    /// Applies variables delta using specified modifier object
    fn apply(self, man: &mut ConfigModifier<'_>);
}

/// Trait that modifies module aliases
pub trait AliasesDelta {
    /// Applies variables delta using specified modifier object
    fn apply(self, man: &mut AliasesModifier);
}

pub trait SealedState {
    fn encode(&self, desc: &RawTypedesc)
        -> Result<EncodedState, Error>;
}

/// Provides state of the session in the binary form
///
/// This trait is sealed.
pub trait State: SealedState + Send + Sync {
}

impl GlobalsModifier<'_> {
    /// Set global variable to a value
    ///
    /// If `key` doesn't contain module name (`::` to be more
    /// specific) then the variable name is resolved using current module.
    /// Otherwise, modules are resolved using aliases if any. Note: modules are
    /// resolved at method call time. This means that a sequence like this:
    /// ```rust,no_run
    /// # use edgedb_tokio::state::Fn;
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let conn = edgedb_tokio::create_client().await.unwrap();
    /// let conn = conn
    ///     .with_globals_fn(|m| m.set("var1", "value1"))
    ///     .with_default_module(Some("another_module"))
    ///     .with_globals_fn(|m| m.set("var1", "value2"));
    /// # }
    /// ```
    /// Will set `var1` in `default` and in `another_module` to different
    /// values.
    ///
    /// # Panics
    ///
    /// This methods panics if `value` cannot be converted into dynamically
    /// typed `Value` (`QueryArg::to_value()` method returns error). To avoid
    /// this panic use either native EdgeDB types (e.g.
    /// `edgedb_protocol::model::Datetime` instead of `std::time::SystemTime`
    /// or call `to_value` manually before passing to `set`.
    pub fn set<T: QueryArg>(&mut self, key: &str, value: T) {
        let value = value.to_value().expect("global can be encoded");
        if let Some(ns_off) = key.rfind("::") {
            if let Some(alias) = self.aliases.get(&key[..ns_off]) {
                self.globals.insert(
                    format!("{alias}::{suffix}", suffix=&key[ns_off+2..]),
                    value,
                );
            } else {
                self.globals.insert(key.into(), value);
            }
        } else {
            self.globals.insert(format!("{}::{}", self.module, key), value);
        }
    }
    /// Unset the global variable
    ///
    /// In most cases this will effectively set the variable to a default
    /// value.
    ///
    /// To set variable to the actual empty value use `set("name",
    /// Value::Nothing)`.
    ///
    /// Note: same namespacing rules like for `set` are applied here.
    pub fn unset(&mut self, key: &str) {
        if let Some(ns_off) = key.rfind("::") {
            if let Some(alias) = self.aliases.get(&key[..ns_off]) {
                self.globals.remove(
                    &format!("{alias}::{suffix}", suffix=&key[ns_off+2..]));
            } else {
                self.globals.remove(key);
            }
        } else {
            self.globals.remove(&format!("{}::{}", self.module, key));
        }
    }
}

impl ConfigModifier<'_> {
    /// Set configuration setting to a value
    ///
    /// # Panics
    ///
    /// This methods panics if `value` cannot be converted into dynamically
    /// typed `Value` (`QueryArg::to_value()` method returns error). To avoid
    /// this panic use either native EdgeDB types (e.g.
    /// `edgedb_protocol::model::Datetime` instead of `std::time::SystemTime`
    /// or call `to_value` manually before passing to `set`.
    pub fn set<T: QueryArg>(&mut self, key: &str, value: T) {
        let value = value.to_value().expect("config can be encoded");
        self.config.insert(key.into(), value);
    }
    /// Unset the global variable
    ///
    /// In most cases this will effectively set the variable to a default
    /// value.
    ///
    /// To set setting to the actual empty value use `set("name",
    /// Value::Nothing)`.
    pub fn unset(&mut self, key: &str) {
        self.config.remove(key);
    }
}

impl AliasesModifier<'_> {
    /// Set a module alias
    pub fn set(&mut self, key: &str, value: &str) {
        self.data.insert(key.into(), value.into());
    }
    /// Unsed a module alias
    pub fn unset(&mut self, key: &str) {
        self.data.remove(key);
    }
}


impl<S: AsRef<str>, I: IntoIterator<Item=S>> GlobalsDelta for Unset<I> {
    fn apply(self, man: &mut GlobalsModifier) {
        for key in self.0.into_iter() {
            man.unset(key.as_ref());
        }
    }
}

impl<S: AsRef<str>, I: IntoIterator<Item=S>> ConfigDelta for Unset<I> {
    fn apply(self, man: &mut ConfigModifier) {
        for key in self.0.into_iter() {
            man.unset(key.as_ref());
        }
    }
}

impl<S: AsRef<str>, I: IntoIterator<Item=S>> AliasesDelta for Unset<I> {
    fn apply(self, man: &mut AliasesModifier) {
        for key in self.0.into_iter() {
            man.unset(key.as_ref());
        }
    }
}

impl<F: FnOnce(&'_ mut GlobalsModifier<'_>)> GlobalsDelta for Fn<F> {
    fn apply(self, man: &mut GlobalsModifier) {
        self.0(man)
    }
}

impl<F: FnOnce(&'_ mut ConfigModifier<'_>)> ConfigDelta for Fn<F> {
    fn apply(self, man: &mut ConfigModifier) {
        self.0(man)
    }
}

impl<F: FnOnce(&'_ mut AliasesModifier<'_>)> AliasesDelta for Fn<F> {
    fn apply(self, man: &mut AliasesModifier) {
        self.0(man)
    }
}

impl<K: AsRef<str>, V: AsRef<str>> AliasesDelta for BTreeMap<K, V> {
    fn apply(self, man: &mut AliasesModifier) {
        for (key, value) in self {
            man.set(key.as_ref(), value.as_ref());
        }
    }
}

impl<K: AsRef<str>, V: AsRef<str>> AliasesDelta for HashMap<K, V> {
    fn apply(self, man: &mut AliasesModifier) {
        for (key, value) in self {
            man.set(key.as_ref(), value.as_ref());
        }
    }
}

impl<K: AsRef<str>, V: AsRef<str>> AliasesDelta for &'_ BTreeMap<K, V> {
    fn apply(self, man: &mut AliasesModifier) {
        for (key, value) in self {
            man.set(key.as_ref(), value.as_ref());
        }
    }
}

impl<K: AsRef<str>, V: AsRef<str>> AliasesDelta for &'_ HashMap<K, V> {
    fn apply(self, man: &mut AliasesModifier) {
        for (key, value) in self {
            man.set(key.as_ref(), value.as_ref());
        }
    }
}

impl<K: AsRef<str>, V: QueryArg> GlobalsDelta for BTreeMap<K, V> {
    fn apply(self, man: &mut GlobalsModifier) {
        for (key, value) in self {
            let value = value.to_value().expect("global can be encoded");
            man.set(key.as_ref(), value);
        }
    }
}

impl<K: AsRef<str>, V: QueryArg> ConfigDelta for BTreeMap<K, V> {
    fn apply(self, man: &mut ConfigModifier) {
        for (key, value) in self {
            let value = value.to_value().expect("global can be encoded");
            man.set(key.as_ref(), value);
        }
    }
}

impl PoolState {
    pub fn with_default_module(&self, module: Option<String>)
        -> Self
    {
        PoolState {
            raw_state: RawState {
                common: Arc::new(CommonState {
                    module,
                    aliases: self.raw_state.common.aliases.clone(),
                    config: self.raw_state.common.config.clone(),
                }),
                globals: self.raw_state.globals.clone(),
            },
            cache: ArcSwapOption::new(None),
        }
    }
    pub fn with_globals(&self, delta: impl GlobalsDelta) -> Self {
        let mut globals = self.raw_state.globals.clone();
        delta.apply(&mut GlobalsModifier {
            module: self.raw_state.common.module
                .as_deref().unwrap_or("default"),
            aliases: &self.raw_state.common.aliases,
            globals: &mut globals,
        });
        PoolState {
            raw_state: RawState {
                common: self.raw_state.common.clone(),
                globals,
            },
            cache: ArcSwapOption::new(None),
        }
    }
    pub fn with_config(&self, delta: impl ConfigDelta) -> Self {
        let mut config = self.raw_state.common.config.clone();
        delta.apply(&mut ConfigModifier {
            config: &mut config,
        });
        PoolState {
            raw_state: RawState {
                common: Arc::new(CommonState {
                    module: self.raw_state.common.module.clone(),
                    aliases: self.raw_state.common.aliases.clone(),
                    config,
                }),
                globals: self.raw_state.globals.clone(),
            },
            cache: ArcSwapOption::new(None),
        }
    }

    pub fn with_aliases(&self, delta: impl AliasesDelta) -> Self {
        let mut aliases = self.raw_state.common.aliases.clone();
        delta.apply(&mut AliasesModifier { data: &mut aliases });
        PoolState {
            raw_state: RawState {
                common: Arc::new(CommonState {
                    module: self.raw_state.common.module.clone(),
                    aliases,
                    config: self.raw_state.common.config.clone(),
                }),
                globals: self.raw_state.globals.clone(),
            },
            cache: ArcSwapOption::new(None),
        }
    }

}

impl SealedState for &PoolState {
    fn encode(&self, desc: &RawTypedesc)
        -> Result<EncodedState, Error>
    {
        if let Some(cache) = &*self.cache.load() {
            if cache.typedesc_id == desc.id {
                return Ok((**cache).clone());
            }
        }
        let typedesc = desc.decode()
            .map_err(ProtocolEncodingError::with_source)?;
        let result = typedesc.serialize_state(&StateBorrow {
            module: &self.raw_state.common.module,
            aliases: &self.raw_state.common.aliases,
            config: &self.raw_state.common.config,
            globals: &self.raw_state.globals,
        })?;
        self.cache.store(Some(Arc::new(result.clone())));
        return Ok(result);
    }
}
impl State for &PoolState {}
impl SealedState for Arc<PoolState> {
    fn encode(&self, desc: &RawTypedesc)
        -> Result<EncodedState, Error>
    {
        (&**self).encode(desc)
    }
}
impl State for Arc<PoolState> {}

impl SealedState for EncodedState {
    fn encode(&self, desc: &RawTypedesc)
        -> Result<EncodedState, Error>
    {
        if self.typedesc_id == Uuid::from_u128(0) ||
            self.typedesc_id == desc.id
        {
            return Ok((*self).clone());
        }
        return Err(ClientError::with_message(
            "state doesn't match state descriptor"
        ));
    }
}
impl State for EncodedState {}
impl SealedState for Arc<EncodedState> {
    fn encode(&self, desc: &RawTypedesc)
        -> Result<EncodedState, Error>
    {
        (&**self).encode(desc)
    }
}
impl State for Arc<EncodedState> {}

impl Default for PoolState {
    fn default() -> PoolState {
        PoolState {
            raw_state: RawState {
                common: Arc::new(CommonState {
                    module: None,
                    aliases: Default::default(),
                    config: Default::default(),
                }),
                globals: Default::default(),
            },
            cache: ArcSwapOption::new(None),
        }
    }
}
