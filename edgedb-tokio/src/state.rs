use std::collections::{BTreeMap, HashMap};
use std::default::Default;
use std::sync::Arc;

use tokio::sync::OnceCell;

use edgedb_protocol::value::Value;
use edgedb_protocol::query_arg::QueryArg;

use bytes::Bytes;

pub struct Unset<I>(pub I);
pub struct Fn<F: FnOnce(&'_ mut VariablesModifier<'_>)>(pub F);

#[derive(Debug)]
pub(crate) struct State {
    raw_state: RawState,
    cache: OnceCell<Bytes>,
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

#[derive(Debug)]
pub struct VariablesModifier<'a> {
    data: &'a mut BTreeMap<String, Value>,
    module: &'a str,
    aliases: &'a BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct AliasesModifier<'a> {
    data: &'a mut BTreeMap<String, String>,
}

pub trait VariablesDelta {
    fn apply(self, man: &mut VariablesModifier<'_>);
}

pub trait AliasesDelta {
    fn apply(self, man: &mut AliasesModifier);
}

impl VariablesModifier<'_> {
    pub fn set<T: QueryArg>(&mut self, key: &str, value: T) {
        let value = value.to_value().expect("global can be encoded");
        if let Some(ns_off) = key.rfind("::") {
            if let Some(alias) = self.aliases.get(&key[..ns_off]) {
                self.data.insert(
                    format!("{alias}::{suffix}", suffix=&key[ns_off+2..]),
                    value,
                );
            } else {
                self.data.insert(key.into(), value);
            }
        } else {
            self.data.insert(format!("{}::{}", self.module, key), value);
        }
    }
    pub fn unset(&mut self, key: &str) {
        if let Some(ns_off) = key.rfind("::") {
            if let Some(alias) = self.aliases.get(&key[..ns_off]) {
                self.data.remove(
                    &format!("{alias}::{suffix}", suffix=&key[ns_off+2..]));
            } else {
                self.data.remove(key);
            }
        } else {
            self.data.remove(&format!("{}::{}", self.module, key));
        }
    }
}

impl AliasesModifier<'_> {
    pub fn set(&mut self, key: &str, value: &str) {
        self.data.insert(key.into(), value.into());
    }
    pub fn unset(&mut self, key: &str) {
        self.data.remove(key);
    }
}


impl<S: AsRef<str>, I: IntoIterator<Item=S>> VariablesDelta for Unset<I> {
    fn apply(self, man: &mut VariablesModifier) {
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

impl<F: FnOnce(&'_ mut VariablesModifier<'_>)> VariablesDelta for Fn<F> {
    fn apply(self, man: &mut VariablesModifier) {
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

impl<K: AsRef<str>, V: QueryArg> VariablesDelta for BTreeMap<K, V> {
    fn apply(self, man: &mut VariablesModifier) {
        for (key, value) in self {
            let value = value.to_value().expect("global can be encoded");
            man.set(key.as_ref(), value);
        }
    }
}

impl State {
    pub fn with_default_module(&self, module: Option<String>) -> Self {
        State {
            raw_state: RawState {
                common: Arc::new(CommonState {
                    module,
                    aliases: self.raw_state.common.aliases.clone(),
                    config: self.raw_state.common.config.clone(),
                }),
                globals: self.raw_state.globals.clone(),
            },
            cache: OnceCell::new(),
        }
    }
    pub fn with_globals(&self, delta: impl VariablesDelta) -> Self {
        let mut globals = self.raw_state.globals.clone();
        delta.apply(&mut VariablesModifier {
            module: self.raw_state.common.module
                .as_deref().unwrap_or("default"),
            aliases: &self.raw_state.common.aliases,
            data: &mut globals,
        });
        State {
            raw_state: RawState {
                common: self.raw_state.common.clone(),
                globals,
            },
            cache: OnceCell::new(),
        }
    }

    pub fn with_aliases(&self, delta: impl AliasesDelta) -> Self {
        let mut aliases = self.raw_state.common.aliases.clone();
        delta.apply(&mut AliasesModifier { data: &mut aliases });
        State {
            raw_state: RawState {
                common: Arc::new(CommonState {
                    module: self.raw_state.common.module.clone(),
                    aliases,
                    config: self.raw_state.common.config.clone(),
                }),
                globals: self.raw_state.globals.clone(),
            },
            cache: OnceCell::new(),
        }
    }
}

impl Default for State {
    fn default() -> State {
        State {
            raw_state: RawState {
                common: Arc::new(CommonState {
                    module: None,
                    aliases: Default::default(),
                    config: Default::default(),
                }),
                globals: Default::default(),
            },
            cache: OnceCell::new(),
        }
    }
}
