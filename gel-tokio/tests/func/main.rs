#[cfg(not(windows))]
mod server;

#[cfg(all(not(windows), feature = "unstable"))]
mod raw;

#[cfg(not(windows))]
mod client;

#[cfg(not(windows))]
mod transactions;

#[cfg(not(windows))]
mod globals;

#[cfg(not(windows))]
mod derive;
