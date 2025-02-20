#[cfg(not(windows))]
mod server;

#[cfg(all(not(windows), feature = "unstable"))]
mod raw;

#[cfg(all(not(windows), feature = "unstable"))]
mod client;

#[cfg(all(not(windows), feature = "unstable"))]
mod transactions;

#[cfg(all(not(windows), feature = "unstable"))]
mod globals;

#[cfg(all(not(windows), feature = "unstable"))]
mod derive;
