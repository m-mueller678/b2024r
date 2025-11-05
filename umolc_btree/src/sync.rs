// used to handle when to use the loom variants of sync features

// #TODO this type need to be adjusted to be used in its loom counterpart
#[cfg(loom)]
pub(crate) use std::sync::atomic::AtomicU8;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::Ordering;
#[cfg(loom)]
pub(crate) use loom::cell::Cell;

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(not(loom))]
pub(crate) use std::cell::Cell;