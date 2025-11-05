// used to handle when to use the loom variants of sync features

#[cfg(loom)]
pub(crate) use loom::cell::Cell;
#[cfg(loom)]
pub(crate) use loom::sync::Mutex;
#[cfg(loom)]
pub(crate) use loom::thread::LocalKey;
// #TODO no counterpart for Once. find a solution
#[cfg(loom)]
pub(crate) use std::sync::Once;


#[cfg(not(loom))]
pub(crate) use std::cell::Cell;
#[cfg(not(loom))]
pub(crate) use std::sync::{Mutex, Once};
#[cfg(not(loom))]
pub(crate) use std::thread::LocalKey;

