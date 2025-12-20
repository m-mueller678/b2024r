// used to handle when to use the loom variants of sync features

#[cfg(loom)]
pub(crate) use loom::sync::Mutex;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::Ordering::{Acquire, Relaxed, Release};
#[cfg(loom)]
pub(crate) use loom::sync::atomic::fence;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicU64;   
// not implementable!!
#[cfg(loom)]
pub(crate) use std::cell::UnsafeCell;
// ---------------------------------------------------------------------------

#[cfg(not(loom))]
pub(crate) use std::sync::Mutex;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{fence, AtomicU64};
#[cfg(not(loom))]
pub(crate) use std::cell::UnsafeCell;