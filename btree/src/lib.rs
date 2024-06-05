#![allow(clippy::missing_safety_doc)]
#![feature(exposed_provenance)]
#![feature(strict_provenance)]
#![feature(is_sorted)]
#![feature(inline_const_pat)]

extern crate core;

mod basic_node;
mod key_source;
mod node;
mod page;

mod tree;

pub use tree::Tree;

const MAX_KEY_SIZE: usize = 512;

use seqlock::seqlock_wrapper;
seqlock_wrapper!(pub W);
