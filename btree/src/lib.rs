#![allow(clippy::missing_safety_doc)]
#![feature(exposed_provenance)]

extern crate core;

mod basic_node;
mod key_source;
mod node;
mod page_id;
#[cfg(test)]
mod test_util;
mod tree;

use seqlock::seqlock_wrapper;
seqlock_wrapper!(pub W);
