#![allow(clippy::missing_safety_doc)]

extern crate core;

mod basic_node;
mod key_source;
mod node;
#[cfg(test)]
mod test_util;

use seqlock::seqlock_wrapper;
seqlock_wrapper!(pub W);

pub type PageId = [u16; 3];
