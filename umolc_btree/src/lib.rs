#![feature(arbitrary_self_types_pointers)]

mod basic_node;
pub mod key_source;
pub mod node;
mod tree;
mod util;

const MAX_KEY_SIZE: usize = 512;
