#![feature(arbitrary_self_types_pointers)]

mod basic_node;
mod key_source;
mod node;
mod tree;
mod util;

pub use tree::Tree;
const MAX_KEY_SIZE: usize = 512;
