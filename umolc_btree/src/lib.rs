#![feature(arbitrary_self_types_pointers)]
#![feature(maybe_uninit_slice)]
extern crate core;
extern crate core;

mod basic_node;
mod fully_dense_leaf;
mod hash_leaf;
mod heap_node;
mod key_source;
mod node;
mod tree;
mod util;

pub use node::Page;
pub use tree::Tree;
const MAX_KEY_SIZE: usize = 512;
const MAX_VAL_SIZE: usize = 512;
