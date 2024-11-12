#![feature(arbitrary_self_types_pointers)]
#![feature(maybe_uninit_slice)]
#![feature(maybe_uninit_uninit_array)]

mod basic_node;
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
