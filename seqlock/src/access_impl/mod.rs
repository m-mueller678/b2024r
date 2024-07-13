#[cfg(any(feature = "impl_atomic_byte", miri))]
mod atomic_byte;

#[cfg(any(feature = "impl_atomic_byte", miri))]
pub use atomic_byte::optimistic_release;

#[cfg(all(feature = "impl_asm_read", not(miri)))]
mod asm_read;

#[cfg(all(feature = "impl_asm_read", not(miri)))]
pub use asm_read::optimistic_release;

#[cfg(all(feature = "impl_ub", not(miri)))]
mod unsafe_mix;
#[cfg(all(feature = "impl_ub", not(miri)))]
pub use unsafe_mix::optimistic_release;

#[cfg(all(any(feature = "impl_ub", feature = "impl_asm_read"), not(miri)))]
mod ref_impl;
