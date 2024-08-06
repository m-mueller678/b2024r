#[cfg(all(feature = "impl_atomic_byte", not(miri)))]
mod atomic_byte;

#[cfg(all(feature = "impl_asm_read", not(miri)))]
mod asm_read;

#[cfg(all(feature = "impl_ub", not(miri)))]
mod unsafe_mix;

#[cfg(all(feature = "impl_extreme_ub", not(miri)))]
mod extreme_ub;

#[cfg(miri)]
mod atomic_byte;

#[cfg(not(any(
    miri,
    feature = "impl_extreme_ub",
    feature = "impl_ub",
    feature = "impl_asm_read",
    feature = "impl_atomic_byte"
)))]
mod unsafe_mix;
