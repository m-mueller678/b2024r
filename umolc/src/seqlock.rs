use crate::{OlcVersion, OptimisticError};
use bytemuck::Zeroable;
use radium::Radium;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{fence, AtomicU64};

#[derive(Zeroable)]
pub struct SeqLock(AtomicU64);

const COUNT_BITS: u32 = 10;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;
const EXCLUSIVE_MASK: u64 = 1 << COUNT_BITS;
const VERSION_SHIFT: u32 = COUNT_BITS + 1;

pub trait VersionFilter: Copy {
    type E;
    type R;
    fn check(self, v: u64) -> Result<(), Self::E>;
    fn map_r(self, v: u64) -> Self::R;
}

impl VersionFilter for () {
    type E = !;
    type R = OlcVersion;
    fn check(self, v: u64) -> Result<(), Self::E> {
        Ok(())
    }

    fn map_r(self, v: u64) -> Self::R {
        OlcVersion { x: v }
    }
}

impl VersionFilter for OlcVersion {
    type E = OptimisticError;
    type R = ();

    fn check(self, v: u64) -> Result<Self::R, Self::E> {
        if v == self.x {
            Ok(())
        } else {
            Err(OptimisticError::new())
        }
    }

    fn map_r(self, v: u64) -> Self::R {
        debug_assert!(v == self.x);
    }
}

impl SeqLock {
    pub fn new() -> Self {
        SeqLock(AtomicU64::new(0))
    }
    pub fn lock_shared<F: VersionFilter>(&self, f: F) -> Result<F::R, F::E> {
        let mut x = self.0.load(Relaxed);
        loop {
            f.check(x >> VERSION_SHIFT)?;
            if x & (COUNT_MASK | EXCLUSIVE_MASK) < COUNT_MASK {
                match self.0.compare_exchange_weak(x, x + 1, Acquire, Relaxed) {
                    Ok(_) => return Ok(f.map_r(x >> VERSION_SHIFT)),
                    Err(v) => x = v,
                }
            } else {
                self.wait();
            }
        }
    }

    pub fn unlock_shared(&self) -> OlcVersion {
        OlcVersion { x: self.0.fetch_sub(1, Release) >> VERSION_SHIFT }
    }

    fn wait(&self) {
        //TODO
        std::thread::yield_now();
    }

    /// returns version before locking
    pub fn lock_exclusive<F: VersionFilter>(&self, f: F) -> Result<F::R, F::E> {
        let mut x = self.0.load(Relaxed);
        loop {
            f.check(x >> VERSION_SHIFT)?;
            if x & EXCLUSIVE_MASK == 0 {
                x = self.0.fetch_or(EXCLUSIVE_MASK, Acquire);
                if x & (EXCLUSIVE_MASK | COUNT_MASK) == 0 {
                    return Ok(f.map_r(x >> VERSION_SHIFT));
                }
                if x & EXCLUSIVE_MASK != 0 {
                    self.wait();
                    continue;
                }
                loop {
                    self.wait();
                    x = self.0.load(Acquire);
                    if x & COUNT_MASK == 0 {
                        return Ok(f.map_r(x >> VERSION_SHIFT));
                    }
                }
            }
        }
    }

    pub fn force_lock_exclusive<F: VersionFilter>(&self) -> OlcVersion {
        let mut x = self.0.fetch_or(EXCLUSIVE_MASK, Acquire);
        debug_assert!(x & (EXCLUSIVE_MASK | COUNT_MASK) == 0);
        OlcVersion { x: x >> VERSION_SHIFT }
    }

    /// returns version after unlocking
    pub fn unlock_exclusive(&self) -> OlcVersion {
        OlcVersion { x: (self.0.fetch_add(EXCLUSIVE_MASK, Release) + EXCLUSIVE_MASK) >> VERSION_SHIFT }
    }

    pub fn lock_optimistic<F: VersionFilter>(&self, f: F) -> Result<F::R, F::E> {
        loop {
            let x = self.0.load(Acquire);
            f.check(x >> VERSION_SHIFT)?;
            if x & EXCLUSIVE_MASK == 0 {
                return Ok(f.map_r(x >> VERSION_SHIFT));
            } else {
                self.wait();
            }
        }
    }

    pub fn try_unlock_optimistic(&self, v: OlcVersion) -> Result<(), OptimisticError> {
        fence(Acquire);
        let x = self.0.load(Relaxed);
        if (x & !COUNT_MASK) == v.x << VERSION_SHIFT {
            Ok(())
        } else {
            Err(OptimisticError::new())
        }
    }
}
