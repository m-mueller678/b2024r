use crate::OptimisticError;
use radium::Radium;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{fence, AtomicU64};

pub struct SeqLock(AtomicU64);

const COUNT_BITS: u32 = 10;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1;
const EXCLUSIVE_MASK: u64 = 1 << COUNT_BITS;
const VERSION_SHIFT: u32 = COUNT_BITS + 1;

impl SeqLock {
    pub fn new()->Self{
        SeqLock(AtomicU64::new(0))
    }
    pub fn lock_shared(&self) -> u64 {
        let mut x = self.0.load(Relaxed);
        loop {
            if x & (COUNT_MASK | EXCLUSIVE_MASK) < COUNT_MASK {
                match self.0.compare_exchange_weak(x, x + 1, Acquire, Relaxed) {
                    Ok(_) => return x >> VERSION_SHIFT,
                    Err(v) => x = v,
                }
            } else {
                self.wait();
            }
        }
    }

    pub fn unlock_shared(&self) -> u64 {
        self.0.fetch_sub(1, Release) >> VERSION_SHIFT
    }

    fn wait(&self) {
        //TODO
        std::thread::yield_now();
    }

    /// returns version before locking
    pub fn lock_exclusive(&self) -> u64 {
        let mut x = self.0.load(Relaxed);
        loop {
            if x & EXCLUSIVE_MASK == 0 {
                x = self.0.fetch_or(EXCLUSIVE_MASK, Acquire);
                if x & (EXCLUSIVE_MASK | COUNT_MASK) == 0 {
                    return x >> VERSION_SHIFT;
                }
                if x & EXCLUSIVE_MASK != 0 {
                    self.wait();
                    continue;
                }
                loop {
                    self.wait();
                    x = self.0.load(Acquire);
                    if x & COUNT_MASK == 0 {
                        return x >> VERSION_SHIFT;
                    }
                }
            }
        }
    }

    /// returns version after unlocking
    pub fn unlock_exclusive(&self) -> u64 {
        (self.0.fetch_add(EXCLUSIVE_MASK, Release) + EXCLUSIVE_MASK) >> VERSION_SHIFT
    }

    pub fn lock_optimistic(&self) -> u64 {
        loop {
            let x = self.0.load(Acquire);
            if x & EXCLUSIVE_MASK == 0 {
                return x >> VERSION_SHIFT;
            } else {
                self.wait();
            }
        }
    }

    pub fn try_unlock_optimistic(&self, v: u64) -> Result<(), OptimisticError> {
        fence(Acquire);
        let x = self.0.load(Relaxed);
        if (x & !COUNT_MASK) == v << VERSION_SHIFT {
            Ok(())
        } else {
            Err(OptimisticError::new())
        }
    }
}
