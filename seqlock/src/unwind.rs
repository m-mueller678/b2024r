use std::panic::{catch_unwind, panic_any, resume_unwind, UnwindSafe};

pub struct OptimisticError;

pub fn start() -> ! {
    panic_any(OptimisticError);
}

pub fn catch<R>(f: impl FnOnce() -> R + UnwindSafe) -> Result<R, OptimisticError> {
    match catch_unwind(f) {
        Ok(r) => Ok(r),
        Err(e) => match e.downcast::<OptimisticError>() {
            Ok(x) => Err(*x),
            Err(e) => resume_unwind(e),
        },
    }
}
