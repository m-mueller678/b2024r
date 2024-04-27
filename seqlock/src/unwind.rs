use std::panic::{catch_unwind, panic_any, resume_unwind, UnwindSafe};

pub struct OptimisticError;

pub fn start() -> ! {
    resume_unwind(Box::new(OptimisticError));
}

pub fn repeat<R, F: FnOnce() -> R + UnwindSafe>(mut f: impl FnMut() -> F) -> R {
    loop {
        if let Ok(x) = catch(f()) {
            return x;
        }
    }
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
