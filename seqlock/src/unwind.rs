use std::panic::{catch_unwind, resume_unwind, UnwindSafe};

pub struct OptimisticError;

pub fn start() -> ! {
    resume_unwind(Box::new(OptimisticError));
}

pub fn repeat<R>(mut f: impl FnMut() -> R) -> R {
    loop {
        if let Ok(x) = catch(&mut f) {
            return x;
        }
    }
}

pub fn catch<R>(f: impl FnOnce() -> R) -> Result<R, OptimisticError> {
    struct IgnoreUnwindSafe<X>(X);
    impl<X> UnwindSafe for IgnoreUnwindSafe<X> {}
    let f2 = IgnoreUnwindSafe(f);
    let result = catch_unwind(move || {
        let f2 = f2;
        f2.0()
    });
    match result {
        Ok(r) => Ok(r),
        Err(e) => match e.downcast::<OptimisticError>() {
            Ok(x) => Err(*x),
            Err(e) => resume_unwind(e),
        },
    }
}
