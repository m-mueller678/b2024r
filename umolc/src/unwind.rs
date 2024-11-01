use std::panic::{catch_unwind, resume_unwind, UnwindSafe};

pub fn start() -> ! {
    resume_unwind(Box::new(OptimisticError::new()));
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

pub struct OptimisticError {
    _private: (),
}

impl OptimisticError {
    fn new() -> Self {
        OptimisticError { _private: () }
    }
}

pub trait OlcErrorHandler {
    fn optimistic_fail() -> !;
    // TODO consider adding a marker type that is returned by functions that may unwind and marked must_use
    fn catch<R>(f: impl FnOnce() -> R) -> Result<R, OptimisticError>;
}
