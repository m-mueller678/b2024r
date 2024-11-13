use std::cell::Cell;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum FillDegree {
    Low,
    Mid,
    NoConvert, // key cannot be inserted after conversion
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum KeyRange {
    Coverable,
    Oob,
    OutsideNnp,
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum Resolution {
    Convert,
    SplitHalf,
    SplitHigh,
    Ok,
}

fn resolve_eager(len_match: bool, fill: FillDegree, range: KeyRange) -> Resolution {
    match (len_match, range, fill) {
        (true, KeyRange::Coverable, FillDegree::Low | FillDegree::Mid | FillDegree::NoConvert) => Resolution::Ok,
        (true, KeyRange::Oob, FillDegree::NoConvert | FillDegree::Mid) => Resolution::SplitHigh,
        (true, KeyRange::Oob, FillDegree::Low) => Resolution::Convert,
        (true, KeyRange::OutsideNnp, FillDegree::Low | FillDegree::Mid) => Resolution::Convert,
        (true, KeyRange::OutsideNnp, FillDegree::NoConvert) => Resolution::SplitHigh,
        (false, KeyRange::Oob | KeyRange::OutsideNnp, FillDegree::Low | FillDegree::Mid | FillDegree::NoConvert) => {
            Resolution::SplitHigh
        }
        (false, KeyRange::Coverable, FillDegree::NoConvert) => Resolution::SplitHalf,
        (false, KeyRange::Coverable, FillDegree::Low | FillDegree::Mid) => Resolution::Convert,
    }
}

pub fn resolve(
    can_convert: impl FnOnce() -> bool,
    is_low: impl FnOnce() -> bool,
    len_is_ok: bool,
    nnp_is_ok: impl FnOnce() -> bool,
    bad_len_is_coverable: impl FnOnce() -> bool,
    is_in_bounds: impl FnOnce() -> bool,
) -> Resolution {
    if len_is_ok {
        if nnp_is_ok() {
            if is_in_bounds() {
                Resolution::Ok
            } else {
                if is_low() {
                    Resolution::Convert
                } else {
                    Resolution::SplitHigh
                }
            }
        } else {
            if can_convert() {
                Resolution::Convert
            } else {
                Resolution::SplitHigh
            }
        }
    } else {
        if bad_len_is_coverable() {
            if can_convert() {
                Resolution::Convert
            } else {
                Resolution::SplitHalf
            }
        } else {
            Resolution::SplitHigh
        }
    }
}

#[cfg(test)]
#[test]
fn fdl_resolver() {
    for len_match in [true, false] {
        for fill in [FillDegree::Low, FillDegree::Mid, FillDegree::NoConvert] {
            for range in [KeyRange::Coverable, KeyRange::Oob, KeyRange::OutsideNnp] {
                let mut allow_bounds_check = Cell::new(false);
                let mut bounds_checked_ok = false;
                let resolve_lazy = resolve(
                    || fill != FillDegree::NoConvert,
                    || fill == FillDegree::Low,
                    len_match,
                    || {
                        assert!(len_match);
                        allow_bounds_check.set(allow_bounds_check.get() || range != KeyRange::OutsideNnp);
                        range != KeyRange::OutsideNnp
                    },
                    || {
                        assert!(!len_match);
                        range == KeyRange::Coverable
                    },
                    || {
                        assert!(allow_bounds_check.get());
                        bounds_checked_ok |= range == KeyRange::Coverable;
                        range == KeyRange::Coverable
                    },
                );
                assert_eq!(
                    resolve_lazy,
                    resolve_eager(len_match, fill, range),
                    "resolution mismatch: {len_match:?}, {fill:?}, {range:?}"
                );
                match resolve_lazy {
                    Resolution::Convert => assert_ne!(fill, FillDegree::NoConvert),
                    Resolution::Ok => assert!(bounds_checked_ok),
                    _ => (),
                }
            }
        }
    }
}
