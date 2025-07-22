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
    if len_match {
        match range {
            KeyRange::Coverable => Resolution::Ok,
            KeyRange::OutsideNnp => match fill {
                FillDegree::Low | FillDegree::Mid => Resolution::Convert,
                FillDegree::NoConvert => Resolution::SplitHigh,
            },
            KeyRange::Oob => match fill {
                FillDegree::Low => Resolution::Convert,
                FillDegree::Mid | FillDegree::NoConvert => Resolution::SplitHigh,
            },
        }
    } else {
        match fill {
            FillDegree::Low | FillDegree::Mid => Resolution::Convert,
            FillDegree::NoConvert => Resolution::SplitHalf,
        }
    }
}
pub fn resolve(
    can_convert: impl FnOnce() -> bool,
    is_low: impl FnOnce() -> bool,
    len_is_ok: bool,
    nnp_is_ok: impl FnOnce() -> bool,
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
        if can_convert() {
            Resolution::Convert
        } else {
            Resolution::SplitHalf
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
                let resolve_lazy = resolve(
                    || fill != FillDegree::NoConvert,
                    || fill == FillDegree::Low,
                    len_match,
                    || {
                        assert!(len_match);
                        allow_bounds_check.set(true);
                        range != KeyRange::OutsideNnp
                    },
                    || {
                        assert!(allow_bounds_check.get());
                        range == KeyRange::Coverable
                    },
                );

                let resolve_expected = resolve_eager(len_match, fill, range);

                if resolve_lazy != resolve_expected {
                    panic!(
                        "resolution mismatch for len_match={:?}, fill={:?}, range={:?}\n  lazy: {:?}\n  eager: {:?}",
                        len_match, fill, range, resolve_lazy, resolve_expected
                    );
                }
            }
        }
    }
}
