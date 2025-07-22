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
    is_in_bounds: impl FnOnce() -> bool,
) -> Resolution {
    if len_is_ok {
        if nnp_is_ok() {
            if is_in_bounds() {
                Resolution::Ok
            } else {
                let is_low_result = is_low();
                println!("🧪 resolve: len_is_ok=true, nnp_is_ok=true, is_in_bounds=false");
                println!("    └─ is_low: {}", is_low_result);
                if is_low_result {
                    Resolution::Convert
                } else {
                    Resolution::SplitHigh
                }
            }
        } else {
            let can_convert_result = can_convert();
            println!("🧪 resolve: len_is_ok=true, nnp_is_ok=false");
            println!("    └─ can_convert: {}", can_convert_result);
            if can_convert_result {
                Resolution::Convert
            } else {
                Resolution::SplitHigh
            }
        }
    } else {
        let can_convert_result = can_convert();
        println!("🧪 resolve: len_is_ok=false, bad_len_is_coverable=true");
        println!("    └─ can_convert: {}", can_convert_result);
        if can_convert_result {
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
