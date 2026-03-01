use vstd::prelude::*;

verus! {

/// Rounds `value` down to the nearest multiple of `alignment`.
/// `alignment` must be a power of two.
pub fn align_down_u64(value: u64, alignment: u64) -> (result: u64)
    requires
        alignment > 0,
        alignment & sub(alignment, 1) == 0, // power of two
    ensures
        result <= value,
        result & sub(alignment, 1) == 0,
        value - result < alignment,
        // idempotent on aligned values
        value & sub(alignment, 1) == 0 ==> result == value,
{
    let mask = alignment - 1;
    let result = value & !mask;

    proof {
        assert(result <= value) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == value & !sub(alignment, 1);
        assert(result & sub(alignment, 1) == 0u64) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == value & !sub(alignment, 1);
        assert(value - result < alignment) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == value & !sub(alignment, 1),
                result <= value;
        assert(value & sub(alignment, 1) == 0 ==> result == value) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == value & !sub(alignment, 1);
    }

    result
}

/// Rounds `value` up to the nearest multiple of `alignment`.
/// Returns `None` on overflow.
/// `alignment` must be a power of two.
pub fn align_up_u64(value: u64, alignment: u64) -> (result: Option<u64>)
    requires
        alignment > 0,
        alignment & sub(alignment, 1) == 0,
    ensures
        result.is_some() ==> result.unwrap() >= value,
        result.is_some() ==> result.unwrap() & sub(alignment, 1) == 0,
        result.is_some() ==> result.unwrap() - value < alignment,
        result.is_some() ==> (value & sub(alignment, 1) == 0 ==> result.unwrap() == value),
{
    let mask = alignment - 1;
    let sum = match value.checked_add(mask) {
        Some(s) => s,
        None => return None,
    };
    let result = sum & !mask;

    proof {
        assert(result >= value) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                sum == add(value, sub(alignment, 1)),
                sum >= value,
                result == sum & !sub(alignment, 1);
        assert(result & sub(alignment, 1) == 0u64) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                result == sum & !sub(alignment, 1);
        assert(result - value < alignment) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                sum == add(value, sub(alignment, 1)),
                sum >= value,
                result == sum & !sub(alignment, 1),
                result >= value;
        assert(value & sub(alignment, 1) == 0 ==> result == value) by (bit_vector)
            requires
                alignment > 0,
                alignment & sub(alignment, 1) == 0,
                sum == add(value, sub(alignment, 1)),
                sum >= value,
                result == sum & !sub(alignment, 1);
    }

    Some(result)
}

} // verus!
