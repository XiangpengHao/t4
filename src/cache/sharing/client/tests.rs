use super::*;

/// A call given up leaves the table; its answer, if the owner still sends
/// it, is dropped: no protocol error. An answer to a call never made is
/// one.
#[test]
fn an_answer_to_a_call_given_up_is_dropped() {
    let table = CallTable::default();
    let given_up = table.wait().unwrap();
    let number = given_up.number;
    drop(given_up);
    assert!(table.calls().waiting.is_empty());
    table
        .answer(OwnerFrame::Committed { call: number })
        .unwrap();

    let never_made = OwnerFrame::Committed { call: number + 1 };
    assert!(table.answer(never_made).is_err());
}
