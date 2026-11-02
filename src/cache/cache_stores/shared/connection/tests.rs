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

/// A read's spans are kept until the owner ends the read, or goes; and
/// once it has gone, spans lent are freed at once.
#[test]
fn spans_lent_come_back_once_the_owner_ends_their_read_or_goes() {
    let region = ClientRegion::make().unwrap();
    let all = region.free_pages();
    let span = || region.span(1).unwrap();
    let lent = LentSpans::default();
    lent.keep_until_ended(1, vec![span()]);
    lent.keep_until_ended(2, vec![span(), span()]);
    assert_eq!(region.free_pages(), all - 3);

    lent.ended(1);
    assert_eq!(region.free_pages(), all - 2);
    lent.owner_gone();
    assert_eq!(region.free_pages(), all);
    lent.keep_until_ended(3, vec![span()]);
    assert_eq!(region.free_pages(), all, "the owner gone, freed at once");
}
