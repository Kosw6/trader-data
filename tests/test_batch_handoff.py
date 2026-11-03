import pytest
from pipeline.batch_handoff import run_batches


def execute_batches(
    *, audit_ok=True, close_ok=True, execute_error=False, partial=False
):
    remaining = list(range(7))
    events = []

    def begin(n, targets):
        events.append(("begin", n, tuple(targets)))
        return n, targets

    def execute(context):
        n, targets = context
        events.append(("execute", n))
        if execute_error:
            raise ValueError("source error")
        for t in targets[:1] if partial else targets:
            remaining.remove(t)
            events.append(("record", t))

    def close(context):
        events.append(("close", context[0]))
        if not close_ok:
            raise RuntimeError("cleanup unknown")

    def audit(context):
        events.append(("audit", context[0]))
        return audit_ok

    return (
        remaining,
        events,
        lambda: run_batches(
            lambda: list(remaining), 2, 3, begin, execute, close, audit, lambda: None
        ),
    )


def test_each_batch_closes_and_verifies_before_the_next_starts():
    remaining, events, run = execute_batches()
    assert run() == 3 and not remaining
    starts = [x for x in events if x[0] == "begin"]
    assert [x[2] for x in starts] == [(0, 1), (2, 3, 4), (5, 6)]
    for n in range(2):
        assert (
            events.index(("close", n))
            < events.index(("audit", n))
            < events.index(starts[n + 1])
        )


@pytest.mark.parametrize(
    "change", [{"audit_ok": False}, {"close_ok": False}, {"execute_error": True}]
)
def test_failure_is_not_a_normal_handoff_or_automatic_restart(change):
    _, events, run = execute_batches(**change)
    with pytest.raises((ValueError, RuntimeError)):
        run()
    assert len([x for x in events if x[0] == "begin"]) == 1
    assert len([x for x in events if x[0] == "close"]) == 1


def test_soft_boundary_defers_unattempted_ids_without_skipping_or_duplicating():
    remaining, events, run = execute_batches(partial=True)
    assert run() == 7 and not remaining
    assert [x[1] for x in events if x[0] == "record"] == list(range(7))


def test_no_progress_cannot_loop_forever():
    with pytest.raises(RuntimeError, match="NO_PROGRESS"):
        run_batches(
            lambda: [1],
            1,
            1,
            lambda n, t: t,
            lambda _: None,
            lambda _: None,
            lambda _: True,
            lambda: None,
        )
