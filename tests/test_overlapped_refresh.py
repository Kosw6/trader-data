import threading

import pytest
from pipeline.overlapped_refresh import run_overlapped


def test_collection_overlaps_single_ordered_publication_with_bounded_prefetch():
    publishing = threading.Event()
    next_collected = threading.Event()
    lock = threading.Lock()
    collected, records, active = [], [], 0

    def collect(target):
        collected.append(target)
        if target == 2:
            assert publishing.wait(2)
            next_collected.set()
        if target == 3:
            assert records, "third asset must wait for first publication"
        return target

    def publish(target, value):
        nonlocal active
        with lock:
            active += 1
            assert active == 1
        if target == 1:
            publishing.set()
            assert next_collected.wait(2)
        with lock:
            active -= 1
        return value

    run_overlapped(
        [1, 2, 3], collect, publish, lambda t, r: records.append(r), lambda: None
    )
    assert collected == records == [1, 2, 3]


def test_publisher_failure_blocks_later_publications_without_retry():
    calls = []

    def publish(t, v):
        calls.append(t)
        raise RuntimeError("unknown commit")

    with pytest.raises(RuntimeError, match="unknown commit"):
        run_overlapped([1, 2, 3], lambda t: t, publish, lambda t, r: None, lambda: None)
    assert calls == [1]


def test_collect_failure_waits_for_inflight_commit_and_preserves_result():
    in_publish = threading.Event()
    failed = threading.Event()
    records = []

    def collect(t):
        if t == 2:
            assert in_publish.wait(2)
            failed.set()
            raise ValueError("source invalid")
        return t

    def publish(t, v):
        in_publish.set()
        assert failed.wait(2)
        return "VERIFIED"

    with pytest.raises(ValueError, match="source invalid"):
        run_overlapped(
            [1, 2, 3],
            collect,
            publish,
            lambda t, r: records.append((t, r)),
            lambda: None,
        )
    assert records == [(1, "VERIFIED")]
