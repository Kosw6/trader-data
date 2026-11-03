import copy

import pytest
from pipeline.price_refresh_continuation import (
    Continuation,
    SafetyStop,
    remaining_targets,
)


def plan(n=3, batch=2):
    return {
        "targets": [
            {"asset_id": i, "provider_symbol": str(i)} for i in range(1, n + 1)
        ],
        "baselineVerified": 2,
        "baselineHeld": 1,
        "universe": n + 3,
        "batchAssets": batch,
        "maxHTTP": 20,
        "maxTokenRequests": 2,
        "maxConsecutiveTransportHolds": 2,
    }


def test_next_batch_runs_only_after_previous_independent_audit():
    events = []
    states = []
    flow = Continuation(
        plan(), lambda s: states.append(copy.deepcopy(s)), lambda _: None
    )

    def begin(n, targets):
        events.append(("begin", n))
        return n

    def item(n, t, f):
        events.append(("item", t["asset_id"]))
        return {
            "assetId": t["asset_id"],
            "outcome": "VERIFIED",
            "independentReadVerified": True,
        }

    def audit(n, results):
        events.append(("audit", n))
        return True

    state = flow.run(begin, item, audit)
    assert events == [
        ("begin", 0),
        ("item", 1),
        ("item", 2),
        ("audit", 0),
        ("begin", 1),
        ("item", 3),
        ("audit", 1),
    ]
    assert (
        state["pending"] == 0
        and state["verifiedBatches"] == 2
        and state["globalVerified"] == 5
    )
    assert state["status"] == "COMPLETE_WITH_REVIEW"


@pytest.mark.parametrize("failure", [False, RuntimeError("unknown commit")])
def test_failed_audit_never_starts_a_following_batch(failure):
    starts = []
    flow = Continuation(plan(), lambda _: None, lambda _: None)

    def begin(n, targets):
        starts.append(n)
        return n

    def item(n, t, f):
        return {
            "assetId": t["asset_id"],
            "outcome": "HELD",
            "reason": "INCOMPLETE_GENERATION",
        }

    def audit(n, r):
        if isinstance(failure, Exception):
            raise failure
        return failure

    state = flow.run(begin, item, audit)
    assert starts == [0] and state["status"] == "HELD_REVIEW_REQUIRED"
    assert state["verifiedBatches"] == 0 and state["pending"] == 1


def test_transport_failure_limit_carries_across_normal_batches():
    p = plan(3, 1)
    flow = Continuation(p, lambda _: None, lambda _: None)
    attempts = []

    def item(n, t, f):
        attempts.append(t["asset_id"])
        return {
            "assetId": t["asset_id"],
            "outcome": "HELD",
            "reason": "TRANSPORT_ATTEMPTS_EXHAUSTED",
        }

    state = flow.run(lambda n, t: n, item, lambda n, r: True)
    assert attempts == [1, 2] and state["reason"] == "WIDESPREAD_TRANSPORT_OUTAGE"
    assert state["verifiedBatches"] == 1 and state["pending"] == 1


def test_request_and_auth_budgets_do_not_reset_at_item_boundary():
    flow = Continuation(plan(), lambda _: None, lambda _: None)
    flow.observe_usage(1, 10, 1)
    flow.observe_usage(1, 10, 1)
    flow.observe_usage(2, 10, 1)
    assert flow.state["httpRequests"] == 20 and flow.state["tokenRequests"] == 2
    with pytest.raises(SafetyStop):
        flow.observe_usage(3, 1, 0)
    with pytest.raises(SafetyStop):
        flow.observe_usage(1, 9, 1)


@pytest.mark.parametrize("kind", ["unknown", "bad-id", "unverified", "duplicate"])
def test_terminal_results_must_match_identity_and_independent_verification(kind):
    flow = Continuation(plan(), lambda _: None, lambda _: None)
    target = flow.plan["targets"][0]
    result = {"assetId": 1, "outcome": "VERIFIED", "independentReadVerified": True}
    if kind == "unknown":
        result["outcome"] = "RUNNING"
    elif kind == "bad-id":
        result["assetId"] = 2
    elif kind == "unverified":
        result["independentReadVerified"] = False
    else:
        flow.record(target, result)
    with pytest.raises(SafetyStop):
        flow.record(target, result)


def test_only_unattempted_targets_survive_and_original_order_is_kept():
    targets = [{"asset_id": x} for x in [5, 1, 9, 7]]
    assert remaining_targets(targets, [1], [7]) == [{"asset_id": 5}, {"asset_id": 9}]
    with pytest.raises(SafetyStop):
        remaining_targets(targets, [1], [1])
    with pytest.raises(SafetyStop):
        remaining_targets(targets + [{"asset_id": 1}], [], [])


def test_small_first_live_batch_then_normal_batches_cover_exact_queue():
    p = plan(7, 3)
    p["firstBatchAssets"] = 2
    starts = []
    flow = Continuation(p, lambda _: None, lambda _: None)

    def begin(n, targets):
        starts.append([x["asset_id"] for x in targets])
        return n

    state = flow.run(
        begin,
        lambda n, t, f: {
            "assetId": t["asset_id"],
            "outcome": "HELD",
            "reason": "QUALITY_REVIEW",
        },
        lambda n, r: True,
    )
    assert starts == [[1, 2], [3, 4, 5], [6, 7]]
    assert state["pending"] == 0 and state["verifiedBatches"] == 3


def test_user_stop_never_admits_next_item_or_new_batch():
    seen = []

    def check(state):
        if state["completed"]:
            raise SafetyStop("USER_PAUSED")

    flow = Continuation(plan(3, 1), lambda _: None, check)

    def item(n, t, f):
        seen.append(t["asset_id"])
        return {
            "assetId": t["asset_id"],
            "outcome": "VERIFIED",
            "independentReadVerified": True,
        }

    state = flow.run(lambda n, t: n, item, lambda n, r: True)
    assert seen == [1] and state["status"] == "PAUSED_USER" and state["pending"] == 2
