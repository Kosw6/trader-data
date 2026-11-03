"""Finite, audit-gated first attempts across batches; never retries a failed batch."""


class SafetyStop(RuntimeError):
    pass


def remaining_targets(targets, completed_ids, held_ids):
    ids = [t["asset_id"] for t in targets]
    if len(ids) != len(set(ids)) or set(completed_ids) & set(held_ids):
        raise SafetyStop("PARTITION_CONFLICT")
    excluded = set(completed_ids) | set(held_ids)
    return [t for t in targets if t["asset_id"] not in excluded]


class Continuation:
    def __init__(self, plan, persist, check):
        self.plan, self.persist, self.check = plan, persist, check
        self.state = {
            "status": "RUNNING",
            "eligible": len(plan["targets"]),
            "baselineVerified": plan["baselineVerified"],
            "baselineHeld": plan["baselineHeld"],
            "completed": [],
            "held": [],
            "pending": len(plan["targets"]),
            "batch": None,
            "current": None,
            "httpRequests": 0,
            "tokenRequests": 0,
            "consecutiveTransportHolds": 0,
            "verifiedBatches": 0,
        }
        self.usage = {}
        self.ids = set()

    def save(self):
        self.state["pending"] = (
            self.state["eligible"]
            - len(self.state["completed"])
            - len(self.state["held"])
        )
        self.state["globalVerified"] = self.state["baselineVerified"] + len(
            self.state["completed"]
        )
        self.state["globalHeld"] = self.state["baselineHeld"] + len(self.state["held"])
        if (
            self.state["globalVerified"]
            + self.state["globalHeld"]
            + self.state["pending"]
            != self.plan["universe"]
        ):
            raise SafetyStop("PARTITION_CONFLICT")
        self.persist(self.state)

    def observe_usage(self, asset_id, requests, tokens):
        old_requests, old_tokens = self.usage.get(asset_id, (0, 0))
        if requests < old_requests or tokens < old_tokens:
            raise SafetyStop("BUDGET_ROLLBACK")
        self.state["httpRequests"] += requests - old_requests
        self.state["tokenRequests"] += tokens - old_tokens
        self.usage[asset_id] = (requests, tokens)
        self.save()
        if (
            self.state["httpRequests"] > self.plan["maxHTTP"]
            or self.state["tokenRequests"] > self.plan["maxTokenRequests"]
        ):
            raise SafetyStop("HTTP_OR_AUTH_BUDGET")

    def record(self, target, result):
        asset = target["asset_id"]
        if asset in self.ids or result.get("assetId") != asset:
            raise SafetyStop("RESULT_IDENTITY_CONFLICT")
        outcome = result.get("outcome")
        if outcome not in {"VERIFIED", "HELD"}:
            raise SafetyStop("UNKNOWN_RESULT")
        if outcome == "VERIFIED" and not result.get("independentReadVerified"):
            raise SafetyStop("UNVERIFIED_COMMIT")
        self.ids.add(asset)
        self.state["completed" if outcome == "VERIFIED" else "held"].append(result)
        transport = result.get("reason") == "TRANSPORT_ATTEMPTS_EXHAUSTED"
        self.state["consecutiveTransportHolds"] = (
            self.state["consecutiveTransportHolds"] + 1 if transport else 0
        )
        self.save()
        if (
            self.state["consecutiveTransportHolds"]
            >= self.plan["maxConsecutiveTransportHolds"]
        ):
            raise SafetyStop("WIDESPREAD_TRANSPORT_OUTAGE")

    def run(self, begin_batch, process_item, audit_batch):
        size = self.plan["batchAssets"]
        if type(size) is not int or size < 1:
            raise SafetyStop("INVALID_BATCH_SIZE")
        targets = self.plan["targets"]
        first = self.plan.get("firstBatchAssets", size)
        if type(first) is not int or not 1 <= first <= size:
            raise SafetyStop("INVALID_FIRST_BATCH_SIZE")
        batches = (
            [targets[:first]]
            + [targets[i : i + size] for i in range(first, len(targets), size)]
            if targets
            else []
        )
        self.save()
        try:
            for number, batch in enumerate(batches):
                self.check(self.state)
                self.state.update(batch=number, current=None, phase="BATCH_PREFLIGHT")
                self.save()
                context = begin_batch(number, batch)
                results = []
                for target in batch:
                    self.check(self.state)
                    self.state["current"] = {
                        "assetId": target["asset_id"],
                        "symbol": target["provider_symbol"],
                    }
                    self.save()
                    result = process_item(context, target, self)
                    self.record(target, result)
                    results.append(result)
                self.state.update(current=None, phase="BATCH_INDEPENDENT_AUDIT")
                self.save()
                if audit_batch(context, results) is not True:
                    raise SafetyStop("BATCH_AUDIT_UNVERIFIED")
                self.state["verifiedBatches"] += 1
                self.state["phase"] = "BATCH_VERIFIED"
                self.save()
            self.state.update(
                status="COMPLETE_WITH_REVIEW",
                phase="ALL_PLANNED_ATTEMPTS_VERIFIED",
                current=None,
            )
        except Exception as error:  # noqa: BLE001 -- halt every unknown failure; never retry it
            reason = (
                str(error) if isinstance(error, SafetyStop) else "UNCLASSIFIED_FAILURE"
            )
            self.state.update(
                status="PAUSED_USER"
                if reason == "USER_PAUSED"
                else "HELD_REVIEW_REQUIRED",
                failureType=type(error).__name__,
                reason=reason,
            )
        self.save()
        return self.state
