"""Audit-gated batch boundaries; never automatically restarts a failed batch."""


def run_batches(remaining, first_size, batch_size, begin, execute, close, audit, check):
    if any(type(x) is not int or x < 1 for x in (first_size, batch_size)):
        raise ValueError("INVALID_BATCH_SIZE")
    number = 0
    while True:
        check()
        targets = remaining()
        if not targets:
            return number
        scope = targets[: first_size if number == 0 else batch_size]
        context = begin(number, scope)
        try:
            execute(context)
        finally:
            # Owned transport and SQL sessions must close before audit or another batch.
            close(context)
        if audit(context) is not True:
            raise RuntimeError("HANDOFF_AUDIT_UNVERIFIED")
        if len(remaining()) >= len(targets):
            raise RuntimeError("NORMAL_BATCH_MADE_NO_PROGRESS")
        number += 1
