"""One collector, one ordered publisher, maximum two assets in flight."""

import queue
import threading


class PipelineStopped(RuntimeError):
    pass


def run_overlapped(targets, collect, publish, record, check, stop=None):
    """No retries: preserve an in-flight publication, then propagate any failure."""
    stop = stop or threading.Event()
    results = queue.Queue(maxsize=1)
    slots = threading.Semaphore(2)
    failures = []

    def offer(value):
        while not stop.is_set():
            try:
                results.put(value, timeout=0.05)
                return True
            except queue.Full:
                continue
        return False

    def producer():
        try:
            for target in targets:
                while not stop.is_set() and not slots.acquire(timeout=0.05):
                    pass
                if stop.is_set():
                    break
                check()
                value = collect(target)
                if not offer((target, value)):
                    break
        except Exception as error:  # noqa: BLE001 -- report unknown failures, never retry
            failures.append(error)
            stop.set()
        finally:
            if not stop.is_set():
                offer(None)

    thread = threading.Thread(target=producer, name="bounded-price-collector")
    thread.start()
    failure = None
    try:
        while not stop.is_set():
            try:
                item = results.get(timeout=0.05)
            except queue.Empty:
                continue
            if item is None:
                break
            target, collected = item
            check()
            # Once publish starts, do not interrupt its atomic commit/independent read.
            result = publish(target, collected)
            record(target, result)
            slots.release()
    except Exception as error:  # noqa: BLE001 -- stop admission, retain any commit evidence
        failure = error
        stop.set()
    finally:
        # Producer's HTTP has a bounded timeout and observes stop before its next request.
        thread.join()
    if failure is not None:
        raise failure
    if failures:
        raise failures[0]
    if stop.is_set():
        raise PipelineStopped("PIPELINE_STOPPED")
