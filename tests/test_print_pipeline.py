# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the double-buffer print pipeline (paintress_daemon/print_pipeline.py).

Drives the pipeline with mock stream/arm callbacks and checks the double-buffer
ordering: it keeps two slots full, streams the next swath only once a slot frees
(on PRINT_COMPLETE), and arms swaths on request. Runs under pytest or as a
plain script (``python tests/test_print_pipeline.py``).
"""

import sys
import time
import threading
from pathlib import Path

# Import the module directly (no package __init__, so no serial dependency).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "paintress_daemon"))
from print_pipeline import PrintPipeline, ArmOutcome  # noqa: E402


def wait_until(pred, timeout=2.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.005)
    return pred()


class MockSender:
    def __init__(self, fail_on=None, fail_times=None, arm_reject_on=None,
                 arm_reject_code='swath_not_ready'):
        self.streamed = []
        self.armed = []
        self.arm_delays = []
        self._fail_on = fail_on
        # None = the failing swath fails every attempt (persistent);
        # N = it fails the first N attempts, then succeeds (transient).
        self._fail_times = fail_times
        self._arm_reject_on = arm_reject_on
        self._arm_reject_code = arm_reject_code
        self._lock = threading.Lock()

    def stream(self, swath_id, lines):
        with self._lock:
            self.streamed.append(swath_id)
            if swath_id == self._fail_on:
                if self._fail_times is None:
                    return False
                if self._fail_times > 0:
                    self._fail_times -= 1
                    return False
        return True

    def arm(self, swath_id, line_delay_us):
        with self._lock:
            self.armed.append(swath_id)
            self.arm_delays.append(line_delay_us)
        if swath_id == self._arm_reject_on:
            return ArmOutcome(False, self._arm_reject_code,
                              f'firmware rejected arm ({self._arm_reject_code})')
        return ArmOutcome(True)


def test_double_buffer_ordering():
    m = MockSender()
    pl = PrintPipeline(m.stream, m.arm, num_slots=2)
    pl.load([(i, [b"x"]) for i in (1, 2, 3, 4)])
    try:
        # Pre-fills exactly two slots and waits.
        assert wait_until(lambda: m.streamed == [1, 2]), m.streamed
        time.sleep(0.05)
        assert m.streamed == [1, 2], "must not stream a 3rd swath while both slots full"

        # Print swath 1; completing it frees a slot -> swath 3 streams.
        assert pl.request_print(1, 353).ok
        assert m.armed == [1]
        pl.on_print_complete(1)
        assert wait_until(lambda: m.streamed == [1, 2, 3]), m.streamed

        # Continue the pipeline to the end.
        assert pl.request_print(2, 353).ok
        pl.on_print_complete(2)
        assert wait_until(lambda: m.streamed == [1, 2, 3, 4]), m.streamed

        assert pl.request_print(3, 353).ok
        pl.on_print_complete(3)
        assert pl.request_print(4, 353).ok
        pl.on_print_complete(4)

        assert m.armed == [1, 2, 3, 4]
        assert m.streamed == [1, 2, 3, 4]
        # The firing interval travels with each arm.
        assert m.arm_delays == [353, 353, 353, 353]
    finally:
        pl.stop()


def test_stream_failure_stops_and_request_fails():
    # Persistent failure: the bounded in-place retries (default 2) are spent,
    # then the pipeline latches the failure exactly as before.
    m = MockSender(fail_on=2)
    errors = []
    retries = []
    pl = PrintPipeline(m.stream, m.arm, num_slots=2,
                       on_error=lambda sid, msg: errors.append((sid, msg)),
                       on_retry=lambda sid, attempt: retries.append((sid, attempt)))
    pl.load([(i, [b"x"]) for i in (1, 2, 3)])
    try:
        assert wait_until(lambda: pl.is_failed()), "pipeline should mark failure"
        # The failure is surfaced via on_error, not just swallowed.
        assert wait_until(lambda: any(sid == 2 for sid, _ in errors)), errors
        # The cap held: 1 initial attempt + 2 retries, both reported.
        assert m.streamed.count(2) == 3, m.streamed
        assert retries == [(2, 1), (2, 2)], retries
        # Swath 2 never becomes ready -> request_print reports the stream
        # failure (not a bare False, so the host can tell why).
        outcome = pl.request_print(2, 353, timeout=0.3)
        assert not outcome.ok
        assert outcome.error == 'stream_failed', outcome
    finally:
        pl.stop()


def test_stream_retry_recovers_from_transient_failure():
    # One flaky transfer (the firmware discards the incomplete swath) must be
    # retried in place and succeed: no fault, the print never notices.
    m = MockSender(fail_on=2, fail_times=1)
    errors = []
    retries = []
    pl = PrintPipeline(m.stream, m.arm, num_slots=2,
                       on_error=lambda sid, msg: errors.append((sid, msg)),
                       on_retry=lambda sid, attempt: retries.append((sid, attempt)))
    pl.load([(i, [b"x"]) for i in (1, 2, 3)])
    try:
        # Swath 2 fails once, is re-sent, and becomes READY.
        assert pl.request_print(1, 353).ok
        outcome = pl.request_print(2, 353, timeout=2.0)
        assert outcome.ok, outcome
        assert not pl.is_failed()
        assert errors == [], errors
        assert retries == [(2, 1)], retries  # the hiccup was counted, not hidden
        assert m.streamed.count(2) == 2, m.streamed
        # The pipeline goes on normally after the recovery.
        pl.on_print_complete(1)
        pl.on_print_complete(2)
        assert pl.request_print(3, 353, timeout=2.0).ok
    finally:
        pl.stop()


def test_firmware_arm_reject_passes_code_through():
    # A streamed (READY) swath that the firmware then rejects at ARM must
    # surface the firmware code, not a generic timeout.
    m = MockSender(arm_reject_on=1)
    pl = PrintPipeline(m.stream, m.arm, num_slots=2)
    pl.load([(i, [b"x"]) for i in (1, 2)])
    try:
        assert wait_until(lambda: 1 in pl._ready)
        outcome = pl.request_print(1, 353)
        assert not outcome.ok
        assert outcome.error == 'swath_not_ready', outcome
        # Rejected arm did not consume the slot as "printing".
        assert m.armed == [1]
    finally:
        pl.stop()


def test_no_error_callback_on_deliberate_stop():
    # A stream that fails because the pipeline is being torn down on purpose
    # (stop() during disconnect/reload) is expected teardown, not a print
    # fault: on_error must NOT run, or it would latch a spurious
    # pipeline_error that nothing clears until the next load_job/abort/reset.
    errors = []
    pl = None

    def blocking_failing_stream(swath_id, lines):
        # Simulate a send_swath that only fails once the link is torn down
        # under it: wait for stop() to flag the pipeline, then fail.
        end = time.monotonic() + 2.0
        while not pl._stop and time.monotonic() < end:
            time.sleep(0.005)
        return False

    m = MockSender()
    pl = PrintPipeline(blocking_failing_stream, m.arm, num_slots=2,
                       on_error=lambda sid, msg: errors.append((sid, msg)))
    pl.load([(1, [b"x"])])
    time.sleep(0.05)  # let the worker enter the stream
    pl.stop()

    assert errors == [], f"teardown failure must not reach on_error: {errors}"


def test_stop_wakes_blocked_request_print():
    # A request_print blocked waiting for its swath must be woken by stop()
    # (abort/reset/disconnect/serial loss) and return 'pipeline_stopped'
    # immediately, instead of burning its full timeout.
    m = MockSender()
    pl = PrintPipeline(m.stream, m.arm, num_slots=2)
    pl.load([(1, [b"x"])])
    try:
        outcomes = []

        def blocked_request():
            # Swath 99 will never stream, so this blocks in the wait loop.
            outcomes.append(pl.request_print(99, 353, timeout=15.0))

        t = threading.Thread(target=blocked_request)
        t.start()
        time.sleep(0.05)  # let it enter the wait
        start = time.monotonic()
        pl.stop()
        t.join(timeout=2.0)
        elapsed = time.monotonic() - start

        assert not t.is_alive(), "request_print did not wake on stop()"
        assert elapsed < 0.1, f"took {elapsed:.3f}s to wake (must be <100ms)"
        assert len(outcomes) == 1
        assert not outcomes[0].ok
        assert outcomes[0].error == 'pipeline_stopped', outcomes[0]
    finally:
        pl.stop()


def test_pipeline_timeout_when_never_streamed():
    # Requesting a swath id the pipeline was never given: times out with the
    # daemon-side reason, distinct from a firmware rejection.
    m = MockSender()
    pl = PrintPipeline(m.stream, m.arm, num_slots=2)
    pl.load([(1, [b"x"])])
    try:
        outcome = pl.request_print(99, 353, timeout=0.2)
        assert not outcome.ok
        assert outcome.error == 'pipeline_timeout', outcome
    finally:
        pl.stop()


def test_load_refuses_reuse_while_worker_stuck():
    # A worker stuck inside a swath-sized stream outlives stop()'s bounded
    # join; load() on the same object must refuse (RuntimeError) instead of
    # putting a second worker on the same callbacks.
    release = threading.Event()
    entered = threading.Event()

    def blocking_stream(swath_id, lines):
        entered.set()
        release.wait(5.0)
        return True

    pl = PrintPipeline(blocking_stream, lambda s, d: ArmOutcome(True),
                       join_timeout=0.1)
    pl.load([(1, [b"x"])])
    try:
        assert entered.wait(2.0), "worker never entered the stream"
        raised = False
        try:
            pl.load([(2, [b"y"])])
        except RuntimeError:
            raised = True
        assert raised, "load() must refuse to restart over a live worker"
    finally:
        release.set()
        assert wait_until(lambda: pl.stop()), "worker never exited after release"


def test_stale_worker_result_is_discarded_by_generation():
    # The reviewer's corruption scenario, white-box: a worker abandoned by a
    # bounded stop() comes back AFTER a newer load took over the state. Its
    # generation is stale, so its result must be discarded: the old swath
    # must not appear in the new run's ready set, nor its failure latch it.
    from collections import deque

    release = threading.Event()
    entered = threading.Event()

    def blocking_stream(swath_id, lines):
        entered.set()
        release.wait(5.0)
        return True

    pl = PrintPipeline(blocking_stream, lambda s, d: ArmOutcome(True),
                       join_timeout=0.05)
    pl.load([(1, [b"x"])])
    try:
        assert entered.wait(2.0), "worker never entered the stream"
        stale_worker = pl._worker
        assert pl.stop() is False  # worker survives the bounded join

        # Simulate a takeover of the same object's state (bypassing load()'s
        # refusal on purpose; this is exactly what the generation protects).
        with pl._cv:
            pl._generation += 1
            pl._pending = deque([2])
            pl._ready = set()
            pl._printing = set()
            pl._failed = False
            pl._stop = False

        release.set()
        assert wait_until(lambda: not stale_worker.is_alive()), \
            "stale worker never exited"
        with pl._cv:
            assert 1 not in pl._ready, "stale worker polluted the new ready set"
            assert pl._failed is False, "stale worker latched the new run"
            assert list(pl._pending) == [2], "new state was disturbed"
    finally:
        release.set()
        pl.request_stop()


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc!r}")
    sys.exit(1 if failures else 0)
