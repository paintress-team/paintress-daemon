# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the single command-worker that serializes mutating commands.

Covers the F2 behavior in server.py: mutating commands run one at a
time, in submission order, on the cmd-worker thread; read-only commands
(`status`) keep the thread pool and answer immediately even while a slow
mutant runs; a watchdog expiry reports 'timeout' (distinguishing "queued
behind" from "started and stuck"), the zombie's late completion is logged,
and the next mutant only runs after the zombie finishes. Runs under pytest
or as a plain script (``python tests/test_command_worker.py``).
"""

import asyncio
import logging
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.server import TCPDaemon  # noqa: E402


class _ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _capped_wait_for(cap):
    """A wrapper for asyncio.wait_for that caps every timeout at `cap`.

    _process_message hard-codes its watchdog buckets (10/30/300 s); the cap
    lets a test drive the real timeout branch in fractions of a second.
    """
    real = asyncio.wait_for

    def wait_for(awaitable, timeout=None):
        if timeout is not None:
            timeout = min(timeout, cap)
        return real(awaitable, timeout)

    return real, wait_for


def test_concurrent_mutants_run_serially():
    # Two concurrent load_job commands must execute one after the other on
    # the worker (no overlap), in submission order, and the daemon's final
    # state must be whatever the second one set.
    async def scenario():
        d = TCPDaemon()
        d._start_cmd_worker()
        try:
            intervals = []
            lock = threading.Lock()

            def fake_load(msg):
                start = time.monotonic()
                time.sleep(0.15)
                d.loaded_job = msg['filepath']
                with lock:
                    intervals.append((start, time.monotonic(), msg['filepath']))
                return {'success': True, 'filepath': msg['filepath']}

            d._cmd_load_job = fake_load

            r1, r2 = await asyncio.gather(
                d._process_message({'cmd': 'load_job', 'filepath': 'A', 'id': 1}),
                d._process_message({'cmd': 'load_job', 'filepath': 'B', 'id': 2}),
            )
            assert r1['success'] and r2['success'], (r1, r2)
            assert len(intervals) == 2
            (a_start, a_end, a_tag), (b_start, b_end, b_tag) = intervals
            assert [a_tag, b_tag] == ['A', 'B'], "submission order not preserved"
            assert a_end <= b_start, (
                f"mutants overlapped: A ran {a_start:.3f}..{a_end:.3f}, "
                f"B started at {b_start:.3f}")
            assert d.loaded_job == 'B', "final state must be the second load_job's"
            assert d._command_in_progress is None
        finally:
            d._stop_cmd_worker()

    asyncio.run(scenario())


def test_status_answers_while_mutant_runs():
    # `status` is read-only and stays on the thread pool: it must answer in
    # <100 ms while a slow mutant occupies the worker, and it must expose
    # that mutant via command_in_progress.
    async def scenario():
        d = TCPDaemon()
        d._start_cmd_worker()
        try:
            started = threading.Event()

            def slow_connect(msg):
                started.set()
                time.sleep(0.6)
                return {'success': True, 'port': msg.get('port')}

            d._cmd_connect = slow_connect

            task = asyncio.ensure_future(
                d._process_message({'cmd': 'connect', 'port': 'COMX', 'id': 1}))
            ok = await asyncio.get_event_loop().run_in_executor(
                None, started.wait, 2.0)
            assert ok, "mutant never started on the worker"

            t0 = time.monotonic()
            resp = await d._process_message({'cmd': 'status', 'id': 2})
            elapsed = time.monotonic() - t0
            assert resp['success'], resp
            assert elapsed < 0.1, (
                f"status took {elapsed:.3f}s behind a mutant (must be <100ms)")
            assert resp['data']['command_in_progress'] == 'connect', resp['data']

            resp1 = await task
            assert resp1['success'], resp1
            assert d._command_in_progress is None
        finally:
            d._stop_cmd_worker()

    asyncio.run(scenario())


def test_mutant_timeout_zombie_and_queued_behind():
    # A mutant that outlives its watchdog gets a 'timeout' response saying it
    # *started* and is stuck; a second mutant timing out while still queued
    # says so (naming the blocker) and only executes after the zombie
    # finishes; the zombie's late completion is logged.
    async def scenario():
        d = TCPDaemon()
        d._start_cmd_worker()

        log_capture = _ListHandler()
        server_logger = logging.getLogger('paintress_daemon.server')
        server_logger.addHandler(log_capture)

        real_wait_for, capped = _capped_wait_for(0.25)
        asyncio.wait_for = capped
        try:
            zombie_done = threading.Event()

            def slow_connect(msg):
                time.sleep(0.8)
                zombie_done.set()
                return {'success': True}

            d._cmd_connect = slow_connect

            ran_after_zombie = []

            def fake_disconnect(msg=None):
                ran_after_zombie.append(zombie_done.is_set())
                return {'success': True}

            d._cmd_disconnect = fake_disconnect

            # Zombie: starts immediately, outlives the (capped) watchdog.
            resp = await d._process_message({'cmd': 'connect', 'port': 'X', 'id': 1})
            assert resp['error'] == 'timeout', resp
            assert 'started and did not finish' in resp['message'], resp['message']

            # Still running -> visible in command_in_progress.
            assert not zombie_done.is_set(), "zombie finished too early for the test"
            assert d._command_in_progress == 'connect'

            # Second mutant: times out while still queued behind the zombie.
            resp2 = await d._process_message({'cmd': 'disconnect', 'id': 2})
            assert resp2['error'] == 'timeout', resp2
            assert 'never started' in resp2['message'], resp2['message']
            assert "'connect'" in resp2['message'], resp2['message']

            # It must still execute, but only after the zombie finished.
            deadline = time.monotonic() + 3.0
            while not ran_after_zombie and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            assert ran_after_zombie, "queued mutant never executed"
            assert ran_after_zombie[0] is True, (
                "queued mutant interleaved with the zombie")

            # The zombie's late completion is logged (result discarded).
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                if any('completed after the timeout' in r.getMessage()
                       for r in log_capture.records):
                    break
                await asyncio.sleep(0.01)
            assert any('completed after the timeout' in r.getMessage()
                       for r in log_capture.records), (
                "late completion of the zombie was not logged")

            assert d._command_in_progress is None
        finally:
            asyncio.wait_for = real_wait_for
            server_logger.removeHandler(log_capture)
            d._stop_cmd_worker()

    asyncio.run(scenario())


def test_daemon_stop_is_idempotent():
    # Shutdown calls stop() twice (signal handler + the runner's finally):
    # the second call must be a no-op, not a re-run of the teardown that
    # double-logs "Daemon stopped" / final stats.
    import paintress_daemon.server as server_mod

    log_capture = _ListHandler()
    server_logger = logging.getLogger(server_mod.__name__)
    prev_level = server_logger.level
    server_logger.setLevel(logging.INFO)  # "Daemon stopped" is INFO
    server_logger.addHandler(log_capture)
    try:
        d = TCPDaemon()
        asyncio.run(d.stop())
        asyncio.run(d.stop())

        stopped_logs = [r for r in log_capture.records
                        if "Daemon stopped" in r.getMessage()]
        assert len(stopped_logs) == 1, (
            f"stop() teardown ran {len(stopped_logs)} times")
    finally:
        server_logger.removeHandler(log_capture)
        server_logger.setLevel(prev_level)


def test_submit_inline_fallback_without_worker():
    # With no worker running (unit tests driving handlers directly, or
    # shutdown) _submit_to_worker executes inline and resolves the future.
    d = TCPDaemon()
    item = d._submit_to_worker('inline', lambda: {'success': True, 'tag': 42})
    assert item.future.done()
    assert item.future.result() == {'success': True, 'tag': 42}
    assert item.started_at is not None

    # An exception is captured on the future, not raised at submit time.
    def boom():
        raise RuntimeError("nope")

    item2 = d._submit_to_worker('inline-error', boom)
    assert item2.future.done()
    assert isinstance(item2.future.exception(), RuntimeError)


def test_worker_stop_joins_thread():
    d = TCPDaemon()
    d._start_cmd_worker()
    worker = d._cmd_worker_thread
    assert worker is not None and worker.is_alive()
    d._stop_cmd_worker()
    assert d._cmd_worker_thread is None
    assert not worker.is_alive()
    # Idempotent: stopping again (or with no worker) is a no-op.
    d._stop_cmd_worker()


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
