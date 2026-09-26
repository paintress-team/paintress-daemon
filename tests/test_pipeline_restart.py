# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for pipeline (re)start hygiene.

Two regressions from bench logs (2026-07-03):

1. Starting a fresh pipeline while the firmware still held swaths from a
   previous one (reloaded job / abort mid-job / daemon restart) made the first
   BEGIN_SWATH fail with NO_SLOT_AVAILABLE and latched a pipeline_error fault.
   `_start_pipeline` must clear stale slots first; that
   means a firmware reboot (RESET), and nothing needs restoring afterwards:
   the firing interval travels with each ARM.

2. `abort`/`reset` stop the pipeline but keep the job loaded; a following
   `print` must restart the pipeline (the old direct-arm fallback was retired
   with S2).

Two more from bench logs (2026-07-05):

3. Re-printing a job that had run to completion left `print` waiting the full
   15 s pipeline timeout: the exhausted pipeline object still existed, so the
   restart-on-None check never fired. `print` must restart whenever the
   pipeline can no longer serve the requested swath.

4. The restart only happens for the job's FIRST swath: a print starts
   over or it doesn't (fail -> cancel; the retry-from-swath-K path was
   retired). A mid-job swath that the pipeline can no longer serve fails
   fast with `swath_out_of_sequence` instead of restarting.

Runs under pytest or as a plain script.
"""

import sys
import threading
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.loaded_job import LoadedJob  # noqa: E402
from paintress_daemon.protocol import ErrorCode, SlotState  # noqa: E402
from paintress_daemon.server import TCPDaemon  # noqa: E402
from paintress_daemon.swath_handler import CommandResponse  # noqa: E402

LINE_DELAY = 353


class FakeHandler:
    """Just enough of SwathHandler for the pipeline paths.

    With slots_dirty=True it mimics a firmware whose two slots are READY from
    a previous run: BEGIN_SWATH (send_swath) is rejected until reset() (the
    reboot) runs.
    """

    def __init__(self, slots_dirty=False):
        self.slots_dirty = slots_dirty
        self.calls = []
        self.io_lock = threading.RLock()

    def get_status(self):
        self.calls.append('get_status')
        state = SlotState.READY if self.slots_dirty else SlotState.EMPTY
        slot = lambda sid: {'state': state, 'swath_id': sid, 'lines_received': 10}  # noqa: E731
        return {'slot_a': slot(1), 'slot_b': slot(2),
                'receiving': False, 'printing': False, 'dac_latched': False}

    def reset(self):
        # The reboot: a clean boot comes back with empty slots.
        self.calls.append('reset')
        self.slots_dirty = False
        return True

    def send_swath(self, swath_id, lines):
        self.calls.append(('send_swath', swath_id))
        if self.slots_dirty:
            return CommandResponse(False, error=ErrorCode.NO_SLOT_AVAILABLE,
                                   error_msg='NACK: no slot available')
        return CommandResponse(True)

    def print_swath(self, swath_id, line_delay_us):
        self.calls.append(('print_swath', swath_id, line_delay_us))
        return CommandResponse(True)


def _daemon(handler):
    d = TCPDaemon()
    d.handler = handler
    # The reboot-wait blocks on the reconnect in production; in these flow
    # tests the fake reset() *is* the whole reboot.
    d._reboot_firmware_and_wait = lambda timeout=8.0: handler.reset()
    d.loaded_job = LoadedJob(filepath='fake',
                             swaths={1: [b'\x00'], 2: [b'\x00'], 3: [b'\x00']})
    return d


def _print(d, swath_id):
    return d._cmd_print({'swath_id': swath_id, 'line_delay_us': LINE_DELAY})


def test_stale_slots_cleared_by_reboot():
    h = FakeHandler(slots_dirty=True)
    d = _daemon(h)
    try:
        d._start_pipeline(d.loaded_job)
        outcome = d.pipeline.request_print(1, LINE_DELAY, timeout=5.0)
        assert outcome.ok, outcome
        # The dirty slots forced a reboot; no timing dance exists anymore:
        # the interval rides in the ARM itself.
        assert 'reset' in h.calls
        assert ('print_swath', 1, LINE_DELAY) in h.calls
        assert d._print_fault is None
    finally:
        d._stop_pipeline()


def test_clean_slots_are_not_reset():
    h = FakeHandler(slots_dirty=False)
    d = _daemon(h)
    try:
        d._start_pipeline(d.loaded_job)
        outcome = d.pipeline.request_print(1, LINE_DELAY, timeout=5.0)
        assert outcome.ok, outcome
        assert 'reset' not in h.calls
    finally:
        d._stop_pipeline()


def test_print_restarts_pipeline_after_abort():
    h = FakeHandler()
    d = _daemon(h)
    try:
        d._start_pipeline(d.loaded_job)
        d._stop_pipeline()  # what _cmd_abort/_cmd_reset do
        assert d.pipeline is None

        resp = _print(d, 1)
        assert resp['success'] is True, resp
        assert d.pipeline is not None
        # The swath was streamed by the restarted pipeline before being armed.
        assert h.calls.index(('send_swath', 1)) \
            < h.calls.index(('print_swath', 1, LINE_DELAY))
    finally:
        d._stop_pipeline()


def test_print_without_job_fails_fast():
    """The direct-arm fallback was retired (S2): no job -> no_job_loaded."""
    h = FakeHandler()
    d = _daemon(h)
    d.loaded_job = None
    resp = _print(d, 1)
    assert resp['success'] is False
    assert resp['error'] == 'no_job_loaded'
    assert all(not (isinstance(c, tuple) and c[0] == 'print_swath')
               for c in h.calls)


def test_print_restarts_pipeline_after_job_completion():
    """A finished (exhausted) pipeline must not stall a re-print for 15 s."""
    h = FakeHandler()
    d = _daemon(h)
    try:
        d._start_pipeline(d.loaded_job)
        # Run the whole 3-swath job through: arm each swath and complete it.
        for sid in (1, 2, 3):
            resp = _print(d, sid)
            assert resp['success'] is True, resp
            d.pipeline.on_print_complete(sid)
        assert d.pipeline is not None
        assert not d.pipeline.can_serve(1)

        # Re-print from the start: must restart the pipeline, not time out.
        h.calls.clear()
        resp = _print(d, 1)
        assert resp['success'] is True, resp
        assert h.calls.index(('send_swath', 1)) \
            < h.calls.index(('print_swath', 1, LINE_DELAY))
    finally:
        d._stop_pipeline()


def test_print_mid_job_swath_fails_out_of_sequence():
    """After an abort, `print 3` must NOT restart the pipeline mid-job (
    fail -> cancel, no retry-from-K): it fails fast so the host restarts the
    print from the first swath."""
    h = FakeHandler()
    d = _daemon(h)
    try:
        d._start_pipeline(d.loaded_job)
        d._stop_pipeline()  # what _cmd_abort/_cmd_reset do
        h.calls.clear()

        resp = _print(d, 3)
        assert resp['success'] is False, resp
        assert resp['error'] == 'swath_out_of_sequence', resp
        assert d.pipeline is None  # nothing was restarted
        assert h.calls == []       # and nothing touched the firmware
    finally:
        d._stop_pipeline()


def test_print_does_not_restart_a_serving_pipeline():
    """The normal swath loop (print N, complete, print N+1) must reuse the
    running pipeline: a restart would reboot the firmware mid-job."""
    h = FakeHandler()
    d = _daemon(h)
    try:
        d._start_pipeline(d.loaded_job)
        pipeline = d.pipeline

        resp = _print(d, 1)
        assert resp['success'] is True, resp
        d.pipeline.on_print_complete(1)
        resp = _print(d, 2)
        assert resp['success'] is True, resp

        assert d.pipeline is pipeline  # same object: never restarted
        assert 'reset' not in h.calls
    finally:
        d._stop_pipeline()


def test_print_unknown_swath_fails_fast():
    h = FakeHandler()
    d = _daemon(h)
    try:
        d._start_pipeline(d.loaded_job)
        resp = _print(d, 99)
        assert resp['success'] is False
        assert resp['error'] == 'swath_not_found'
    finally:
        d._stop_pipeline()


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
