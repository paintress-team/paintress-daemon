# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for TCP command parameter validation.

The firing interval travels with each `print`
(`line_delay_us`); it packs a u16, so out-of-range / non-integer values must
be rejected with a clear `invalid_line_delay` error instead of blowing
struct.pack into an opaque internal_error (e.g. 90 dpi at 1 mm/s derives
282222 us > 65535). Runs under pytest or as a plain script.
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.server import TCPDaemon  # noqa: E402


def test_rejects_missing_line_delay():
    d = TCPDaemon()
    resp = d._cmd_print({'swath_id': 1})
    assert resp['success'] is False
    assert resp['error'] == 'missing_params', resp


def test_rejects_out_of_range_values():
    d = TCPDaemon()
    for bad in (0, -1, 70000, 282222):
        resp = d._cmd_print({'swath_id': 1, 'line_delay_us': bad})
        assert resp['success'] is False, bad
        assert resp['error'] == 'invalid_line_delay', resp


def test_rejects_non_integer_values():
    d = TCPDaemon()
    for bad in ("353", 353.5, True):
        resp = d._cmd_print({'swath_id': 1, 'line_delay_us': bad})
        assert resp['success'] is False, bad
        assert resp['error'] == 'invalid_line_delay', resp


def test_valid_values_pass_validation():
    # With no job loaded the command fails *after* the validation gate:
    # reaching 'no_job_loaded' proves the line_delay was accepted (integral
    # JSON floats are tolerated and coerced).
    d = TCPDaemon()
    for good in (1, 353, 353.0, 65535):
        resp = d._cmd_print({'swath_id': 1, 'line_delay_us': good})
        assert resp['error'] == 'no_job_loaded', resp


def test_purge_rejects_invalid_channel():
    # channel/pulses are single wire bytes: values must be validated with a
    # clear error, never silently wrapped by `& 0xFF` (300 -> 44).
    d = TCPDaemon()
    for bad in (-1, 300, "2", 2.5, True, None):
        resp = d._cmd_purge({'channel': bad, 'pulses': 10})
        assert resp['success'] is False, bad
        assert resp['error'] == 'invalid_channel', resp


def test_purge_rejects_invalid_pulses():
    d = TCPDaemon()
    for bad in (0, -1, 256, 300, "10", 10.5, True, None):
        resp = d._cmd_purge({'channel': 0, 'pulses': bad})
        assert resp['success'] is False, bad
        assert resp['error'] == 'invalid_pulses', resp


def test_purge_valid_params_reach_the_handler():
    class _Handler:
        def __init__(self):
            self.calls = []

        def purge(self, channel, pulses):
            self.calls.append((channel, pulses))
            class _R:
                success = True
            return _R()

    d = TCPDaemon()
    d.handler = _Handler()
    resp = d._cmd_purge({'channel': 2, 'pulses': 255})
    assert resp['success'] is True, resp
    assert d.handler.calls == [(2, 255)]


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
