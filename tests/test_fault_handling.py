# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the daemon's print-fault latching.

A firmware PRINT_ERROR / TRIGGER_TIMEOUT (or a pipeline streaming failure) must
latch a fault so the next `print` fails fast with the cause, instead of the host
printing on obliviously. The fault is cleared on a fresh job / reset / abort.
Runs under pytest or as a plain script.
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.server import TCPDaemon  # noqa: E402
from paintress_daemon.protocol import Event  # noqa: E402


def _swath_bytes(swath_id):
    return bytes([swath_id & 0xFF, (swath_id >> 8) & 0xFF])


def test_trigger_timeout_latches_and_print_fails_fast():
    d = TCPDaemon()
    assert d._print_fault is None

    d._on_event(Event.TRIGGER_TIMEOUT, _swath_bytes(3))
    assert d._print_fault is not None
    assert d._print_fault['error'] == 'trigger_timeout'

    # `print` fails fast with the cause, without touching the (absent) handler.
    resp = d._cmd_print({'swath_id': 4})
    assert resp['success'] is False
    assert resp['error'] == 'trigger_timeout'


def test_first_cause_is_kept():
    d = TCPDaemon()
    d._on_event(Event.TRIGGER_TIMEOUT, _swath_bytes(1))
    d._on_event(Event.PRINT_ERROR, _swath_bytes(2))
    assert d._print_fault['error'] == 'trigger_timeout'  # root cause wins


def test_print_complete_does_not_latch():
    d = TCPDaemon()
    d._on_event(Event.PRINT_COMPLETE, _swath_bytes(1))
    assert d._print_fault is None


def test_status_exposes_fault():
    d = TCPDaemon()
    assert d._cmd_daemon_status()['data']['print_fault'] is None
    d._on_event(Event.PRINT_ERROR, _swath_bytes(2))
    fault = d._cmd_daemon_status()['data']['print_fault']
    assert fault is not None and fault['error'] == 'print_error'


def test_clear_resets_fault():
    d = TCPDaemon()
    d._on_event(Event.PRINT_ERROR, _swath_bytes(1))
    assert d._print_fault is not None
    d._clear_print_fault()
    assert d._print_fault is None


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
