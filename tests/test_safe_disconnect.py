# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the safe disconnect (stop the hardware before the link goes away).

Closing the port stops nothing on the board: an armed swath keeps waiting for
its trigger (~10 s) and a firing swath ejects ink to its natural end. So a
deliberate disconnect/shutdown first sends ABORT (electrical kill) and RESET
(reboot to a clean, unlatched idle), both ACK-confirmed, unless the link is
already lost (the port-lost path cancelled everything) or the caller asked for
a transport-only teardown (e.g. after a wire-protocol mismatch, or when
re-attaching to the same board immediately).

Runs under pytest or as a plain script.
"""

import sys
import threading
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.server import TCPDaemon  # noqa: E402


class FakeHandler:
    def __init__(self, abort_ok=True, reset_ok=True):
        self.calls = []
        self._abort_ok = abort_ok
        self._reset_ok = reset_ok
        self.io_lock = threading.RLock()

    def abort(self):
        self.calls.append('abort')
        return self._abort_ok

    def reset(self):
        self.calls.append('reset')
        return self._reset_ok


class FakeSerialManager:
    def __init__(self, connected=True):
        self._connected = connected
        self.disconnected = False

    def is_connected(self):
        return self._connected

    def disconnect(self):
        self.disconnected = True


def _daemon(connected=True, **handler_kw):
    d = TCPDaemon()
    d.handler = FakeHandler(**handler_kw)
    d.serial = FakeSerialManager(connected=connected)
    d.serial_port = 'COMX'
    return d


def test_disconnect_stops_firmware_first():
    d = _daemon()
    h = d.handler
    serial = d.serial
    assert d._disconnect_serial() is True
    assert h.calls == ['abort', 'reset'], h.calls
    assert serial.disconnected
    assert d.serial is None and d.handler is None


def test_transport_only_skips_firmware_stop():
    d = _daemon()
    h = d.handler
    serial = d.serial
    assert d._disconnect_serial(transport_only=True) is True
    assert h.calls == [], "transport-only teardown must not command the board"
    assert serial.disconnected


def test_safe_stop_skipped_when_link_already_lost():
    # Port already gone (USB pull): there is nothing reachable to stop, and
    # the port-lost path already cancelled the print. No commands, no error.
    d = _daemon(connected=False)
    h = d.handler
    assert d._safe_stop_firmware() is True
    assert h.calls == []


def test_safe_stop_reports_unconfirmed_stop():
    # The board did not ACK: the safe stop must say so (returns False; the
    # caller logs the high-visibility safety error), not pretend it stopped.
    d = _daemon(abort_ok=False)
    assert d._safe_stop_firmware() is False

    d2 = _daemon(reset_ok=False)
    assert d2._safe_stop_firmware() is False


def test_safe_stop_marks_reboot_drop_as_expected():
    # The RESET's USB drop is ours: the expected-reboot window must be armed
    # so _on_serial_lost logs it as expected instead of latching serial_lost.
    import time
    d = _daemon()
    d._expected_reboot_deadline = 0.0
    assert d._safe_stop_firmware() is True
    assert d._expected_reboot_deadline > time.monotonic()


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
