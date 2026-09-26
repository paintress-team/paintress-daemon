# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the serial write-error path (single idempotent port-lost path).

A fatal SerialException/OSError on WRITE must be treated exactly like one on
read: the port is lost, is_connected() goes false and the disconnect callback
fires once. It used to be swallowed inside _send_batch/_send_direct (only
tx_errors was bumped), so the link kept reporting healthy (device_connected
true, no callback) while every frame was silently dropped.

Runs under pytest or as a plain script.
"""

import sys
import threading
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import serial as pyserial  # noqa: E402

from paintress_daemon.serial_manager import SerialManager  # noqa: E402


class ExplodingPort:
    """A fake pyserial port whose writes always raise (device vanished)."""

    is_open = True
    in_waiting = 0

    def write(self, data):
        raise pyserial.SerialException("device gone")

    def flush(self):
        pass

    def read(self, n):
        return b""

    def close(self):
        pass


def _manager():
    m = SerialManager("FAKE")
    m.serial = ExplodingPort()
    m._running = True
    lost = []
    m.set_disconnect_callback(lambda: lost.append(1))
    return m, lost


def test_send_direct_write_error_marks_port_lost():
    m, lost = _manager()
    assert m._send_direct(b"abc") is False
    assert m._lost is True
    assert m.is_connected() is False
    assert lost == [1], "disconnect callback must fire exactly once"
    assert m.stats["tx_errors"] == 1


def test_send_batch_write_error_reaches_tx_loop_port_lost():
    # _send_batch re-raises into _tx_loop's handler: the loop exits and the
    # port-lost path runs (callback + is_connected false), instead of the
    # error being swallowed with the loop spinning on a dead port.
    m, lost = _manager()
    m._tx_queue.put(b"xyz")
    t = threading.Thread(target=m._tx_loop, daemon=True)
    t.start()
    t.join(timeout=2.0)
    assert not t.is_alive(), "TX loop must exit on a fatal write error"
    assert m._lost is True
    assert m.is_connected() is False
    assert lost == [1]
    assert m.stats["tx_errors"] == 1


def test_port_lost_is_idempotent_across_paths():
    # A second failure (any path) must not re-fire the callback.
    m, lost = _manager()
    assert m._send_direct(b"a") is False
    assert m._send_direct(b"b") is False
    assert lost == [1]


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
