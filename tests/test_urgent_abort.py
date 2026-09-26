# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the F4 urgent abort/reset path + response demux.

ABORT is a pure kill-switch that never touches the
receiving slot; F4 makes the *host* fast too:

1. abort()/reset() are sent OUTSIDE the io_lock, so an in-progress ~6 s
   send_swath that holds the lock cannot delay an emergency stop.
2. Responses are demultiplexed by the echoed command byte, so a normal
   command (END_SWATH under io_lock) and a concurrent urgent ABORT get their
   own ACKs even in flight together, in any order.
3. Registering the same command twice while one is in flight is refused.
4. The pipeline can be signalled to stop without joining (request_stop), so
   abort never blocks on the worker; a stream that then fails with _stop set
   latches no fault (integration with the deliberate-stop suppression).

Runs under pytest or as a plain script.
"""

import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.protocol import (  # noqa: E402
    Command, Frame, MessageType,
)
from paintress_daemon.swath_handler import SwathHandler  # noqa: E402
from paintress_daemon.print_pipeline import PrintPipeline  # noqa: E402


def _ack_for(frame: Frame) -> Frame:
    """Build the ACK the firmware would echo for a sent command frame."""
    return Frame(MessageType.ACK, bytes([frame.payload[0]]))


class AutoAckSerial:
    """Serial fake that ACKs every command it is sent, after a tiny delay."""

    def __init__(self, delay: float = 0.005):
        self.delay = delay
        self.handler: SwathHandler = None  # wired up after construction

    def _schedule_ack(self, frame: Frame):
        ack = _ack_for(frame)
        threading.Thread(
            target=lambda: (time.sleep(self.delay), self.handler.handle_frame(ack)),
            daemon=True,
        ).start()

    def send_frame(self, frame, timeout=1.0):
        self._schedule_ack(frame)
        return True

    def send_urgent(self, frame):
        self._schedule_ack(frame)
        return True

    def send_bytes(self, data, timeout=1.0):
        return True

    def wait_tx_empty(self, timeout=5.0):
        return True


class SilentSerial:
    """Serial fake that accepts everything but never responds."""

    def send_frame(self, frame, timeout=1.0):
        return True

    def send_urgent(self, frame):
        return True

    def send_bytes(self, data, timeout=1.0):
        return True

    def wait_tx_empty(self, timeout=5.0):
        return True


def _make_handler(serial):
    handler = SwathHandler(serial, default_timeout=2.0)
    if isinstance(serial, AutoAckSerial):
        serial.handler = handler
    return handler


# --- 1. abort() is not delayed by a send_swath holding the io_lock -----------

def test_abort_returns_fast_while_io_lock_is_held():
    handler = _make_handler(AutoAckSerial())

    # Simulate an in-progress send_swath: hold the io_lock for 2 s.
    holding = threading.Event()

    def hog():
        with handler.io_lock:
            holding.set()
            time.sleep(2.0)

    threading.Thread(target=hog, daemon=True).start()
    assert holding.wait(1.0)

    start = time.monotonic()
    ok = handler.abort()
    elapsed = time.monotonic() - start

    assert ok is True
    assert elapsed < 0.2, f"abort took {elapsed:.3f}s (io_lock should not block it)"


# --- 2. demux: two commands in flight, responses delivered out of order ------

def test_demux_delivers_out_of_order_responses():
    handler = _make_handler(SilentSerial())
    results = {}

    # A normal command (holds io_lock) and an urgent one (bypasses it) in flight.
    def send_normal():
        results['identify'] = handler._send_command(Command.IDENTIFY)

    def send_urgent():
        results['abort'] = handler._send_command(
            Command.ABORT, urgent=True, bypass_io_lock=True)

    threading.Thread(target=send_normal, daemon=True).start()
    threading.Thread(target=send_urgent, daemon=True).start()

    # Wait until both waiters are registered.
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        with handler._response_lock:
            if {int(Command.IDENTIFY), int(Command.ABORT)} <= set(handler._waiters):
                break
        time.sleep(0.005)
    with handler._response_lock:
        assert {int(Command.IDENTIFY), int(Command.ABORT)} <= set(handler._waiters)

    # Deliver ABORT's ACK first, then IDENTIFY's, the reverse of send order.
    handler.handle_frame(Frame(MessageType.ACK, bytes([int(Command.ABORT)])))
    handler.handle_frame(Frame(MessageType.ACK, bytes([int(Command.IDENTIFY)])))

    deadline = time.monotonic() + 1.0
    while len(results) < 2 and time.monotonic() < deadline:
        time.sleep(0.005)

    assert results['abort'].success is True
    assert results['identify'].success is True


# --- 3. a duplicate command in flight is refused -----------------------------

def test_duplicate_command_in_flight_is_refused():
    handler = _make_handler(SilentSerial())

    # First ABORT stays in flight (SilentSerial never answers).
    threading.Thread(
        target=lambda: handler._send_command(
            Command.ABORT, urgent=True, bypass_io_lock=True),
        daemon=True,
    ).start()

    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        with handler._response_lock:
            if int(Command.ABORT) in handler._waiters:
                break
        time.sleep(0.005)

    resp = handler._send_command(
        Command.ABORT, urgent=True, bypass_io_lock=True)
    assert resp.success is False
    assert "already in flight" in resp.error_msg


# --- 4. request_stop() does not join; a stopped stream latches no fault -------

def test_request_stop_is_non_blocking_and_suppresses_fault():
    errors = []
    release = threading.Event()

    def stream_fn(swath_id, lines):
        # Block until released, then fail, as if the link was torn down.
        release.wait(2.0)
        return False

    def arm_fn(swath_id, line_delay_us):
        from paintress_daemon.print_pipeline import ArmOutcome
        return ArmOutcome(True)

    pl = PrintPipeline(stream_fn, arm_fn,
                       on_error=lambda sid, msg: errors.append((sid, msg)))
    pl.load([(1, [b'x']), (2, [b'y'])])

    # Let the worker enter stream_fn.
    time.sleep(0.05)

    start = time.monotonic()
    pl.request_stop()          # must return immediately (no join)
    assert time.monotonic() - start < 0.1

    release.set()              # now the in-flight stream fails
    pl.stop()                  # join the worker

    # _stop was set before the failure, so no fault is surfaced.
    assert errors == [], f"a deliberate stop should not latch a fault: {errors}"


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
