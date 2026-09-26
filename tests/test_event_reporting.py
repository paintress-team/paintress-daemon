# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for firmware-event reporting.

1. A PRINT_ERROR latches a fault with its code name. (One firmware producer:
   an ARM already queued to core 1 when a DAC kill landed is refused at
   power-up and reported with code DAC_LATCHED. The daemon latches whatever
   arrives; there is no expected-abort suppression anymore.)
2. Event broadcasts must be JSON-serializable (the raw event payload is bytes;
   it used to crash json.dumps inside a fire-and-forget coroutine, so no
   client ever received the documented `event` pushes).
3. ACK/NACK responses are correlated by the echoed command byte: a stale
   response from an earlier timed-out command must not be attributed to the
   next command.

Runs under pytest or as a plain script.
"""

import asyncio
import json
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.protocol import (  # noqa: E402
    Command, ErrorCode, Event, Frame, MessageType,
)
from paintress_daemon.server import TCPDaemon  # noqa: E402
from paintress_daemon.swath_handler import SwathHandler  # noqa: E402


# --- PRINT_ERROR fault semantics ---------------------------------------------

def test_print_error_latches_with_code_name():
    d = TCPDaemon()
    d._on_event(Event.PRINT_ERROR,
                bytes([2, 0, int(ErrorCode.LINE_COUNT_MISMATCH)]))
    assert d._print_fault is not None
    assert d._print_fault['error'] == 'print_error'
    assert 'line_count_mismatch' in d._print_fault['message']


def test_print_error_with_unknown_code_still_latches():
    d = TCPDaemon()
    d._on_event(Event.PRINT_ERROR, bytes([2, 0, 0x7E]))
    assert d._print_fault is not None
    assert '0x7E' in d._print_fault['message']


# --- JSON-safe event broadcast -----------------------------------------------

def test_event_broadcast_is_json_serializable():
    d = TCPDaemon()
    sent = []

    async def fake_broadcast(msg):
        json.dumps(msg)  # raises if anything non-serializable slipped through
        sent.append(msg)

    d._broadcast = fake_broadcast

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    d._loop = loop
    try:
        d._on_event(Event.PRINT_COMPLETE, b'\x07\x00')
        deadline = time.monotonic() + 2.0
        while not sent and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=2.0)

    assert sent, "event broadcast never ran (was it dropped by a crash?)"
    msg = sent[0]
    assert msg['event'] == 'print_complete'
    assert msg['swath_id'] == 7
    assert msg['data'] == '0700'


# --- ACK/NACK correlation by command echo ------------------------------------

class _FakeSerial:
    def send_frame(self, frame, timeout=1.0):
        return True

    def send_urgent(self, frame):
        return True


def test_stale_response_is_discarded():
    handler = SwathHandler(_FakeSerial(), default_timeout=2.0)

    stale = Frame(MessageType.ACK, bytes([int(Command.IDENTIFY)]) + bytes(11))
    good = Frame(MessageType.ACK, bytes([int(Command.GET_STATUS)]) + bytes(17))

    def responder():
        time.sleep(0.1)
        handler.handle_frame(stale)   # late ACK of a previous IDENTIFY
        time.sleep(0.05)
        handler.handle_frame(good)    # the real GET_STATUS ACK

    threading.Thread(target=responder, daemon=True).start()

    resp = handler._send_command(Command.GET_STATUS)
    assert resp.success is True
    assert len(resp.data) == 17  # the status body, not the 11-byte identity


def test_only_stale_responses_time_out():
    handler = SwathHandler(_FakeSerial(), default_timeout=0.3)

    stale = Frame(MessageType.ACK, bytes([int(Command.IDENTIFY)]) + bytes(11))

    def responder():
        time.sleep(0.05)
        handler.handle_frame(stale)

    threading.Thread(target=responder, daemon=True).start()

    resp = handler._send_command(Command.GET_STATUS)
    assert resp.success is False
    assert resp.error_msg == "Timeout"


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
