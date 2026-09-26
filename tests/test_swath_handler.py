# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for SwathHandler: send_lines accounting and response validation.

1. A line only counts as sent once its chunk is accepted by the TX queue.
   Counting on append used to report the lines of a failed chunk, and a
   failed FINAL flush still claimed sent == total, so send_swath proceeded to
   END as if the transfer had succeeded (only the firmware's complete=0
   caught it, with a misleading count).
2. BEGIN/END responses are validated strictly: the wire has no request id, so
   a late ACK from an earlier timed-out command lands on the current waiter;
   the echoed swath_id (and the slot domain) is the only thing that unmasks
   it. A mismatched or short ACK must fail the transfer, never seed
   _current_swath with another swath's slot or count a dataless END ACK as a
   confirmed-complete swath.

Runs under pytest or as a plain script.
"""

import struct
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.swath_handler import (  # noqa: E402
    CommandResponse, SwathHandler, SwathInfo,
)


class FakeSerial:
    """Accepts send_bytes calls until told to fail from call N on."""

    def __init__(self, fail_from_call=None):
        self.calls = 0
        self._fail_from = fail_from_call

    def send_bytes(self, data, timeout=1.0):
        self.calls += 1
        if self._fail_from is not None and self.calls >= self._fail_from:
            return False
        return True

    # end_swath's pre-flight drain check.
    def wait_tx_empty(self, timeout=5.0):
        return True

    def is_connected(self):
        return True


def _handler(serial):
    h = SwathHandler(serial)
    h._current_swath = SwathInfo(swath_id=1, line_count=3)
    return h


# 30 KB lines force a mid-loop chunk flush against the 64 KB CHUNK_MAX:
# lines 1+2 fill the first chunk (flushed when line 3 arrives), line 3
# rides the final flush.
_LINES = [bytes(30000)] * 3


def test_all_lines_counted_on_success():
    fake = FakeSerial()
    h = _handler(fake)
    assert h.send_lines(_LINES) == 3
    assert fake.calls == 2  # mid-loop flush + final flush


def test_failed_mid_loop_chunk_counts_nothing_from_it():
    # First flush (lines 1+2) fails: none of its lines may be reported sent.
    fake = FakeSerial(fail_from_call=1)
    h = _handler(fake)
    assert h.send_lines(_LINES) == 0


def test_failed_final_flush_does_not_claim_completion():
    # Mid-loop flush succeeds (lines 1+2), the FINAL flush (line 3) fails:
    # sent must be 2, never 3; send_swath must fail before END_SWATH.
    fake = FakeSerial(fail_from_call=2)
    h = _handler(fake)
    assert h.send_lines(_LINES) == 2


# --- response validation (no request id on the wire) -------------------------

def _handler_with_ack(data):
    """Handler whose _send_command returns a successful ACK carrying `data`."""
    h = SwathHandler(FakeSerial())
    h._send_command = lambda *a, **k: CommandResponse(success=True, data=data)
    return h


def test_begin_accepts_matching_response():
    h = _handler_with_ack(struct.pack('<HB', 7, 1))  # id=7, slot=1
    resp = h.begin_swath(7, 10)
    assert resp.success
    assert h.get_current_swath().slot == 1


def test_begin_rejects_stale_ack_with_other_swath_id():
    # The reviewer's scenario: BEGIN(1) timed out, BEGIN(2) is in flight, and
    # the late ACK for swath 1 lands on swath 2's waiter. The id mismatch is
    # the only thing that unmasks it; it must fail, not seed the slot.
    h = _handler_with_ack(struct.pack('<HB', 1, 0))  # stale: id=1
    resp = h.begin_swath(2, 10)
    assert not resp.success
    assert 'mismatch' in resp.error_msg
    assert h.get_current_swath() is None


def test_begin_rejects_out_of_domain_slot():
    h = _handler_with_ack(struct.pack('<HB', 3, 7))  # slot 7 does not exist
    resp = h.begin_swath(3, 10)
    assert not resp.success
    assert h.get_current_swath() is None


def test_begin_rejects_short_ack_payload():
    h = _handler_with_ack(b'\x03')  # 1 byte instead of 3
    resp = h.begin_swath(3, 10)
    assert not resp.success
    assert h.get_current_swath() is None


def _end_handler(ack_data):
    h = SwathHandler(FakeSerial())
    h._current_swath = SwathInfo(swath_id=5, line_count=3, lines_sent=3)
    h._send_command = lambda *a, **k: CommandResponse(success=True, data=ack_data)
    return h


def test_end_accepts_matching_response():
    h = _end_handler(struct.pack('<HB', 5, 1))  # id=5, complete=1
    resp = h.end_swath()
    assert resp.success
    assert h.stats['swaths_sent'] == 1


def test_end_rejects_dataless_ack():
    # An ACK without the 3-byte payload used to count as success, i.e. as
    # the firmware confirming a complete swath it never confirmed.
    h = _end_handler(None)
    resp = h.end_swath()
    assert not resp.success
    assert h.stats['swaths_sent'] == 0


def test_end_rejects_stale_ack_with_other_swath_id():
    h = _end_handler(struct.pack('<HB', 4, 1))  # stale: id=4, ending id=5
    resp = h.end_swath()
    assert not resp.success
    assert 'mismatch' in resp.error_msg
    assert h.stats['swaths_sent'] == 0


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
