# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for the abort/reset flow (ABORT = kill-switch, RESET = reboot).

The firmware ABORT only drops the DAC pins and latches the DAC off; it
interrupts nothing (a firing swath runs dry). RESET is a full chip reboot:
the firmware ACKs and reboots, the serial drops (expected, no fault) and the
daemon waits for the reconnect. The daemon therefore:

1. `_cmd_abort` sends ABORT first (electrical kill, the urgent part) and then
   rides a reboot; `_cmd_reset` is the reboot alone.
2. `_reboot_firmware_and_wait` arms the expected-reboot window, sends RESET,
   waits for the USB drop and reconnects inline via `_connect_serial` (
   there is no reconnect monitor).
3. `_on_serial_lost` inside the window latches nothing; outside it latches
   `serial_lost` as before.
4. `_clear_stale_slots` reboots a board whose slots are clean but whose DAC is
   latched; otherwise every ARM of the fresh pipeline would be NACKed
   DAC_LATCHED.
5. `parse_status_response` / `parse_identify_response` expose the new
   `dac_latched` and `boot_flags` bytes.

Runs under pytest or as a plain script.
"""

import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.protocol import (  # noqa: E402
    SlotState, parse_identify_response, parse_status_response,
)
from paintress_daemon.server import TCPDaemon  # noqa: E402


class FakeHandler:
    """Just enough of SwathHandler for the abort/reset paths, with the
    latch behaviour: abort() latches, reset() (= the reboot)
    boots clean."""

    def __init__(self, latched=False, reset_ok=True):
        self.dac_latched = latched
        self.reset_ok = reset_ok
        self.calls = []
        self.io_lock = threading.RLock()

    def abort(self):
        self.calls.append('abort')
        self.dac_latched = True
        return True

    def reset(self):
        self.calls.append('reset')
        if self.reset_ok:
            self.dac_latched = False  # the clean boot starts unlatched
        return self.reset_ok

    def get_status(self):
        self.calls.append('get_status')
        slot = lambda: {'state': SlotState.EMPTY, 'swath_id': 0, 'lines_received': 0}  # noqa: E731
        return {'slot_a': slot(), 'slot_b': slot(),
                'receiving': False, 'printing': False,
                'dac_latched': self.dac_latched}


def _stub_reboot(d, h):
    """Replace the blocking reboot-wait with the fake handler's reset."""
    d._reboot_firmware_and_wait = lambda timeout=8.0: h.reset()


def test_reboot_wait_reconnects_inline():
    h = FakeHandler()
    d = TCPDaemon()
    d.handler = h
    d.serial_port = 'COMX'

    windows = []
    connects = []

    def fake_connect(port):
        connects.append(port)
        # The window must still be armed while we reconnect, so the drop and
        # this connect are recognized as ours (no fault, no pipeline start).
        windows.append(time.monotonic() <= d._expected_reboot_deadline)
        d.handler = FakeHandler()
        return True

    d._connect_serial = fake_connect
    # Simulate the USB drop shortly after the RESET went out.
    threading.Timer(0.05, lambda: setattr(d, 'handler', None)).start()

    assert d._reboot_firmware_and_wait(timeout=3.0) is True
    assert 'reset' in h.calls
    assert connects == ['COMX']
    assert windows == [True]
    # The round trip is over: the window is disarmed again.
    assert time.monotonic() > d._expected_reboot_deadline


def test_reboot_wait_times_out_when_board_never_drops():
    h = FakeHandler()
    d = TCPDaemon()
    d.handler = h
    d.serial_port = 'COMX'
    d._connect_serial = lambda port: True
    # The handler never drops (no _on_serial_lost): the wait must give up.
    assert d._reboot_firmware_and_wait(timeout=0.2) is False


def test_reboot_wait_times_out_when_board_never_returns():
    h = FakeHandler()
    d = TCPDaemon()
    d.handler = h
    d.serial_port = 'COMX'
    d._connect_serial = lambda port: False  # port never reopens
    threading.Timer(0.05, lambda: setattr(d, 'handler', None)).start()
    assert d._reboot_firmware_and_wait(timeout=0.4) is False


def test_abort_kills_then_reboots():
    h = FakeHandler()
    d = TCPDaemon()
    d.handler = h
    _stub_reboot(d, h)

    resp = d._cmd_abort()
    assert resp['success'] is True
    # Electrical kill first, reboot second.
    assert h.calls.index('abort') < h.calls.index('reset')
    # The clean boot left the board unlatched and armable.
    assert h.dac_latched is False


def test_abort_reports_failure_when_reboot_fails():
    h = FakeHandler(reset_ok=False)
    d = TCPDaemon()
    d.handler = h
    _stub_reboot(d, h)

    resp = d._cmd_abort()
    assert resp['success'] is False
    assert h.dac_latched is True  # still latched: the caller must know


def test_abort_with_lost_handler_fails_cleanly():
    # A serial loss can clear self.handler between the requires_serial gate
    # and the handler call: the snapshot must answer device_not_connected,
    # never an AttributeError-turned-internal_error.
    d = TCPDaemon()
    d.handler = None
    resp = d._cmd_abort()
    assert resp['success'] is False
    assert resp['error'] == 'device_not_connected', resp

    resp = d._cmd_get_status()
    assert resp['success'] is False
    assert resp['error'] == 'device_not_connected', resp


def test_expected_reboot_drop_latches_no_fault():
    d = TCPDaemon()
    d._expected_reboot_deadline = time.monotonic() + 5.0
    d._on_serial_lost()
    assert d._print_fault is None


def test_unexpected_drop_still_latches_serial_lost():
    d = TCPDaemon()
    d._on_serial_lost()
    assert d._print_fault is not None
    assert d._print_fault['error'] == 'serial_lost'


def test_latched_board_with_clean_slots_is_rebooted_before_streaming():
    h = FakeHandler(latched=True)
    d = TCPDaemon()
    d.handler = h
    _stub_reboot(d, h)

    d._clear_stale_slots()
    assert 'reset' in h.calls
    assert h.dac_latched is False


def test_clean_unlatched_board_is_not_rebooted():
    h = FakeHandler()
    d = TCPDaemon()
    d.handler = h
    _stub_reboot(d, h)

    d._clear_stale_slots()
    assert 'reset' not in h.calls


def _status_payload(latched, size=17):
    data = bytearray(size)  # both slots EMPTY, nothing receiving/printing
    if size > 16:
        data[16] = 1 if latched else 0
    return bytes(data)


def test_parse_status_exposes_dac_latched():
    assert parse_status_response(_status_payload(True))['dac_latched'] is True
    assert parse_status_response(_status_payload(False))['dac_latched'] is False
    # A shorter 16-byte body parses with the latch defaulting off.
    assert parse_status_response(_status_payload(False, size=16))['dac_latched'] is False


def test_parse_identify_exposes_boot_flags():
    body = bytes([0x01, 0x00]) + bytes(8) + bytes([1])  # wire_id 0x0001 + flags
    parsed = parse_identify_response(body)
    assert parsed['wire_id'] == 0x0001
    assert parsed['boot_flags'] == 1
    # A shorter 10-byte body parses with flags defaulting to 0.
    assert parse_identify_response(body[:10])['boot_flags'] == 0


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
