# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Tests for serial-loss detection and the daemon's fail->cancel reaction.

Covers SerialManager firing its disconnect callback exactly once on a fatal
port error, and the daemon treating an unexpected loss as a terminal print
fault: latch serial_lost, drop the handler, and do NOT reconnect on its own;
recovery is an explicit `reconnect` from the host. Runs under pytest or as a
plain script.
"""

import sys
import threading
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from paintress_daemon.serial_manager import SerialManager  # noqa: E402
from paintress_daemon.server import TCPDaemon  # noqa: E402


def test_port_lost_fires_callback_once():
    sm = SerialManager("COMX")
    sm._running = True
    sm.serial = object()  # pretend the port is open
    assert sm.is_connected() is True

    calls = []
    sm.set_disconnect_callback(lambda: calls.append(1))

    sm._handle_port_lost(OSError("unplugged"))
    assert calls == [1]
    assert sm.is_connected() is False
    assert sm._running is False

    # A second fatal error (the other loop) must not re-fire the callback.
    sm._handle_port_lost(OSError("again"))
    assert calls == [1]


def test_daemon_serial_lost_latches_fault_and_does_not_reconnect():
    class _Pipeline:
        def __init__(self):
            self.stop_requested = False
            self.joined = False

        def request_stop(self):
            self.stop_requested = True

        def stop(self):
            self.joined = True

    d = TCPDaemon()
    d.handler = object()  # pretend connected
    d.serial_port = 'COMX'
    d.pipeline = _Pipeline()

    connects = []
    d._connect_serial = lambda port: connects.append(port) or True

    d._on_serial_lost()

    assert d.handler is None
    assert d._print_fault is not None
    assert d._print_fault['error'] == 'serial_lost'

    # The pipeline is signalled without joining: _on_serial_lost runs on the
    # exiting serial RX thread and must not block on the worker (the joining
    # stop() runs later, on the cmd-worker).
    assert d.pipeline is not None
    assert d.pipeline.stop_requested is True
    assert d.pipeline.joined is False

    # Fail -> cancel: the daemon must not try to reopen the port by itself.
    # Give any stray thread a moment to prove it does not exist.
    for t in threading.enumerate():
        assert 'reconnect' not in t.name.lower(), t.name
    assert connects == []


def test_wait_tx_empty_fails_fast_when_port_is_dead():
    sm = SerialManager("COMX")          # never connected: is_connected False
    sm._tx_queue.put(b'x')              # queue will never drain

    import time
    start = time.monotonic()
    assert sm.wait_tx_empty(timeout=5.0) is False
    assert time.monotonic() - start < 1.0, "must not burn the full timeout"


def test_manual_reconnect_recovers_after_loss():
    d = TCPDaemon()
    d.handler = object()
    d.serial_port = 'COMX'
    d._connect_serial = lambda port: True

    d._on_serial_lost()
    assert d._print_fault is not None

    resp = d._cmd_reconnect({})
    assert resp['success'] is True, resp
    assert resp['port'] == 'COMX'


class _DeadManager:
    """A SerialManager whose port was lost: pyserial's is_open still lies
    True (only close() clears it), but the manager knows it is lost."""

    def __init__(self):
        self.serial = type('S', (), {'is_open': True})()
        self.disconnected = False
        self.stats = {}

    def is_connected(self):
        return False

    def get_tx_queue_size(self):
        return 0

    def disconnect(self):
        self.disconnected = True
        self.serial = None


def test_connect_after_loss_reconnects_instead_of_fake_success():
    # After a USB pull, `connect` to the same port must reconnect for real,
    # not short-circuit on pyserial's stale is_open with "Already connected".
    d = TCPDaemon()
    d.serial = _DeadManager()
    d.serial_port = 'COMX'
    d.handler = None  # what _on_serial_lost leaves behind

    connects = []
    d._connect_serial = lambda port: connects.append(port) or True

    resp = d._cmd_connect({'port': 'COMX'})
    assert resp['success'] is True, resp
    assert connects == ['COMX'], "connect did not actually reconnect"


def test_connect_shortcut_only_when_link_is_usable():
    # A genuinely usable link (handler alive, manager not lost) keeps the
    # cheap "Already connected" shortcut.
    class _LiveManager(_DeadManager):
        def is_connected(self):
            return True

    d = TCPDaemon()
    d.serial = _LiveManager()
    d.serial_port = 'COMX'
    d.handler = object()

    connects = []
    d._connect_serial = lambda port: connects.append(port) or True

    resp = d._cmd_connect({'port': 'COMX'})
    assert resp['success'] is True, resp
    assert 'Already connected' in resp.get('message', ''), resp
    assert connects == [], "shortcut path must not reopen the port"


def test_status_reports_device_disconnected_after_loss():
    d = TCPDaemon()
    d.serial = _DeadManager()  # stale manager still referenced
    d.handler = None           # ...but the link is gone

    resp = d._cmd_daemon_status()
    assert resp['success'] is True
    assert resp['data']['device_connected'] is False


def test_job_firmware_profile_mismatch_latches_fault():
    from paintress_daemon.loaded_job import LoadedJob

    d = TCPDaemon()
    d.loaded_job = LoadedJob(filepath='fake')
    # LoadedJob.metadata comes from JobMetadata.to_dict(), which renders the
    # fingerprint as an "0x%08X" STRING; the check must parse it (comparing
    # the raw str against the firmware's int declared every job a mismatch).
    d.loaded_job.metadata = {'geometry_fingerprint': '0x11111111'}
    d.firmware_identity = {'profile_hash': 0x22222222}

    assert d._job_matches_firmware() is False
    assert d._print_fault is not None
    assert d._print_fault['error'] == 'profile_mismatch'

    # A MATCHING job (string form, the real one) agrees and latches nothing.
    d._clear_print_fault()
    d.firmware_identity = {'profile_hash': 0x11111111}
    assert d._job_matches_firmware() is True
    assert d._print_fault is None

    # Int form (hand-built metadata) and unknown identity also agree.
    d.loaded_job.metadata = {'geometry_fingerprint': 0x11111111}
    assert d._job_matches_firmware() is True
    d.firmware_identity = None
    assert d._job_matches_firmware() is True
    # Unparseable fingerprint: benefit of the doubt, no crash, no latch.
    d.firmware_identity = {'profile_hash': 0x22222222}
    d.loaded_job.metadata = {'geometry_fingerprint': 'garbage'}
    assert d._job_matches_firmware() is True
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
