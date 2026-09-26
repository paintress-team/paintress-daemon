# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Swath handler.

Swath flow:
    1. begin_swath(swath_id, line_count) -> ACK with the allocated slot
    2. send_lines(lines)                 -> no intermediate ACK
    3. end_swath()                       -> ACK confirming reception
    4. print_swath(swath_id)             -> ACK plus progress events
"""

import logging
import struct
import threading
import time
from typing import Optional, Callable, List
from dataclasses import dataclass

from .protocol import (
    Frame, MessageType, Command, Event, ErrorCode,
    make_cmd_frame, encode_data_frame,
    make_begin_swath_params, make_arm_params,
    parse_ack, parse_nack, parse_event, parse_log,
    parse_status_response, parse_begin_swath_response, parse_end_swath_response,
    parse_identify_response,
)
from .serial_manager import SerialManager


logger = logging.getLogger(__name__)


@dataclass
class SwathInfo:
    """State of a swath currently being sent."""
    swath_id: int
    line_count: int
    lines_sent: int = 0
    slot: int = -1

    @property
    def progress(self) -> float:
        if self.line_count == 0:
            return 0.0
        return (self.lines_sent / self.line_count) * 100.0

    @property
    def is_complete(self) -> bool:
        return self.lines_sent >= self.line_count


@dataclass
class CommandResponse:
    """Result of a command sent to the firmware."""
    success: bool
    data: Optional[bytes] = None
    error: Optional[ErrorCode] = None
    error_msg: Optional[str] = None


class _Waiter:
    """One in-flight command awaiting its ACK/NACK, keyed by the command echo.

    handle_frame() drops the matching response frame here and sets the event;
    the sender wakes and reads it. One waiter per command byte at a time.

    Known limit: the wire has no request id, so the echoed command byte is the
    ONLY correlation. After a timeout removes a waiter, a late response can
    still arrive, and if a new command of the same byte is already in flight,
    it is delivered to the new waiter. The callers therefore validate every
    response field they can (begin/end echo the swath_id; see begin_swath /
    end_swath) so a stale ACK is rejected instead of trusted. A wire-level
    request id is the real fix (candidate).
    """
    __slots__ = ('event', 'frame')

    def __init__(self):
        self.event = threading.Event()
        self.frame: Optional[Frame] = None


# The firmware's PSRAM double buffer has exactly two slots (protocol status
# reports slot_a/slot_b); any other value in a BEGIN ACK is a corrupt or
# stale response.
_NUM_SLOTS = 2


class SwathHandler:
    """High-level swath and control operations over the serial link."""

    def __init__(self, serial_manager: SerialManager, default_timeout: float = 5.0):
        self.serial = serial_manager
        self.default_timeout = default_timeout

        self._current_swath: Optional[SwathInfo] = None
        self._lock = threading.Lock()

        # Serializes serial command sequences across threads: the print
        # pipeline's background streaming worker vs. an arm/control command.
        # Reentrant so a full send_swath (begin/data/end) is atomic while its
        # inner _send_command calls re-enter on the same thread. The urgent
        # abort/reset path deliberately bypasses this lock (see _send_command).
        self.io_lock = threading.RLock()

        # Callback for firmware events.
        self._on_event: Optional[Callable[[Event, dict], None]] = None
        self._on_log: Optional[Callable[[int, str], None]] = None

        # In-flight command responses, demultiplexed by the echoed command byte
        # (payload[0] of every ACK/NACK). A single slot could not tell a normal
        # command (END_SWATH under io_lock) apart from a concurrent urgent
        # ABORT/RESET; the dict routes each response to its own waiter.
        self._waiters: dict[int, _Waiter] = {}
        self._response_lock = threading.Lock()

        self.stats = {
            'swaths_sent': 0,
            'lines_sent': 0,
            'prints_completed': 0,
            'errors': 0,
        }

    def set_event_callback(self, callback: Callable[[Event, dict], None]):
        """Register the callback for firmware events."""
        self._on_event = callback

    def set_log_callback(self, callback: Callable[[int, str], None]):
        """Register the callback for firmware LOG frames (level, text)."""
        self._on_log = callback

    def handle_frame(self, frame: Frame):
        """Process a frame received from the firmware."""
        if frame.msg_type == MessageType.ACK or frame.msg_type == MessageType.NACK:
            # Route by the echoed command byte (payload[0] of every ACK/NACK) so
            # a response reaches the exact command that is waiting for it, even
            # when a normal command and an urgent ABORT/RESET are in flight at
            # once. A response with no matching waiter is a late reply to an
            # already-finished (timed-out) command: log and drop it.
            echoed = frame.payload[0] if len(frame.payload) >= 1 else None
            with self._response_lock:
                waiter = self._waiters.get(echoed) if echoed is not None else None
                if waiter is not None:
                    waiter.frame = frame
                    waiter.event.set()
                else:
                    logger.warning(
                        "Discarding %s echoing cmd 0x%02X with no waiter",
                        frame.msg_type.name,
                        echoed if echoed is not None else 0xFF,
                    )

        elif frame.msg_type == MessageType.EVENT:
            self._handle_event(frame)

        elif frame.msg_type == MessageType.LOG:
            self._handle_log(frame)

    def _handle_log(self, frame: Frame):
        """Process a firmware LOG frame."""
        parsed = parse_log(frame.payload)
        if self._on_log:
            self._on_log(parsed['level'], parsed['text'])

    def _handle_event(self, frame: Frame):
        """Process a firmware event frame."""
        parsed = parse_event(frame.payload)

        if parsed['event'] and self._on_event:
            try:
                event = Event(parsed['event'])
                self._on_event(event, parsed.get('data', {}))
            except ValueError:
                pass

    def _send_command(self, cmd: Command, params: bytes = b'',
                      timeout: Optional[float] = None, urgent: bool = False,
                      bypass_io_lock: bool = False) -> CommandResponse:
        """Send a command and wait for its ACK/NACK response.

        Normal commands take the io_lock so a full send_swath (begin/data/end)
        stays atomic against them. bypass_io_lock is the urgent emergency path
        (abort/reset): it skips the io_lock so an in-progress send_swath holding
        it cannot delay an emergency stop, and relies on the response demux to
        tell the two concurrent responses apart.
        """
        if bypass_io_lock:
            return self._send_command_demux(cmd, params, timeout, urgent)
        with self.io_lock:
            return self._send_command_demux(cmd, params, timeout, urgent)

    def _send_command_demux(self, cmd: Command, params: bytes = b'',
                            timeout: Optional[float] = None,
                            urgent: bool = False) -> CommandResponse:
        timeout = timeout or self.default_timeout
        frame = make_cmd_frame(cmd, params)
        key = int(cmd)

        # Register this command's waiter before sending, so a fast response is
        # never missed. Only one instance of a given command may be in flight:
        # with the cmd-worker serializing mutants, the concurrent senders are
        # the pipeline worker, the cmd-worker and the urgent path (all
        # distinct command bytes), so a duplicate here means a real bug.
        waiter = _Waiter()
        with self._response_lock:
            if key in self._waiters:
                return CommandResponse(
                    success=False,
                    error_msg=f"command 0x{key:02X} already in flight")
            self._waiters[key] = waiter

        try:
            if urgent:
                success = self.serial.send_urgent(frame)
            else:
                success = self.serial.send_frame(frame)

            if not success:
                return CommandResponse(success=False, error_msg="Failed to send command")

            if not waiter.event.wait(timeout):
                return CommandResponse(success=False, error_msg="Timeout")

            response = waiter.frame
        finally:
            with self._response_lock:
                self._waiters.pop(key, None)

        if response is None:
            return CommandResponse(success=False, error_msg="Timeout")

        if response.msg_type == MessageType.ACK:
            parsed = parse_ack(response.payload)
            return CommandResponse(success=True, data=parsed.get('data'))

        parsed = parse_nack(response.payload)
        return CommandResponse(
            success=False,
            error=parsed.get('error'),
            error_msg=f"NACK: {parsed.get('error')}",
        )

    # === Basic commands ===

    def reset(self) -> bool:
        """Reset = chip reboot.

        The firmware ACKs and reboots ~50 ms later: the serial link WILL drop
        right after a successful reset and re-enumerate in ~1-2 s. Callers use
        the daemon's _reboot_firmware_and_wait, which treats the drop as
        expected and waits for the reconnect.

        Sent urgent and OUTSIDE the io_lock: a reboot must reach the wire even
        while the pipeline worker holds the lock streaming a swath (the demux
        keeps the RESET ACK apart from that swath's END_SWATH ACK).
        """
        resp = self._send_command(Command.RESET, urgent=True, bypass_io_lock=True)
        if resp.success:
            with self._lock:
                self._current_swath = None
        return resp.success

    def abort(self) -> bool:
        """Electrical kill-switch: DAC off + latched until RESET.

        The firmware interrupts nothing else: reception and a firing swath
        wind down dry. Follow with reset() (= reboot) to unlatch and recover.

        Sent urgent and OUTSIDE the io_lock so it reaches the wire in ms even
        while the pipeline worker holds the lock mid-send_swath: the whole
        point of an emergency stop (the demux keeps the ABORT ACK apart from a
        concurrent END_SWATH ACK).
        """
        resp = self._send_command(Command.ABORT, urgent=True, bypass_io_lock=True)
        if resp.success:
            with self._lock:
                self._current_swath = None
        return resp.success

    def get_status(self) -> Optional[dict]:
        """Query the firmware slot status."""
        resp = self._send_command(Command.GET_STATUS)
        if resp.success and resp.data:
            return parse_status_response(resp.data)
        return None

    def identify(self) -> Optional[dict]:
        """Query the firmware identity (wire_id, profile_hash, fw_build)."""
        resp = self._send_command(Command.IDENTIFY)
        if resp.success and resp.data:
            return parse_identify_response(resp.data)
        return None

    # === Swath operations ===

    def begin_swath(self, swath_id: int, line_count: int) -> CommandResponse:
        """Start a new swath.

        Args:
            swath_id: Swath identifier (chosen by the host).
            line_count: Number of lines in the swath.

        Returns:
            CommandResponse whose data carries the allocated slot.
        """
        if swath_id == 0 or line_count == 0:
            return CommandResponse(success=False, error_msg="Invalid parameters")

        params = make_begin_swath_params(swath_id, line_count)
        resp = self._send_command(Command.BEGIN_SWATH, params)
        if not resp.success:
            return resp

        # Validate the response strictly: the wire has no request id, so a
        # late ACK from an earlier timed-out BEGIN can land on this waiter.
        # The echoed swath_id must be OURS and the slot must exist; anything
        # else is a stale/corrupt response and must fail the transfer, not
        # seed _current_swath with another swath's slot.
        parsed = parse_begin_swath_response(resp.data) if resp.data else None
        if parsed is None:
            self.stats['errors'] += 1
            return CommandResponse(
                success=False,
                error_msg="BEGIN_SWATH ACK missing/short payload")
        if parsed['swath_id'] != swath_id or not (0 <= parsed['slot'] < _NUM_SLOTS):
            self.stats['errors'] += 1
            logger.warning(
                "BEGIN_SWATH response mismatch: sent id=%d, ACK carries id=%d "
                "slot=%d (stale ACK from a timed-out command?)",
                swath_id, parsed['swath_id'], parsed['slot'])
            return CommandResponse(
                success=False,
                error_msg=(f"BEGIN_SWATH response mismatch (asked id "
                           f"{swath_id}, got id {parsed['swath_id']} slot "
                           f"{parsed['slot']}); stale ACK rejected"))

        with self._lock:
            self._current_swath = SwathInfo(
                swath_id=swath_id,
                line_count=line_count,
                slot=parsed['slot'],
            )
        return CommandResponse(success=True, data=resp.data)

    def send_lines(self, lines: List[bytes],
                   progress_callback: Optional[Callable[[int, int], None]] = None) -> int:
        """Send swath lines as chunked, pre-encoded DATA frames.

        Lines are encoded with encode_data_frame() and accumulated into
        large chunks. This keeps per-line overhead (queue items, locks,
        allocations) and CRC cost low when streaming a whole swath.

        Args:
            lines: Line payloads (each the job's bytes_per_line in size; the
                frame is sized from the line itself).
            progress_callback: Called as callback(lines_sent, total_lines).

        Returns:
            Number of lines enqueued successfully.
        """
        sent = 0
        total = len(lines)

        # 64 KB keeps the queue item count low and reduces syscalls in SerialTX.
        CHUNK_MAX = 65536
        chunk = bytearray()
        chunk_lines = 0

        # Take a stable reference once (avoid locking twice per line).
        with self._lock:
            if not self._current_swath:
                return 0

        # A line only counts as sent once its CHUNK is accepted by the TX
        # queue; counting on append used to report the lines of a failed
        # chunk (and a failed FINAL flush still claimed sent == total,
        # pushing failure detection all the way to the firmware's END
        # complete=0 with a misleading count).
        for i, line in enumerate(lines):
            try:
                frame_bytes = encode_data_frame(line)
            except ValueError:
                break

            if len(chunk) + len(frame_bytes) > CHUNK_MAX:
                if not self.serial.send_bytes(bytes(chunk)):
                    break
                sent += chunk_lines
                chunk.clear()
                chunk_lines = 0

            chunk.extend(frame_bytes)
            chunk_lines += 1

            if progress_callback and (i % 200 == 0 or i == total - 1):
                # Progress is informational: report lines QUEUED so far
                # (accepted chunks + the one being built), at per-line
                # granularity. The strict accepted-only count is what the
                # RETURN value keeps.
                progress_callback(sent + chunk_lines, total)

        if chunk and self.serial.send_bytes(bytes(chunk)):
            sent += chunk_lines

        # Update counters in a single lock acquisition.
        with self._lock:
            if self._current_swath:
                self._current_swath.lines_sent += sent
        self.stats["lines_sent"] += sent

        return sent

    def end_swath(self) -> CommandResponse:
        """Finish the current swath.

        The host must wait for this ACK before starting a new swath or
        issuing a print. The ACK is validated strictly: it must carry the
        3-byte payload and echo the swath we are ending. With no request id
        on the wire, a late END ACK from an earlier timed-out command is
        otherwise indistinguishable, and a dataless ACK must never count as
        the firmware confirming a complete swath.
        """
        with self._lock:
            expected_id = (self._current_swath.swath_id
                           if self._current_swath else None)

        # Wait for the TX queue to drain first. A slow-but-alive drain is
        # fine (END rides the same ordered queue behind the data), but a
        # drain that failed because the LINK died can fail fast here instead
        # of asking a dead firmware to confirm the swath.
        if not self.serial.wait_tx_empty(timeout=10.0) \
                and not self.serial.is_connected():
            with self._lock:
                self._current_swath = None
            return CommandResponse(
                success=False,
                error_msg="serial link lost mid-transfer (TX never drained)")

        resp = self._send_command(Command.END_SWATH)

        # The transfer is over either way; never leave a stale SwathInfo.
        with self._lock:
            self._current_swath = None

        if not resp.success:
            return resp

        parsed = parse_end_swath_response(resp.data) if resp.data else None
        if parsed is None:
            self.stats['errors'] += 1
            return CommandResponse(
                success=False,
                error_msg="END_SWATH ACK missing/short payload")
        if expected_id is not None and parsed['swath_id'] != expected_id:
            self.stats['errors'] += 1
            logger.warning(
                "END_SWATH response mismatch: ending id=%d, ACK carries "
                "id=%d (stale ACK from a timed-out command?)",
                expected_id, parsed['swath_id'])
            return CommandResponse(
                success=False,
                error_msg=(f"END_SWATH response mismatch (ending swath "
                           f"{expected_id}, ACK is for {parsed['swath_id']}); "
                           "stale ACK rejected"))

        self.stats['swaths_sent'] += 1
        if not parsed['complete']:
            # The firmware reported the swath as incomplete (send_swath
            # turns this into a failed transfer).
            self.stats['errors'] += 1

        return resp

    def print_swath(self, swath_id: int, line_delay_us: int) -> CommandResponse:
        """Arm a swath; it fires on the hardware start trigger.

        line_delay_us is the firing-grid interval for this swath: the timing
        travels with each ARM. The arm command is sent as
        urgent to keep latency low.
        """
        params = make_arm_params(swath_id, line_delay_us)
        return self._send_command(Command.ARM, params, urgent=True)

    # === Full swath transfer ===

    def send_swath(
        self,
        swath_id: int,
        lines: List[bytes],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> CommandResponse:
        """Send a complete swath (begin + lines + end).

        Held under io_lock so the whole begin/data/end sequence is atomic with
        respect to an arm or control command issued from another thread (e.g.
        the print pipeline streaming the next swath while one is being armed).
        """
        with self.io_lock:
            resp = self.begin_swath(swath_id, len(lines))
            if not resp.success:
                return resp

            sent = self.send_lines(lines, progress_callback)
            if sent != len(lines):
                # No firmware-side cleanup here: ABORT would latch the DAC
                # for what is a transfer failure, not an
                # emergency. The half-received slot is cleaned by the RESET of
                # the recovery path (fault latch -> host abort/reset).
                return CommandResponse(
                    success=False,
                    error_msg=f"Only {sent}/{len(lines)} lines were sent",
                )

            resp = self.end_swath()
            # The firmware discards a short swath (never marks it READY) and
            # reports complete=0. Treat that as a failed transfer so the pipeline
            # never arms a truncated slot; it latches a fault instead.
            if resp.success and resp.data:
                parsed = parse_end_swath_response(resp.data)
                if parsed and not parsed['complete']:
                    return CommandResponse(
                        success=False,
                        error_msg=(f"firmware received swath {swath_id} incomplete "
                                   "(short line count); slot discarded"),
                    )
            return resp

    def get_current_swath(self) -> Optional[SwathInfo]:
        """Return the swath currently being sent, if any."""
        with self._lock:
            return self._current_swath

    # === Control commands ===

    def purge(self, channel: int, pulses: int) -> CommandResponse:
        """Run a purge cycle.

        Args:
            channel: Channel to purge.
            pulses: Number of purge cycles.
        """
        params = bytes([channel & 0xFF, pulses & 0xFF])
        return self._send_command(Command.PURGE, params)

    # Note: SET_TIMING/GET_TIMING and SET_DAC_POWER were retired in protocol
    # 2.4: the firing interval travels inside each ARM (print_swath), and the
    # engine owns the DAC per swath/purge.
