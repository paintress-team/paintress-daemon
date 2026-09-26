# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""
TCP Daemon - NDJSON server for printer control

The daemon loads job files directly and sends swath data to the firmware.
Serial connection is established via 'connect' command (not automatic).
Clients send commands to control the printing process.
"""

import asyncio
import concurrent.futures
import json
import logging
import queue
import threading
import time
from typing import Callable, Optional, Set, Dict, List

from . import head_profiles
from . import paintress_job as pjob
from ._console import (
    CYAN, GREEN, YELLOW, MAGENTA, BLUE, RED, RESET, BOLD, DIM,
    _format_bytes_hex, _format_timestamp,
)
from .commands import CommandHandlersMixin
from .loaded_job import LoadedJob
from .print_pipeline import PrintPipeline, ArmOutcome
from .protocol import ErrorCode, Event, SlotState, WIRE_ID
from .serial_manager import SerialManager
from .swath_handler import SwathHandler


logger = logging.getLogger(__name__)


class _CommandItem:
    """One unit of work for the cmd-worker thread.

    `future` is a concurrent.futures.Future; the asyncio side wraps it
    (`asyncio.wrap_future`) to wait on it. `started_at` stays None while the
    item is queued: the watchdog timeout message uses it to distinguish
    "never started (queued behind another command)" from "started and stuck".
    """

    __slots__ = ('label', 'fn', 'future', 'started_at')

    def __init__(self, label: str, fn: Callable[[], dict]):
        self.label = label
        self.fn = fn
        self.future: concurrent.futures.Future = concurrent.futures.Future()
        self.started_at: Optional[float] = None


# Posted to the command queue to make the worker thread exit.
_WORKER_STOP = object()


class TCPDaemon(CommandHandlersMixin):
    """TCP Daemon with NDJSON protocol and comprehensive logging."""
    
    VERSION = "0.1"  # tracks the Paintress version
    
    def __init__(
        self,
        baudrate: int = 2000000,
        host: str = '127.0.0.1',
        port: int = 9000,
        head: str = head_profiles.DEFAULT_HEAD,
    ):
        # host defaults to loopback on purpose: the NDJSON protocol has no
        # authentication, and its commands fire ink, reboot the board and
        # read job files. Exposing it beyond localhost is an explicit,
        # trusted-network-only decision (pass a reachable bind address).
        self.baudrate = baudrate
        self.host = host
        self.port = port

        # Which printhead is bolted to this machine. The firmware cannot report
        # it (one build serves every head sharing the frame) so it is
        # configuration here, and load_job refuses a job packed for another.
        self.head = head_profiles.head(head)
        
        # Serial port (set via connect command)
        self.serial_port: Optional[str] = None
        
        self._running = False
        self._stopping = False  # stop() ran once (it must be idempotent)
        self._server: Optional[asyncio.Server] = None
        self._clients: Set[asyncio.StreamWriter] = set()
        self._clients_lock = asyncio.Lock()
        
        # Serial and handler (initially disconnected)
        self.serial: Optional[SerialManager] = None
        self.handler: Optional[SwathHandler] = None

        # Firmware identity from the IDENTIFY handshake (set on connect).
        self.firmware_identity: Optional[dict] = None

        # Event loop for synchronous callbacks
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
        # Loaded job
        self.loaded_job: Optional[LoadedJob] = None

        # Double-buffer streaming pipeline (built per loaded job once connected).
        self.pipeline: Optional[PrintPipeline] = None

        # Latched print fault. Set when the firmware reports PRINT_ERROR /
        # TRIGGER_TIMEOUT or the pipeline fails to stream; makes the next
        # `print` fail fast with the cause. Cleared on load_job / reset / abort.
        self._print_fault: Optional[dict] = None

        # Single-worker serialization of mutating commands. A serial device is
        # sequential: running connect/load_job/print/... concurrently on the
        # thread pool bought nothing and raced on loaded_job/pipeline/serial
        # (and let a timed-out zombie interleave with the next command). The
        # worker serializes client-initiated mutations; internal reactions
        # (_on_serial_lost, _on_event) stay event-driven and only touch
        # thread-safe structures. Read-only commands keep the pool.
        self._cmd_queue: "queue.Queue" = queue.Queue()
        self._cmd_worker_thread: Optional[threading.Thread] = None
        self._command_in_progress: Optional[str] = None

        # RESET is a chip reboot. When this daemon issues it
        # (reset/abort command, stale-slot pre-clear) the ensuing serial drop
        # is expected: _on_serial_lost must not latch a fault, and
        # _connect_serial must not auto-restart the pipeline (whoever asked
        # for the reboot restarts what it needs). Any drop OUTSIDE this window
        # is a terminal print fault: the daemon never reconnects on its own
        # (fail -> cancel, like Klipper); recovery is an explicit
        # reconnect/reset/abort from the host.
        self._expected_reboot_deadline = 0.0

        # Statistics for logging
        self._stats = {
            'tcp_commands_received': 0,
            'tcp_responses_sent': 0,
            'serial_bytes_sent': 0,
            'serial_bytes_received': 0,
            'serial_frames_received': 0,
            'jobs_loaded': 0,
            # (swath transfer counters live in handler.stats; the daemon
            # keeping a duplicate here only invited them to diverge)
            'prints_completed': 0,
            # Bounded in-place re-sends of a swath whose stream failed (the
            # pipeline retries before latching a fault). A count that keeps
            # growing means a flaky link to investigate, not a feature working.
            'stream_retries': 0,
        }
    
    async def start(self):
        """Start the daemon (TCP server only, no serial connection)."""
        self._running = True
        self._stopping = False  # a restarted instance must be stoppable again
        self._loop = asyncio.get_event_loop()
        
        logger.info(f"{BOLD}{'='*60}{RESET}")
        logger.info(f"{BOLD}TCP Daemon v{self.VERSION} starting...{RESET}")
        logger.info(f"{BOLD}{'='*60}{RESET}")
        
        # Start TCP server (no automatic serial connection)
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            limit=1024 * 1024  # 1MB buffer limit
        )
        
        addr = self._server.sockets[0].getsockname()
        logger.info(f"{GREEN}TCP server started on {addr[0]}:{addr[1]}{RESET}")
        logger.info(f"{YELLOW}Serial not connected. Use 'connect' command with port path.{RESET}")

        self._start_cmd_worker()

        # Keepalive task
        asyncio.create_task(self._keepalive_task())
    
    async def stop(self):
        """Stop the daemon (idempotent).

        Both the signal handler and the runner's finally block call this on
        shutdown; the second call must be a no-op instead of re-running the
        teardown (it double-logged "Daemon stopped"/final stats).
        """
        if self._stopping:
            return
        self._stopping = True
        self._running = False

        # Close clients
        async with self._clients_lock:
            for writer in self._clients:
                writer.close()
            self._clients.clear()

        # Close server
        if self._server:
            self._server.close()
            await self._server.wait_closed()

        # Stop the command worker (in the executor: join may take a moment if
        # a mutant is mid-flight, and this coroutine must not block the loop).
        await asyncio.get_event_loop().run_in_executor(None, self._stop_cmd_worker)

        # Disconnect serial, the SAFE way (in the executor: it blocks on the
        # ABORT/RESET ACKs and the pipeline join). Killing the process must
        # not leave the board armed or firing with no host attached.
        if self.serial:
            await asyncio.get_event_loop().run_in_executor(
                None, self._disconnect_serial)

        logger.info(f"{BOLD}Daemon stopped{RESET}")
        logger.info(f"Final stats: {self._stats}")

    # --- command worker ------------------------------------------------------

    def _start_cmd_worker(self) -> None:
        """Start the single thread that executes mutating commands in order."""
        if self._cmd_worker_thread is not None and self._cmd_worker_thread.is_alive():
            return
        self._cmd_worker_thread = threading.Thread(
            target=self._cmd_worker_loop, name="cmd-worker", daemon=True
        )
        self._cmd_worker_thread.start()

    def _stop_cmd_worker(self) -> None:
        """Post the stop sentinel and join the worker (bounded: a wedged
        zombie command must not hang shutdown forever)."""
        worker = self._cmd_worker_thread
        if worker is None or not worker.is_alive():
            return
        self._cmd_queue.put(_WORKER_STOP)
        worker.join(timeout=5.0)
        if worker.is_alive():
            logger.warning(f"{YELLOW}cmd-worker did not exit within 5s "
                           f"(stuck on '{self._command_in_progress}'){RESET}")
        self._cmd_worker_thread = None

    def _cmd_worker_loop(self) -> None:
        """Run queued mutating commands one at a time, in submission order.

        There is no cancellation: a command that outlives its watchdog keeps
        running here to completion (its result is discarded by the client
        side), but it can no longer *interleave* with the next command: the
        worker only picks up the next item when the current one finishes.
        """
        while True:
            item = self._cmd_queue.get()
            if item is _WORKER_STOP:
                return
            item.started_at = time.monotonic()
            self._command_in_progress = item.label
            try:
                result = item.fn()
            except Exception as exc:
                logger.error(f"{RED}[CMD] Error executing {item.label}: {exc}{RESET}")
                import traceback
                traceback.print_exc()
                result = {'success': False, 'error': 'internal_error',
                          'message': str(exc)}
            finally:
                self._command_in_progress = None
            try:
                item.future.set_result(result)
            except concurrent.futures.InvalidStateError:
                pass  # waiter gave up and cancelled; result discarded

    def _submit_to_worker(self, label: str, fn: Callable[[], dict]) -> _CommandItem:
        """Queue a mutating operation for the cmd-worker; returns its item.

        Fallback: with no worker running (unit tests driving handlers
        directly, or shutdown) the operation runs inline, same semantics as
        the pre-worker daemon, minus the serialization.
        """
        item = _CommandItem(label, fn)
        if self._cmd_worker_thread is None or not self._cmd_worker_thread.is_alive():
            logger.debug(f"cmd-worker not running; executing {label} inline")
            item.started_at = time.monotonic()
            try:
                item.future.set_result(fn())
            except Exception as exc:
                item.future.set_exception(exc)
            return item
        self._cmd_queue.put(item)
        return item

    def _connect_serial(self, port: str) -> bool:
        """Connect to serial port (blocking, called from thread pool)."""
        try:
            # Disconnect existing connection
            if self.serial:
                self.serial.disconnect()
                self.serial = None
                self.handler = None
            
            self.serial = SerialManager(
                port=port,
                baudrate=self.baudrate
            )
            
            if not self.serial.connect():
                logger.warning(f"{YELLOW}Could not connect to {port}{RESET}")
                self.serial = None
                return False
            
            self.serial_port = port
            self.handler = SwathHandler(self.serial)

            # Callbacks
            self.serial.set_frame_callback(self._on_frame_received)
            self.serial.set_raw_callback(self._on_raw_data)
            self.serial.set_disconnect_callback(self._on_serial_lost)
            self.handler.set_event_callback(self._on_event)
            self.handler.set_log_callback(self._on_firmware_log_frame)

            # IDENTIFY handshake: refuse a firmware whose wire-protocol major
            # version does not match this daemon's generated bindings.
            identity = self.handler.identify()
            if identity is None:
                logger.warning(f"{YELLOW}Firmware did not answer IDENTIFY{RESET}")
                self.firmware_identity = None
            else:
                self.firmware_identity = identity
                if (identity['wire_id'] >> 8) != (WIRE_ID >> 8):
                    logger.error(
                        f"{RED}Wire protocol mismatch: firmware wire_id="
                        f"0x{identity['wire_id']:04X}, daemon=0x{WIRE_ID:04X}{RESET}"
                    )
                    # transport_only: do not command a board whose protocol
                    # version we just refused to speak.
                    self._disconnect_serial(transport_only=True)
                    return False
                logger.info(
                    f"{GREEN}Firmware identity: wire_id=0x{identity['wire_id']:04X} "
                    f"profile_hash=0x{identity['profile_hash']:08X} "
                    f"fw_build=0x{identity['fw_build']:08X}{RESET}"
                )
                boot_flags = identity.get('boot_flags', 0)
                if boot_flags & 2:
                    # bit1: watchdog reboot WITHOUT the commanded-reset
                    # sentinel: the hardware watchdog recovered a wedged core.
                    logger.warning(f"{RED}Firmware boot came from a WEDGE "
                                   f"recovery: the hardware watchdog rebooted "
                                   f"the board on its own{RESET}")
                elif boot_flags & 1:
                    # bit0 alone: a commanded RESET (ours, or another host's).
                    logger.info(f"{YELLOW}Firmware boot came from a commanded "
                                f"reset (watchdog reboot){RESET}")

            expected_reboot = time.monotonic() <= self._expected_reboot_deadline
            if expected_reboot:
                # Reconnect after OUR OWN reset/reboot: the caller sitting in
                # _reboot_firmware_and_wait owns this connect and restarts
                # what it needs; auto-starting the pipeline here would
                # re-stream the job under its feet.
                pass
            elif self.loaded_job is not None:
                # If a job was loaded before connecting, start streaming it
                # now so both slots are filled regardless of connect/open
                # ordering, but never stream a job packed for another head:
                # load_job could only check the fingerprint if a firmware was
                # already identified, so the load-then-connect order lands
                # here unchecked. (The firmware would silently drop every
                # wrong-sized DATA line and the stream would die late, with a
                # misleading "incomplete" error.)
                if self._job_matches_firmware():
                    self._start_pipeline(self.loaded_job)

            logger.info(f"{GREEN}Connected to {port} @ {self.baudrate} baud{RESET}")
            return True

        except Exception as e:
            logger.error(f"{RED}Error connecting to serial: {e}{RESET}")
            import traceback
            traceback.print_exc()
            self.serial = None
            self.handler = None
            return False
    
    def _job_matches_firmware(self) -> bool:
        """True when the loaded job's geometry matches the identified firmware.

        The same agreement `load_job` enforces, re-checked for the
        load-then-connect order. On a mismatch the fault is latched with the
        real cause (the next `print` fails fast with it); loading the right
        job clears it (load_job clears the fault).
        """
        if self.loaded_job is None or self.firmware_identity is None:
            return True  # nothing to disagree about
        fw_hash = self.firmware_identity.get('profile_hash')
        job_fp = self.loaded_job.metadata.get('geometry_fingerprint')
        if isinstance(job_fp, str):
            # LoadedJob.metadata is JobMetadata.to_dict(), which renders the
            # fingerprint as "0x%08X"; parse it back before comparing, or a
            # str-vs-int compare declares every job a mismatch (and the hex
            # formatting below blows up on the str).
            try:
                job_fp = int(job_fp, 0)
            except ValueError:
                job_fp = None
        if fw_hash is None or job_fp is None or job_fp == fw_hash:
            return True
        logger.error(
            f"{RED}Loaded job geometry 0x{job_fp:08X} does not match firmware "
            f"profile 0x{fw_hash:08X}; not streaming it{RESET}"
        )
        self._set_print_fault(
            'profile_mismatch',
            f'loaded job geometry 0x{job_fp:08X} does not match firmware '
            f'profile 0x{fw_hash:08X}; load a job encoded for this head')
        return False

    def _safe_stop_firmware(self) -> bool:
        """Leave the hardware safe before the link goes away.

        Closing the port does NOT stop the board: an armed swath keeps waiting
        for its trigger (up to ~10 s) and a firing swath ejects ink to its
        natural end. So a deliberate disconnect/shutdown first sends ABORT
        (electrical kill: ink stops in ms) and then RESET (chip reboot), both
        ACK-confirmed: the board comes back up clean and unlatched, idle, with
        empty slots, so the next connect finds a fresh device. The reboot's
        USB drop is marked expected so it never latches a serial_lost fault.

        Skipped (returns True) when there is nothing alive to stop: the
        port-lost path already cancelled everything. Returns False, after a
        high-visibility safety log, if the firmware did not confirm.
        """
        handler = self.handler
        if handler is None or self.serial is None \
                or not self.serial.is_connected():
            return True  # link already dead: nothing reachable to stop

        # The RESET drop below is ours: don't let it latch serial_lost.
        self._expected_reboot_deadline = time.monotonic() + 4.0
        aborted = False
        reset_ok = False
        try:
            aborted = handler.abort()
            reset_ok = handler.reset()
        except Exception:
            logger.exception("safe-stop raised")
        if not (aborted and reset_ok):
            logger.error(
                f"{RED}SAFETY: firmware did not confirm "
                f"{'ABORT' if not aborted else 'RESET'} before disconnect: "
                f"the head may still be armed or firing (armed wait is "
                f"bounded at ~10 s; a firing swath runs to its end){RESET}")
        return aborted and reset_ok

    def _disconnect_serial(self, transport_only: bool = False) -> bool:
        """Disconnect from serial port.

        By default this is a SAFE disconnect: the firmware is stopped
        (ABORT + RESET, ACK-confirmed) before the port closes, so the board
        is never left armed or firing with no host attached. transport_only
        skips that; used only where commanding the board is wrong (e.g.
        tearing down after a wire-protocol mismatch).
        """
        if self.serial:
            port = self.serial_port
            if self.pipeline:
                # Signal (no join): the urgent abort below must not wait on an
                # in-flight swath-sized stream; _stop_pipeline joins after.
                self.pipeline.request_stop()
            if not transport_only:
                self._safe_stop_firmware()
            self._stop_pipeline()  # stop streaming before the handler goes away
            self.serial.disconnect()
            self.serial = None
            self.handler = None
            self.firmware_identity = None
            logger.info(f"{YELLOW}Disconnected from {port}{RESET}")
            return True
        return False

    def _on_serial_lost(self) -> None:
        """Serial port lost (called from the SerialManager thread).

        Fail -> cancel (like Klipper): an unexpected loss is a terminal print
        fault: latch it, tell the clients, stop the pipeline, and do NOT try
        to reconnect. Recovery is an explicit reconnect/reset/abort from the
        host. The drop right after our own RESET (= chip reboot)
        is expected and latches nothing: the command sitting in
        _reboot_firmware_and_wait reconnects inline.
        Must not block (the serial thread is exiting).
        """
        expected = time.monotonic() <= self._expected_reboot_deadline
        if expected:
            logger.info(f"{YELLOW}Serial dropped for a firmware reboot (expected){RESET}")
        else:
            logger.error(f"{RED}Serial link lost - print cancelled; reconnect "
                         f"explicitly to recover{RESET}")
        self.handler = None
        # Signal the pipeline WITHOUT joining: this runs on the exiting serial
        # RX thread and the worker may be stuck failing out of an in-flight
        # transfer (up to several seconds). request_stop() wakes request_print
        # immediately; the joining stop() happens later on the cmd-worker,
        # inside the next _start_pipeline/_stop_pipeline.
        if self.pipeline:
            self.pipeline.request_stop()
        if not expected:
            self._set_print_fault('serial_lost',
                                  'serial link to the controller board was lost')
            if self._loop:
                asyncio.run_coroutine_threadsafe(
                    self._broadcast({
                        'type': 'event', 'event': 'serial_lost',
                        'error': 'serial_lost',
                        'message': 'serial link to the controller board was '
                                   'lost - print cancelled',
                        'timestamp': time.time(),
                    }),
                    self._loop,
                )

    def _on_frame_received(self, frame):
        """Callback for received frames (called from serial thread)."""
        self._stats['serial_frames_received'] += 1
        
        frame_hex = _format_bytes_hex(frame) if isinstance(frame, bytes) else str(frame)
        logger.debug(f"{MAGENTA}[SERIAL RX FRAME] {_format_timestamp()} len={len(frame) if isinstance(frame, bytes) else '?'} data={frame_hex}{RESET}")
        
        if self.handler:
            self.handler.handle_frame(frame)
    
    def _on_raw_data(self, data: bytes):
        """Callback for raw firmware data (printf)."""
        self._stats['serial_bytes_received'] += len(data)
        
        try:
            text = data.decode('utf-8', errors='replace').rstrip()
            if text:
                logger.debug(f"{CYAN}[SERIAL RX RAW] {_format_timestamp()} len={len(data)} hex={_format_bytes_hex(data)}{RESET}")
                print(f"{CYAN}[FW] {text}{RESET}")
                
                if self._loop:
                    asyncio.run_coroutine_threadsafe(
                        self._broadcast_firmware_log(text),
                        self._loop
                    )
        except Exception as e:
            logger.debug(f"Error processing raw data: {e}")
    
    def _on_firmware_log_frame(self, level: int, text: str):
        """Callback for framed firmware LOG messages (from the serial thread)."""
        if text and self._loop:
            asyncio.run_coroutine_threadsafe(
                self._broadcast_firmware_log(text),
                self._loop,
            )

    async def _broadcast_firmware_log(self, text: str):
        """Send firmware log to all clients."""
        msg = {
            'type': 'firmware_log',
            'text': text,
            'timestamp': time.time()
        }
        await self._broadcast(msg)
    
    def _on_event(self, event: Event, data: dict):
        """Callback for firmware events."""
        logger.info(f"{MAGENTA}[SERIAL EVENT] {_format_timestamp()} event={event.name} data={data}{RESET}")

        if event == Event.PRINT_COMPLETE:
            self._stats['prints_completed'] += 1

        # The firmware frees a slot on any print end (complete, error or
        # trigger timeout); let the pipeline stream the next swath into it.
        swath_id = None
        if isinstance(data, (bytes, bytearray)) and len(data) >= 2:
            swath_id = data[0] | (data[1] << 8)
        if event in (Event.PRINT_COMPLETE, Event.PRINT_ERROR, Event.TRIGGER_TIMEOUT):
            if self.pipeline and swath_id is not None:
                self.pipeline.on_print_complete(swath_id)

        # A failed swath (no trigger, or a firmware error) is a print fault:
        # latch it so the next `print` fails fast with the cause.
        error_name = None
        if event == Event.PRINT_ERROR:
            # One firmware producer: an ARM that was already queued to core 1
            # when a DAC kill (ABORT/RESET/safety) landed is refused at
            # power-up and reported here with code DAC_LATCHED; nothing
            # fired. (Nothing interrupts a *started* print: ABORT runs dry,
            # RESET reboots.) Latch whatever arrives.
            code = data[2] if isinstance(data, (bytes, bytearray)) and len(data) >= 3 else None
            try:
                error_name = ErrorCode(code).name.lower() if code is not None else None
            except ValueError:
                error_name = f'0x{code:02X}'
            self._set_print_fault(
                'print_error',
                f'firmware PRINT_ERROR on swath {swath_id} (code {error_name})')
        elif event == Event.TRIGGER_TIMEOUT:
            self._set_print_fault('trigger_timeout',
                                  f'swath {swath_id} got no start trigger (TRIGGER_TIMEOUT)')

        if self._loop:
            # JSON-safe payload: the raw event bytes go out as hex, alongside
            # the parsed fields (json.dumps cannot serialize bytes).
            extra = {'swath_id': swath_id}
            if isinstance(data, (bytes, bytearray)):
                extra['data'] = data.hex()
            elif data:
                extra['data'] = data
            if error_name is not None:
                extra['error'] = error_name
            asyncio.run_coroutine_threadsafe(
                self._broadcast_event(event, extra),
                self._loop
            )

    async def _broadcast_event(self, event: Event, extra: dict):
        """Send event to all clients (`extra` must be JSON-serializable)."""
        msg = {
            'type': 'event',
            'event': event.name.lower(),
            'timestamp': time.time(),
            **extra,
        }
        await self._broadcast(msg)
    
    async def _broadcast(self, msg: dict):
        """Send message to all clients."""
        line = json.dumps(msg) + '\n'
        data = line.encode()
        
        async with self._clients_lock:
            dead = []
            for writer in self._clients:
                try:
                    writer.write(data)
                    await writer.drain()
                except:
                    dead.append(writer)
            
            for w in dead:
                self._clients.discard(w)
    
    async def _keepalive_task(self):
        """Send periodic keepalive."""
        while self._running:
            await asyncio.sleep(30)
            msg = {'type': 'keepalive', 'timestamp': time.time()}
            await self._broadcast(msg)
    
    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Process client connection."""
        addr = writer.get_extra_info('peername')
        logger.info(f"{GREEN}[TCP] Client connected: {addr}{RESET}")
        
        async with self._clients_lock:
            self._clients.add(writer)
        
        # Welcome message
        welcome = {
            'type': 'welcome',
            'version': self.VERSION,
            # Same truth as `status`: the handler lives exactly as long as the
            # link is usable (pyserial's is_open lies after an unplug).
            'device_connected': self.handler is not None,
            'serial_port': self.serial_port,
            'job_loaded': self.loaded_job is not None,
            'timestamp': time.time()
        }
        await self._send(writer, welcome)
        logger.debug(f"{BLUE}[TCP TX] {_format_timestamp()} -> {addr} WELCOME{RESET}")
        
        try:
            while self._running:
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=300.0)
                except asyncio.TimeoutError:
                    continue
                
                if not line:
                    logger.debug(f"[TCP] Connection closed by client {addr}")
                    break
                
                self._stats['tcp_commands_received'] += 1
                logger.debug(f"{GREEN}[TCP RX] {_format_timestamp()} <- {addr} raw_len={len(line)} data={line[:200]!r}{RESET}")
                
                try:
                    msg = json.loads(line.decode().strip())
                    self._log_received_command(addr, msg)
                    
                    response = await self._process_message(msg)
                    if response:
                        await self._send(writer, response)
                        self._stats['tcp_responses_sent'] += 1
                        self._log_sent_response(addr, response)
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"{YELLOW}[TCP] Invalid JSON from {addr}: {e}{RESET}")
                    error_response = {
                        'type': 'error',
                        'error': 'invalid_json',
                        'timestamp': time.time()
                    }
                    await self._send(writer, error_response)
                    self._log_sent_response(addr, error_response)
                    
                except Exception as e:
                    logger.error(f"{RED}[TCP] Error processing message from {addr}: {e}{RESET}")
                    error_response = {
                        'type': 'error',
                        'error': 'processing_error',
                        'message': str(e),
                        'timestamp': time.time()
                    }
                    await self._send(writer, error_response)
                    self._log_sent_response(addr, error_response)
                    
        except asyncio.CancelledError:
            pass
        except ConnectionResetError:
            logger.info(f"{YELLOW}[TCP] Client {addr} disconnected abruptly{RESET}")
        except Exception as e:
            logger.error(f"{RED}[TCP] Error with client {addr}: {e}{RESET}")
        finally:
            async with self._clients_lock:
                self._clients.discard(writer)
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except Exception:
                pass
            logger.info(f"{YELLOW}[TCP] Client disconnected: {addr}{RESET}")
    
    def _log_received_command(self, addr, msg: dict):
        """Log received TCP command with appropriate detail level."""
        cmd = msg.get('cmd', 'unknown')
        msg_id = msg.get('id', '-')
        
        if cmd == 'connect':
            port = msg.get('port', '?')
            logger.info(f"{GREEN}[TCP RX CMD] {_format_timestamp()} <- {addr} "
                       f"cmd={cmd} id={msg_id} port={port}{RESET}")
        elif cmd == 'load_job':
            filepath = msg.get('filepath', '?')
            logger.info(f"{GREEN}[TCP RX CMD] {_format_timestamp()} <- {addr} "
                       f"cmd={cmd} id={msg_id} filepath={filepath}{RESET}")
        elif cmd == 'print':
            swath_id = msg.get('swath_id', '?')
            line_delay = msg.get('line_delay_us', '?')
            logger.info(f"{GREEN}[TCP RX CMD] {_format_timestamp()} <- {addr} "
                       f"cmd={cmd} id={msg_id} swath_id={swath_id} "
                       f"line_delay={line_delay}us{RESET}")
        elif cmd == 'purge':
            pulses = msg.get('pulses')
            channel = msg.get('channel')
            logger.info(f"{GREEN}[TCP RX CMD] {_format_timestamp()} <- {addr} "
                       f"cmd={cmd} id={msg_id} channel={channel} pulses={pulses}{RESET}")

        else:
            log_msg = {k: v for k, v in msg.items()}
            logger.info(f"{GREEN}[TCP RX CMD] {_format_timestamp()} <- {addr} "
                       f"cmd={cmd} id={msg_id} params={log_msg}{RESET}")
    
    def _log_sent_response(self, addr, response: dict):
        """Log sent TCP response."""
        resp_type = response.get('type', 'unknown')
        cmd = response.get('cmd', '-')
        msg_id = response.get('id', '-')
        success = response.get('success', '-')
        
        if resp_type == 'response':
            if success:
                summary_keys = ['swath_id', 'lines_sent', 'filepath', 'swath_count', 'port']
                summary = {k: response[k] for k in summary_keys if k in response}
                logger.info(f"{BLUE}[TCP TX RSP] {_format_timestamp()} -> {addr} "
                           f"cmd={cmd} id={msg_id} success=True {summary}{RESET}")
            else:
                error = response.get('error', 'unknown')
                error_msg = response.get('message', '')
                logger.warning(f"{YELLOW}[TCP TX RSP] {_format_timestamp()} -> {addr} "
                              f"cmd={cmd} id={msg_id} success=False error={error} msg={error_msg}{RESET}")
        elif resp_type == 'error':
            error = response.get('error', 'unknown')
            logger.warning(f"{YELLOW}[TCP TX RSP] {_format_timestamp()} -> {addr} "
                          f"type=error error={error}{RESET}")
        else:
            logger.debug(f"{BLUE}[TCP TX] {_format_timestamp()} -> {addr} "
                        f"type={resp_type}{RESET}")
    
    async def _send(self, writer: asyncio.StreamWriter, msg: dict):
        """Send message to a client."""
        try:
            line = json.dumps(msg) + '\n'
            writer.write(line.encode())
            await writer.drain()
        except:
            pass
    
    async def _process_message(self, msg: dict) -> Optional[dict]:
        """Process client message."""
        cmd = msg.get('cmd')
        msg_id = msg.get('id')
        
        if not cmd:
            return {
                'type': 'error',
                'error': 'missing_cmd',
                'id': msg_id,
                'timestamp': time.time()
            }
        
        # Timeout based on command type. This is a *watchdog*, not a hard
        # cancel: every underlying operation carries its own bounded timeout
        # (serial commands, request_print, TX drains), so expiry here means
        # something is genuinely stuck or oversized.
        timeout = 30.0
        if cmd in ('status', 'get_status', 'disconnect'):
            timeout = 10.0
        # connect/reconnect stay in the 30 s bucket: with a job loaded they
        # start the pipeline, whose stale-slot pre-clear can ride a firmware
        # reboot (up to the 8 s bounded wait) before the command returns.
        elif cmd == 'load_job':
            # Data-sized operation: reading a whole job from disk. Matches the
            # client's own 300 s allowance (a big job used to time out here at
            # 30 s while the client waited 300 s).
            timeout = 300.0
        # reset/abort stay in the default 30 s bucket: they now ride a chip
        # reboot + reconnect (~1-2 s typical, 8 s bounded wait inside).

        # Route by classification: mutating commands serialize on the single
        # cmd-worker (submission order preserved, no interleaving with a
        # timed-out zombie); read-only commands run on the thread pool, so
        # `status` keeps answering while a mutant is in flight. Neither can be
        # cancelled: shield the future so a watchdog expiry does not cancel it
        # mid-flight, and log its late completion instead of letting a zombie
        # finish (and mutate state) invisibly after the client was already
        # told 'timeout'. The watchdog counts from submission either way.
        entry = self._COMMANDS.get(cmd)
        item = None
        if entry is not None and entry[2]:  # mutating
            item = self._submit_to_worker(
                cmd, lambda: self._execute_command(cmd, msg))
            future = asyncio.wrap_future(item.future)
        else:
            future = asyncio.get_event_loop().run_in_executor(
                None,
                self._execute_command,
                cmd,
                msg
            )
        try:
            result = await asyncio.wait_for(asyncio.shield(future), timeout=timeout)

            response = {
                'type': 'response',
                'cmd': cmd,
                'id': msg_id,
                'timestamp': time.time(),
                **result
            }
            return response

        except asyncio.TimeoutError:
            if item is not None and item.started_at is None:
                # Never left the queue: the watchdog expired behind another
                # mutant, not because this command wedged.
                blocker = self._command_in_progress or 'another command'
                situation = (f"never started: still queued behind '{blocker}'")
            else:
                situation = (
                    'started and did not finish; the daemon cannot cancel it: '
                    'it may still be running and complete later (its result '
                    'is discarded)'
                )
            logger.error(
                f"{RED}[CMD] Timeout executing {cmd} ({timeout:.0f}s): "
                f"{situation}{RESET}"
            )

            def _log_late_completion(fut):
                try:
                    exc = fut.exception()
                    if exc is not None:
                        outcome = f'error: {exc}'
                    else:
                        res = fut.result()
                        outcome = res.get('error') or (
                            'OK' if res.get('success') else 'FAIL')
                except Exception:
                    outcome = 'unknown'
                logger.warning(
                    f"{YELLOW}[CMD] {cmd}: completed after the timeout response "
                    f"was already sent (outcome: {outcome}; result discarded){RESET}"
                )

            future.add_done_callback(_log_late_completion)
            return {
                'type': 'response',
                'cmd': cmd,
                'id': msg_id,
                'success': False,
                'error': 'timeout',
                'message': (
                    f'Command {cmd} exceeded its {timeout:.0f}s watchdog '
                    f'({situation}). Check `status` (command_in_progress) '
                    'before retrying.'
                ),
                'timestamp': time.time()
            }
            
        except Exception as e:
            logger.error(f"{RED}[CMD] Error executing {cmd}: {e}{RESET}")
            import traceback
            traceback.print_exc()
            return {
                'type': 'response',
                'cmd': cmd,
                'id': msg_id,
                'success': False,
                'error': 'internal_error',
                'message': str(e),
                'timestamp': time.time()
            }

    def _reboot_firmware_and_wait(self, timeout: float = 8.0) -> bool:
        """Send RESET (= chip reboot) and reconnect, inline.

        The firmware ACKs and reboots ~50 ms later; the USB drop is expected
        (no fault latched; see _on_serial_lost). This command owns the whole
        round trip: wait for the drop, then reopen the port (the board
        re-enumerates in ~1-2 s) and re-IDENTIFY via _connect_serial. Returns
        True once the board is back. self.handler is a NEW object afterwards:
        callers must re-read it and re-apply anything they need (there is
        nothing to re-apply in the normal flow; timing travels with each ARM
        ).
        """
        handler = self.handler
        if handler is None:
            return False
        port = self.serial_port
        deadline = time.monotonic() + timeout
        # The window outlives the wait a little, so a drop that arrives just
        # after we give up is still recognized as ours instead of latching
        # serial_lost.
        self._expected_reboot_deadline = deadline + 4.0
        if not handler.reset():
            self._expected_reboot_deadline = 0.0
            return False

        # Wait for the USB drop (the disconnect callback clears self.handler).
        # Reconnecting before the old link dies would just reopen the
        # pre-reboot firmware and drop again mid-handshake.
        while self.handler is not None:
            if time.monotonic() >= deadline:
                logger.warning(f"{YELLOW}firmware never dropped off the bus "
                               f"within {timeout:.0f}s of the reset (reboot){RESET}")
                return False
            time.sleep(0.05)

        # Reopen the port until the board re-enumerates or the wait is spent.
        while time.monotonic() < deadline:
            time.sleep(0.5)
            try:
                if self._connect_serial(port):
                    self._expected_reboot_deadline = 0.0
                    return True
            except Exception:
                logger.exception("reconnect attempt after the reboot failed")
        logger.warning(f"{YELLOW}firmware did not come back within "
                       f"{timeout:.0f}s of the reset (reboot){RESET}")
        return False

    def _clear_stale_slots(self) -> None:
        """Empty the firmware's swath slots before streaming a fresh pipeline.

        A fresh pipeline assumes both slots are free, but the firmware may still
        hold swaths from before: a reloaded job, an abort mid-job, or a daemon
        restart against a running board. BEGIN_SWATH would then be rejected with
        NO_SLOT_AVAILABLE and kill the stream on its first swath. RESET (= chip
        reboot) is the only cleanup: it also clears the DAC latch; a latched
        board with clean slots still needs it, or every ARM would be NACKed
        DAC_LATCHED. self.handler is a new object after a reboot.
        """
        try:
            handler = self.handler
            if handler is None:
                return
            status = handler.get_status()
            if status is not None:
                slots_clean = all(
                    (status.get(k) or {}).get('state') == SlotState.EMPTY
                    for k in ('slot_a', 'slot_b')
                )
                if slots_clean and not status.get('receiving') \
                        and not status.get('printing') \
                        and not status.get('dac_latched'):
                    return
            if self._reboot_firmware_and_wait():
                logger.info(f"{YELLOW}[PIPELINE] rebooted the firmware to clear "
                            f"stale slots/latch before streaming{RESET}")
        except Exception as exc:
            # Non-fatal: if the link is really broken the stream itself will
            # fail loudly and latch a fault.
            logger.warning(f"{YELLOW}[PIPELINE] slot pre-clear failed: {exc}{RESET}")

    def _start_pipeline(self, job: LoadedJob) -> None:
        """Build and start the double-buffer streaming pipeline for a job.

        The pipeline keeps both firmware slots fed: it streams the next swath
        while one is printing, so the host only has to arm + sweep each swath.
        It always streams the whole job in order, from the first swath: a
        print is either running or cancelled; there is no mid-job restart
        (fail -> cancel; the retry-from-swath-K path was retired).
        """
        self._stop_pipeline()
        if not self.handler:
            return  # streaming starts once a job is loaded with serial connected

        # The pre-clear may reboot the firmware, replacing the
        # handler; grab it only afterwards.
        self._clear_stale_slots()
        handler = self.handler
        if handler is None:
            return

        def stream_fn(swath_id, lines):
            try:
                resp = handler.send_swath(swath_id, lines)
                if not resp.success:
                    code = (resp.error.name.lower() if hasattr(resp.error, 'name')
                            else str(resp.error) if resp.error else 'failed')
                    logger.error(
                        f"{RED}[PIPELINE] stream swath {swath_id} rejected: "
                        f"{code} ({resp.error_msg}){RESET}"
                    )
                return resp.success
            except Exception as exc:
                logger.error(f"{RED}[PIPELINE] stream swath {swath_id} failed: {exc}{RESET}")
                return False

        def arm_fn(swath_id, line_delay_us):
            try:
                resp = handler.print_swath(swath_id, line_delay_us)
                if resp.success:
                    return ArmOutcome(True)
                # Firmware rejected the arm (e.g. SWATH_NOT_READY): pass the
                # code through and dump the slot states so an intermittent
                # rejection shows exactly what the firmware held at that moment.
                code = resp.error.name.lower() if hasattr(resp.error, 'name') else str(resp.error)
                self._log_slot_states_on_arm_fail(swath_id, code)
                return ArmOutcome(False, code,
                                  resp.error_msg or f'firmware rejected arm ({code})')
            except Exception as exc:
                logger.error(f"{RED}[PIPELINE] arm swath {swath_id} failed: {exc}{RESET}")
                return ArmOutcome(False, 'arm_exception', str(exc))

        def on_retry(swath_id, attempt):
            # Loud on purpose: a retry that recovers is still a link hiccup
            # worth counting; repeated retries are a hardware problem to fix.
            self._stats['stream_retries'] += 1
            logger.warning(
                f"{YELLOW}[PIPELINE] stream swath {swath_id} failed; "
                f"retrying in place (attempt {attempt + 1}){RESET}"
            )

        swaths = [(sid, job.swaths[sid]) for sid in job.swath_ids]
        self.pipeline = PrintPipeline(stream_fn, arm_fn,
                                      on_error=self._on_pipeline_error,
                                      on_retry=on_retry)
        self.pipeline.load(swaths)
        logger.info(f"{GREEN}[PIPELINE] streaming {len(swaths)} swaths "
                    f"(from swath {swaths[0][0]}) into the double buffer{RESET}")

    def _log_slot_states_on_arm_fail(self, swath_id: int, code: str) -> None:
        """On a rejected ARM, dump the firmware's two slot states.

        An intermittent SWATH_NOT_READY is a timing/slot bug; seeing which
        swath_id and state each slot held at the failing arm is what pins it.
        """
        def _slot(s):
            if not s:
                return '?'
            st = s.get('state')
            st = st.name if hasattr(st, 'name') else st
            return f"{st}(id={s.get('swath_id')},lines={s.get('lines_received')})"

        try:
            status = self.handler.get_status() if self.handler else None
        except Exception:
            status = None

        if status:
            logger.error(
                f"{RED}[ARM FAIL] swath {swath_id} rejected ({code}); firmware slots: "
                f"A={_slot(status.get('slot_a'))} B={_slot(status.get('slot_b'))} "
                f"receiving={status.get('receiving')} printing={status.get('printing')}{RESET}"
            )
        else:
            logger.error(
                f"{RED}[ARM FAIL] swath {swath_id} rejected ({code}); "
                f"slot status unavailable{RESET}"
            )

    def _on_pipeline_error(self, swath_id: int, message: str) -> None:
        """Background streaming failed: log it, latch the fault, tell clients."""
        logger.error(f"{RED}[PIPELINE] swath {swath_id}: {message}{RESET}")
        self._set_print_fault('pipeline_error', f'swath {swath_id}: {message}')
        if self._loop:
            asyncio.run_coroutine_threadsafe(
                self._broadcast({
                    'type': 'event',
                    'event': 'pipeline_error',
                    'swath_id': swath_id,
                    'message': message,
                    'timestamp': time.time(),
                }),
                self._loop,
            )

    def _set_print_fault(self, error: str, message: str) -> None:
        """Latch a print fault so the next `print` fails fast with the cause."""
        if self._print_fault is None:  # keep the first (root) cause
            self._print_fault = {'error': error, 'message': message}
            logger.error(f"{RED}[FAULT] {error}: {message}{RESET}")

    def _clear_print_fault(self) -> None:
        self._print_fault = None

    def _stop_pipeline(self) -> None:
        if self.pipeline:
            if not self.pipeline.stop():
                # The worker outlived the bounded join: it is stuck inside a
                # swath-sized stream. Its load generation is now stale so it
                # cannot corrupt state, but it may hold the serial io_lock
                # until that stream fails or finishes; say so instead of
                # dropping the reference silently.
                logger.warning(
                    f"{YELLOW}[PIPELINE] worker still draining an in-flight "
                    f"stream after stop; it will exit when the transfer "
                    f"fails/finishes{RESET}")
            self.pipeline = None
