# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""TCP command handlers, mixed into the daemon (TCPDaemon).

Each ``_cmd_*`` returns a result dict; ``_execute_command`` is the declarative
router (see ``_COMMANDS``). Split out of server.py so the transport and
orchestration (TCPDaemon) stay separate from the command handling. These methods
run on the TCPDaemon instance, so ``self`` is the full daemon (serial, handler,
pipeline, loaded_job, stats, ...).
"""

import logging
import time

from . import paintress_job as pjob
from .loaded_job import LoadedJob
from .protocol import SlotState
from ._console import GREEN, YELLOW, RED, RESET

logger = logging.getLogger(__name__)


class CommandHandlersMixin:
    # Command table: name -> (handler method, requires a serial connection,
    # mutates daemon state).

    # send_swath / set_timing / get_timing / dac_power were retired with
    # the pipeline owns streaming (manual/debug pokes go through
    # hw_tests/, raw serial), the firing interval travels inside each ARM
    # (`print` carries line_delay_us), and the engine owns the DAC.
    #
    # The `mutating` flag routes the command through the single cmd-worker
    # thread (see server._cmd_worker_loop): a serial device is sequential, so
    # concurrent mutations buy nothing and race on loaded_job/pipeline/serial.
    # Read-only commands keep the thread pool; `status` especially must never
    # queue behind a mutant: the plugin polls it *during* a print.
    _COMMANDS = {
        "status":     ("_cmd_daemon_status", False, False),
        "connect":    ("_cmd_connect", False, True),
        "disconnect": ("_cmd_disconnect", False, True),
        "reconnect":  ("_cmd_reconnect", False, True),
        "load_job":   ("_cmd_load_job", False, True),
        "unload_job": ("_cmd_unload_job", False, True),
        "job_info":   ("_cmd_job_info", False, False),
        "reset":      ("_cmd_reset", True, True),
        "abort":      ("_cmd_abort", True, True),
        "get_status": ("_cmd_get_status", True, False),
        "identify":   ("_cmd_identify", True, False),
        "print":      ("_cmd_print", True, True),
        "purge":      ("_cmd_purge", True, True),
    }

    def _execute_command(self, cmd: str, msg: dict) -> dict:
        """Run one command (on the cmd-worker for mutants, on the thread pool
        for read-only): look it up, enforce the serial requirement, time it
        and log the outcome."""
        entry = self._COMMANDS.get(cmd)
        if entry is None:
            logger.warning(f"{YELLOW}[CMD] unknown_command: {cmd}{RESET}")
            return {"success": False, "error": "unknown_command"}

        handler_name, requires_serial, _ = entry
        if requires_serial and not self.handler:
            logger.warning(f"{YELLOW}[CMD] {cmd}: device_not_connected{RESET}")
            return {
                "success": False,
                "error": "device_not_connected",
                "message": 'Use the "connect" command first to open the serial device',
            }

        start = time.perf_counter()
        result = getattr(self, handler_name)(msg)
        elapsed = (time.perf_counter() - start) * 1000
        outcome = "OK" if result.get("success") else (result.get("error") or "FAIL")
        logger.info(f"[CMD] {cmd}: {outcome} ({elapsed:.2f}ms)")
        return result

    # === Command Handlers ===

    def _cmd_daemon_status(self, msg: dict = None) -> dict:
        """Daemon status."""
        # Snapshots: status runs on the thread pool while the cmd-worker (or a
        # serial loss) may be clearing these attributes; same rule as the
        # serial-command handlers.
        serial = self.serial
        handler = self.handler
        status = {
            'version': self.VERSION,
            # The handler lives exactly as long as the link is usable: created
            # on connect, cleared by _on_serial_lost/disconnect. (pyserial's
            # own is_open stays True after an unplug, so it must not be used.)
            'device_connected': handler is not None,
            'serial_port': self.serial_port,
            'baudrate': self.baudrate,
            'daemon_stats': self._stats.copy(),
            'job_loaded': self.loaded_job is not None,
            # Latched print fault (None when clean) so a host can poll for a
            # mid-print failure right after a sweep, not only on the next arm.
            'print_fault': self._print_fault,
            # Mutating command the cmd-worker is executing right now (None when
            # idle). Makes the "check `status` before retrying" advice of a
            # timeout response actionable: a client can see whether its command
            # is still running or the queue is stuck behind another one.
            'command_in_progress': self._command_in_progress,
        }
        
        loaded_job = self.loaded_job
        if loaded_job:
            status['job_filepath'] = loaded_job.filepath
            status['job_swath_count'] = len(loaded_job.swaths)

        if serial:
            status['serial_stats'] = serial.stats.copy()
            status['tx_queue_size'] = serial.get_tx_queue_size()

        if handler:
            status['handler_stats'] = handler.stats.copy()
            current = handler.get_current_swath()
            if current:
                status['current_swath'] = {
                    'swath_id': current.swath_id,
                    'line_count': current.line_count,
                    'lines_sent': current.lines_sent,
                    'progress': current.progress
                }
        
        return {'success': True, 'data': status}

    def _cmd_connect(self, msg: dict) -> dict:
        """Connect to serial device."""
        port = msg.get('port')
        
        if not port:
            return {'success': False, 'error': 'missing_port',
                    'message': 'Requires port parameter (e.g., /dev/ttyACM0 or COM3)'}
        
        # Check if already connected to this port. "Connected" means the link
        # is USABLE (handler alive, manager not lost), NOT merely "the port
        # was opened once": after a USB unplug pyserial's is_open stays True
        # (only close() clears it), and trusting it here made a post-loss
        # `connect` return a fake "Already connected" without reconnecting.
        already_open = (self.handler is not None
                        and self.serial is not None
                        and self.serial.is_connected())
        if already_open and self.serial_port == port:
            return {'success': True, 'port': port,
                    'message': 'Already connected to this port'}
        if self.serial:
            # Different port, or a dead/lost manager: tear it down first. The
            # same-port case is a re-attach (dead manager after a USB pull),
            # transport-only, like reconnect: a safe-stop RESET would knock
            # the board off the bus for the reopen that follows. Moving to a
            # different port abandons the old board → full safe stop.
            self._disconnect_serial(transport_only=(port == self.serial_port))
        
        if self._connect_serial(port):
            return {'success': True, 'port': port, 'baudrate': self.baudrate}
        
        return {'success': False, 'error': 'connection_failed',
                'message': f'Could not connect to {port}'}

    def _cmd_disconnect(self, msg: dict = None) -> dict:
        """Disconnect from serial device.

        SAFE by default: the firmware is stopped first (ABORT + RESET,
        ACK-confirmed) so the board is never left armed or firing with no
        host attached: closing the port alone stops nothing on the device.
        Pass transport_only=true to skip the stop and just close the port
        (e.g. to hand the port to another tool without disturbing the board).
        """
        if not self.serial:
            return {'success': True, 'message': 'Not connected'}

        transport_only = bool(msg.get('transport_only')) if msg else False
        port = self.serial_port
        self._disconnect_serial(transport_only=transport_only)
        return {'success': True, 'disconnected_from': port,
                'transport_only': transport_only}

    def _cmd_reconnect(self, msg: dict) -> dict:
        """Reconnect to serial device (optionally with new port)."""
        port = msg.get('port', self.serial_port)

        if not port:
            return {'success': False, 'error': 'no_port',
                    'message': 'No port specified and no previous connection'}

        if self.serial:
            # Same port: transport-only teardown; we are re-attaching to this
            # board immediately, and the safe-stop's RESET would knock it off
            # the bus for the very reopen that follows. A different port
            # abandons the old board, so that one gets the full safe stop.
            self._disconnect_serial(transport_only=(port == self.serial_port))
        
        if self._connect_serial(port):
            return {'success': True, 'port': port, 'baudrate': self.baudrate}
        
        return {'success': False, 'error': 'connection_failed',
                'message': f'Could not reconnect to {port}'}

    def _cmd_load_job(self, msg: dict) -> dict:
        """Load a job: a versioned JSON header plus its packed binary sidecar.

        The format is defined by the shared :mod:`paintress_job` module
        (``<name>.json`` + ``<name>.bin``). ``filepath`` is the header path; the
        sidecar is read straight from disk, pass by pass, into ``LoadedJob``.
        """
        filepath = msg.get('filepath')
        if not filepath:
            return {'success': False, 'error': 'missing_filepath'}

        try:
            loaded = pjob.load_job(filepath)
            # Internal consistency only; the firmware-geometry agreement is the
            # profile_hash / geometry_fingerprint check below (a job packed for
            # another head is rejected there, not against a daemon-local size).
            pjob.validate(loaded)
        except FileNotFoundError:
            return {
                'success': False,
                'error': 'file_not_found',
                'message': f'File not found: {filepath}',
            }
        except pjob.JobError as e:
            return {
                'success': False,
                'error': 'invalid_job',
                'message': str(e),
            }
        except Exception as e:
            logger.error(f'Error loading job: {e}')
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': 'load_error',
                'message': str(e),
            }

        meta = loaded.metadata

        # Two checks, against two different authorities.
        #
        # The FRAME is the firmware's to vouch for: the line size, clocks and
        # packing it shifts out. If a firmware identity is known, refuse a job
        # whose frame disagrees.
        if self.firmware_identity is not None:
            fw_hash = self.firmware_identity.get('profile_hash')
            if fw_hash is not None and meta.geometry_fingerprint != fw_hash:
                return {
                    'success': False,
                    'error': 'profile_mismatch',
                    'message': (
                        f'Job frame 0x{meta.geometry_fingerprint:08X} does not '
                        f'match firmware frame 0x{fw_hash:08X}'
                    ),
                }

        # Which HEAD is fitted is ours: one firmware build serves every head
        # that shares the frame, so the board cannot tell them apart and this
        # is the only place the mismatch can be caught. Without it, a job for
        # another head would stream happily and print nonsense.
        if meta.head_fingerprint and meta.head_fingerprint != self.head.head_fingerprint:
            return {
                'success': False,
                'error': 'head_mismatch',
                'message': (
                    f'Job is packed for head {meta.head_name or "?"} '
                    f'(0x{meta.head_fingerprint:08X}) but this machine is '
                    f'configured for {self.head.name} '
                    f'(0x{self.head.head_fingerprint:08X}). Re-RIP for '
                    f'{self.head.name}, or start the daemon with --head '
                    f'{meta.head_name or "<head>"} if the head was swapped.'
                ),
            }

        job = LoadedJob(filepath=filepath)
        job.metadata = meta.to_dict()
        job.y_positions = [p.y_position_mm for p in meta.passes]
        job.y_deltas = [p.y_delta_mm for p in meta.passes]

        # Swaths are 1-indexed; read each pass's lines from the sidecar.
        for pass_idx, _ in enumerate(meta.passes):
            job.swaths[pass_idx + 1] = list(loaded.iter_pass_lines(pass_idx))

        if not job.swaths:
            return {
                'success': False,
                'error': 'no_swaths',
                'message': 'Job contains no swaths',
            }

        self.loaded_job = job
        self._stats['jobs_loaded'] += 1

        total_lines = sum(len(lines) for lines in job.swaths.values())
        logger.info(
            f"{GREEN}[JOB] Loaded {filepath}: {len(job.swaths)} swaths, "
            f"{total_lines} lines (fingerprint 0x{meta.geometry_fingerprint:08X}){RESET}"
        )

        # A fresh job is a clean slate: clear any fault from a previous run and
        # (re)start the double-buffer pipeline (pre-streams into both slots).
        self._clear_print_fault()
        self._start_pipeline(job)

        return {
            'success': True,
            'filepath': filepath,
            'swath_count': len(job.swaths),
            'swath_ids': job.swath_ids,
            'total_lines': total_lines,
            'metadata': job.metadata,
            'y_positions_mm': job.y_positions,
            'y_deltas_mm': job.y_deltas,
        }

    def _cmd_unload_job(self, msg: dict = None) -> dict:

        """Unload current job."""
        self._stop_pipeline()
        if self.loaded_job:
            filepath = self.loaded_job.filepath
            self.loaded_job = None
            logger.info(f"[JOB] Unloaded {filepath}")
            return {'success': True, 'unloaded': filepath}
        return {'success': True, 'unloaded': None}

    def _cmd_job_info(self, msg: dict = None) -> dict:
        """Get info about loaded job."""
        if not self.loaded_job:
            return {'success': False, 'error': 'no_job_loaded'}
        
        return {'success': True, 'data': self.loaded_job.get_info()}

    def _cmd_print(self, msg: dict) -> dict:
        """Arm a swath for printing.

        The daemon's double-buffer pipeline owns this: it waits until the
        swath is streamed into a slot, arms it, and keeps the next swath
        streaming while this one prints. line_delay_us (the firing-grid
        interval) is required: it travels with each ARM.
        """
        swath_id = msg.get('swath_id')
        if swath_id is None:
            return {'success': False, 'error': 'missing_swath_id'}

        # Fail fast if a fault is latched (firmware error / trigger timeout /
        # streaming failure): the root cause outranks any parameter problem.
        if self._print_fault is not None:
            return {'success': False, **self._print_fault}

        # The wire format is a u16: reject out-of-range values here with a
        # clear error instead of letting struct.pack blow up into an opaque
        # internal_error (e.g. low dpi x low print_speed derives > 65535 us).
        line_delay_us = msg.get('line_delay_us')
        if line_delay_us is None:
            return {'success': False, 'error': 'missing_params',
                    'message': 'Requires line_delay_us (the '
                               'firing interval travels with each print)'}
        if (isinstance(line_delay_us, bool)
                or not isinstance(line_delay_us, (int, float))
                or line_delay_us != int(line_delay_us)):
            return {'success': False, 'error': 'invalid_line_delay',
                    'message': f'line_delay_us must be an integer number of '
                               f'microseconds, got {line_delay_us!r}'}
        line_delay_us = int(line_delay_us)
        if not (1 <= line_delay_us <= 0xFFFF):
            return {'success': False, 'error': 'invalid_line_delay',
                    'message': f'line_delay_us must be in 1..65535 (16-bit '
                               f'wire format), got {line_delay_us}'}

        if self.loaded_job is None:
            # The direct-arm path was retired (S2): arming
            # stale slot contents was a debug-only footgun; raw pokes go
            # through hw_tests/.
            return {'success': False, 'error': 'no_job_loaded',
                    'message': 'Load a job first (`load_job`)'}

        # Fail fast on a swath the job does not contain, instead of letting
        # request_print wait its full timeout for a swath that can never
        # become READY.
        if swath_id not in self.loaded_job.swaths:
            return {'success': False, 'error': 'swath_not_found',
                    'message': f'Swath {swath_id} not found in loaded job'}

        # (Re)start the streaming pipeline when the current one can no longer
        # serve this swath, but only for a print that starts over from the
        # job's first swath (after abort/reset stopped the pipeline, or after
        # the job ran to completion). There is no mid-job restart: a print is
        # either running in sequence or cancelled (fail -> cancel; the
        # retry-from-swath-K path was retired with it).
        if self.pipeline is None or not self.pipeline.can_serve(swath_id):
            first_id = self.loaded_job.swath_ids[0]
            if swath_id != first_id:
                return {'success': False, 'error': 'swath_out_of_sequence',
                        'message': (
                            f'Swath {swath_id} is not being served (the print '
                            f'was cancelled or already consumed it); restart '
                            f'the print from swath {first_id}')}
            self._start_pipeline(self.loaded_job)

        if self.pipeline:
            # Bounded wait: a swath is pre-streamed so readiness is near-instant;
            # a long block means trouble -> fail fast, well under the daemon-loop
            # and client timeouts so they never race.
            outcome = self.pipeline.request_print(swath_id, line_delay_us,
                                                  timeout=15.0)
            if outcome.ok:
                return {'success': True, 'swath_id': swath_id}
            # A latched fault (firmware PRINT_ERROR / trigger timeout / stream
            # failure) is the real root cause -> prefer it. Otherwise report the
            # specific reason the arm did not happen (daemon timeout vs the
            # firmware's own arm-reject code) instead of a generic 'not ready'.
            if self._print_fault is not None:
                return {'success': False, 'swath_id': swath_id, **self._print_fault}
            return {'success': False, 'swath_id': swath_id,
                    'error': outcome.error or 'print_timeout',
                    'message': outcome.message or f'Swath {swath_id} was not ready in time'}

        return {'success': False, 'error': 'not_connected',
                'message': 'No streaming pipeline (serial disconnected?)'}

    def _cmd_reset(self, msg: dict = None) -> dict:
        """Reset = chip reboot; returns once the board is back.

        The firmware ACKs and reboots; the daemon rides the expected serial
        drop and reconnects inline (re-IDENTIFY included). A clean boot clears
        slots, DAC latch and everything else; nothing to restore afterwards
        (the firing interval travels with each ARM).

        Signal the pipeline to stop WITHOUT joining first: the reboot must not
        wait on an in-progress ~6 s send_swath. The RESET (urgent, off the
        io_lock) reaches the wire immediately; the link then drops, the
        in-flight stream fails and, with _stop set, the worker exits without
        latching a fault. _stop_pipeline() at the end joins the spent worker.
        """
        if self.pipeline:
            self.pipeline.request_stop()
        self._clear_print_fault()
        ok = self._reboot_firmware_and_wait()
        self._stop_pipeline()
        return {'success': ok}

    def _cmd_abort(self, msg: dict = None) -> dict:
        """Emergency stop: firmware ABORT (electrical kill) then a reboot.

        The firmware ABORT is a pure kill-switch: it drops the
        DAC pins and latches the DAC off (ink stops immediately), but
        interrupts nothing else (a firing swath runs dry to its natural end).
        The follow-up RESET (= chip reboot) is the recovery:
        a clean boot clears the latch and the slots, and the daemon waits for
        the board to re-enumerate (~1-2 s) so the caller gets it back ready
        for a retry.

        Latency is the whole point, so nothing blocks before the kill: the
        pipeline is only *signalled* to stop (no join: that would wait on an
        in-progress ~6 s send_swath), and the ABORT goes urgent, off the
        io_lock, so it reaches the wire in ms even while the pipeline worker
        holds the lock streaming. The follow-up reboot then tears the link
        down; the in-flight stream fails and, with _stop set, the worker exits
        without latching a fault. _stop_pipeline() at the end joins it.
        """
        handler = self.handler  # snapshot: a serial loss may clear it mid-call
        if handler is None:
            return {'success': False, 'error': 'device_not_connected',
                    'message': 'serial link lost'}
        if self.pipeline:
            self.pipeline.request_stop()
        aborted = handler.abort()
        self._clear_print_fault()
        rebooted = self._reboot_firmware_and_wait()
        self._stop_pipeline()
        return {'success': aborted and rebooted}

    def _cmd_get_status(self, msg: dict = None) -> dict:
        """Query the firmware slot status."""
        handler = self.handler  # snapshot: a serial loss may clear it mid-call
        if handler is None:
            return {'success': False, 'error': 'device_not_connected',
                    'message': 'serial link lost'}
        status = handler.get_status()
        if status:
            return {'success': True, 'data': self._serialize_status(status)}
        return {'success': False, 'error': 'timeout'}

    def _cmd_identify(self, msg: dict = None) -> dict:
        """Query the firmware identity (wire_id, profile_hash, fw_build)."""
        handler = self.handler  # snapshot: a serial loss may clear it mid-call
        if handler is None:
            return {'success': False, 'error': 'not_connected',
                    'message': 'No serial connection'}

        identity = handler.identify()
        if identity is None:
            return {'success': False, 'error': 'no_response',
                    'message': 'Firmware did not answer IDENTIFY'}

        self.firmware_identity = identity
        return {'success': True, **identity}

    @staticmethod
    def _valid_u8(value) -> bool:
        """True for a plain int that fits the wire's single byte."""
        return (not isinstance(value, bool) and isinstance(value, int)
                and 0 <= value <= 255)

    def _cmd_purge(self, msg: dict) -> dict:
        """Execute purge.

        channel/pulses are single bytes on the wire: validate here with a
        clear error instead of letting `& 0xFF` silently wrap them (300 -> 44)
        or a wrong type blow up into an opaque internal_error. The upper bound
        of `channel` is the firmware's CH_COUNT; its NACK covers that (the
        daemon does not hard-code head geometry).
        """
        channel = msg.get('channel', 0)
        pulses = msg.get('pulses', 10)

        if not self._valid_u8(channel):
            return {'success': False, 'error': 'invalid_channel',
                    'message': f'channel must be an integer 0..255, '
                               f'got {channel!r}'}
        if not self._valid_u8(pulses) or pulses == 0:
            return {'success': False, 'error': 'invalid_pulses',
                    'message': f'pulses must be an integer 1..255, '
                               f'got {pulses!r}'}

        handler = self.handler
        if handler is None:
            return {'success': False, 'error': 'device_not_connected',
                    'message': 'serial link lost'}

        resp = handler.purge(channel, pulses)

        if resp.success:
            return {'success': True, 'channel': channel, 'pulses': pulses}
        
        return {'success': False, 'error': str(resp.error) if resp.error else 'failed',
                'message': resp.error_msg}

    def _serialize_status(self, status: dict) -> dict:
        """Serialize status for JSON."""
        def serialize_slot(slot: dict) -> dict:
            state = slot.get('state')
            if isinstance(state, SlotState):
                slot['state'] = state.name.lower()
            return slot
        
        if 'slot_a' in status:
            status['slot_a'] = serialize_slot(status['slot_a'])
        if 'slot_b' in status:
            status['slot_b'] = serialize_slot(status['slot_b'])

        return status
