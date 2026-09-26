# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Serial communication manager.

Responsibilities:
- Batched, queued frame transmission
- Urgent commands that bypass the normal TX queue
- Capture of raw firmware output (printf text)
- Thread-safe access to the serial port
"""

import time
import logging
import threading
from queue import Queue, Empty
from typing import Callable, Optional

import serial

from .protocol import Frame, START_BYTE, HEADER_SIZE


logger = logging.getLogger(__name__)


class SerialManager:
    """Manages the serial link to the controller board firmware."""

    def __init__(
        self,
        port: str,
        baudrate: int = 2000000,
        timeout: float = 0.001,
    ):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.serial: Optional[serial.Serial] = None

        self._running = False
        self._rx_thread: Optional[threading.Thread] = None
        self._tx_thread: Optional[threading.Thread] = None

        # Normal TX queue.
        self._tx_queue: Queue[bytes] = Queue(maxsize=100000)

        # Urgent TX queue (bypasses the normal queue).
        self._urgent_queue: Queue[bytes] = Queue(maxsize=100)

        # Callback for decoded protocol frames.
        self._on_frame_received: Optional[Callable[[Frame], None]] = None

        # Callback for raw firmware output (printf text).
        self._on_raw_data: Optional[Callable[[bytes], None]] = None
        self._raw_buffer = bytearray()

        # Called once when the port is lost (USB unplug / fatal serial error).
        self._on_disconnect: Optional[Callable[[], None]] = None
        self._lost = False
        self._lost_lock = threading.Lock()

        # Serializes writes to the serial port.
        self._tx_lock = threading.Lock()

        self.stats = {
            'tx_bytes': 0,
            'rx_bytes': 0,
            'tx_frames': 0,
            'rx_frames': 0,
            'crc_errors': 0,
            'tx_errors': 0,
            'raw_lines': 0,
        }

    def connect(self) -> bool:
        """Open the serial port and start the RX/TX threads."""
        try:
            self.serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.timeout,
                write_timeout=1.0,
            )

            # Low-latency tweak (Linux only; ignored on other platforms).
            try:
                import termios
                attrs = termios.tcgetattr(self.serial.fileno())
                attrs[3] = attrs[3] & ~termios.ICANON
                termios.tcsetattr(self.serial.fileno(), termios.TCSANOW, attrs)
            except Exception:
                pass

            self.serial.reset_input_buffer()
            self.serial.reset_output_buffer()

            self._running = True
            with self._lost_lock:
                self._lost = False

            self._rx_thread = threading.Thread(target=self._rx_loop, daemon=True, name="SerialRX")
            self._rx_thread.start()

            self._tx_thread = threading.Thread(target=self._tx_loop, daemon=True, name="SerialTX")
            self._tx_thread.start()

            return True

        except serial.SerialException as e:
            logger.error("Failed to open serial port %s: %s", self.port, e)
            return False

    def disconnect(self):
        """Stop the RX/TX threads and close the serial port."""
        self._running = False

        if self._rx_thread:
            self._rx_thread.join(timeout=2.0)
        if self._tx_thread:
            self._tx_thread.join(timeout=2.0)

        if self.serial:
            try:
                self.serial.close()
            except Exception:
                # Closing a handle whose device vanished (USB unplug) can
                # raise on some platforms; the teardown must not care.
                logger.debug("closing serial port raised", exc_info=True)
            self.serial = None

    def send_frame(self, frame: Frame, timeout: float = 1.0) -> bool:
        """Send a protocol frame through the normal TX queue."""
        try:
            self._tx_queue.put(frame.encode(), timeout=timeout)
            return True
        except Exception:
            return False

    def send_bytes(self, data: bytes, timeout: float = 1.0) -> bool:
        """Send pre-encoded bytes through the normal TX queue."""
        try:
            self._tx_queue.put(data, timeout=timeout)
            return True
        except Exception:
            return False

    def send_urgent(self, frame: Frame) -> bool:
        """Send a frame ahead of the normal TX queue."""
        try:
            self._urgent_queue.put_nowait(frame.encode())
            return True
        except Exception:
            # Urgent queue full: fall back to a direct write.
            return self._send_direct(frame.encode())

    def _send_direct(self, data: bytes) -> bool:
        """Write bytes directly to the serial port."""
        if not self.serial or not self.serial.is_open:
            return False

        acquired = self._tx_lock.acquire(timeout=0.1)
        if not acquired:
            return False

        try:
            written = self.serial.write(data)
            self.serial.flush()
            self.stats['tx_bytes'] += written
            self.stats['tx_frames'] += 1
            return written == len(data)
        except (serial.SerialException, OSError) as e:
            # A fatal write error is a lost port, same as on the RX side: it
            # must enter the single port-lost path, not just bump a counter;
            # otherwise the link keeps reporting healthy while every frame is
            # silently dropped. Called here (not raised) because _send_direct
            # also runs on caller threads via send_urgent's fallback.
            self.stats['tx_errors'] += 1
            self._handle_port_lost(e)
            return False
        finally:
            self._tx_lock.release()

    def _tx_loop(self):
        """Transmission loop: drains the urgent and normal TX queues."""
        batch = []
        MAX_BATCH_SIZE = 65536  # 64 KB

        while self._running:
            try:
                # 1. The urgent queue takes priority.
                while not self._urgent_queue.empty():
                    try:
                        urgent_data = self._urgent_queue.get_nowait()
                        # Flush the pending batch before the urgent write.
                        if batch:
                            self._send_batch(batch)
                            batch = []
                        self._send_direct(urgent_data)
                    except Empty:
                        break

                # 2. Normal queue.
                try:
                    data = self._tx_queue.get(timeout=0.001)
                    batch.append(data)

                    # Accumulate more data while it is available.
                    batch_size = len(data)
                    while batch_size < MAX_BATCH_SIZE:
                        try:
                            more = self._tx_queue.get_nowait()
                            batch.append(more)
                            batch_size += len(more)
                        except Empty:
                            break

                    self._send_batch(batch)
                    batch = []

                except Empty:
                    pass

            except (serial.SerialException, OSError) as e:
                self._handle_port_lost(e)
                return
            except Exception as e:
                logger.error("TX loop error: %s", e)
                time.sleep(0.01)

    def _send_batch(self, batch: list):
        """Send a batch of pre-encoded byte buffers."""
        if not self.serial or not batch:
            return

        # Avoid an extra copy for the common case where callers already batch.
        bulk_data = batch[0] if len(batch) == 1 else b"".join(batch)

        with self._tx_lock:
            try:
                # Handle rare partial writes without allocating new buffers.
                view = memoryview(bulk_data)
                total_written = 0
                while total_written < len(view):
                    written = self.serial.write(view[total_written:])
                    if written is None:
                        written = 0
                    if written <= 0:
                        break
                    total_written += written

                if total_written < len(view):
                    # A truncated batch means lost DATA frames: the firmware
                    # will report the swath incomplete, but the local trace
                    # of WHY must not be silent.
                    self.stats['tx_errors'] += 1
                    logger.warning("serial TX truncated: %d/%d bytes written",
                                   total_written, len(view))

                self.stats['tx_bytes'] += total_written
                self.stats['tx_frames'] += len(batch)
            except (serial.SerialException, OSError):
                # Count it, then re-raise into _tx_loop's port-lost handler
                # (the single idempotent path). Swallowing it here left the
                # link looking healthy (is_connected() true, no disconnect
                # callback) while DATA frames vanished.
                self.stats['tx_errors'] += 1
                raise

    def _rx_loop(self):
        """Reception loop: reads bytes and extracts frames and raw text."""
        buffer = bytearray()

        while self._running:
            try:
                if self.serial and self.serial.in_waiting:
                    data = self.serial.read(min(4096, self.serial.in_waiting))
                    if data:
                        buffer.extend(data)
                        self.stats['rx_bytes'] += len(data)
                        self._process_rx_buffer(buffer)
                else:
                    time.sleep(0.0001)

            except (serial.SerialException, OSError) as e:
                # Port gone (USB unplug / fatal error): stop and notify.
                self._handle_port_lost(e)
                return
            except Exception as e:
                logger.error("RX loop error: %s", e)
                time.sleep(0.01)

    def _is_printable_ascii(self, byte: int) -> bool:
        """Return True if the byte is printable ASCII or whitespace."""
        return (0x20 <= byte <= 0x7E) or byte in (0x09, 0x0A, 0x0D)

    def _process_rx_buffer(self, buffer: bytearray):
        """Extract protocol frames and raw text from the RX buffer."""
        while len(buffer) > 0:
            # A protocol frame starts with START_BYTE.
            if buffer[0] == START_BYTE:
                # Wait until the header is complete.
                if len(buffer) < HEADER_SIZE:
                    break

                frame_size = Frame.frame_size(bytes(buffer))
                if frame_size is None:
                    # Invalid header: treat the bytes as raw output.
                    self._extract_raw_until_frame(buffer)
                    continue

                if len(buffer) < frame_size:
                    break  # Frame still incomplete.

                frame_data = bytes(buffer[:frame_size])
                frame = Frame.decode(frame_data)

                if frame:
                    self.stats['rx_frames'] += 1
                    self._handle_frame(frame)
                else:
                    self.stats['crc_errors'] += 1

                del buffer[:frame_size]

            else:
                # Not a START_BYTE: raw firmware output (printf).
                self._extract_raw_until_frame(buffer)

    def _extract_raw_until_frame(self, buffer: bytearray):
        """Pull raw bytes off the buffer until the next valid frame."""
        raw_end = len(buffer)
        for i in range(1, len(buffer)):
            if buffer[i] == START_BYTE:
                # Check whether this position looks like a real frame.
                if len(buffer) - i >= HEADER_SIZE:
                    test_size = Frame.frame_size(bytes(buffer[i:]))
                    if test_size is not None:
                        raw_end = i
                        break

        if raw_end > 0:
            raw_data = bytes(buffer[:raw_end])
            del buffer[:raw_end]
            self._process_raw_data(raw_data)

    def _process_raw_data(self, data: bytes):
        """Buffer raw firmware output and emit complete text lines."""
        if not self._on_raw_data:
            return

        self._raw_buffer.extend(data)

        while b'\n' in self._raw_buffer:
            idx = self._raw_buffer.index(b'\n')
            line_data = bytes(self._raw_buffer[:idx + 1])
            del self._raw_buffer[:idx + 1]

            if self._looks_like_text(line_data):
                self.stats['raw_lines'] += 1
                try:
                    self._on_raw_data(line_data)
                except Exception as e:
                    logger.error("Raw data callback error: %s", e)

        # Cap the buffer to avoid unbounded growth.
        if len(self._raw_buffer) > 4096:
            self._raw_buffer = self._raw_buffer[-1024:]

    def _looks_like_text(self, data: bytes) -> bool:
        """Return True if the bytes look like ASCII text."""
        if not data:
            return False
        valid = sum(1 for b in data if self._is_printable_ascii(b))
        return valid >= len(data) * 0.7

    def _handle_frame(self, frame: Frame):
        """Dispatch a decoded frame to the registered callback."""
        if self._on_frame_received:
            self._on_frame_received(frame)

    def set_frame_callback(self, callback: Optional[Callable[[Frame], None]]):
        """Register the callback for decoded protocol frames."""
        self._on_frame_received = callback

    def set_raw_callback(self, callback: Optional[Callable[[bytes], None]]):
        """Register the callback for raw firmware output."""
        self._on_raw_data = callback

    def set_disconnect_callback(self, callback: Optional[Callable[[], None]]):
        """Register a callback fired once when the port is lost (USB unplug)."""
        self._on_disconnect = callback

    def is_connected(self) -> bool:
        """True while the port is open and has not been lost."""
        return self._running and not self._lost and self.serial is not None

    def _handle_port_lost(self, exc: Exception):
        """A fatal serial error (port gone): stop the loops and notify once."""
        with self._lost_lock:
            if self._lost:
                return
            self._lost = True
        logger.error("Serial port %s lost: %s", self.port, exc)
        self._running = False  # stop both loops; they exit their while-guard
        if self._on_disconnect:
            try:
                self._on_disconnect()
            except Exception:
                logger.exception("serial disconnect callback failed")

    def get_tx_queue_size(self) -> int:
        """Return the number of items waiting in the TX queue."""
        return self._tx_queue.qsize()

    def wait_tx_empty(self, timeout: float = 5.0) -> bool:
        """Block until the TX queue drains, then flush the serial port."""
        start = time.monotonic()
        while self._tx_queue.qsize() > 0:
            if not self.is_connected():
                return False  # port dead: the queue will never drain
            if time.monotonic() - start > timeout:
                return False
            time.sleep(0.01)

        if self.serial:
            try:
                self.serial.flush()
            except Exception:
                pass

        return True
