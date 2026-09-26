# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Double-buffer print pipeline.

Keeps the firmware's two PSRAM slots fed: while one swath prints, the next is
streamed into the free slot, so printing never stalls between swaths. The daemon
owns this, so the Klipper plugin only has to move, arm and sweep: no async
reactor I/O and no streaming during motion.

Two callbacks are injected (both must be serialized by the swath handler's
io_lock, since they share the serial link):

    stream_fn(swath_id, lines) -> bool        # begin + data + end into a free slot
    arm_fn(swath_id) -> ArmOutcome            # ARM a READY swath

arm_fn returns an ArmOutcome so a firmware arm-reject (e.g. SWATH_NOT_READY)
carries its reason through instead of collapsing into a bare False; see
request_print, which distinguishes "never streamed" from "firmware rejected".

The firmware's event handler must call on_print_complete(swath_id) when a swath
finishes (which frees its slot).

State (in_flight = ready + printing, capped at num_slots):
    pending  - swaths, in order, not yet streamed
    ready    - streamed into a slot, not yet armed
    printing - armed and printing (slot freed on PRINT_COMPLETE)
"""

import threading
import time
from collections import deque
from typing import Callable, Dict, List, NamedTuple, Optional, Sequence, Tuple


class ArmOutcome(NamedTuple):
    """Result of an ARM attempt (what arm_fn returns).

    error is a short machine code on failure (e.g. 'swath_not_ready',
    'print_in_progress', 'arm_exception'); message is human-readable.
    """
    ok: bool
    error: Optional[str] = None
    message: Optional[str] = None


class PrintOutcome(NamedTuple):
    """Result of request_print: says whether the arm happened and, if not, why.

    error is one of 'stream_failed' / 'pipeline_timeout' / 'pipeline_stopped'
    (daemon-side) or the firmware's own arm-reject code passed straight
    through from ArmOutcome.
    """
    ok: bool
    error: Optional[str] = None
    message: Optional[str] = None


StreamFn = Callable[[int, Sequence[bytes]], bool]
# arm_fn(swath_id, line_delay_us): the firing interval travels with each ARM
# so request_print carries it through to the arm.
ArmFn = Callable[[int, int], ArmOutcome]


class PrintPipeline:
    def __init__(self, stream_fn: StreamFn, arm_fn: ArmFn, num_slots: int = 2,
                 on_error: "Callable[[int, str], None] | None" = None,
                 stream_retries: int = 2,
                 on_retry: "Callable[[int, int], None] | None" = None,
                 join_timeout: float = 5.0):
        self._stream_fn = stream_fn
        self._arm_fn = arm_fn
        self._num_slots = num_slots
        # Bound on stop()'s join: how long a teardown waits for the worker to
        # come back from an in-flight stream before reporting it still alive.
        self._join_timeout = join_timeout
        # Called (off the lock) when background streaming of a swath fails, so
        # the failure is surfaced instead of the worker just stopping silently.
        self._on_error = on_error
        # A failed stream is retried in place up to stream_retries times
        # before it counts as a failure: an incomplete transfer is discarded
        # by the firmware (the slot frees), so re-sending is safe and cheap,
        # and one transient USB hiccup should not kill a whole print. Each
        # retry is reported through on_retry(swath_id, attempt) so the host
        # can count and log it: a retry that repeats is a hardware problem
        # to fix, not one to hide.
        self._stream_retries = stream_retries
        self._on_retry = on_retry

        self._cv = threading.Condition()
        self._lines: Dict[int, Sequence[bytes]] = {}
        self._pending: "deque[int]" = deque()
        self._streaming: Optional[int] = None  # popped from pending, mid-stream
        self._ready: set[int] = set()
        self._printing: set[int] = set()
        self._failed = False
        self._stop = False
        self._worker: threading.Thread | None = None
        # Load generation. Each load() bumps it and hands the new value to the
        # worker it starts; a worker only writes state while its generation is
        # still current. Without this, a worker that outlived stop()'s bounded
        # join (an in-flight send_swath can take ~6 s) would come back and
        # write into the NEXT load's state, inserting its old swath into
        # _ready, latching _failed, or slipping past the retry guards once the
        # new load reset _stop to False.
        self._generation = 0

    # --- lifecycle ---------------------------------------------------------

    def load(self, swaths: List[Tuple[int, Sequence[bytes]]]) -> None:
        """(Re)start the pipeline for an ordered list of (swath_id, lines).

        Raises RuntimeError if the previous worker is still alive after the
        bounded stop: restarting this object would put two workers on the same
        stream/arm callbacks. The daemon never hits this (it builds a fresh
        PrintPipeline per job; see _start_pipeline); the raise is the contract
        for any other caller.
        """
        if not self.stop():
            raise RuntimeError(
                "previous pipeline worker is still running (stuck in a "
                "stream); refusing to restart on the same object")
        with self._cv:
            self._generation += 1
            gen = self._generation
            self._lines = {sid: lines for sid, lines in swaths}
            self._pending = deque(sid for sid, _ in swaths)
            self._streaming = None
            self._ready = set()
            self._printing = set()
            self._failed = False
            self._stop = False
        self._worker = threading.Thread(target=self._run, args=(gen,),
                                        name="print-pipeline", daemon=True)
        self._worker.start()

    def request_stop(self) -> None:
        """Signal the worker to stop, WITHOUT waiting for it to finish.

        Wakes request_print and the worker's wait immediately, but the worker
        may still be mid-stream_fn (a ~6 s send_swath): it exits after that call
        returns. Used by the urgent abort/reset path so an emergency stop never
        blocks on a swath-sized transfer join; the in-flight stream then fails
        against the torn-down link and, with _stop set, exits without latching a
        fault (see _run's `stopping`). Call stop() afterwards to actually join.
        """
        with self._cv:
            self._stop = True
            self._cv.notify_all()

    def stop(self) -> bool:
        """Signal the worker and join it (synchronous teardown).

        Returns False if the worker did not exit within the bounded join (it
        is stuck inside a swath-sized stream call). The reference is kept so a
        later stop() can re-join; the abandoned worker can no longer corrupt
        state: its generation is stale the moment load() runs again (on a
        fresh object in the daemon's flow), but it may still hold the serial
        io_lock until its in-flight stream fails or finishes.
        """
        self.request_stop()
        if self._worker is not None:
            self._worker.join(timeout=self._join_timeout)
            if self._worker.is_alive():
                return False
            self._worker = None
        return True

    # --- worker ------------------------------------------------------------

    def _in_flight(self) -> int:
        return len(self._ready) + len(self._printing)

    def _has_streamable(self) -> bool:
        # Called holding the condition lock.
        return bool(self._pending) and self._in_flight() < self._num_slots

    def _run(self, gen: int) -> None:
        # gen is this worker's load generation: every state write below is
        # guarded by it, so a worker that outlived a bounded stop() can never
        # touch a later load's state. Note _stop alone is NOT that guard: a
        # new load() resets it to False, which would un-stop a stale worker.
        def cancelled() -> bool:
            return self._stop or gen != self._generation

        while True:
            with self._cv:
                while not cancelled() and not self._has_streamable():
                    self._cv.wait()
                if cancelled():
                    return
                swath_id = self._pending.popleft()
                self._streaming = swath_id
                lines = self._lines[swath_id]

            # Stream outside the lock (serial I/O); serialized vs arm by io_lock.
            # A failure is retried in place (bounded) before it counts: the
            # firmware discards an incomplete swath, so the slot is free for a
            # clean re-send. No retry once stop() was called: a teardown
            # failure is expected and must stay silent.
            ok = self._stream_fn(swath_id, lines)
            attempts = 1
            while not ok and attempts <= self._stream_retries and not cancelled():
                if self._on_retry is not None:
                    try:
                        self._on_retry(swath_id, attempts)
                    except Exception:
                        pass
                time.sleep(0.2)  # let the link settle before re-sending
                if cancelled():
                    break
                ok = self._stream_fn(swath_id, lines)
                attempts += 1

            with self._cv:
                if gen != self._generation:
                    # A newer load owns this object now: discard the result
                    # entirely: the old swath must not appear in the new
                    # run's ready set, nor its failure latch the new run.
                    return
                self._streaming = None
                stopping = self._stop
                if ok:
                    self._ready.add(swath_id)
                else:
                    self._pending.appendleft(swath_id)
                    self._failed = True
                self._cv.notify_all()
            if not ok:
                # Surface the failure (off the lock) instead of stopping
                # quietly, unless stop() was already called: then the stream
                # failed because the pipeline (or the serial link under it) was
                # being torn down on purpose, and reporting it would latch a
                # spurious fault that nothing clears until the next
                # load_job/abort/reset.
                if not stopping and self._on_error is not None:
                    try:
                        self._on_error(
                            swath_id,
                            f"failed to stream swath into a slot "
                            f"(gave up after {attempts} attempts)")
                    except Exception:
                        pass
                return  # stop the pipeline on a streaming failure

    # --- public API --------------------------------------------------------

    def request_print(self, swath_id: int, line_delay_us: int,
                      timeout: float = 30.0) -> PrintOutcome:
        """Wait until swath_id is streamed (READY), then ARM it. Blocks.

        line_delay_us is handed to arm_fn: the firing interval travels with
        each ARM. Returns a PrintOutcome that distinguishes
        *why* an arm did not happen: 'stream_failed' (background streaming
        died), 'pipeline_timeout' (never reached a slot in time),
        'pipeline_stopped' (stop() woke the wait: abort/reset/teardown), or
        the firmware's own arm-reject code.
        """
        deadline = time.monotonic() + timeout
        with self._cv:
            while swath_id not in self._ready:
                if self._stop:
                    # stop() (abort/reset/disconnect/serial loss) wakes this
                    # wait instead of letting it burn the full timeout.
                    return PrintOutcome(
                        False, 'pipeline_stopped',
                        f'pipeline was stopped while waiting for swath '
                        f'{swath_id}')
                if self._failed:
                    return PrintOutcome(
                        False, 'stream_failed',
                        f'swath {swath_id} was never streamed into a slot '
                        '(background streaming failed)')
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._cv.wait(timeout=remaining):
                    return PrintOutcome(
                        False, 'pipeline_timeout',
                        f'swath {swath_id} not streamed into a slot within '
                        f'{timeout:.0f}s')

        outcome = self._arm_fn(swath_id, line_delay_us)
        if outcome.ok:
            with self._cv:
                self._ready.discard(swath_id)
                self._printing.add(swath_id)
                self._cv.notify_all()
        return PrintOutcome(outcome.ok, outcome.error, outcome.message)

    def can_serve(self, swath_id: int) -> bool:
        """True if this pipeline will still stream/arm swath_id.

        That means it is queued (pending), being streamed right now, or already
        in a slot (ready), and background streaming has not failed. A swath
        that was already consumed (printed, currently printing, or behind the
        stream cursor after the job ran to completion) needs a fresh pipeline
        (see the daemon's _cmd_print, which restarts one only for a print that
        starts over from the job's first swath).
        """
        with self._cv:
            return (not self._failed
                    and (swath_id == self._streaming
                         or swath_id in self._ready
                         or swath_id in self._pending))

    def on_print_complete(self, swath_id: int) -> None:
        """Mark a swath's print finished; its slot is now free for the next one."""
        with self._cv:
            self._printing.discard(swath_id)
            self._ready.discard(swath_id)  # defensive
            self._cv.notify_all()

    def is_failed(self) -> bool:
        with self._cv:
            return self._failed
