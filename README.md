<p align="center">
  <img src="docs/img/paintress.svg" alt="Paintress" width="160">
</p>

# paintress-daemon

A small TCP server that sits between Klipper and the controller board. It
loads job files, keeps their passes in memory, talks to the board over USB
serial, sends it the print data, and passes the board's events back to its
clients.

Part of [Paintress](https://paintress.dev), an open-source controller for
piezo inkjet printheads. The other published parts are
[paintress-rip-encoder](https://github.com/paintress-team/paintress-rip-encoder),
[paintress-protocol](https://github.com/paintress-team/paintress-protocol) and
[paintress-klipper-extras](https://github.com/paintress-team/paintress-klipper-extras).
The firmware is not published yet.

> **Status: experimental.** Paintress is not ready for general use yet, and
> things can change without notice.

This is where it sits in the chain:

```
  rip.py → RIP payload → encoder.py → job (.json+.bin) → [ paintress-daemon ] → serial frames → firmware
```

The daemon doesn't move anything. Klipper moves the head, through
[paintress-klipper-extras](https://github.com/paintress-team/paintress-klipper-extras).
The daemon only gets the data to the board and reports what the board does.

## Contents

- [What it does](#what-it-does)
- [How it works](#how-it-works)
- [Installation](#installation)
- [Running the daemon](#running-the-daemon)
- [The TCP protocol](#the-tcp-protocol)
- [Commands](#commands)
- [Messages the daemon sends on its own](#messages-the-daemon-sends-on-its-own)
- [When things go wrong](#when-things-go-wrong)
- [The job file](#the-job-file)
- [An example session](#an-example-session)
- [Files](#files)
- [Known issues](#known-issues)
- [Use of AI](#use-of-ai)

## What it does

The daemon listens on one TCP port and speaks **NDJSON**: one JSON object per
line. A client, normally `paintressd_client.py` from the Klipper add-on,
connects, sends commands and reads the answers.

It:

- loads a job file made by the encoder and keeps every pass in memory;
- opens and closes the USB serial link to the board. It doesn't open it at
  startup; a client asks for it with `connect`;
- sends the pass data to the board in the binary serial protocol, in large
  batches so it goes fast;
- passes on the board's control commands (`purge`, `abort`, `reset`,
  `identify`, `get_status`). There are no timing or power commands: the
  firing interval is sent with every pass, and the board handles the head
  voltage itself;
- forwards the board's events and log lines to every connected client.

## How it works

The daemon has an `asyncio` TCP server and a serial side that runs in its
own threads:

```
   ┌──────────────────────────────────────────────────────────┐
   │  asyncio event loop (main thread)                         │
   │   • accepts TCP clients, reads NDJSON commands             │
   │   • sends events / logs / keepalive to clients             │
   └──────────────────────────────────────────────────────────┘
            │  blocking commands go to worker threads
            ▼
   ┌──────────────────────────────────────────────────────────┐
   │  cmd-worker thread + thread pool                          │
   │   • cmd-worker: runs commands that change state, one at a  │
   │     time, in the order they came (connect/load_job/print…) │
   │   • thread pool: runs read-only commands (status, ...)     │
   │   • waits for the board's ACK/NACK without blocking        │
   └──────────────────────────────────────────────────────────┘
            │  frames in / out
            ▼
   ┌──────────────────────────────────────────────────────────┐
   │  SerialManager: two threads                               │
   │   • SerialTX: empties the send queue in 64 KB writes       │
   │   • SerialRX: reads bytes, splits frames from log text     │
   └──────────────────────────────────────────────────────────┘
```

Every command to the board waits for an `ACK` or a `NACK`. That wait blocks,
so commands run outside the `asyncio` loop, which stays free to serve other
clients and forward events.

Commands that change state (`connect`, `disconnect`, `reconnect`,
`load_job`, `unload_job`, `reset`, `abort`, `print`, `purge`) all run on one
thread, `cmd-worker`, one after the other. The board handles one thing at a
time anyway, and running them side by side only caused races. Read-only
commands (`status`, `get_status`, `identify`, `job_info`) run in a thread
pool, so `status` always answers, even while a slow command is running.

There is also a background thread that sends the next pass while the current
one prints. It and the command threads all go through `SwathHandler`, which
has a lock (`io_lock`) so a whole `send_swath` can't be cut in half by a
command from another thread. `abort` and `reset` are the exception: they skip
the lock and jump the queue, so a stop is never held up by a transfer that
takes about 6 s. Answers are matched to commands by the command byte they
echo, so the `ACK` of an `ABORT` and the `ACK` of the pass's `END_SWATH` don't
get mixed up when both are in flight.

**Sending a pass.** A pass is thousands of lines of the same size. To keep
the cost per line low, `send_lines()` turns each line straight into wire
bytes (`encode_data_frame`, with the C-backed CRC) and gathers them into
64 KB chunks before queuing them. There is no `ACK` per line; the board
confirms the whole pass on `END_SWATH`.

**Two passes on the board.** The board has two memory slots, so it can
receive one pass while it prints the other, and the daemon keeps both full.
When a job is loaded, it sends the first passes to both slots. A `print` only
waits until its pass is in a slot, then starts it. When a pass finishes, its
slot is free (on a `print_complete`, `trigger_timeout` or `print_error`
event) and the daemon sends the next one. So pass N+1 goes over while pass N
prints, and the Klipper add-on never sends data while the head moves.

### The code

- `server.py`: `TCPDaemon`, the asyncio server. It accepts clients, reads
  NDJSON, holds the loaded job and the sending pipeline, and sends messages to
  clients. The commands themselves are in `commands.py`.
- `commands.py`: `CommandHandlersMixin`. A table of commands,
  `{name: (handler, requires_serial, mutating)}`, and one `_cmd_*` function
  per command. `mutating` decides which thread runs it.
- `print_pipeline.py`: `PrintPipeline`, which keeps both slots on the board
  full.
- `serial_manager.py`: `SerialManager`. It owns the serial port and the two
  threads. The send thread groups frames into writes of about 64 KB; urgent
  frames (`ABORT`, `ARM`) go first. The receive thread separates protocol
  frames from the board's `LOG` text.
- `swath_handler.py`: `SwathHandler`, the steps of each operation:
  `begin_swath`, `send_lines`, `end_swath`, the start of a pass
  (`print_swath`) and purge.
- `loaded_job.py`: `LoadedJob`, the job held in memory.
- `protocol.py`: the binary serial protocol (frames, CRC-16, frame builders,
  answer parsers), using the codes from the generated `paintress_protocol`.
- `head_profiles.py`: the head list, generated in
  [paintress-protocol](https://github.com/paintress-team/paintress-protocol).
  It gives the `--head` choices and the `head_fingerprint` that `load_job`
  checks.
- `_console.py`: colours and log formatting.
- `__main__.py`: the command line (`python -m paintress_daemon`).

## Installation

You need **Python 3.8 or newer** and one package,
[`pyserial`](https://pyserial.readthedocs.io/). From the repository root:

```sh
pip install -r requirements.txt
```

Everything else comes with Python.

## Running the daemon

From the repository root, run it as a module:

```sh
python -m paintress_daemon
```

This starts the TCP server but does **not** open the serial port. A client has
to send `connect` with the serial device before any board command works.

### Options

| Option              | Default     | What it does |
|---------------------|-------------|--------------|
| `--tcp-port`, `-t`  | `9000`      | TCP port of the NDJSON server. |
| `--host`            | `127.0.0.1` | Address the server listens on. Only this machine by default; see the security note below. |
| `--baud`, `-b`      | `2000000`   | Serial speed to the board. |
| `--head`            | `c6n90`     | Which printhead is fitted. One firmware serves every head with the same line format, so the board can't tell which one is on. The daemon refuses a job made for a different head. |
| `--debug`, `-d`     | off         | More detailed logging. |

> **Security.** The NDJSON protocol has **no password**, and its commands fire
> ink (`purge`), reset the board and read files from disk. That's why the
> daemon only listens on this machine, which is all the Klipper add-on needs.
> Listening on a network address (`--host 0.0.0.0`) gives those commands to
> everyone on the network. Only do it on a network you fully trust.

Examples:

```sh
python -m paintress_daemon
python -m paintress_daemon --tcp-port 9000
python -m paintress_daemon -t 9000 --debug
```

Stop it with `Ctrl+C`. It also handles `SIGINT` and `SIGTERM` where the
system supports them.

### Running it as a systemd service

On the machine that runs Klipper, systemd usually starts the daemon next to
it. `paintress-daemon.service` in this repository works on a Raspberry Pi set
up the usual way: user `pi`, Klipper's Python in `~/klippy-env`, and the
`paintress_daemon` folder copied to `/home/pi`. Change `User`,
`WorkingDirectory` and the Python path in `ExecStart` if your setup is
different, and add `--head <name>` to `ExecStart` for the head you have.

```sh
sudo cp paintress-daemon.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now paintress-daemon.service
journalctl -u paintress-daemon -f     # follow the log
```

The service gives the daemon 30 s to stop (`TimeoutStopSec`). That's enough
for what it does on the way out (send the board `ABORT` and `RESET`, then
close the port), so stopping it never leaves the head powered.

## The TCP protocol

Every message is one JSON object on one line, ending with `\n`.

### Requests

A client sends commands like this:

```json
{"cmd": "connect", "id": 1, "port": "/dev/ttyACM0"}
```

- `cmd` (required) is the command name.
- `id` (optional) is sent back in the answer, so the client can tell which
  answer belongs to which request.
- Any other field is a parameter of the command.

### Answers

Each command gets one `response`:

```json
{"type": "response", "cmd": "connect", "id": 1, "success": true,
 "port": "/dev/ttyACM0", "baudrate": 2000000, "timestamp": 1747800000.0}
```

- `type` is always `"response"`.
- `cmd` and `id` are copied from the request.
- `success` is `true` or `false`.
- `error` and `message` are there when `success` is `false`.
- `timestamp` is the Unix time of the answer.
- The other fields depend on the command.

A line that isn't valid JSON, or has no `cmd`, gets an `error` message
instead of a `response`.

### Timeouts

Each kind of command has a time limit. Past it, the answer is
`success: false` with `error: "timeout"`. For commands that change state, the
clock starts when the command is queued, so a command stuck behind another
one can time out before it even starts. Its `message` then says so and names
the command it was waiting for.

| Commands | Limit |
|----------|-------|
| `status`, `get_status`, `disconnect` | 10 s |
| `load_job` | 300 s |
| everything else, including `reset`, `abort`, `connect` and `reconnect`, which may have to wait for the board to reboot and come back (up to 8 s) | 30 s |

## Commands

The commands are grouped by what they need.

### Always available

| Command      | Parameters | What it does |
|--------------|------------|--------------|
| `status`     | none       | The daemon's status: version, connection state, counters, a summary of the loaded job, serial statistics, `print_fault` (the latched fault, or `null`) and `command_in_progress` (the command the worker is running, or `null`). |
| `connect`    | `port`     | Opens the serial link to `port` (for example `/dev/ttyACM0` or `COM3`). Returns `port` and `baudrate`. |
| `disconnect` | `transport_only` (optional) | **Safe** disconnect. It first stops the board (`ABORT` then `RESET`, each confirmed by an `ACK`), then closes the port. Closing the port alone stops nothing: a pass that is waiting for its trigger keeps waiting for up to about 10 s, and a pass that is firing runs to its end. The board reboots into a clean state. `transport_only: true` skips the stop and only closes the port. Returns `disconnected_from`. If the link is already lost, the stop is skipped. |
| `reconnect`  | `port` (optional) | Closes and reopens the serial link, on the last port if none is given. On the same port it only reopens it (the reboot of a safe stop would drop the board off USB right when it reopens). On a different port it stops the old board first. |

### Jobs (no serial link needed)

| Command      | Parameters | What it does |
|--------------|------------|--------------|
| `load_job`   | `filepath` | Loads a job `.json` and its `.bin`. Returns `swath_count`, `swath_ids`, `total_lines`, `metadata`, `y_positions_mm` and `y_deltas_mm`. |
| `unload_job` | none       | Drops the job from memory. |
| `job_info`   | none       | Details of the loaded job: line count, size and Y position of each pass. |

### Board commands (serial link needed)

| Command      | Parameters | What it does |
|--------------|------------|--------------|
| `reset`      | none       | Resets the board, which means a **full reboot of the chip**. The daemon sends `RESET`, waits through the expected USB drop, reconnects, runs `IDENTIFY` again and answers once the board is back (about 1 to 2 s). The reboot clears the slots, the voltage lock and everything else. Nothing has to be set up again, because the firing interval comes with every `print`. |
| `abort`      | none       | Emergency stop. Sends `ABORT` ahead of everything else. On the board it turns the head voltage off and keeps it off, so ink stops at once. Then it reboots the board like `reset`, so it's clean for another try. |
| `get_status` | none       | The state of the board's two slots: pass ids, lines received, and the `receiving`, `printing` and `dac_latched` flags. |
| `identify`   | none       | Who the board is: `wire_id`, `profile_hash`, `fw_build` and `boot_flags` (bit 0 set means the last boot came from a watchdog reboot). It also runs by itself on every connect, and the daemon refuses a firmware with a different protocol MAJOR version. `profile_hash` is the **frame** fingerprint. `load_job` checks the job against it, and separately checks the job's `head_fingerprint` against `--head`, because the board can't know which head is fitted. |

### Passes (serial link and a loaded job needed)

| Command | Parameters | What it does |
|---------|------------|--------------|
| `print` | `swath_id`, `line_delay_us` | Starts a pass. The daemon waits until the pass is in a slot on the board, then sends `ARM` with `line_delay_us` (the firing interval), and keeps sending the next pass. `line_delay_us` must be a whole number from 1 to 65535 (it is 16 bits on the wire), otherwise the answer is `invalid_line_delay`. Returns `swath_id`. |

Pass ids start at 1 and follow the order of the passes in the job file.

### Purge (serial link needed)

| Command | Parameters | What it does |
|---------|------------|--------------|
| `purge` | `channel` (default `0`), `pulses` (default `10`) | Fires the nozzles to clear or prime them. The board checks that `channel` exists. |

### Error codes

A `success: false` answer carries one of these, among others:

`missing_cmd`, `unknown_command`, `missing_params`, `device_not_connected`,
`not_connected`, `connection_failed`, `missing_port`, `no_port`,
`no_job_loaded`, `no_swaths`, `missing_filepath`, `missing_swath_id`,
`swath_not_found`, `swath_out_of_sequence`, `file_not_found`, `load_error`,
`invalid_job`, `invalid_json`, `invalid_line_delay`, `invalid_channel`,
`invalid_pulses`, `serial_lost`, `no_response`, `timeout`,
`processing_error`, `internal_error`.

Two of them come from the job checks (see [The job file](#the-job-file)):
`profile_mismatch` means the job's line format doesn't match the connected
firmware, and `head_mismatch` means the job was made for another head than
`--head`.

When the board refuses something, the answer carries the protocol's
`ErrorCode` (for example `NO_SLOT_AVAILABLE` or `SWATH_NOT_READY`).

A `timeout` doesn't cancel anything. The daemon can't stop the worker thread,
so the command may still finish later; its result is then thrown away and
logged. It can't overlap with the next command, though, because the worker
only takes the next command when the current one is done. Look at `status`
(`command_in_progress`) before you retry a command that changes state.

## Messages the daemon sends on its own

Besides answers, the daemon sends messages nobody asked for. A client has to
read every line, not only the answer to its last command.

| `type`         | When | Main fields |
|----------------|------|-------------|
| `welcome`      | right after a client connects | `version`, `device_connected`, `serial_port`, `job_loaded` |
| `event`        | the board reports an event, or the daemon a fault | `event` (`print_started`, `print_complete`, `print_error`, `trigger_timeout`, `pipeline_error`, `serial_lost`), `swath_id`, `data` (the raw event payload in hex), `error` (for `print_error`, the name of the code) |
| `firmware_log` | the board sends a `LOG` frame | `text` |
| `keepalive`    | every 30 seconds | `timestamp` |
| `error`        | a line it received was invalid | `error` |

## When things go wrong

The daemon decides when a print has failed. It works like Klipper: **a
communication failure cancels the print**, with the cause kept, and getting
back always takes an explicit command from the host. The daemon never
reconnects or retries by itself, with one small exception below.

**Print faults.** A `print_error` or `trigger_timeout` from the board, or a
failure to send the next pass, is latched as a fault (with its cause) and
sent to the clients. The next `print` then fails at once with that cause, so
the host stops instead of printing on. `load_job`, `reset` and `abort` clear
the fault; `abort` and `reset` also stop the sending pipeline so it doesn't
work against the stop. With the job still loaded, a `print` of the job's
**first pass** starts the pipeline again (after an abort or reset, or to
print the job again). A pass in the middle of the job that the pipeline can
no longer serve fails with `swath_out_of_sequence`: a print starts from the
beginning or not at all. A `print` of a pass the job doesn't have fails with
`swath_not_found`.

**Aborted prints.** On the board, `ABORT` only cuts the head voltage and
keeps it off; it doesn't stop the pass. A pass that was firing runs to its
end without ink and still reports `PRINT_COMPLETE`. The reboot right after
leaves the board clean. So a `print_error` event is always a real error and
always latched.

**Reboots the daemon asked for.** When this daemon sends `RESET` (from
`reset`, `abort`, or the slot cleanup below), it expects the USB drop: no
`serial_lost` fault, no event. The command that asked for the reboot waits
for the drop, reopens the port (the board comes back in about 1 to 2 s) and
runs `IDENTIFY` again before it answers. After reconnecting, the log says
whether the board's last boot came from a watchdog reboot (`boot_flags`).

**Leftovers on the board.** Before a fresh start (a job load, a serial
connect, or the restart after an abort), the daemon reads the slot status. If
the slots still hold passes from an earlier run, or the head voltage is still
locked off (every `ARM` would be refused with `DAC_LATCHED`), it reboots the
board and waits for it. Without this the first `BEGIN_SWATH` would be refused
with `NO_SLOT_AVAILABLE` and the print would stop at pass 1.

**Why a pass didn't start.** A `print` that can't start says why:
`pipeline_timeout` (the pass didn't reach a slot in time), `stream_failed`
(the sending thread died), or the board's own refusal code (for example
`swath_not_ready`). When the board refuses, the daemon also logs the state of
both slots.

**Short passes.** If a pass's data ends before its line count, the board
throws the slot away without printing it, and the daemon reports the
transfer as failed. A cut-off pass is never printed.

**One retry for a failed transfer.** This is the only automatic retry. A
pass whose transfer fails (for example a damaged send, which the board throws
away, so sending again is safe) is sent again, up to 2 times, before it
becomes a `pipeline_error`. Each retry is logged as a warning and counted in
`daemon_stats.stream_retries`. One glitch doesn't kill a print, and a count
that keeps growing points at a bad cable. A retry never crosses a reboot or a
lost link, and a pass that runs out of retries cancels the print as usual.

**Lost serial link means a cancelled print.** If the board's USB link drops
unexpectedly, the daemon latches a `serial_lost` fault, sends the event and
stops the pipeline. It does **not** try to reconnect, the same way Klipper
handles a lost MCU. A `reconnect` (or `reset`, or `abort`) from the host
brings the link back for a *new* print. An interrupted print is never
resumed.

**Lost TCP link.** The client (`paintressd_client`) reconnects by itself on
its next command.

## The job file

The daemon reads the job made by the encoder in
[paintress-rip-encoder](https://github.com/paintress-team/paintress-rip-encoder):
a JSON header and a binary file next to it (`<name>.json` and `<name>.bin`).
Both are defined by the `paintress_job` module from
[paintress-protocol](https://github.com/paintress-team/paintress-protocol).

```jsonc
// print.json  (next to it: print.bin)
{
  "format_version": "0.1.0",
  "geometry_fingerprint": "0x108865B9",   // the frame, checked against the firmware
  "head_name": "c6n90",                   // which head, checked against --head
  "head_fingerprint": "0x9C244CD5",
  "dpi": 630,
  "bytes_per_line": 147,
  "total_passes": 42,
  "passes": [
    { "y_position_mm": 0.0,  "y_delta_mm": 0.04, "line_count": 1500 },
    { "y_position_mm": 0.04, "y_delta_mm": 0.04, "line_count": 1500 }
    // ...
  ],
  "data": { "file": "print.bin", "byte_count": 9261000,
            "line_count": 63000, "crc32": "0x1A2B3C4D" }
}
```

On `load_job` the daemon (with `paintress_job.load_job` and `validate`):

1. Reads the header and checks its version, then checks the size and CRC-32
   of the `.bin`. If they don't match, loading fails instead of printing
   garbage.
2. Runs **two** checks, each against a different source:
   - **Frame.** The job's `geometry_fingerprint` must equal the
     `profile_hash` the firmware sent in `IDENTIFY`. That covers what the
     firmware sends to the pins: line size, clocks, bit layout. If not, it
     fails with `profile_mismatch`.
   - **Head.** The job's `head_fingerprint` must match the head the daemon
     was started with (`--head`). One firmware serves every head with the
     same frame, so the board can't tell them apart, and this is the only
     place a wrong head can be caught. Without it, a job for another head
     would print nonsense. If not, it fails with `head_mismatch`, and the
     message says whether to RIP again or restart the daemon with the other
     `--head`.

   The daemon has no line size of its own: it comes from each job
   (`bytes_per_line`), and each data frame is as long as its line.
3. Reads each pass's lines straight from the `.bin`, by jumping to
   (lines before it) × `bytes_per_line`. Passes are numbered from 1
   (`swath_id`).
4. Gives the Klipper add-on each pass's `y_position_mm` and `y_delta_mm` (as
   `y_positions_mm` and `y_deltas_mm`) so it can place the passes.
5. Keeps the passes in memory until `unload_job`, another `load_job`, or
   until the daemon stops.

The line size comes from the head profile the encoder used, and the frame
fingerprint checks it against the firmware. The head profiles live in
[paintress-protocol](https://github.com/paintress-team/paintress-protocol),
whose README explains how the two fingerprints split the work.

## An example session

This is roughly how the client (the Klipper add-on) runs a print:

```text
1.  connect    {port}                    → open the serial link
2.  load_job   {filepath}                → load the job; the daemon starts
                                           sending passes to both slots
        for each swath_id in the job:
        (Klipper moves the head to the pass's Y position)
3.    print    {swath_id, line_delay_us} → wait until the pass is in a slot,
                                           then start it (the ARM carries the
                                           firing interval); the daemon keeps
                                           sending the next pass
        ← event: print_started
        ← event: print_complete          → a slot is free; the next pass goes
4.  disconnect                           → stop the board (ABORT + RESET,
                                           confirmed), then close the port
```

The client never sends pass data itself. The daemon keeps both slots full,
so pass N+1 goes over while pass N prints.

## Files

```
paintress-daemon/
├── paintress_daemon/
│   ├── __init__.py        Package info; exports TCPDaemon.
│   ├── __main__.py        Command line (python -m paintress_daemon).
│   ├── server.py          TCPDaemon: NDJSON server, job and pipeline state.
│   ├── commands.py        CommandHandlersMixin: command table and handlers.
│   ├── print_pipeline.py  PrintPipeline: keeps both slots full.
│   ├── serial_manager.py  SerialManager: serial port and its threads.
│   ├── swath_handler.py   SwathHandler: the steps of each board operation.
│   ├── loaded_job.py      LoadedJob: the job in memory.
│   ├── protocol.py        Binary serial protocol: frames, CRC, codes.
│   ├── paintress_protocol.py  Generated protocol codes (copied from paintress-protocol).
│   ├── head_profiles.py   Generated head list (copied from paintress-protocol).
│   ├── paintress_job.py   Job file module (copied from paintress-protocol).
│   └── _console.py        Colours and log helpers.
├── tests/                 Tests that run without hardware.
├── hw_tests/              Tests that need a board.
├── paintress-daemon.service  systemd service file.
└── requirements.txt       Python packages to install (pyserial).
```

## Known issues

Things we know are wrong and haven't fixed yet.

- **Stopping doesn't drop queued commands.** `_stop_cmd_worker()` in
  `server.py` puts the stop marker at the end of the command queue, so
  commands already waiting (`purge`, `print`, `reset`) still run after the
  stop begins. It isn't an electrical risk, because the safe stop sends
  `ABORT` and `RESET` before the port closes, but the order is wrong. The fix
  is to empty the queue, or to put the marker at the front.
- **Answers are matched by command byte.** The wire protocol has no request
  id, so an answer is matched to its request by the command byte it echoes
  (see `_Waiter` in `swath_handler.py`). A late answer to a command that timed
  out can reach the next command of the same type. Every payload is checked,
  which covers the case we know of. The real fix is a request id in the wire
  format, in
  [paintress-protocol](https://github.com/paintress-team/paintress-protocol).

## Use of AI

The architecture of Paintress was planned by people, and so was the reverse
engineering behind it: probing the original controller with an oscilloscope
and a logic analyser, and working out from those captures how the head is
driven. The first tests and the first printed lines were also done by
people, on the bench.

AI tools were used in a limited way: to fix bugs, to help keep the stages of
the pipeline consistent with each other, to write and edit the
documentation, and most of all on the communication between the daemon and
the firmware.

## License

Copyright (C) 2026 paintress-team.

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version. The full text is in [`LICENSE`](LICENSE).

In short: if you share a changed version of this code, or a product that
includes it, you have to share its source under the same license.

Contributions are welcome. Sign off your commits as explained in
[`CONTRIBUTING.md`](CONTRIBUTING.md).
