#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Entry point for running the daemon as a module:

    python -m paintress_daemon [options]
"""

import sys
import signal
import asyncio
import logging
import argparse

from . import head_profiles
from .server import TCPDaemon


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="paintress_daemon",
        description="TCP control daemon for the Paintress controller board",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m paintress_daemon
  python -m paintress_daemon --tcp-port 9000
  python -m paintress_daemon -t 9000 --debug
        """,
    )

    parser.add_argument(
        "--tcp-port", "-t",
        type=int,
        default=9000,
        help="TCP port for the NDJSON server (default: 9000)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind address for the TCP server (default: 127.0.0.1, loopback "
             "only). The protocol has NO authentication and its commands "
             "fire ink, reset hardware and read job files; only bind a "
             "reachable address (e.g. --host 0.0.0.0) on a network you "
             "fully trust.",
    )
    parser.add_argument(
        "--baud", "-b",
        type=int,
        default=2000000,
        help="Serial baud rate for the firmware link (default: 2000000)",
    )
    parser.add_argument(
        "--head",
        default=head_profiles.DEFAULT_HEAD,
        choices=sorted(head_profiles.HEADS),
        help="Which printhead is fitted to this machine (default: "
             f"{head_profiles.DEFAULT_HEAD}). One firmware build serves every "
             "head that shares the wire frame, so the board cannot report this "
             "-- the daemon refuses a job packed for a different head.",
    )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    daemon = TCPDaemon(baudrate=args.baud, host=args.host, port=args.tcp_port,
                       head=args.head)

    async def run() -> None:
        loop = asyncio.get_running_loop()

        def request_stop() -> None:
            asyncio.create_task(daemon.stop())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, request_stop)
            except NotImplementedError:
                pass  # Signal handlers are not available on Windows.

        await daemon.start()

        try:
            while daemon._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await daemon.stop()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nStopped by user")

    return 0


if __name__ == "__main__":
    sys.exit(main())
