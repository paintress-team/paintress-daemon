# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Paintress daemon: TCP control server for piezo inkjet printing.

Bridges the Klipper host and the controller board firmware: it accepts
NDJSON commands over TCP, loads print jobs, and streams swath data to
the firmware over a serial link.

Run as a module:
    python -m paintress_daemon
"""

from .server import TCPDaemon

__version__ = "0.1"

__all__ = ["TCPDaemon", "__version__"]
