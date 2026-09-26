# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""ANSI colour codes and small logging-format helpers shared by the daemon."""

from datetime import datetime

CYAN = '\033[96m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
MAGENTA = '\033[95m'
BLUE = '\033[94m'
RED = '\033[91m'
RESET = '\033[0m'
BOLD = '\033[1m'
DIM = '\033[2m'


def _format_bytes_hex(data: bytes, max_bytes: int = 32) -> str:
    """Format bytes as a hex string for logging."""
    if len(data) <= max_bytes:
        return data.hex()
    return f"{data[:max_bytes].hex()}... ({len(data)} bytes total)"


def _format_timestamp() -> str:
    """Format the current time for logging."""
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]
