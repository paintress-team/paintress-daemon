# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""
Serial protocol for the Paintress controller board (host side framing).

Frame layout:
    [START_BYTE] [LEN_LO] [LEN_HI] [TYPE] [PAYLOAD...] [CRC_LO] [CRC_HI]

The wire codes (message types, commands, events, errors, slot states) and the
frame constants come from the generated, single-source module
``paintress_protocol`` (vendored from the paintress-protocol repo); this file
only adds the host-side framing helpers (CRC, encode/decode, parsers). See the
firmware README for the canonical command/event tables.
"""

import binascii
import struct
from dataclasses import dataclass
from typing import Optional

from .paintress_protocol import (
    Command,
    ErrorCode,
    Event,
    MessageType,
    SlotState,
    START_BYTE,
    HEADER_SIZE,
    CRC_SIZE,
    MAX_PAYLOAD_SIZE,
    WIRE_ID,
    PROTOCOL_VERSION,
)


# Note: the DATA line size is NOT a daemon constant. It travels with each job
# (the encoder's bytes_per_line, checked against the firmware profile_hash at
# load_job), and encode_data_frame() below sizes each frame from the line it is
# given, so the daemon carries no assumption about a specific head's geometry.


def crc16(data: bytes) -> int:
    """Compute the CRC-16-CCITT of a buffer (initial value 0xFFFF)."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc


@dataclass
class Frame:
    """A single protocol frame."""
    msg_type: MessageType
    payload: bytes

    def encode(self) -> bytes:
        """Encode the frame into wire bytes."""
        length = len(self.payload)

        header = bytes([
            START_BYTE,
            length & 0xFF,
            (length >> 8) & 0xFF,
            self.msg_type,
        ])

        data = header + self.payload
        crc = crc16(data)

        return data + struct.pack('<H', crc)

    @classmethod
    def decode(cls, data: bytes) -> Optional['Frame']:
        """Decode wire bytes into a Frame, or None if invalid/incomplete."""
        if len(data) < HEADER_SIZE + CRC_SIZE:
            return None

        if data[0] != START_BYTE:
            return None

        length = data[1] | (data[2] << 8)
        msg_type = data[3]

        expected_size = HEADER_SIZE + length + CRC_SIZE
        if len(data) < expected_size:
            return None

        payload = data[HEADER_SIZE:HEADER_SIZE + length]

        received_crc = struct.unpack('<H', data[HEADER_SIZE + length:HEADER_SIZE + length + 2])[0]
        calculated_crc = crc16(data[:HEADER_SIZE + length])

        if received_crc != calculated_crc:
            return None

        try:
            return cls(
                msg_type=MessageType(msg_type),
                payload=payload,
            )
        except ValueError:
            return None

    @classmethod
    def frame_size(cls, data: bytes) -> Optional[int]:
        """Return the total frame size, or None if incomplete/invalid."""
        if len(data) < HEADER_SIZE:
            return None

        if data[0] != START_BYTE:
            return None

        length = data[1] | (data[2] << 8)

        if length > MAX_PAYLOAD_SIZE:
            return None

        return HEADER_SIZE + length + CRC_SIZE


# === Frame builders ===

def make_cmd_frame(cmd: Command, params: bytes = b'') -> Frame:
    """Build a command frame."""
    payload = bytes([cmd]) + params
    return Frame(msg_type=MessageType.COMMAND, payload=payload)


def make_begin_swath_params(swath_id: int, line_count: int) -> bytes:
    """Build the parameters for a BEGIN_SWATH command."""
    return struct.pack('<HI', swath_id, line_count)


def make_arm_params(swath_id: int, line_delay_us: int) -> bytes:
    """Build the parameters for an ARM command.

    The firing interval travels with each ARM: there is no
    timing state on the device to set or lose across a reset.
    """
    return struct.pack('<HH', swath_id, line_delay_us)


def encode_data_frame(line_data: bytes) -> bytes:
    """Encode a DATA frame straight to wire bytes.

    The frame length is taken from the line itself, so the line size is
    per-job (the encoder's bytes_per_line) rather than a daemon-side constant;
    firmware agreement is guaranteed by the geometry-fingerprint check at
    load_job. DATA frames dominate swath streaming (thousands per swath), so
    this skips the Frame object and uses the C-backed binascii.crc_hqx, which
    is CRC-16-CCITT with initial value 0xFFFF, identical to crc16().
    """
    n = len(line_data)
    body = bytes([START_BYTE, n & 0xFF, (n >> 8) & 0xFF, int(MessageType.DATA)]) + line_data
    crc = binascii.crc_hqx(body, 0xFFFF)
    return body + struct.pack('<H', crc)


# === Response parsers ===

def parse_ack(payload: bytes) -> dict:
    """Parse an ACK payload."""
    if len(payload) < 1:
        return {'cmd': None}

    result = {'cmd': payload[0]}

    if len(payload) > 1:
        result['data'] = payload[1:]

    return result


def parse_nack(payload: bytes) -> dict:
    """Parse a NACK payload."""
    if len(payload) < 2:
        return {'cmd': None, 'error': None}

    return {
        'cmd': payload[0],
        'error': ErrorCode(payload[1]) if payload[1] in [e.value for e in ErrorCode] else payload[1],
    }


def parse_event(payload: bytes) -> dict:
    """Parse an EVENT payload."""
    if len(payload) < 1:
        return {'event': None}

    result = {'event': payload[0]}

    if len(payload) > 1:
        result['data'] = payload[1:]

    return result


def parse_log(payload: bytes) -> dict:
    """Parse a LOG payload (level byte + UTF-8 text)."""
    if len(payload) < 1:
        return {'level': None, 'text': ''}

    return {
        'level': payload[0],
        'text': payload[1:].decode('utf-8', errors='replace'),
    }


def parse_status_response(data: bytes) -> Optional[dict]:
    """Parse the payload of a STATUS response."""
    if len(data) < 16:
        return None

    return {
        'slot_a': {
            'state': SlotState(data[0]) if data[0] < 4 else data[0],
            'swath_id': struct.unpack('<H', data[1:3])[0],
            'lines_received': struct.unpack('<I', data[3:7])[0],
        },
        'slot_b': {
            'state': SlotState(data[7]) if data[7] < 4 else data[7],
            'swath_id': struct.unpack('<H', data[8:10])[0],
            'lines_received': struct.unpack('<I', data[10:14])[0],
        },
        'receiving': bool(data[14]),
        'printing': bool(data[15]),
        # DAC latched off by an ABORT or a safety fault; the
        # firmware NACKs ARM/PURGE/SET_DAC_POWER(on) until a RESET clears it.
        'dac_latched': bool(data[16]) if len(data) > 16 else False,
    }


def parse_begin_swath_response(data: bytes) -> Optional[dict]:
    """Parse the payload of a BEGIN_SWATH response."""
    if len(data) < 3:
        return None

    return {
        'swath_id': struct.unpack('<H', data[0:2])[0],
        'slot': data[2],
    }


def parse_end_swath_response(data: bytes) -> Optional[dict]:
    """Parse the payload of an END_SWATH response."""
    if len(data) < 3:
        return None

    return {
        'swath_id': struct.unpack('<H', data[0:2])[0],
        'complete': bool(data[2]),
    }


def parse_identify_response(data: bytes) -> Optional[dict]:
    """Parse the payload of an IDENTIFY response."""
    if len(data) < 10:
        return None

    return {
        'wire_id': struct.unpack('<H', data[0:2])[0],
        'profile_hash': struct.unpack('<I', data[2:6])[0],
        'fw_build': struct.unpack('<I', data[6:10])[0],
        # bit0 = this boot came from a watchdog reboot (a host
        # RESET or the core-1 watchdog auto-recovery).
        'boot_flags': data[10] if len(data) > 10 else 0,
    }
