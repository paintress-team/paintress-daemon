# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""Paintress print-job container.

The encoder's output, read by the daemon and the Klipper plugin. This module is
the single definition of the format: the three repos vendor this one file
instead of poking at dict keys.

A job is two sibling files:

    <name>.json   versioned, typed header (defined here)
    <name>.bin    the packed column lines, fixed size, concatenated pass by pass

The header indexes the binary: every pass carries a ``line_count``, every line
is ``bytes_per_line`` bytes, and passes are stored contiguously. So pass ``i``
starts at ``(sum of earlier line_counts) * bytes_per_line`` and runs for
``line_count`` lines, so the daemon can seek straight to a swath and stream it.
A CRC-32 over the whole ``.bin`` is recorded in the header so a corrupt or
mismatched sidecar fails loud instead of printing garbage.

Replaces the v1 format, which inlined every line as base64 inside the JSON
(convenient while bringing the hardware up, wasteful now that it works).

Stdlib only, so it vendors cleanly into every consumer.
"""

from __future__ import annotations

import json
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

FORMAT_VERSION = "0.1.0"
_SUPPORTED_MAJOR = 0


class JobError(Exception):
    """Raised when a job header is malformed or its sidecar does not match."""


@dataclass
class PassInfo:
    """One swath: where the head sits in Y and how many columns it fires."""

    y_position_mm: float
    y_delta_mm: float
    line_count: int


@dataclass
class JobMetadata:
    """The ``.json`` header. Geometry the encoder used + the pass index."""

    dpi: int
    image_width_px: int
    image_height_px: int
    print_width_mm: float
    print_height_mm: float
    padded_width_px: int
    padded_width_mm: float
    padded_height_mm: float
    swath_pass_width_mm: float
    nozzle_count: int
    bytes_per_line: int
    passes_per_band: int
    channel_offsets_px: dict[str, int]
    passes: list[PassInfo]
    # Fingerprint of the FRAME the firmware shifts out: line size, clocks,
    # edges, packing. The daemon refuses a job whose frame does not match the
    # connected firmware (PROFILE_MISMATCH). Shared by every head that rides the
    # same frame, so it says nothing about which head this job is for.
    geometry_fingerprint: int = 0
    # Which printhead the job was packed for. Checked host-side against the
    # machine's configuration, never against the firmware: the board has no way
    # of knowing which head is bolted on, so it cannot vouch for this.
    head_name: str = ""
    head_fingerprint: int = 0
    packing: str = "contiguous"
    format_version: str = FORMAT_VERSION

    @property
    def total_passes(self) -> int:
        return len(self.passes)

    @property
    def total_lines(self) -> int:
        return sum(p.line_count for p in self.passes)

    @property
    def data_byte_count(self) -> int:
        return self.total_lines * self.bytes_per_line

    def pass_offset(self, index: int) -> int:
        """Byte offset of pass ``index`` inside the ``.bin``."""
        return sum(p.line_count for p in self.passes[:index]) * self.bytes_per_line

    def to_dict(self) -> dict:
        """The header as a JSON-serialisable dict (without the ``data`` block).

        This is also what the daemon hands back to the plugin, so the plugin
        reads the same field names the encoder wrote.
        """
        return {
            "format_version": self.format_version,
            "geometry_fingerprint": f"0x{self.geometry_fingerprint:08X}",
            "head_name": self.head_name,
            "head_fingerprint": f"0x{self.head_fingerprint:08X}",
            "dpi": self.dpi,
            "image_width_px": self.image_width_px,
            "image_height_px": self.image_height_px,
            "print_width_mm": self.print_width_mm,
            "print_height_mm": self.print_height_mm,
            "padded_width_px": self.padded_width_px,
            "padded_width_mm": self.padded_width_mm,
            "padded_height_mm": self.padded_height_mm,
            "swath_pass_width_mm": self.swath_pass_width_mm,
            "nozzle_count": self.nozzle_count,
            "bytes_per_line": self.bytes_per_line,
            "passes_per_band": self.passes_per_band,
            "packing": self.packing,
            "channel_offsets_px": self.channel_offsets_px,
            "total_passes": self.total_passes,
            "passes": [
                {
                    "y_position_mm": p.y_position_mm,
                    "y_delta_mm": p.y_delta_mm,
                    "line_count": p.line_count,
                }
                for p in self.passes
            ],
        }


@dataclass
class Job:
    """A loaded header bound to its sidecar on disk."""

    metadata: JobMetadata
    bin_path: Path
    crc32: int
    byte_count: int

    def verify_bin(self) -> bool:
        """True if the sidecar's size and CRC-32 match the header."""
        data = self.bin_path.read_bytes()
        return len(data) == self.byte_count and zlib.crc32(data) == self.crc32

    def iter_pass_lines(self, index: int) -> Iterator[bytes]:
        """Yield each fixed-size line of pass ``index`` straight from disk."""
        meta = self.metadata
        info = meta.passes[index]
        bpl = meta.bytes_per_line
        with self.bin_path.open("rb") as fh:
            fh.seek(meta.pass_offset(index))
            for _ in range(info.line_count):
                line = fh.read(bpl)
                if len(line) != bpl:
                    raise JobError(f"pass {index}: sidecar truncated")
                yield line

    def pass_bytes(self, index: int) -> bytes:
        """The full contiguous block of pass ``index``."""
        meta = self.metadata
        info = meta.passes[index]
        with self.bin_path.open("rb") as fh:
            fh.seek(meta.pass_offset(index))
            return fh.read(info.line_count * meta.bytes_per_line)


# --- JSON (de)serialisation -------------------------------------------------


def _meta_to_dict(meta: JobMetadata, *, data_block: dict) -> dict:
    doc = meta.to_dict()
    doc["data"] = data_block
    return doc


def _meta_from_dict(doc: dict) -> JobMetadata:
    try:
        fingerprint = doc.get("geometry_fingerprint", 0)
        if isinstance(fingerprint, str):
            fingerprint = int(fingerprint, 0)
        head_fingerprint = doc.get("head_fingerprint", 0)
        if isinstance(head_fingerprint, str):
            head_fingerprint = int(head_fingerprint, 0)
        return JobMetadata(
            dpi=doc["dpi"],
            image_width_px=doc["image_width_px"],
            image_height_px=doc["image_height_px"],
            print_width_mm=doc["print_width_mm"],
            print_height_mm=doc["print_height_mm"],
            padded_width_px=doc["padded_width_px"],
            padded_width_mm=doc["padded_width_mm"],
            padded_height_mm=doc["padded_height_mm"],
            swath_pass_width_mm=doc["swath_pass_width_mm"],
            nozzle_count=doc["nozzle_count"],
            bytes_per_line=doc["bytes_per_line"],
            passes_per_band=doc["passes_per_band"],
            channel_offsets_px=doc["channel_offsets_px"],
            passes=[
                PassInfo(p["y_position_mm"], p["y_delta_mm"], p["line_count"])
                for p in doc["passes"]
            ],
            geometry_fingerprint=fingerprint,
            head_name=doc.get("head_name", ""),
            head_fingerprint=head_fingerprint,
            packing=doc.get("packing", "contiguous"),
            format_version=doc["format_version"],
        )
    except KeyError as exc:
        raise JobError(f"job header missing field {exc}") from None


# --- save / load ------------------------------------------------------------


def save_job(
    json_path: str | Path,
    metadata: JobMetadata,
    passes_lines: Iterable[Iterable[bytes]],
    *,
    bin_name: str | None = None,
) -> Job:
    """Write ``<name>.json`` + ``<name>.bin`` from packed line data.

    ``passes_lines`` is one iterable of fixed-size ``bytes`` lines per pass, in
    pass order. The actual line counts and CRC-32 come from the data written,
    the header can never disagree with the sidecar. ``metadata.passes`` supplies
    the per-pass Y geometry; its ``line_count`` values are filled in here.
    """
    json_path = Path(json_path)
    bin_path = json_path.with_suffix(".bin") if bin_name is None else json_path.parent / bin_name

    bpl = metadata.bytes_per_line
    crc = 0
    byte_count = 0
    line_counts: list[int] = []

    with bin_path.open("wb") as fh:
        for pass_index, lines in enumerate(passes_lines):
            count = 0
            for line in lines:
                if len(line) != bpl:
                    raise JobError(
                        f"pass {pass_index} line {count}: {len(line)} bytes, "
                        f"expected {bpl}"
                    )
                fh.write(line)
                crc = zlib.crc32(line, crc)
                byte_count += bpl
                count += 1
            line_counts.append(count)

    if len(line_counts) != len(metadata.passes):
        raise JobError(
            f"got {len(line_counts)} passes of data, header declares "
            f"{len(metadata.passes)}"
        )
    for info, count in zip(metadata.passes, line_counts):
        info.line_count = count

    data_block = {
        "file": bin_path.name,
        "byte_count": byte_count,
        "line_count": sum(line_counts),
        "crc32": f"0x{crc:08X}",
    }
    json_path.write_text(
        json.dumps(_meta_to_dict(metadata, data_block=data_block), indent=2),
        encoding="utf-8",
    )
    return Job(metadata, bin_path, crc, byte_count)


def load_job(json_path: str | Path) -> Job:
    """Load and validate a job header; the sidecar is read lazily on demand."""
    json_path = Path(json_path)
    doc = json.loads(json_path.read_text(encoding="utf-8"))

    major = int(str(doc.get("format_version", "0")).split(".", 1)[0] or 0)
    if major != _SUPPORTED_MAJOR:
        raise JobError(
            f"job format_version {doc.get('format_version')!r} not supported "
            f"(need major {_SUPPORTED_MAJOR})"
        )

    meta = _meta_from_dict(doc)
    data = doc.get("data")
    if not isinstance(data, dict):
        raise JobError("job header missing 'data' block")

    # A malformed data block must fail as a JobError with the field named,
    # not leak a bare KeyError/ValueError that callers report as a cryptic
    # internal error (same contract _meta_from_dict already keeps).
    try:
        bin_name = data["file"]
        crc = int(str(data["crc32"]), 0)
        byte_count = int(data["byte_count"])
    except KeyError as exc:
        raise JobError(f"job data block missing field {exc}") from None
    except ValueError as exc:
        raise JobError(f"job data block field is malformed: {exc}") from None

    # The sidecar must be a SIBLING of the header: a plain file name, never a
    # path. Without this, a crafted header could point the loader at any file
    # on the system (../../x, an absolute path, a FIFO...), which callers
    # then open and read whole.
    if not bin_name or Path(bin_name).name != bin_name:
        raise JobError(
            f"data.file must be a plain sibling file name, got {bin_name!r}")
    bin_path = json_path.parent / bin_name

    expected_lines = sum(p.line_count for p in meta.passes)
    if data.get("line_count") != expected_lines:
        raise JobError("data.line_count disagrees with the pass index")
    if byte_count != expected_lines * meta.bytes_per_line:
        raise JobError("data.byte_count disagrees with the pass index")

    return Job(meta, bin_path, crc, byte_count)


def validate(job: Job, *, expected_bytes_per_line: int | None = None) -> None:
    """Raise :class:`JobError` if the job is internally inconsistent.

    Pass ``expected_bytes_per_line`` (the firmware's ``USB_LINE_SIZE``) to also
    check the job was packed for this build's line geometry.
    """
    meta = job.metadata
    if meta.bytes_per_line <= 0:
        raise JobError("bytes_per_line must be positive")
    if any(p.line_count < 0 for p in meta.passes):
        raise JobError("negative line_count")
    if expected_bytes_per_line is not None and meta.bytes_per_line != expected_bytes_per_line:
        raise JobError(
            f"job packed for {meta.bytes_per_line}-byte lines, firmware uses "
            f"{expected_bytes_per_line}"
        )
    if not job.bin_path.exists():
        raise JobError(f"sidecar not found: {job.bin_path}")
    if not job.verify_bin():
        raise JobError("sidecar size or CRC-32 does not match the header")
