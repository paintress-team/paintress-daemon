# SPDX-FileCopyrightText: 2026 paintress-team
# SPDX-License-Identifier: GPL-3.0-or-later

"""In-memory representation of a loaded print job (the daemon side).

The encoder produces the on-disk job (``.json`` header + ``.bin`` sidecar,
parsed by ``paintress_job``); ``LoadedJob`` is the daemon's runtime view of it:
the swath lines (1-indexed) plus the per-swath Y positions and the metadata it
hands back to the plugin.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class LoadedJob:
    """Job loaded in memory."""
    filepath: str
    swaths: Dict[int, List[bytes]] = field(default_factory=dict)  # swath_id -> lines (raw bytes)
    metadata: Dict = field(default_factory=dict)
    y_positions: List[float] = field(default_factory=list)
    y_deltas: List[float] = field(default_factory=list)

    @property
    def swath_ids(self) -> List[int]:
        return sorted(self.swaths.keys())

    def get_swath(self, swath_id: int) -> Optional[List[bytes]]:
        return self.swaths.get(swath_id)

    def get_y_position(self, swath_id: int) -> Optional[float]:
        idx = swath_id - 1
        if 0 <= idx < len(self.y_positions):
            return self.y_positions[idx]
        return None

    def get_y_delta(self, swath_id: int) -> Optional[float]:
        idx = swath_id - 1
        if 0 <= idx < len(self.y_deltas):
            return self.y_deltas[idx]
        return None

    def get_info(self) -> dict:
        """Return job info as a dictionary."""
        info = {
            'filepath': self.filepath,
            'swath_count': len(self.swaths),
            'swath_ids': self.swath_ids,
            'metadata': self.metadata,
        }

        # Line size comes from the job itself (the encoder's bytes_per_line),
        # not a daemon constant: a different head packs a different line.
        bytes_per_line = self.metadata.get('bytes_per_line', 0)

        total_lines = 0
        swaths_info = []
        for swath_id in self.swath_ids:
            lines = self.swaths[swath_id]
            line_count = len(lines)
            total_lines += line_count

            swath_info = {
                'swath_id': swath_id,
                'line_count': line_count,
                'size_bytes': line_count * bytes_per_line,
            }

            y_pos = self.get_y_position(swath_id)
            if y_pos is not None:
                swath_info['y_position_mm'] = y_pos

            y_delta = self.get_y_delta(swath_id)
            if y_delta is not None:
                swath_info['y_delta_mm'] = y_delta

            swaths_info.append(swath_info)

        info['swaths'] = swaths_info
        info['total_lines'] = total_lines
        info['total_bytes'] = total_lines * bytes_per_line

        return info
