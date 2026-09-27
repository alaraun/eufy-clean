"""Generate the cross-language render-classification fixture.

The client renderer and ``render_map_png`` must colour identical bytes
identically. Nothing else checks that: the Python suite never runs the JS, and
the jsdom suite never runs Python, so a divergence like the dropped room
sub-type passes both suites green while the map visibly differs in a browser.

This writes one fixture consumed by BOTH sides:
  * ``tests/frontend/fixtures/classification.json`` — a real ``build_map_geometry``
    payload plus the label ``render_map_png``'s own rule assigns to every cell.

``tests/test_map_geometry.py`` regenerates it and fails if it drifted;
``tests/frontend/renderer.test.mjs`` decodes the payload with the shipped client
code and fails if any cell disagrees. Run this module to refresh it.
"""
from __future__ import annotations

import base64
import json
import random
import zlib
from pathlib import Path

from custom_components.robovac_mqtt.api.map_geometry import build_map_geometry
from custom_components.robovac_mqtt.api.map_stream import MapData

FIXTURE = Path(__file__).parent / "frontend" / "fixtures" / "classification.json"
_W = _H = 16


def build() -> dict:
    """Build a map exercising every (sub_type, pv, room) combination."""
    random.seed(5)  # fixed: the fixture must be reproducible
    raw = bytearray((_W * _H + 3) // 4)
    mask = bytearray(_W * _H)
    for i in range(_W * _H):
        pv = random.randrange(4)
        sub = random.randrange(4)
        rid = random.randrange(0, 4)
        raw[i >> 2] |= pv << ((i & 3) * 2)
        if rid:
            mask[i] = (rid << 2) | sub
    map_data = MapData(
        raw_pixels=bytes(raw), width=_W, height=_H,
        origin_x=0, origin_y=0, resolution=5,
        room_pixels=bytes(mask),
        room_outline_width=_W, room_outline_height=_H,
        room_outline_origin_x=0, room_outline_origin_y=0,
        room_names={k: f"R{k}" for k in range(4)},
        room_id_offset=1,
    )
    payload = build_map_geometry(map_data, revision=1)

    occupancy = zlib.decompress(base64.b64decode(payload["occupancy"]))
    rooms = zlib.decompress(base64.b64decode(payload["rooms_grid"]))
    expected = []
    for i, cell in enumerate(occupancy):
        pv = cell & 3
        sub_type = (cell >> 2) & 3
        stored = rooms[i]
        # render_map_png's own rule, map_stream.py:486-492
        if stored > 0 and (sub_type == 0 or pv in (2, 3)):
            expected.append(f"room{stored}")
        else:
            expected.append(f"pv{pv}")
    return {"payload": payload, "expected": expected}


if __name__ == "__main__":
    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE.write_text(json.dumps(build(), indent=2) + "\n")
    print(f"wrote {FIXTURE}")
