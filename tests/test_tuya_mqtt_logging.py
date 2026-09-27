"""Debug-log cost of the Tuya mobile-MQTT frame handler (api/tuya_mqtt.py)."""

import logging
from types import SimpleNamespace

from custom_components.robovac_mqtt.api import tuya_mqtt as tm


class _CountingBytes(bytes):
    """bytes that count hex() calls, including on slices."""

    calls = 0

    def hex(self, *args, **kwargs):
        type(self).calls += 1
        return super().hex(*args, **kwargs)

    def __getitem__(self, key):
        item = super().__getitem__(key)
        return _CountingBytes(item) if isinstance(key, slice) else item


def _handle(payload: bytes) -> None:
    sub = tm.TuyaMobileMQTT(
        SimpleNamespace(), on_pose=lambda x, y, t: None  # type: ignore[arg-type]
    )
    sub._handle_message(None, None, SimpleNamespace(payload=payload))


def test_frame_debug_args_are_not_built_when_debug_is_off():
    """With debug off, a frame costs no hex formatting."""
    _CountingBytes.calls = 0
    logger = logging.getLogger(tm.__name__)
    old = logger.level
    logger.setLevel(logging.INFO)
    try:
        _handle(_CountingBytes(bytes(80)))
    finally:
        logger.setLevel(old)
    assert _CountingBytes.calls == 0


def test_frame_debug_line_is_logged_when_debug_is_on(caplog):
    """With debug on, the frame summary is still logged."""
    with caplog.at_level(logging.DEBUG, logger=tm.__name__):
        _handle(bytes(80))
    assert any("Tuya mobile-MQTT frame" in r.getMessage() for r in caplog.records)
