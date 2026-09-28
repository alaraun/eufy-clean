"""Shared helpers for protocol-dependent entity support.

Entities declare the DPS protocols they support via ``supported_api_types``, a
tuple of protocol names or ``None`` (the default) for universal entities.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from homeassistant.helpers.entity import Entity

from .const import API_TYPE_NOVEL, API_TYPE_SCALAR

if TYPE_CHECKING:
    from .coordinator import EufyCleanCoordinator

_EntityT = TypeVar("_EntityT", bound=Entity)


def effective_api_type(api_type: str) -> str:
    """Map a cloud-reported api type onto the two supported DPS protocols.

    Anything not scalar (novel, legacy, unknown) is treated as novel, mirroring
    api/parser.update_state dispatch.
    """
    return API_TYPE_SCALAR if api_type == API_TYPE_SCALAR else API_TYPE_NOVEL


def has_firmware_api(coordinator: EufyCleanCoordinator) -> bool:
    """Whether firmware status and OTA calls can reach the Tuya Thing API."""
    login = getattr(coordinator, "eufy_login", None)
    return getattr(login, "tuya_thing_client", None) is not None


def filter_supported_entities(
    coordinator: EufyCleanCoordinator, entities: list[_EntityT]
) -> list[_EntityT]:
    """Return only the entities supported by the device's DPS protocol."""
    api_type = effective_api_type(coordinator.api_type)
    return [
        entity
        for entity in entities
        if (supported := getattr(entity, "supported_api_types", None)) is None
        or api_type in supported
    ]
