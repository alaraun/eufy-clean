"""Test component setup."""

import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import homeassistant
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import issue_registry as ir
from homeassistant.setup import async_setup_component
from packaging.requirements import Requirement
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.robovac_mqtt import async_remove_config_entry_device
from custom_components.robovac_mqtt.api.cloud import (
    EufyLoginChallengeError,
    EufyLoginError,
    EufyLoginRateLimitedError,
    EufyLoginTransientError,
    EufySessionReplacedError,
)
from custom_components.robovac_mqtt.auth_store import AuthCache, AuthStore
from custom_components.robovac_mqtt.const import DOMAIN

_COMPONENT_DIR = Path(__file__).parent.parent / "custom_components" / "robovac_mqtt"


async def test_load_unload_entry(hass: HomeAssistant):
    """Test loading and unloading the integration."""
    # Create a mock config entry
    config_entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_USERNAME: "test_user",
            CONF_PASSWORD: "test_password",
        },
        entry_id="test_entry_id",
    )
    config_entry.add_to_hass(hass)

    # Mock EufyLogin and EufyCleanCoordinator
    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls, patch(
        "custom_components.robovac_mqtt.EufyCleanCoordinator"
    ) as mock_coord_cls:

        # Setup Login mock
        mock_login = mock_login_cls.return_value
        mock_login.init = AsyncMock()
        mock_login.mqtt_devices = [
            {
                "deviceId": "test_device_id",
                "deviceModel": "T2118",
                "deviceName": "Test Vac",
                "dps": {},
            }
        ]
        mock_login.cloud_devices = []

        # Setup Coordinator mock
        mock_coord = mock_coord_cls.return_value
        mock_coord.initialize = AsyncMock()
        # Unloading the entry awaits the coordinator's teardown.
        mock_coord.async_teardown = AsyncMock()
        mock_coord.device_id = "test_device_id"
        mock_coord.device_name = "Test Vac"
        mock_coord.device_model = "T2118"
        mock_coord.data = MagicMock()  # Mock the VacuumState data

        # Mock client and disconnect method
        mock_coord.client = MagicMock()
        mock_coord.client.disconnect = AsyncMock()

        # Setup the config entry
        result = await hass.config_entries.async_setup(config_entry.entry_id)
        assert result is True, f"Async setup failed, result: {result}"

        await hass.async_block_till_done()

        # Check if the entry state is LOADED
        assert (
            config_entry.state == ConfigEntryState.LOADED
        ), f"Entry state is {config_entry.state}, expected {ConfigEntryState.LOADED}"

        # Verify calls
        mock_login_cls.assert_called_with(
            "test_user",
            "test_password",
            unittest.mock.ANY,
            websession=unittest.mock.ANY,
            auth_cache=unittest.mock.ANY,
        )
        mock_login.init.assert_called_once()
        mock_coord_cls.assert_called_once()
        mock_coord.initialize.assert_called_once()

        # Unload the config entry
        unload_result = await hass.config_entries.async_unload(config_entry.entry_id)
        assert unload_result is True, f"Unload failed, result: {unload_result}"

        await hass.async_block_till_done()

        # Check if the entry state is NOT_LOADED
        assert (
            config_entry.state == ConfigEntryState.NOT_LOADED
        ), f"Entry state {config_entry.state}, expected {ConfigEntryState.NOT_LOADED}"


async def test_bundled_eufy_clean_card_registered(hass: HomeAssistant):
    """The bundled card registers once `frontend` is set up (load-order safe)."""
    config_entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_USERNAME: "test_user",
            CONF_PASSWORD: "test_password",
        },
        entry_id="card_entry_id",
    )
    config_entry.add_to_hass(hass)

    when_setup_calls: list = []

    def fake_when_setup(_hass, component, callback):
        when_setup_calls.append((component, callback))

    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls, patch(
        "custom_components.robovac_mqtt.EufyCleanCoordinator"
    ) as mock_coord_cls, patch(
        "custom_components.robovac_mqtt.add_extra_js_url"
    ) as mock_add_js, patch(
        "custom_components.robovac_mqtt.async_when_setup", side_effect=fake_when_setup
    ):
        mock_login = mock_login_cls.return_value
        mock_login.init = AsyncMock()
        # Card registration is independent of devices, but setup requires at
        # least one initialized coordinator (ConfigEntryNotReady otherwise).
        mock_login.mqtt_devices = [
            {
                "deviceId": "card_device_id",
                "deviceModel": "T2118",
                "deviceName": "Card Vac",
                "dps": {},
            }
        ]
        mock_login.cloud_devices = []

        mock_coord = mock_coord_cls.return_value
        mock_coord.initialize = AsyncMock()
        # Unloading the entry awaits the coordinator's teardown.
        mock_coord.async_teardown = AsyncMock()
        mock_coord.device_id = "card_device_id"
        mock_coord.device_name = "Card Vac"
        mock_coord.device_model = "T2118"
        mock_coord.data = MagicMock()
        mock_coord.client = MagicMock()
        mock_coord.client.disconnect = AsyncMock()

        result = await hass.config_entries.async_setup(config_entry.entry_id)
        assert result is True, f"Async setup failed, result: {result}"
        await hass.async_block_till_done()

        # Registration is DEFERRED to the frontend component, not done inline — this
        # is what prevents the load-order race (entry set up before frontend) that
        # left the card unregistered on some installs (#140).
        assert len(when_setup_calls) == 1
        component, register_cb = when_setup_calls[0]
        assert component == "frontend"
        mock_add_js.assert_not_called()  # nothing registered until frontend is up

        # Frontend becomes ready -> the card registers once, with a cache-bust URL.
        await register_cb(hass, "frontend")
        assert hass.data[DOMAIN]["card_registered"] is True
        mock_add_js.assert_called_once()
        registered_url = mock_add_js.call_args.args[1]
        assert registered_url.startswith("/robovac_mqtt/eufy-clean-card.js?v=")

        # Idempotent: a second frontend-ready callback does nothing.
        await register_cb(hass, "frontend")
        mock_add_js.assert_called_once()


async def test_mixed_mqtt_and_cloud_device_setup(hass: HomeAssistant):
    """Test setup with both MQTT (novel) and cloud (legacy) devices."""
    config_entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_USERNAME: "test_user",
            CONF_PASSWORD: "test_password",
        },
        entry_id="test_mixed_entry",
    )
    config_entry.add_to_hass(hass)

    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls, patch(
        "custom_components.robovac_mqtt.EufyCleanCoordinator"
    ) as mock_coord_cls:

        mock_login = mock_login_cls.return_value
        mock_login.init = AsyncMock()
        mock_login.mqtt_devices = [
            {
                "deviceId": "mqtt_dev_1",
                "deviceModel": "T2261",
                "deviceName": "X8 Pro",
                "dps": {"153": "something"},
                "apiType": "novel",
                "mqtt": True,
            }
        ]
        mock_login.cloud_devices = [
            {
                "deviceId": "cloud_dev_1",
                "deviceModel": "T2210",
                "deviceName": "G30",
                "dps": {"15": "Running"},
                "apiType": "legacy",
                "mqtt": False,
            }
        ]

        # Track coordinator creation calls
        coordinators = []

        def make_coordinator(*args, **kwargs):
            coord = MagicMock()
            coord.initialize = AsyncMock()
            device_info = args[2] if len(args) > 2 else kwargs.get("device_info", {})
            coord.device_id = device_info["deviceId"]
            coord.device_name = device_info["deviceName"]
            coord.device_model = device_info["deviceModel"]
            coord.data = MagicMock()
            coord.client = MagicMock()
            coord.client.disconnect = AsyncMock()
            coordinators.append((coord, device_info))
            return coord

        mock_coord_cls.side_effect = make_coordinator

        result = await hass.config_entries.async_setup(config_entry.entry_id)
        assert result is True
        await hass.async_block_till_done()

        # Should have created 2 coordinators
        assert len(coordinators) == 2

        device_ids = {info["deviceId"] for _, info in coordinators}
        assert "mqtt_dev_1" in device_ids
        assert "cloud_dev_1" in device_ids

        # Both should have initialize() called
        for coord, _ in coordinators:
            coord.initialize.assert_called_once()


async def test_setup_auth_failure_raises_config_entry_auth_failed(hass: HomeAssistant):
    """Login failure with EufyLoginError should result in SETUP_ERROR (auth failed)."""
    config_entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_USERNAME: "bad_user", CONF_PASSWORD: "bad_pass"},
        entry_id="test_auth_fail",
    )
    config_entry.add_to_hass(hass)

    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls:
        mock_login = mock_login_cls.return_value
        mock_login.init = AsyncMock(side_effect=EufyLoginError("Invalid credentials"))

        await hass.config_entries.async_setup(config_entry.entry_id)
        await hass.async_block_till_done()

    assert config_entry.state == ConfigEntryState.SETUP_ERROR


async def test_setup_network_failure_raises_config_entry_not_ready(hass: HomeAssistant):
    """Network errors should result in SETUP_RETRY (not ready)."""
    config_entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_USERNAME: "user", CONF_PASSWORD: "pass"},
        entry_id="test_network_fail",
    )
    config_entry.add_to_hass(hass)

    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls:
        mock_login = mock_login_cls.return_value
        mock_login.init = AsyncMock(
            side_effect=aiohttp.ClientError("Connection refused")
        )

        await hass.config_entries.async_setup(config_entry.entry_id)
        await hass.async_block_till_done()

    assert config_entry.state == ConfigEntryState.SETUP_RETRY


def _mock_login(mock_login_cls, *device_ids: str) -> MagicMock:
    mock_login = mock_login_cls.return_value
    mock_login.init = AsyncMock()
    mock_login.mqtt_devices = [
        {"deviceId": d, "deviceModel": "T2118", "deviceName": f"Vac {d}", "dps": {}}
        for d in device_ids
    ]
    mock_login.cloud_devices = []
    return mock_login


def _mock_coordinator(device_id: str, init_error: Exception | None = None) -> MagicMock:
    coord = MagicMock()
    coord.initialize = AsyncMock(side_effect=init_error)
    coord.async_teardown = AsyncMock()
    coord.device_id = device_id
    coord.device_name = f"Vac {device_id}"
    coord.device_model = "T2118"
    coord.last_seen_segments = None
    coord.data = MagicMock()
    return coord


async def test_failed_coordinator_init_is_torn_down(hass: HomeAssistant):
    """A coordinator whose initialize() raises is torn down, not just dropped.

    initialize() writes the MQTT cert/key files and may start a transport before
    it fails; every ConfigEntryNotReady retry would otherwise leak another set.
    """
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_USERNAME: "user@example.com", CONF_PASSWORD: "pw"},
        entry_id="init_fail_entry",
    )
    entry.add_to_hass(hass)
    failed = _mock_coordinator("dev_fail", init_error=OSError("broker refused"))

    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls, patch(
        "custom_components.robovac_mqtt.EufyCleanCoordinator", return_value=failed
    ):
        _mock_login(mock_login_cls, "dev_fail")
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_RETRY
    failed.async_teardown.assert_awaited_once()


async def test_transient_login_failure_retries_and_keeps_tokens(hass: HomeAssistant):
    """A 429/5xx/network login failure retries setup and keeps the cached tokens."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_USERNAME: "user@example.com", CONF_PASSWORD: "pw"},
        entry_id="transient_entry",
    )
    entry.add_to_hass(hass)

    await AuthStore(hass, entry.entry_id).async_save(
        AuthCache(openudid="0123456789abcdef", session={"access_token": "kept"})
    )

    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls:
        mock_login_cls.return_value.init = AsyncMock(
            side_effect=EufyLoginTransientError("HTTP 503")
        )
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_RETRY
    saved = await AuthStore(hass, entry.entry_id).async_load()
    assert saved.session == {"access_token": "kept"}


async def test_global_registration_runs_once_for_several_entries(hass: HomeAssistant):
    """The frontend hook and websocket commands register in async_setup, once."""
    with patch(
        "custom_components.robovac_mqtt.async_when_setup"
    ) as mock_when_setup, patch(
        "custom_components.robovac_mqtt.async_setup_websocket_api"
    ) as mock_ws, patch(
        "custom_components.robovac_mqtt.EufyLogin"
    ) as mock_login_cls, patch(
        "custom_components.robovac_mqtt.EufyCleanCoordinator",
        side_effect=lambda *a, **k: _mock_coordinator(a[2]["deviceId"]),
    ):
        _mock_login(mock_login_cls, "dev_a")
        for n in range(2):
            MockConfigEntry(
                domain=DOMAIN,
                data={CONF_USERNAME: f"user{n}@example.com", CONF_PASSWORD: "pw"},
                entry_id=f"entry_{n}",
            ).add_to_hass(hass)
        assert await async_setup_component(hass, DOMAIN, {})
        await hass.async_block_till_done()
        await hass.config_entries.async_reload("entry_0")
        await hass.async_block_till_done()

    assert mock_when_setup.call_count == 1
    assert mock_ws.call_count == 1


async def test_remove_device_refused_while_the_api_still_returns_it(
    hass: HomeAssistant,
):
    """Only a device the Eufy API no longer returns can be removed by the user."""
    entry = MockConfigEntry(domain=DOMAIN, data={}, entry_id="rm_entry")
    entry.add_to_hass(hass)
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        "coordinators": [_mock_coordinator("dev_live")]
    }
    registry = dr.async_get(hass)
    live = registry.async_get_or_create(
        config_entry_id=entry.entry_id, identifiers={(DOMAIN, "dev_live")}
    )
    gone = registry.async_get_or_create(
        config_entry_id=entry.entry_id, identifiers={(DOMAIN, "dev_gone")}
    )

    assert await async_remove_config_entry_device(hass, entry, live) is False
    assert await async_remove_config_entry_device(hass, entry, gone) is True


def _key_tree(node: object, prefix: str = "") -> set[str]:
    if not isinstance(node, dict):
        return {prefix}
    keys: set[str] = set()
    for key, value in node.items():
        keys |= _key_tree(value, f"{prefix}/{key}")
    return keys


def test_translations_match_strings_json():
    """translations/en.json carries every strings.json key.

    Custom integrations load only translations/<lang>.json, so a key that exists
    only in strings.json shows up in the UI as its raw key.
    """
    strings = json.loads((_COMPONENT_DIR / "strings.json").read_text())
    en = json.loads((_COMPONENT_DIR / "translations" / "en.json").read_text())

    assert _key_tree(en) == _key_tree(strings)


def test_manifest_declares_every_runtime_import_within_ha_constraints():
    """paho-mqtt and protobuf are imported at runtime, so the manifest declares them.

    HA installs them only for its own mqtt integration; a Core venv without it
    fails to import the component. The ranges must admit HA's pinned versions.
    """
    manifest = json.loads((_COMPONENT_DIR / "manifest.json").read_text())
    reqs = {
        (r := Requirement(line)).name.lower(): r for line in manifest["requirements"]
    }
    assert {"paho-mqtt", "protobuf"} <= set(reqs)

    constraints = (
        Path(homeassistant.__file__).parent / "package_constraints.txt"
    ).read_text()
    pins = {
        name.lower(): version
        for name, _, version in (
            line.partition("==") for line in constraints.splitlines() if "==" in line
        )
    }
    for name in ("paho-mqtt", "protobuf"):
        assert reqs[name].specifier.contains(pins[name]), (name, pins[name])


async def test_session_replaced_raises_reauth_and_a_repair_issue(hass: HomeAssistant):
    """Another client's login: reauth (the user's decision), plus a repair issue."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_USERNAME: "user@example.com", CONF_PASSWORD: "pw"},
        entry_id="replaced_entry",
        title="user@example.com",
    )
    entry.add_to_hass(hass)

    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls:
        mock_login_cls.return_value.init = AsyncMock(
            side_effect=EufySessionReplacedError("kicked out")
        )
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_ERROR
    assert ir.async_get(hass).async_get_issue(DOMAIN, "session_replaced_replaced_entry")
    assert any(
        flow["context"]["source"] == "reauth"
        for flow in hass.config_entries.flow.async_progress()
    )


async def test_rate_limit_retries_and_persists_the_hold_off(hass: HomeAssistant):
    """A throttle keeps the tokens and persists the hold-off for the next retry."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_USERNAME: "user@example.com", CONF_PASSWORD: "pw"},
        entry_id="throttled_entry",
    )
    entry.add_to_hass(hass)
    await AuthStore(hass, entry.entry_id).async_save(
        AuthCache(openudid="0123456789abcdef", session={"access_token": "kept"})
    )

    def _factory(*args, **kwargs):
        cache = kwargs["auth_cache"]
        instance = MagicMock()

        async def _init():
            cache.throttle["hold_off"] = {"login": 9e12}
            raise EufyLoginRateLimitedError("100028", retry_after=7200, code=100028)

        instance.init = AsyncMock(side_effect=_init)
        return instance

    with patch("custom_components.robovac_mqtt.EufyLogin", side_effect=_factory):
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_RETRY
    saved = await AuthStore(hass, entry.entry_id).async_load()
    assert saved.session == {"access_token": "kept"}
    assert saved.throttle["hold_off"] == {"login": 9e12}


async def test_login_challenge_asks_for_reauth(hass: HomeAssistant):
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_USERNAME: "user@example.com", CONF_PASSWORD: "pw"},
        entry_id="challenge_entry",
    )
    entry.add_to_hass(hass)

    with patch("custom_components.robovac_mqtt.EufyLogin") as mock_login_cls:
        mock_login_cls.return_value.init = AsyncMock(
            side_effect=EufyLoginChallengeError("captcha", code=100032)
        )
        await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_ERROR
