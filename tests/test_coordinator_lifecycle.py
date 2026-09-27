"""Coordinator lifecycle: setup failure, teardown, background work and transport state.

Complements test_coordinator.py, which covers parsing and the map pipeline.
"""

# pylint: disable=redefined-outer-name, protected-access

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.exceptions import ServiceValidationError

from custom_components.robovac_mqtt.api.map_stream import MapData
from custom_components.robovac_mqtt.coordinator import (
    _CONNECTION_LOSS_GRACE,
    _MAP_STATE_SAVE_DELAY,
    EufyCleanCoordinator,
    _move_shapes,
)
from custom_components.robovac_mqtt.models import VacuumState

_MOD = "custom_components.robovac_mqtt.coordinator"


def _make_map(width: int = 40, height: int = 40, tag: int = 0) -> MapData:
    cells = width * height
    return MapData(
        raw_pixels=bytes([0b10101010 ^ tag]) * ((cells + 3) // 4),
        width=width,
        height=height,
        origin_x=-100,
        origin_y=-50,
        resolution=5,
    )


@pytest.fixture
def hass():
    """A hass mock whose tasks and executor jobs run on the test loop."""
    hass = MagicMock()
    hass.async_create_task = lambda coro, *a, **kw: asyncio.ensure_future(coro)
    hass.config_entries.async_get_entry.return_value = None

    async def _executor(func, *args):
        return func(*args)

    hass.async_add_executor_job = _executor
    return hass


@pytest.fixture
def login():
    login = MagicMock()
    login.openudid = "test_udid"
    login.checkLogin = AsyncMock()
    login.mqtt_credentials = {
        "user_id": "uid",
        "app_name": "app",
        "thing_name": "thing",
        "certificate_pem": "cert",
        "private_key": "key",
        "endpoint_addr": "broker.example.com",
    }
    return login


def _coordinator(hass, login, **extra) -> EufyCleanCoordinator:
    info = {"deviceId": "dev1", "deviceModel": "T2118", "deviceName": "Test Vac"}
    info.update(extra)
    coordinator = EufyCleanCoordinator(hass, login, info)
    coordinator._store = MagicMock()
    coordinator._store.async_load = AsyncMock(return_value={})
    coordinator._store.async_save = AsyncMock()
    return coordinator


# --- setup failure and teardown -----------------------------------------------


async def test_a_failed_mqtt_connect_disconnects_the_client(hass, login):
    """The client wrote its key files; a failed setup must remove them."""
    coordinator = _coordinator(hass, login)
    with patch(f"{_MOD}.EufyCleanClient") as client_cls:
        client = client_cls.return_value
        client.connect = AsyncMock(side_effect=OSError("no route"))
        client.disconnect = AsyncMock()
        with pytest.raises(OSError):
            await coordinator.initialize()

    client.set_connection_listener.assert_called_once_with(
        coordinator._on_connection_change
    )
    client.disconnect.assert_awaited_once()
    assert coordinator.client is None


async def test_a_failed_local_setup_after_connect_stops_the_listener(hass, login):
    """A raise after connect() must not leave the listen/heartbeat tasks running."""
    coordinator = _coordinator(
        hass, login, connection_type="local", local_key="k" * 16,
        local_host="192.168.1.50",
    )
    coordinator.async_ensure_legacy_map = AsyncMock(side_effect=RuntimeError("boom"))
    client = MagicMock()
    client.connect = AsyncMock()
    client.disconnect = AsyncMock()
    client.version = 3.3
    with patch(f"{_MOD}.LocalTuyaClient", return_value=client):
        with pytest.raises(RuntimeError):
            await coordinator.initialize()

    client.disconnect.assert_awaited_once()
    assert coordinator.client is None


async def test_local_setup_hands_dps_dicts_to_the_coordinator(hass, login):
    """The local transport calls the DPS entry point, not a JSON envelope."""
    coordinator = _coordinator(
        hass, login, connection_type="local", local_key="k" * 16,
        local_host="192.168.1.50",
    )
    client = MagicMock()
    client.connect = AsyncMock()
    client.version = 3.3
    with patch(f"{_MOD}.LocalTuyaClient", return_value=client):
        await coordinator.initialize()

    client.set_on_dps.assert_called_once_with(coordinator._handle_dps)
    client.set_on_message.assert_not_called()


async def test_teardown_is_safe_on_a_coordinator_that_never_initialized(hass, login):
    """__init__ tears down after any initialize() failure, however early."""
    coordinator = _coordinator(hass, login)
    await coordinator.async_teardown()
    await coordinator.async_teardown()
    # Nothing was loaded, so nothing may overwrite what is on disk.
    coordinator._store.async_save.assert_not_awaited()


async def test_teardown_persists_the_latest_map_state(hass, login):
    """Changes inside the save delay must survive a restart."""
    coordinator = _coordinator(hass, login)
    await coordinator.async_load_storage()
    coordinator._map_data = _make_map()
    coordinator._robot_trail = [(1, 2), (3, 4)]

    await coordinator.async_teardown()

    saved = coordinator._store.async_save.await_args[0][0]
    assert saved["robot_trail"] == [(1, 2), (3, 4)]
    assert saved["map_data"]["width"] == 40


async def test_teardown_ends_every_map_subscription(hass, login):
    """Subscribers get a terminal event so the card resubscribes to the new instance."""
    coordinator = _coordinator(hass, login)
    first, second = [], []

    def _broken(_event):
        raise RuntimeError("dead socket")

    coordinator.async_add_map_listener(_broken)
    coordinator.async_add_map_listener(first.append)
    coordinator.async_add_map_listener(second.append)
    flush_cancel = MagicMock()
    coordinator._map_flush_cancel = flush_cancel

    await coordinator.async_teardown()

    assert first == [{"t": "gone"}]
    assert second == [{"t": "gone"}]
    assert not coordinator._map_listeners
    flush_cancel.assert_called_once()
    assert coordinator._map_flush_cancel is None


# --- teardown racing background work -----------------------------------------


async def test_teardown_during_login_starts_no_pose_stream(hass, login):
    """A login that returns after teardown must not start paho or arm timers."""
    coordinator = _coordinator(hass, login, apiType="legacy")
    thing = MagicMock()
    thing.sid = None

    async def _login(**_kw):
        coordinator.async_shutdown_timers()

    thing.login = AsyncMock(side_effect=_login)
    login.tuya_thing_client = thing
    with patch(f"{_MOD}.build_connect_params", return_value=object()), patch(
        f"{_MOD}.TuyaMobileMQTT"
    ) as mqtt_cls, patch(f"{_MOD}.async_track_time_interval") as track:
        await coordinator.async_start_legacy_pose_stream()

    mqtt_cls.assert_not_called()
    track.assert_not_called()
    assert coordinator._tuya_mqtt is None


async def test_the_refresh_timer_arms_the_keepalive_a_failed_start_skipped(
    hass, login
):
    """After a failed first start, the 90 min reconnect must bring the keepalive too."""
    coordinator = _coordinator(hass, login, apiType="legacy")
    thing = MagicMock()
    thing.sid = "sid"
    thing.login = AsyncMock()
    login.tuya_thing_client = thing
    with patch(f"{_MOD}.build_connect_params", return_value=None), patch(
        f"{_MOD}.async_track_time_interval", return_value=MagicMock()
    ):
        await coordinator.async_start_legacy_pose_stream()
    assert coordinator._map_keepalive_cancel is None, "no stream, no keepalive"

    with patch(f"{_MOD}.build_connect_params", return_value=object()), patch(
        f"{_MOD}.TuyaMobileMQTT"
    ), patch(f"{_MOD}.async_track_time_interval", return_value=MagicMock()):
        await coordinator._async_refresh_legacy_pose_stream()
    assert coordinator._map_keepalive_cancel is not None


async def test_background_work_is_owned_by_the_config_entry(hass, login):
    """Unload cancels entry background tasks; plain hass tasks outlive it."""
    coordinator = _coordinator(hass, login)
    entry = MagicMock()
    coordinator.config_entry = entry

    async def _work():
        return None

    coordinator._spawn(_work(), "probe")
    entry.async_create_background_task.assert_called_once()
    entry.async_create_background_task.call_args[0][1].close()

    coordinator.async_shutdown_timers()
    coro = _work()
    coordinator._spawn(coro, "probe")
    assert entry.async_create_background_task.call_count == 1
    assert coro.cr_frame is None, "closed, never scheduled"


# --- request pacing and persistence ---------------------------------------------


def test_schedule_fetch_retries_at_most_every_cooldown(login):
    """Until a fetch succeeds each DPS message asked again, up to 6 requests each."""
    hass = MagicMock()
    coordinator = _coordinator(hass, login, apiType="legacy")
    login.tuya_thing_client = MagicMock()

    coordinator._maybe_refresh_legacy_schedules()
    coordinator._maybe_refresh_legacy_schedules()
    assert hass.async_create_task.call_count == 1

    coordinator._sched_attempt_ts -= 1000
    coordinator._maybe_refresh_legacy_schedules()
    assert hass.async_create_task.call_count == 2


async def test_map_changes_are_saved_on_the_trailing_edge(hass, login):
    """The last change inside the window is written, not dropped."""
    coordinator = _coordinator(hass, login)
    await coordinator.async_load_storage()
    coordinator._map_data = _make_map()

    coordinator._rerender_map()
    coordinator._robot_trail = [(5, 6)]
    coordinator._rerender_map()

    calls = coordinator._store.async_delay_save.call_args_list
    assert calls and all(c.args[1] == _MAP_STATE_SAVE_DELAY for c in calls)
    doc = calls[-1].args[0]()
    assert doc["robot_trail"] == [(5, 6)]
    coordinator._store.async_save.assert_not_awaited()


async def test_concurrent_first_saves_share_one_document(hass, login):
    """Two first writers must not each load a dict and drop the other's keys."""
    coordinator = _coordinator(hass, login)
    gate = asyncio.Event()

    async def _slow_load():
        await gate.wait()
        return {}

    coordinator._store.async_load = AsyncMock(side_effect=_slow_load)
    first = asyncio.ensure_future(coordinator._async_store_save(a=1))
    second = asyncio.ensure_future(coordinator._async_store_save(b=2))
    await asyncio.sleep(0)
    gate.set()
    await asyncio.gather(first, second)

    assert coordinator._store.async_load.await_count == 1
    assert coordinator._store_data == {"a": 1, "b": 2}


# --- one parse path ---------------------------------------------------------------


async def test_cloud_polled_devices_get_error_notifications(hass, login):
    """The cloud-poll path shares the push path's error and dock handling."""
    coordinator = _coordinator(hass, login, mqtt=False, apiType="legacy")
    coordinator._notify_error = MagicMock()
    login.getCloudDevice = AsyncMock(return_value={"106": 5})
    parsed = VacuumState(error_code=5, error_message="stuck", dock_status="Washing")
    with patch.object(
        coordinator, "_parse_dps",
        return_value=(parsed, {"error_code": 5, "dock_status": "Washing"}),
    ), patch(f"{_MOD}.async_call_later") as later:
        state = await coordinator._async_update_data()

    coordinator._notify_error.assert_called_once_with(5, "stuck")
    assert coordinator._pending_dock_status == "Washing"
    later.assert_called_once()
    # Published only once the debounce commits it.
    assert state.dock_status == coordinator.data.dock_status


def test_a_raw_dps_only_message_does_not_fan_out(login):
    """Only raw_dps changed: no entity state write for any entity."""
    coordinator = _coordinator(MagicMock(), login)
    coordinator.async_set_updated_data = MagicMock()
    new_state = VacuumState(raw_dps={"999": 1})
    with patch(f"{_MOD}.update_state", return_value=(new_state, {"raw_dps": {"999": 1}})):
        coordinator._handle_dps({"999": 1})

    coordinator.async_set_updated_data.assert_not_called()
    assert coordinator.data.raw_dps == {"999": 1}


# --- the novel map stream is decoded off the loop --------------------------------


async def test_a_large_biz_frame_is_decoded_in_the_executor(login):
    """The LZ4/protobuf decode must not run inside the MQTT callback."""
    hass = MagicMock()
    pending: list = []

    def _task(coro, *_a, **_kw):
        pending.append(asyncio.ensure_future(coro))
        return pending[-1]

    hass.async_create_task = _task
    hass.async_add_executor_job = AsyncMock(return_value=("map", _make_map()))
    coordinator = _coordinator(hass, login)
    decode = MagicMock()
    with patch(f"{_MOD}.parse_biz_protocol41", return_value=(3, "ab" * 500)), patch(
        f"{_MOD}.try_extract_map_data", decode
    ), patch(f"{_MOD}.try_extract_map_description", decode):
        coordinator._handle_biz_message(b"frame")
        decode.assert_not_called()
        assert len(pending) == 1
        await pending[0]

    assert hass.async_add_executor_job.await_args.args[1] == "ab" * 500
    assert coordinator._map_data is not None
    assert coordinator._map_data_chan_id == 3


async def test_a_stale_map_decode_does_not_overwrite_a_newer_one(login):
    """Executor jobs can finish out of order; the older frame is dropped."""
    hass = MagicMock()
    pending: list = []

    def _task(coro, *_a, **_kw):
        pending.append(asyncio.ensure_future(coro))
        return pending[-1]

    hass.async_create_task = _task
    maps = {"aa" * 500: _make_map(tag=1), "bb" * 500: _make_map(tag=2)}
    gates = {hex_data: asyncio.Event() for hex_data in maps}

    async def _executor(_func, hex_data, _candidate):
        await gates[hex_data].wait()
        return "map", maps[hex_data]

    hass.async_add_executor_job = _executor
    coordinator = _coordinator(hass, login)
    coordinator._rerender_map = MagicMock()
    for hex_data in ("aa" * 500, "bb" * 500):
        with patch(f"{_MOD}.parse_biz_protocol41", return_value=(3, hex_data)):
            coordinator._handle_biz_message(b"frame")
    older, newer = pending[0], pending[1]
    await asyncio.sleep(0)

    gates["bb" * 500].set()
    await newer
    gates["aa" * 500].set()
    await older

    assert coordinator._map_data is maps["bb" * 500]


# --- logs carry counts, not payloads --------------------------------------------


async def test_logs_carry_no_host_or_command_payload(hass, login, caplog):
    """The LAN address and command contents stay out of the log."""
    caplog.set_level(logging.DEBUG, logger=_MOD)
    coordinator = _coordinator(
        hass, login, connection_type="local", local_key="k" * 16,
        local_host="192.168.1.50",
    )
    client = MagicMock()
    client.connect = AsyncMock()
    client.send_command = AsyncMock()
    client.version = 3.3
    with patch(f"{_MOD}.LocalTuyaClient", return_value=client):
        await coordinator.initialize()
    await coordinator.async_send_command({"154": "c2VjcmV0LXpvbmU="})
    coordinator._on_legacy_pose(12345, 23456, 90)

    text = caplog.text
    assert "192.168.1.50" not in text
    assert "c2VjcmV0LXpvbmU=" not in text
    assert "12345" not in text


# --- service input -----------------------------------------------------------------


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), "nan"])
def test_non_finite_map_points_are_a_validation_error(login, bad):
    """NaN passed the clamp; inf reached round() and raised a raw error."""
    coordinator = _coordinator(MagicMock(), login)
    coordinator._map_data = _make_map()
    with pytest.raises(ServiceValidationError):
        coordinator._normalized_to_cm(bad, 0.5)
    with pytest.raises(ServiceValidationError):
        coordinator.room_id_at_normalized(0.5, bad)
    with pytest.raises(ServiceValidationError):
        _move_shapes([[(0, 0), (10, 0)]], [{"index": 0, "dx": bad}], "walls")
    with pytest.raises(ServiceValidationError):
        _move_shapes([[(0, 0), (10, 0)]], [{"index": 0, "rotate": bad}], "walls")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"mode": "room"},
        {"mode": "spot"},
        {"rooms": [1]},
        {"water": "High"},
        {"clean_times": 2},
    ],
)
async def test_scalar_schedules_reject_fields_dps_151_cannot_carry(hass, login, kwargs):
    """Silently ignored fields looked applied in the Schedules sensor."""
    coordinator = _coordinator(hass, login, apiType="scalar")
    coordinator.async_send_command = AsyncMock()
    with pytest.raises(ServiceValidationError):
        await coordinator.async_set_schedule(time="08:00", **kwargs)
    coordinator.async_send_command.assert_not_awaited()


async def test_a_scalar_schedule_stores_only_what_was_sent(hass, login):
    """The optimistic entry mirrors DPS 151: no clean_times."""
    coordinator = _coordinator(hass, login, apiType="scalar")
    coordinator.async_send_command = AsyncMock()

    await coordinator.async_set_schedule(time="08:00", mode="Random")

    coordinator.async_send_command.assert_awaited_once()
    (entry,) = coordinator.data.schedules
    assert "clean_times" not in entry
    assert entry["pattern"] == "Random"


# --- transport availability ----------------------------------------------------------


def _armed(later: MagicMock):
    """The callback the last async_call_later was given."""
    return later.call_args.args[2]


def test_a_short_disconnect_keeps_entities_available(login):
    """Paho reconnects within seconds; flapping to unavailable is noise."""
    coordinator = _coordinator(MagicMock(), login)
    cancel = MagicMock()
    with patch(f"{_MOD}.async_call_later", return_value=cancel) as later:
        coordinator._on_connection_change(False, False)
        assert later.call_args.args[1] == _CONNECTION_LOSS_GRACE
        coordinator._on_connection_change(True, False)

    cancel.assert_called_once()
    assert coordinator.last_update_success is True


def test_a_sustained_disconnect_marks_entities_unavailable(login):
    """Down for the whole grace period: unavailable until the broker is back."""
    coordinator = _coordinator(MagicMock(), login)
    listener = MagicMock()
    coordinator.async_add_listener(listener)
    with patch(f"{_MOD}.async_call_later", return_value=MagicMock()) as later:
        coordinator._on_connection_change(False, False)
    _armed(later)(None)
    assert coordinator.last_update_success is False

    # An optimistic write while down must not flip it back.
    coordinator.async_set_updated_data(VacuumState(battery_level=10))
    assert coordinator.last_update_success is False

    coordinator._on_connection_change(True, False)
    assert coordinator.last_update_success is True
    assert listener.call_count >= 2


def test_repeated_auth_refusals_warn_once(login, caplog):
    """One WARNING on the third refusal in a row, not one per retry."""
    coordinator = _coordinator(MagicMock(), login)
    with patch(f"{_MOD}.async_call_later", return_value=MagicMock()):
        for _ in range(5):
            coordinator._on_connection_change(False, True)
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    login.checkLogin.assert_not_called()


def test_teardown_cancels_the_pending_unavailable_timer(login):
    """A timer firing on a torn-down coordinator would touch dead entities."""
    coordinator = _coordinator(MagicMock(), login)
    cancel = MagicMock()
    with patch(f"{_MOD}.async_call_later", return_value=cancel):
        coordinator._on_connection_change(False, False)
    coordinator.async_shutdown_timers()
    cancel.assert_called_once()
    # Late events from paho's queue are ignored.
    coordinator._on_connection_change(False, False)
    assert coordinator._connection_loss_cancel is None
