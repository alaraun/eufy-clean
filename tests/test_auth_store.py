"""Unit tests for auth_store.py and the cached-login fast path in api/cloud.py."""

import json
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from custom_components.robovac_mqtt.api.cloud import (
    EufyLogin,
    EufyLoginTransientError,
    EufySessionReplacedError,
)
from custom_components.robovac_mqtt.api.http import _LOGIN_CONFIGS, EufyHTTPClient
from custom_components.robovac_mqtt.api.tuya_cloud import TuyaCloudError
from custom_components.robovac_mqtt.auth_store import (
    AuthCache,
    certificate_not_after,
    mqtt_credentials_fresh,
    new_openudid,
    session_expiry,
    trim_session,
    trim_user_info,
)

# ---------------------------------------------------------------------------
# Certificate expiry
# ---------------------------------------------------------------------------


def _self_signed(not_after_delta: timedelta) -> str:
    """A throwaway self-signed cert expiring at now+delta, as PEM."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + not_after_delta)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def _creds(pem: str) -> dict:
    """A complete credential — every field coordinator._initialize_mqtt reads."""
    return {
        "user_id": "u",
        "app_name": "eufy_home",
        "thing_name": "t",
        "certificate_pem": pem,
        "private_key": "k",
        "endpoint_addr": "e",
    }


def test_certificate_not_after_reads_a_real_cert():
    pem = _self_signed(timedelta(days=30))
    parsed = certificate_not_after(pem)
    assert parsed is not None
    assert parsed > datetime.now(timezone.utc)


def test_certificate_not_after_is_none_for_garbage():
    """An unparsable cert must read as 'cannot vouch for it', not as valid."""
    assert certificate_not_after("not a certificate") is None
    assert certificate_not_after("") is None


def test_mqtt_credentials_fresh_accepts_a_live_cert():
    assert mqtt_credentials_fresh(_creds(_self_signed(timedelta(days=30)))) is True


def test_mqtt_credentials_stale_when_the_cert_has_expired():
    assert mqtt_credentials_fresh(_creds(_self_signed(timedelta(days=-1)))) is False


def test_mqtt_credentials_stale_inside_the_expiry_skew():
    """A cert dying in an hour must not be reused across a restart."""
    assert mqtt_credentials_fresh(_creds(_self_signed(timedelta(hours=1)))) is False


def test_mqtt_credentials_stale_when_a_field_is_missing():
    creds = _creds(_self_signed(timedelta(days=30)))
    del creds["private_key"]
    assert mqtt_credentials_fresh(creds) is False


def test_mqtt_credentials_stale_when_absent():
    assert mqtt_credentials_fresh(None) is False


# ---------------------------------------------------------------------------
# AuthCache
# ---------------------------------------------------------------------------

def test_cache_generates_an_openudid_and_round_trips():
    cache = AuthCache()
    assert len(cache.openudid) == 32
    assert AuthCache.from_dict(cache.to_dict()).openudid == cache.openudid


def test_cache_from_dict_ignores_unknown_keys():
    """A store written by a future version must not crash this one."""
    cache = AuthCache.from_dict({"openudid": "abc", "something_new": 1})
    assert cache.openudid == "abc"


def test_cache_from_dict_handles_a_missing_or_empty_store():
    assert len(AuthCache.from_dict(None).openudid) == 32
    assert len(AuthCache.from_dict({}).openudid) == 32
    assert len(AuthCache.from_dict({"openudid": ""}).openudid) == 32


def test_can_skip_login_requires_a_token_and_a_live_cert():
    fresh = _creds(_self_signed(timedelta(days=30)))
    assert AuthCache(
        session={"access_token": "t"}, user_info={"x": 1}, mqtt_credentials=fresh
    ).can_skip_login is True
    # no token
    assert AuthCache(user_info={"x": 1}, mqtt_credentials=fresh).can_skip_login is False
    # expired cert
    assert AuthCache(
        session={"access_token": "t"},
        user_info={"x": 1},
        mqtt_credentials=_creds(_self_signed(timedelta(days=-1))),
    ).can_skip_login is False


def test_can_skip_login_allows_a_fallback_session_account():
    """Accounts with no user_center have no MQTT creds; that is still complete."""
    assert AuthCache(session={"access_token": "t"}).can_skip_login is True
    # ...but a user_center account missing its MQTT creds is NOT complete.
    assert AuthCache(
        session={"access_token": "t"}, user_info={"x": 1}
    ).can_skip_login is False


def test_clear_tokens_keeps_identity_and_probe_memos():
    """Rotating the openudid would invalidate the account's tokens."""
    cache = AuthCache(
        openudid="stable",
        login_label="v1 (Eufy Clean app)",
        tuya_region="US",
        session={"access_token": "t"},
        user_info={"x": 1},
        mqtt_credentials={"a": 1},
    )
    cache.clear_tokens()
    assert cache.openudid == "stable"
    assert cache.login_label == "v1 (Eufy Clean app)"
    assert cache.tuya_region == "US"
    assert cache.session is None and cache.user_info is None
    assert cache.mqtt_credentials is None


def test_new_openudid_is_random():
    assert new_openudid() != new_openudid()


# ---------------------------------------------------------------------------
# Login-config memoization
# ---------------------------------------------------------------------------

def _http() -> EufyHTTPClient:
    return EufyHTTPClient("u", "p", "udid", websession=MagicMock())


def test_login_configs_default_to_declared_order():
    """v2 must be tried before v1 for a first-time login."""
    assert [c["label"] for c in _http()._ordered_login_configs()] == [
        c["label"] for c in _LOGIN_CONFIGS
    ]


def test_login_configs_put_the_remembered_winner_first():
    client = _http()
    client.login_label = _LOGIN_CONFIGS[1]["label"]
    order = [c["label"] for c in client._ordered_login_configs()]
    assert order[0] == _LOGIN_CONFIGS[1]["label"]
    # Reordered, never filtered — the other entry is still a fallback.
    assert set(order) == {c["label"] for c in _LOGIN_CONFIGS}


def test_login_configs_ignore_an_unrecognised_memo():
    client = _http()
    client.login_label = "some endpoint that no longer exists"
    assert [c["label"] for c in client._ordered_login_configs()] == [
        c["label"] for c in _LOGIN_CONFIGS
    ]


# ---------------------------------------------------------------------------
# EufyLogin.init() fast path
# ---------------------------------------------------------------------------

def _login_with_cache(cache):
    login = EufyLogin("u", "p", "udid", websession=MagicMock(), auth_cache=cache)
    login.getDevices = AsyncMock()
    login.tuya_login = AsyncMock()
    login.getCloudDevices = AsyncMock()
    login.login = AsyncMock()
    return login


def _usable_cache() -> AuthCache:
    return AuthCache(
        session={"access_token": "t", "user_id": "uid"},
        user_info={"user_center_token": "x", "gtoken": "g"},
        mqtt_credentials=_creds(_self_signed(timedelta(days=30))),
        login_label="v2 (Eufy app)",
        tuya_region="US",
    )


@pytest.mark.asyncio
async def test_init_skips_the_login_when_the_cache_is_usable():
    login = _login_with_cache(_usable_cache())

    async def discover():
        login.mqtt_devices = [{"deviceId": "d1"}]

    login.getDevices.side_effect = discover
    await login.init()

    login.login.assert_not_awaited()
    login.getDevices.assert_awaited_once()


@pytest.mark.asyncio
async def test_init_logs_in_when_there_is_no_cache():
    login = _login_with_cache(AuthCache())
    await login.init()
    login.login.assert_awaited_once()


@pytest.mark.asyncio
async def test_stale_cache_falls_back_to_a_full_login():
    """The safety property: a stale cache must never leave us deviceless.

    The cached attempt finds nothing, so the cache is discarded and the full
    login runs — ending in exactly the state an uncached start would reach.
    """
    cache = _usable_cache()
    login = _login_with_cache(cache)
    calls = {"n": 0}

    async def discover():
        calls["n"] += 1
        # First (cached) pass finds nothing; the post-login pass finds the device.
        login.mqtt_devices = [] if calls["n"] == 1 else [{"deviceId": "d1"}]

    login.getDevices.side_effect = discover
    await login.init()

    login.login.assert_awaited_once()
    assert calls["n"] == 2
    assert login.mqtt_devices == [{"deviceId": "d1"}]


@pytest.mark.asyncio
async def test_stale_cache_clears_tokens_but_keeps_the_openudid():
    cache = _usable_cache()
    cache.openudid = "stable"
    login = _login_with_cache(cache)
    login.getDevices.side_effect = lambda: None  # never finds anything

    await login.init()

    assert cache.openudid == "stable"
    # The failed tokens must not be written back for the next restart.
    assert cache.session is None or cache.session == login.eufyApi.session


@pytest.mark.asyncio
async def test_init_captures_refreshed_tokens_into_the_cache():
    cache = AuthCache()
    login = _login_with_cache(cache)
    login.eufyApi.session = {"access_token": "new"}
    login.eufyApi.user_info = {"user_center_token": "n"}
    login.eufyApi.login_label = "v1 (Eufy Clean app)"
    login.mqtt_credentials = {"certificate_pem": "x"}

    async def discover():
        login.mqtt_devices = [{"deviceId": "d1"}]

    login.getDevices.side_effect = discover
    await login.init()

    assert cache.session == {"access_token": "new"}
    assert cache.login_label == "v1 (Eufy Clean app)"
    assert cache.mqtt_credentials == {"certificate_pem": "x"}


@pytest.mark.asyncio
async def test_a_cache_of_none_disables_caching_entirely():
    """The config-flow validation path passes no cache and must still work."""
    login = _login_with_cache(None)
    await login.init()
    login.login.assert_awaited_once()


# ---------------------------------------------------------------------------
# Tuya region memoization
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tuya_login_tries_the_remembered_region_first():
    """A US account should not pay a failed EU login on every start."""
    cache = AuthCache(tuya_region="US")
    login = EufyLogin("u", "p", "udid", websession=MagicMock(), auth_cache=cache)
    login._eufy_user_id = "uid"
    tried: list[str] = []

    class FakeClient:
        def __init__(self, region, websession=None):
            self.region = region
            tried.append(region)

        @staticmethod
        async def login(user_id):
            return None

    with patch("custom_components.robovac_mqtt.api.cloud.TuyaCloudClient", FakeClient), \
            patch("custom_components.robovac_mqtt.api.cloud.TuyaThingClient", MagicMock()):
        await login.tuya_login()

    assert tried == ["US"]
    assert login._tuya_probe_region == "US"


@pytest.mark.asyncio
async def test_tuya_login_defaults_to_eu_first_without_a_memo():
    login = EufyLogin("u", "p", "udid", websession=MagicMock(), auth_cache=AuthCache())
    login._eufy_user_id = "uid"
    tried: list[str] = []

    class FakeClient:
        def __init__(self, region, websession=None):
            self.region = region
            tried.append(region)

        @staticmethod
        async def login(user_id):
            return None

    with patch("custom_components.robovac_mqtt.api.cloud.TuyaCloudClient", FakeClient), \
            patch("custom_components.robovac_mqtt.api.cloud.TuyaThingClient", MagicMock()):
        await login.tuya_login()

    assert tried == ["EU"]


@pytest.mark.asyncio
async def test_tuya_login_falls_back_when_the_remembered_region_fails():
    """A remembered region is a hint, not a filter."""
    cache = AuthCache(tuya_region="US")
    login = EufyLogin("u", "p", "udid", websession=MagicMock(), auth_cache=cache)
    login._eufy_user_id = "uid"
    tried: list[str] = []

    class FakeClient:
        def __init__(self, region, websession=None):
            self.region = region
            tried.append(region)

        async def login(self, user_id):
            if self.region == "US":
                raise TuyaCloudError("ERR", "nope")

    with patch("custom_components.robovac_mqtt.api.cloud.TuyaCloudClient", FakeClient), \
            patch("custom_components.robovac_mqtt.api.cloud.TuyaThingClient", MagicMock()):
        await login.tuya_login()

    assert tried == ["US", "EU"]
    assert login._tuya_probe_region == "EU"


@pytest.mark.asyncio
async def test_tuya_login_raises_when_every_region_fails():
    login = EufyLogin("u", "p", "udid", websession=MagicMock(), auth_cache=AuthCache())
    login._eufy_user_id = "uid"

    class FakeClient:
        def __init__(self, region, websession=None):
            self.region = region

        @staticmethod
        async def login(user_id):
            raise TuyaCloudError("ERR", "nope")

    with patch("custom_components.robovac_mqtt.api.cloud.TuyaCloudClient", FakeClient), \
            patch("custom_components.robovac_mqtt.api.cloud.TuyaThingClient", MagicMock()):
        with pytest.raises(TuyaCloudError):
            await login.tuya_login()


# ---------------------------------------------------------------------------
# Session expiry + trimming (the login response carries `expires_in`)
# ---------------------------------------------------------------------------

def test_trim_session_keeps_only_the_fields_that_are_read():
    """The raw login response carries a refresh token and unrelated account data."""
    raw = {
        "access_token": "tok",
        "user_id": "uid",
        "refresh_token": "SECRET-REFRESH",
        "email": "someone@example.com",
        "devices": [{"id": 1}],
        "has_blood_pressure": False,
    }
    trimmed = trim_session(raw)
    assert trimmed == {"access_token": "tok", "user_id": "uid"}
    assert "refresh_token" not in trimmed
    assert "email" not in trimmed


def test_trim_user_info_keeps_only_the_user_center_fields():
    """The user-center response carries account data that is never read."""
    raw = {
        "user_center_id": "uc1",
        "user_center_token": "uct",
        "gtoken": "g",
        "email": "someone@example.com",
        "nick_name": "Someone",
    }
    assert trim_user_info(raw) == {
        "user_center_id": "uc1",
        "user_center_token": "uct",
        "gtoken": "g",
    }
    assert trim_user_info(None) is None


def test_capture_persists_a_trimmed_user_info():
    """The cache written for the next restart holds only the used fields."""
    cache = AuthCache()
    login = _login_with_cache(cache)
    login.eufyApi.user_info = {
        "user_center_id": "uc1",
        "user_center_token": "uct",
        "gtoken": "g",
        "email": "someone@example.com",
    }

    login._capture_auth_cache()

    assert cache.user_info == {
        "user_center_id": "uc1",
        "user_center_token": "uct",
        "gtoken": "g",
    }


def test_cache_from_dict_trims_a_stored_user_info():
    """A store written with the full response is trimmed on load."""
    cache = AuthCache.from_dict(
        {"user_info": {"user_center_id": "uc1", "email": "someone@example.com"}}
    )
    assert cache.user_info == {"user_center_id": "uc1"}


def test_trim_session_handles_absent_input():
    assert trim_session(None) is None
    assert trim_session({}) is None


def test_session_expiry_uses_expires_in():
    """Observed live: expires_in = 2592000 (30 days)."""
    before = time.time()
    got = session_expiry({"expires_in": 2592000})
    assert got is not None
    assert before + 2592000 <= got <= time.time() + 2592000


def test_session_expiry_is_none_without_a_usable_value():
    assert session_expiry({}) is None
    assert session_expiry({"expires_in": "not a number"}) is None
    assert session_expiry({"expires_in": 0}) is None
    assert session_expiry({"expires_in": -1}) is None
    assert session_expiry(None) is None


def test_can_skip_login_rejects_an_expired_session():
    cache = AuthCache(
        session={"access_token": "t"},
        session_expires_at=time.time() - 1,
        user_info={"x": 1},
        mqtt_credentials=_creds(_self_signed(timedelta(days=30))),
    )
    assert cache.can_skip_login is False


def test_can_skip_login_rejects_a_session_inside_the_expiry_skew():
    """A token dying in an hour must not be carried across a restart."""
    cache = AuthCache(
        session={"access_token": "t"},
        session_expires_at=time.time() + 3600,
        user_info={"x": 1},
        mqtt_credentials=_creds(_self_signed(timedelta(days=30))),
    )
    assert cache.can_skip_login is False


def test_can_skip_login_accepts_a_session_with_time_left():
    cache = AuthCache(
        session={"access_token": "t"},
        session_expires_at=time.time() + 30 * 86400,
        user_info={"x": 1},
        mqtt_credentials=_creds(_self_signed(timedelta(days=30))),
    )
    assert cache.can_skip_login is True


def test_can_skip_login_tolerates_an_unknown_expiry():
    """A cache written before session_expires_at existed must still load."""
    cache = AuthCache(
        session={"access_token": "t"},
        user_info={"x": 1},
        mqtt_credentials=_creds(_self_signed(timedelta(days=30))),
    )
    assert cache.session_expires_at is None
    assert cache.can_skip_login is True


# ---------------------------------------------------------------------------
# Hostile / hand-edited store contents must not crash setup
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "session,user_info,creds",
    [
        ("a string, not a dict", {"x": 1}, None),
        ({"access_token": "t"}, "not a dict", None),
        ({"access_token": "t"}, {"x": 1}, ["not", "a", "dict"]),
        ({"access_token": "t"}, {"x": 1}, "nope"),
    ],
)
def test_can_skip_login_survives_a_corrupt_store(session, user_info, creds):
    cache = AuthCache(session=session, user_info=user_info, mqtt_credentials=creds)
    assert cache.can_skip_login is False  # and, crucially, does not raise


def test_can_skip_login_survives_a_non_numeric_expiry():
    cache = AuthCache(
        session={"access_token": "t"},
        session_expires_at="tomorrow",
        user_info={"x": 1},
        mqtt_credentials=_creds(_self_signed(timedelta(days=30))),
    )
    assert cache.can_skip_login is False


def test_clear_tokens_also_clears_the_expiry():
    cache = AuthCache(session={"access_token": "t"}, session_expires_at=time.time() + 99)
    cache.clear_tokens()
    assert cache.session_expires_at is None


def test_cache_round_trips_through_json():
    """Store serialises to JSON; every field must survive it."""
    cache = AuthCache(
        openudid="u", login_label="v2 (Eufy app)", tuya_region="US",
        session={"access_token": "t", "user_id": "i"},
        session_expires_at=time.time() + 99,
        user_info={"gtoken": "g"}, mqtt_credentials={"certificate_pem": "p"},
    )
    restored = AuthCache.from_dict(json.loads(json.dumps(cache.to_dict())))
    assert restored == cache


def test_capture_records_the_probed_region_not_a_redirect_code():
    """TuyaCloudClient rewrites .region to Tuya's own code after a redirect.

    Storing that would put a value like "AZ" in the memo, which is not a probe
    candidate — silently disabling the optimisation forever with no signal.
    """
    cache = AuthCache()
    login = EufyLogin("u", "p", "udid", websession=MagicMock(), auth_cache=cache)
    login.tuya_client = MagicMock()
    login.tuya_client.region = "AZ"      # what a redirect leaves behind
    login._tuya_probe_region = "EU"      # what we actually probed
    login._capture_auth_cache()
    assert cache.tuya_region == "EU"


def test_capture_does_not_erase_a_good_login_label_with_none():
    """The fallback-session path never sets a label; it must not wipe the memo."""
    cache = AuthCache(login_label="v1 (Eufy Clean app)")
    login = EufyLogin("u", "p", "udid", websession=MagicMock(), auth_cache=cache)
    login.eufyApi.login_label = None
    login._capture_auth_cache()
    assert cache.login_label == "v1 (Eufy Clean app)"


# ---------------------------------------------------------------------------
# The real safety property (review finding: the old test was tautological)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_tuya_must_not_vouch_for_a_dead_eufy_session():
    """The headline bug the review found.

    Tuya authenticates with the eufy USER ID alone and never sees the bearer
    token, so on an account owning both an MQTT robot and a Tuya robot, Tuya
    discovery succeeds against a completely dead eufy session. Accepting that
    as proof would silently drop every MQTT device AND re-persist the dead
    token, repeating on every restart.
    """
    cache = _usable_cache()
    login = _login_with_cache(cache)
    calls = {"n": 0}

    async def discover():
        calls["n"] += 1
        # Eufy side is dead: the HTTP layer records the 401 rejection ...
        if calls["n"] == 1:
            login.eufyApi.auth_error = True
        login.eufy_api_devices = []
        login.mqtt_devices = []
        # ... while Tuya happily returns a device.
        login.cloud_devices = [{"deviceId": "tuya1"}]

    login.getDevices.side_effect = discover

    async def tuya():
        login.cloud_devices = [{"deviceId": "tuya1"}]

    login.getCloudDevices.side_effect = tuya
    await login.init()

    # It must NOT have accepted the cached run.
    login.login.assert_awaited_once()
    assert calls["n"] == 2, "the eufy session was never re-validated"


@pytest.mark.asyncio
async def test_tuya_only_account_still_uses_the_cache():
    """An account with 0 MQTT + 1 cloud device has empty eufy device lists;
    that must not be read as a dead token."""
    cache = _usable_cache()
    login = _login_with_cache(cache)

    async def discover():
        login.eufy_api_devices = []
        login.mqtt_devices = []
        login.cloud_devices = [{"deviceId": "tuya1"}]

    login.getDevices.side_effect = discover
    await login.init()

    login.login.assert_not_awaited()


def test_auth_error_alone_invalidates_a_cached_session():
    login = _login_with_cache(_usable_cache())
    login.cloud_devices = [{"deviceId": "tuya1"}]
    assert login._eufy_session_proved() is True
    login.eufyApi.auth_error = True
    assert login._eufy_session_proved() is False


@pytest.mark.asyncio
async def test_eufy_session_proof_is_rejection_based_not_emptiness_based():
    """Rejection is the signal; an empty list is not.

    An empty eufy list means "this account owns none of that kind" at least as
    often as it means "the token is dead" — proven live on a 0-MQTT account —
    so only an actual 401/403 invalidates the cache.
    """
    login = _login_with_cache(_usable_cache())
    login.eufy_api_devices, login.mqtt_devices, login.cloud_devices = ["x"], [], []
    assert login._eufy_session_proved() is True
    login.eufy_api_devices, login.mqtt_devices = [], ["y"]
    assert login._eufy_session_proved() is True
    # Tuya-only account: accepted, just nothing on the eufy side.
    login.eufy_api_devices, login.mqtt_devices, login.cloud_devices = [], [], ["z"]
    assert login._eufy_session_proved() is True
    # Nothing found anywhere -> something is wrong regardless.
    login.cloud_devices = []
    assert login._eufy_session_proved() is False
    # An explicit rejection always wins, even with devices present.
    login.cloud_devices = ["z"]
    login.eufyApi.auth_error = True
    assert login._eufy_session_proved() is False


@pytest.mark.asyncio
async def test_stale_cache_actually_clears_the_cached_tokens():
    """Replaces a tautological test that passed even with clear_tokens() a no-op."""
    cache = _usable_cache()
    cache.openudid = "stable"
    original_token = cache.session["access_token"]
    login = _login_with_cache(cache)
    seen = {}

    async def discover():
        # Record what clear_tokens() left behind, before _capture repopulates.
        if "after_reset" not in seen and login.login.await_count == 1:
            seen["after_reset"] = cache.session
        login.eufy_api_devices = ["d"] if login.login.await_count else []

    login.getDevices.side_effect = discover
    await login.init()

    assert seen.get("after_reset") is None, "tokens were not cleared before re-login"
    assert cache.openudid == "stable", "identity must survive a token reset"
    assert cache.session != {"access_token": original_token}


# ---------------------------------------------------------------------------
# Session-replaced latch and throttle persistence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_kicked_out_cached_session_latches_and_does_not_log_in():
    """26084 on the cached run: no login, the latch is set, the tokens dropped."""
    cache = _usable_cache()
    login = _login_with_cache(cache)

    async def discover():
        login.eufyApi.auth_error = True
        login.eufyApi.session_replaced = True

    login.getDevices.side_effect = discover

    with pytest.raises(EufySessionReplacedError):
        await login.init()

    login.login.assert_not_awaited()
    assert cache.session_replaced_at is not None
    assert cache.session is None


@pytest.mark.asyncio
async def test_a_latched_cache_refuses_before_any_request():
    cache = _usable_cache()
    cache.session_replaced_at = time.time()
    login = _login_with_cache(cache)

    with pytest.raises(EufySessionReplacedError):
        await login.init()

    login.getDevices.assert_not_awaited()
    login.login.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_server_error_on_the_cached_run_spends_no_login():
    """5xx on the device lists is transient: the cached session is kept."""
    cache = _usable_cache()
    login = _login_with_cache(cache)

    async def discover():
        login.eufyApi.transient_error = True

    login.getDevices.side_effect = discover

    with pytest.raises(EufyLoginTransientError):
        await login.init()

    login.login.assert_not_awaited()
    assert cache.session == {"access_token": "t", "user_id": "uid"}


def test_throttle_and_latch_survive_clear_tokens_and_round_trip():
    cache = _usable_cache()
    cache.throttle = {"logins": [1.0], "hold_off": {"login": 2.0}}
    cache.session_replaced_at = 3.0
    cache.clear_tokens()

    again = AuthCache.from_dict(cache.to_dict())
    assert again.throttle == {"logins": [1.0], "hold_off": {"login": 2.0}}
    assert again.session_replaced_at == 3.0
    assert again.session is None


def test_the_login_throttle_state_lives_in_the_cache():
    cache = AuthCache()
    login = EufyLogin("u", "p", "udid", websession=MagicMock(), auth_cache=cache)
    login.throttle.note_login()
    assert len(cache.throttle["logins"]) == 1
