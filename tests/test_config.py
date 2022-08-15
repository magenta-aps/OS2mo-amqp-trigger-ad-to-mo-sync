# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=unused-argument
"""Test our settings handling."""
from typing import Any

import pytest
from more_itertools import one
from pydantic import ValidationError

from adguidsync.config import ServerConfig
from adguidsync.config import Settings


@pytest.mark.parametrize(
    "overrides,expectations",
    [
        ({}, ["client_secret"]),
        (
            {
                "CLIENT_SECRET": "Hunter2",
            },
            [
                "ad_controllers",
                "ad_domain",
                "ad_password",
                "ad_cpr_attribute",
                "ad_search_base",
            ],
        ),
        (
            {
                "CLIENT_SECRET": "Hunter2",
                "AD_CONTROLLERS": "[]",
            },
            [
                "ad_domain",
                "ad_password",
                "ad_cpr_attribute",
                "ad_search_base",
            ],
        ),
    ],
)
def test_required_settings(
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any],
    expectations: list[str],
) -> None:
    """Test that we must add certain settings."""
    for key, value in overrides.items():
        monkeypatch.setenv(key, value)

    with pytest.raises(ValidationError) as excinfo:
        Settings()

    exc_str = str(excinfo.value)
    assert " validation error" in exc_str
    for expected in expectations:
        assert f"{expected}\n  field required (type=value_error.missing)" in exc_str


def test_minimal_overrides(
    load_settings_overrides: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the minimum set of overrides that yield valid settings."""
    # We can construct settings here
    Settings()

    # Verify that removing any of the environmental variables, raises a ValidationError
    for key, value in load_settings_overrides.items():
        monkeypatch.delenv(key)
        with pytest.raises(ValidationError):
            Settings()
        monkeypatch.setenv(key, value)


def test_happy_path_server_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that we can construct a server config object with the right arguments."""
    overrides = {"host": "localhost"}
    ServerConfig(**overrides)

    with pytest.raises(ValidationError) as excinfo:
        # mypy complains about the missing host argument here, so we silence it
        ServerConfig()  # type: ignore
    assert "host\n  field required (type=value_error.missing)" in str(excinfo.value)


def test_happy_path(settings: Settings) -> None:
    """Test that settings are parsed as expected."""
    assert settings.fastramqpi.auth_realm == "mo"

    assert settings.ad_user == "os2mo"
    assert settings.ad_password.get_secret_value() == "Hunter2"

    ad_controller = one(settings.ad_controllers)
    assert ad_controller.host == "localhost"
    assert ad_controller.port is None
