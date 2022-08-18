# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=unused-argument
"""Test our settings handling."""
from typing import Any
from uuid import uuid4

import pytest
from more_itertools import one
from pydantic import parse_obj_as
from pydantic import ValidationError

from ad2mosync.config import ADMapping
from ad2mosync.config import ADMappingList
from ad2mosync.config import ServerConfig
from ad2mosync.config import Settings


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
                "ad_mappings",
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
                "ad_mappings",
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


def test_happy_path_server_config() -> None:
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

    ad_mapping = one(settings.ad_mappings)
    assert ad_mapping.ad_field == "mail"
    assert ad_mapping.mo_address_type_uuid is None


def test_admapping() -> None:
    """Test that we can construct an admapping with the right arguments."""
    overrides = {"ad_field": "mail", "mo_address_type_user_key": "AD-Email"}
    ADMapping(**overrides)

    with pytest.raises(ValidationError) as excinfo:
        # mypy complains about the missing arguments here, so we silence it
        ADMapping()  # type: ignore
    assert "ad_field\n  field required (type=value_error.missing)" in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        # mypy complains about the missing arguments here, so we silence it
        ADMapping(ad_field="mail")  # type: ignore
    assert "One of 'mo_address_type_user_key'" in str(excinfo.value)


def test_admapping_list() -> None:
    """Test that we can construct an admappinglist with the right arguments."""
    with pytest.raises(ValidationError) as excinfo:
        parse_obj_as(ADMappingList, [])
    assert "ensure this value has at least 1 items" in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        parse_obj_as(ADMappingList, [1])
    assert "value is not a valid dict" in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        parse_obj_as(ADMappingList, [{}])
    assert "ad_field\n  field required" in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        parse_obj_as(
            ADMappingList,
            [
                {
                    "ad_field": "mail",
                    "mo_address_type_user_key": "AD-Email",
                },
                {
                    "ad_field": "email",
                    "mo_address_type_user_key": "AD-Email",
                },
            ],
        )
    assert "'mo_address_type_user_key' must be unique across entire list." in str(
        excinfo.value
    )

    uuid = uuid4()
    with pytest.raises(ValidationError) as excinfo:
        parse_obj_as(
            ADMappingList,
            [
                {
                    "ad_field": "mail",
                    "mo_address_type_uuid": uuid,
                },
                {
                    "ad_field": "email",
                    "mo_address_type_uuid": uuid,
                },
            ],
        )
    assert "'mo_address_type_uuid' must be unique across entire list." in str(
        excinfo.value
    )
