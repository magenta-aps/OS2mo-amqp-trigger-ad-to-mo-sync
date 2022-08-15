# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=redefined-outer-name
# pylint: disable=duplicate-code
"""Test ensure_adguid_itsystem."""
from collections import ChainMap
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID
from uuid import uuid4

import pytest
from ramodels.mo.details import ITUser as RAITUser
from strawberry.dataloader import DataLoader
from structlog.testing import capture_logs

from adguidsync.calculate import ensure_adguid_itsystem
from adguidsync.config import Settings
from adguidsync.dataloaders import Dataloaders
from adguidsync.dataloaders import ITUser
from adguidsync.dataloaders import User


async def load_itsystems(keys: list[str]) -> list[UUID | None]:
    """NOOP Implementation of load_itsystems.

    Args:
        keys: List of ITSystem user-keys.

    Return:
        List of Nones
    """
    return [None] * len(keys)


async def load_users(keys: list[UUID]) -> list[User | None]:
    """NOOP Implementation of load_users.

    Args:
        keys: List of user UUIDs.

    Return:
        List of Nones
    """
    return [None] * len(keys)


async def load_adguid(keys: list[str]) -> list[UUID | None]:
    """NOOP Implementation of load_adguid.

    Args:
        keys: List of CPR numbers.

    Return:
        List of Nones
    """
    return [None] * len(keys)


async def upload_itusers(keys: list[RAITUser]) -> list[Any | None]:
    """NOOP Implementation of upload_itusers.

    Args:
        keys: List of RAITUser objects.

    Return:
        List of Nones
    """
    return [None] * len(keys)


@pytest.fixture
def dataloaders() -> Iterator[Dataloaders]:
    """Fixture to construct noop Dataloaders.

    Yields:
        NOOP Dataloaders with dataloaders that always return None.
    """
    dataloaders = Dataloaders(
        users_loader=DataLoader(load_fn=load_users),
        itsystems_loader=DataLoader(load_fn=load_itsystems),
        adguid_loader=DataLoader(load_fn=load_adguid),
        ituser_uploader=DataLoader(load_fn=upload_itusers),
    )
    yield dataloaders


async def test_ensure_adguid_itsystem(
    settings: Settings,
    dataloaders: Dataloaders,
) -> None:
    """Test that itsystem UUID is only looked up when required."""
    user_uuid = uuid4()
    # When no adguid_itsystem_uuid is set, we expect to look it up
    with capture_logs() as captured_logs:
        with pytest.raises(ValueError) as exc_info:
            await ensure_adguid_itsystem(user_uuid, settings, dataloaders)
    assert "Unable to find itsystem by user-key" in str(exc_info.value)
    assert captured_logs == [
        {
            "event": "Unable to find itsystem by user-key",
            "log_level": "warning",
            "itsystem_user_key": "ADGUID",
            "user_uuid": user_uuid,
        }
    ]

    # When adguid_itsystem_uuid is set, we expect not to look it up
    adguid_it_system_uuid = uuid4()
    settings = Settings(
        **ChainMap(dict(adguid_itsystem_uuid=adguid_it_system_uuid), settings.dict())
    )
    with capture_logs() as captured_logs:
        with pytest.raises(ValueError) as exc_info:
            await ensure_adguid_itsystem(user_uuid, settings, dataloaders)
    assert "Unable to find user by uuid" in str(exc_info.value)
    assert captured_logs == [
        {
            "event": "Unable to find user by uuid",
            "log_level": "warning",
            "user_uuid": user_uuid,
        }
    ]


async def test_ensure_adguid_itsystem_user_and_ituser_found(
    settings: Settings,
    dataloaders: Dataloaders,
) -> None:
    """Test nothing happens if ituser already exists."""
    user_uuid = uuid4()
    adguid_it_system_uuid = uuid4()
    settings = Settings(
        **ChainMap(dict(adguid_itsystem_uuid=adguid_it_system_uuid), settings.dict())
    )

    # When user is found, and it has the expected it-user
    loader_func = AsyncMock()
    loader_func.return_value = [
        User(
            itusers=[
                ITUser(
                    itsystem_uuid=adguid_it_system_uuid, uuid=uuid4(), user_key="ADGUID"
                )
            ],
            cpr_no="0101700000",
            user_key="Fiktiv Bruger",
            uuid=user_uuid,
        )
    ]
    dataloaders.users_loader = DataLoader(load_fn=loader_func)

    with capture_logs() as captured_logs:
        result = await ensure_adguid_itsystem(
            user_uuid,
            settings,
            dataloaders,
        )
        assert result is False
    assert captured_logs == [
        {
            "event": "ITUser already exists",
            "log_level": "info",
            "user_uuid": user_uuid,
        }
    ]


async def test_ensure_adguid_itsystem_user_found_ituser_not_found(
    settings: Settings,
    dataloaders: Dataloaders,
) -> None:
    """Test we want to create an ituser if it does not exists."""
    user_uuid = uuid4()
    adguid_it_system_uuid = uuid4()
    settings = Settings(
        **ChainMap(dict(adguid_itsystem_uuid=adguid_it_system_uuid), settings.dict())
    )

    # When user is found, and it does not have the expected it-user
    # But we do not find the user in AD
    loader_func = AsyncMock()
    loader_func.return_value = [
        User(
            itusers=[],
            cpr_no="0101700000",
            user_key="Fiktiv Bruger",
            uuid=user_uuid,
        )
    ]
    dataloaders.users_loader = DataLoader(load_fn=loader_func)

    with capture_logs() as captured_logs:
        result = await ensure_adguid_itsystem(
            user_uuid,
            settings,
            dataloaders,
        )
        assert result is False
    assert captured_logs == [
        {
            "event": "Unable to find ad user by cpr number",
            "log_level": "info",
            "user_uuid": user_uuid,
        }
    ]


async def test_ensure_adguid_itsystem_happy_path(
    settings: Settings,
    dataloaders: Dataloaders,
) -> None:
    """Test we create an ituser if it does not exist."""
    user_uuid = uuid4()
    adguid_it_system_uuid = uuid4()
    settings = Settings(
        **ChainMap(dict(adguid_itsystem_uuid=adguid_it_system_uuid), settings.dict())
    )
    # When user is found, and it does not have the expected it-user
    # And we do find the user in AD
    loader_func = AsyncMock()
    loader_func.return_value = [
        User(
            itusers=[],
            cpr_no="0101700000",
            user_key="Fiktiv Bruger",
            uuid=user_uuid,
        )
    ]
    dataloaders.users_loader = DataLoader(load_fn=loader_func)

    adguid = uuid4()
    loader_func = AsyncMock()
    loader_func.return_value = [adguid]
    dataloaders.adguid_loader = DataLoader(load_fn=loader_func)

    loader_func = AsyncMock()
    loader_func.return_value = ["RESPONSE_HERE"]
    dataloaders.ituser_uploader = DataLoader(load_fn=loader_func)

    with capture_logs() as captured_logs:
        result = await ensure_adguid_itsystem(
            user_uuid,
            settings,
            dataloaders,
        )
        assert result is True
    loader_func.assert_called_once()
    assert captured_logs == [
        {
            "event": "Creating ITUser for user",
            "log_level": "info",
            "user_uuid": user_uuid,
            "adguid": adguid,
        },
        {
            "event": "Creating ITUser response",
            "log_level": "debug",
            "response": "RESPONSE_HERE",
            "user_uuid": user_uuid,
            "adguid": adguid,
        },
    ]
