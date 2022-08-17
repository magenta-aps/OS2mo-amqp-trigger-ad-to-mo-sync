# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=protected-access
"""Test ensure_adguid_itsystem."""
import asyncio
import json
from collections.abc import Iterator
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from more_itertools import one

from ad2mosync.config import Settings
from ad2mosync.dataloaders import configure_dataloaders
from ad2mosync.dataloaders import Dataloaders
from ad2mosync.dataloaders import User


@pytest.fixture
def graphql_session() -> Iterator[AsyncMock]:
    """Fixture to construct a mock graphql_session.

    Yields:
        A mock for graphql_session.
    """
    yield AsyncMock()


@pytest.fixture
def ad_connection() -> Iterator[MagicMock]:
    """Fixture to construct a mock ad_connection.

    Yields:
        A mock for ad_connection.
    """
    yield MagicMock()


@pytest.fixture
def model_client() -> Iterator[AsyncMock]:
    """Fixture to construct a mock model_client.

    Yields:
        A mock for model_client.
    """
    yield AsyncMock()


@pytest.fixture
def dataloaders(
    graphql_session: AsyncMock,
    ad_connection: MagicMock,
    model_client: AsyncMock,
    settings: Settings,
) -> Iterator[Dataloaders]:
    """Fixture to construct a dataloaders object using fixture mocks.

    Yields:
        Dataloaders with mocked clients.
    """
    dataloaders = configure_dataloaders(
        {
            "graphql_session": graphql_session,
            "user_context": {
                "settings": settings,
                "ad_connection": ad_connection,
            },
            "model_client": model_client,
        }
    )
    yield dataloaders


async def test_upload_itusers(
    model_client: AsyncMock, dataloaders: Dataloaders
) -> None:
    """Test that upload_itusers works as expected."""
    model_client.upload.return_value = ["1", None, "3"]

    results = await asyncio.gather(
        dataloaders.ituser_uploader.load(1),
        dataloaders.ituser_uploader.load(2),
        dataloaders.ituser_uploader.load(3),
    )
    assert results == ["1", None, "3"]
    model_client.upload.assert_called_with([1, 2, 3])


async def test_load_adguid(ad_connection: MagicMock, dataloaders: Dataloaders) -> None:
    """Test that load_adguid works as expected."""
    uuid1 = uuid4()
    uuid2 = uuid4()
    ad_connection.response_to_json.return_value = json.dumps(
        {
            "entries": [
                {
                    "attributes": {
                        "extensionAttribute3": "0101690420",
                        "objectGUID": f"{str(uuid2)}",
                    },
                    "dn": "CN=Hanne Efternavn,OU=...,DC=Kommune,DC=net",
                },
                {
                    "attributes": {
                        "extensionAttribute3": "0101709999",
                        "objectGUID": f"{str(uuid1)}",
                    },
                    "dn": "CN=John Efternavn,OU=...,DC=Kommune,DC=net",
                },
            ]
        }
    )

    results = await asyncio.gather(
        dataloaders.adguid_loader.load("1212121212"),
        dataloaders.adguid_loader.load("0101709999"),
        dataloaders.adguid_loader.load("1111111111"),
        dataloaders.adguid_loader.load("0101690420"),
    )
    assert results == [None, uuid1, None, uuid2]
    ad_connection.response_to_json.assert_called_with()
    ad_connection.search.assert_called_with(
        search_base="OU=Fiktiv kommune,DC=fiktiv,DC=net",
        search_filter=(
            "(&(objectclass=user)(|"
            "(extensionAttribute3=1212121212)"
            "(extensionAttribute3=0101709999)"
            "(extensionAttribute3=1111111111)"
            "(extensionAttribute3=0101690420)"
            "))"
        ),
        search_scope="SUBTREE",
        attributes=["extensionAttribute3", "objectGUID"],
    )


async def test_load_itsystem(
    graphql_session: AsyncMock, dataloaders: Dataloaders
) -> None:
    """Test that load_itsystem works as expected."""
    uuid = uuid4()
    graphql_session.execute.return_value = {
        "itsystems": [{"uuid": str(uuid), "user_key": "Active Directory (ADGUID)"}]
    }

    results = await asyncio.gather(
        dataloaders.itsystems_loader.load("Mogens"),
        dataloaders.itsystems_loader.load("Active Directory (ADGUID)"),
        dataloaders.itsystems_loader.load("UKENDT"),
    )
    assert results == [None, uuid, None]
    graphql_session.execute.assert_called_once()


async def test_load_users(graphql_session: AsyncMock, dataloaders: Dataloaders) -> None:
    """Test that load_users works as expected."""
    uuid1 = uuid4()
    uuid2 = uuid4()
    uuid3 = uuid4()
    graphql_session.execute.return_value = {
        "employees": [
            {
                "objects": [
                    {
                        "uuid": str(uuid2),
                        "user_key": "JohnDeere",
                        "cpr_no": "0101709999",
                        "itusers": [],
                    }
                ]
            }
        ]
    }

    results = await asyncio.gather(
        dataloaders.users_loader.load(uuid1),
        dataloaders.users_loader.load(uuid2),
        dataloaders.users_loader.load(uuid3),
        dataloaders.users_loader.load(uuid3),
        dataloaders.users_loader.load(uuid2),
    )
    ituser = User(uuid=uuid2, user_key="JohnDeere", cpr_no="0101709999", itusers=[])
    assert results == [None, ituser, None, None, ituser]
    call = one(graphql_session.execute.mock_calls)
    assert call.kwargs == {
        "variable_values": {"uuids": [str(uuid1), str(uuid2), str(uuid3)]}
    }
