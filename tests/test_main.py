# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=protected-access
"""Test ensure_adguid_itsystem."""
from collections.abc import AsyncIterator
from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import AsyncMock
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import UUID
from uuid import uuid4

import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastramqpi.main import FastRAMQPI
from more_itertools import one
from structlog.testing import capture_logs

from adguidsync.dataloaders import Dataloaders
from adguidsync.main import create_app
from adguidsync.main import create_fastramqpi
from adguidsync.main import open_ad_connection
from adguidsync.main import seed_dataloaders


@pytest.fixture
def random_uuid() -> Iterator[UUID]:
    """Fixture to generate random uuids.

    Yields:
        A random UUID4
    """
    yield uuid4()


@pytest.fixture
def disable_metrics(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Fixture to set the ENABLE_METRICS environmental variable to False.

    Yields:
        None
    """
    monkeypatch.setenv("ENABLE_METRICS", "False")
    yield


@pytest.fixture
def fastramqpi(
    disable_metrics: None, load_settings_overrides: dict[str, str]
) -> Iterator[FastRAMQPI]:
    """Fixture to construct a FastRAMQPI system.

    Yields:
        FastRAMQPI system.
    """
    yield create_fastramqpi()


@pytest.fixture
def app(fastramqpi: FastRAMQPI) -> Iterator[FastAPI]:
    """Fixture to construct a FastAPI application.

    Yields:
        FastAPI application.
    """
    yield fastramqpi.get_app()


@pytest.fixture
async def lifespan_app(app: FastAPI) -> AsyncIterator[FastAPI]:
    """Fixture to construct a FastAPI application with life-cycle management.

    Yields:
        FastAPI application.
    """
    async with LifespanManager(app):
        yield app


@pytest.fixture
def test_client(app: FastAPI) -> Iterator[TestClient]:
    """Fixture to construct a FastAPI test-client.

    Note:
        The app does not do lifecycle management.

    Yields:
        TestClient for the FastAPI application.
    """
    yield TestClient(app)


def test_create_app(
    disable_metrics: None, load_settings_overrides: dict[str, str]
) -> None:
    """Test that we can construct our FastAPI application."""
    app = create_app()
    assert isinstance(app, FastAPI)


def test_create_fastramqpi(
    disable_metrics: None, load_settings_overrides: dict[str, str]
) -> None:
    """Test that we can construct our FastRAMQPI system."""
    fastramqpi = create_fastramqpi()
    assert isinstance(fastramqpi, FastRAMQPI)


async def test_root_endpoint(test_client: TestClient) -> None:
    """Test the root endpoint on our app."""
    response = test_client.get("/")
    assert response.status_code == 200
    assert response.json() == {"name": "adguidsync"}


async def test_liveness_endpoint(test_client: TestClient) -> None:
    """Test the liveness endpoint on our app."""
    response = test_client.get("/health/live")
    assert response.status_code == 204


async def test_readiness_endpoint(
    fastramqpi: FastRAMQPI, test_client: TestClient
) -> None:
    """Test the readiness endpoint on our app."""
    # Remove all the standard healthchecks, keeping only the non-standard ones
    healthchecks = fastramqpi.get_context()["healthchecks"]
    healthchecks = {
        key: value
        for key, value in healthchecks.items()
        if key not in {"AMQP", "GraphQL", "Service API"}
    }

    # Assert our healthchecks are there
    assert healthchecks.keys() == {"ADConnection"}

    # Override the healthchecks and check health
    fastramqpi.get_context()["healthchecks"] = healthchecks

    # Override the ADConnection with a mock
    ad_connection = MagicMock()
    fastramqpi.add_context(ad_connection=ad_connection)

    ad_connection.bound = False
    with capture_logs() as captured_logs:
        response = test_client.get("/health/ready")
        assert response.status_code == 503
    assert captured_logs == [
        {"event": "ADConnection is not ready", "log_level": "warning"}
    ]

    ad_connection.bound = True
    with capture_logs() as captured_logs:
        response = test_client.get("/health/ready")
        assert response.status_code == 204
    assert captured_logs == []


async def test_seed_dataloaders(fastramqpi: FastRAMQPI) -> None:
    """Test the seed_dataloaders asynccontextmanager."""
    fastramqpi.get_context()["graphql_session"] = MagicMock()

    user_context = fastramqpi.get_context()["user_context"]
    assert user_context.get("dataloaders") is None

    async with seed_dataloaders(fastramqpi):
        dataloaders = user_context.get("dataloaders")

    assert dataloaders is not None
    assert isinstance(dataloaders, Dataloaders)


async def test_open_ad_connection() -> None:
    """Test the open_ad_connection."""
    state = []

    @contextmanager
    def manager() -> Iterator[None]:
        state.append(1)
        yield
        state.append(2)

    ad_connection = manager()

    assert not state
    async with open_ad_connection(ad_connection):
        assert state == [1]
    assert state == [1, 2]


@pytest.mark.parametrize("success", [False, True])
async def test_update_employee_endpoint(
    success: bool, random_uuid: UUID, fastramqpi: FastRAMQPI, test_client: TestClient
) -> None:
    """Test the the update_employee endpoint calls gen_ensure_adguid_itsystem."""
    fastramqpi.add_context(settings="Whatever")
    fastramqpi.add_context(dataloaders="Whatever")

    with patch(
        "adguidsync.main.ensure_adguid_itsystem", new_callable=AsyncMock
    ) as result_mock:
        result_mock.return_value = success

        response = test_client.post(f"/trigger/{str(random_uuid)}")

        result_mock.assert_called_with(
            random_uuid, settings="Whatever", dataloaders="Whatever"
        )

    assert response.status_code == 200
    assert response.json() == {"status": "OK", "changes": 1 if success else 0}


async def test_update_employee_endpoint_exception(
    random_uuid: UUID, fastramqpi: FastRAMQPI, test_client: TestClient
) -> None:
    """Test the the update_employee endpoint handles exceptions nicely."""
    fastramqpi.add_context(settings="Whatever")
    fastramqpi.add_context(dataloaders="Whatever")

    with patch(
        "adguidsync.main.ensure_adguid_itsystem", new_callable=AsyncMock
    ) as result_mock:
        exception = ValueError("Unable to find user by uuid")
        result_mock.side_effect = exception

        with capture_logs() as captured_logs:
            response = test_client.post(f"/trigger/{str(random_uuid)}")
        captured_log = one(captured_logs)
        traceback = captured_log.pop("trace")
        assert captured_log == {
            "event": "Uncaught exception",
            "exc_info": True,
            "exception": exception,
            "log_level": "error",
        }
        assert "Traceback" in traceback
        assert "update_employee" in traceback
        assert "gen_ensure_adguid_itsystem" in traceback

        result_mock.assert_called_with(
            random_uuid, settings="Whatever", dataloaders="Whatever"
        )

    assert response.status_code == 500
    assert response.json() == {"status": "ERROR", "info": "Uncaught exception"}


async def test_update_no_employees_endpoint(
    fastramqpi: FastRAMQPI, test_client: TestClient
) -> None:
    """Test the the update_all_employee endpoint work without employees."""
    fastramqpi.add_context(settings="Whatever")
    fastramqpi.add_context(dataloaders="Whatever")
    graphql_session = AsyncMock()
    graphql_session.execute.return_value = {"employees": []}
    fastramqpi.get_context()["graphql_session"] = graphql_session

    with patch(
        "adguidsync.main.ensure_adguid_itsystem", new_callable=AsyncMock
    ) as result_mock:
        result_mock.return_value = True

        response = test_client.post("/trigger/all")

        result_mock.assert_not_called()

    assert response.status_code == 200
    assert response.json() == {"status": "OK", "changes": 0}


async def test_update_all_employees_endpoint(
    fastramqpi: FastRAMQPI, test_client: TestClient
) -> None:
    """Test the the update_all_employee endpoint work with employees."""
    fastramqpi.add_context(settings="Whatever")
    fastramqpi.add_context(dataloaders="Whatever")

    uuids = [uuid4() for _ in range(10)]

    graphql_session = AsyncMock()
    graphql_session.execute.return_value = {
        "employees": [{"uuid": str(uuid)} for uuid in uuids]
    }
    fastramqpi.get_context()["graphql_session"] = graphql_session

    with patch(
        "adguidsync.main.ensure_adguid_itsystem", new_callable=AsyncMock
    ) as result_mock:
        result_mock.return_value = True

        response = test_client.post("/trigger/all")

        result_mock.assert_has_calls(
            [call(uuid, settings="Whatever", dataloaders="Whatever") for uuid in uuids]
        )

    assert response.status_code == 200
    assert response.json() == {"status": "OK", "changes": 10}
