# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Event handling."""
import traceback
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from operator import itemgetter
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

import structlog
from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import Query
from fastapi import Request
from fastapi.responses import JSONResponse
from fastramqpi.context import Context
from fastramqpi.main import FastRAMQPI
from gql import gql
from ldap3 import Connection
from ra_utils.asyncio_utils import gather_with_concurrency

from .calculate import ensure_adguid_itsystem
from .config import Settings
from .dataloaders import configure_dataloaders
from .ldap import ad_healthcheck
from .ldap import configure_ad_connection


logger = structlog.get_logger()
fastapi_router = APIRouter()


def gen_ensure_adguid_itsystem(context: Context) -> Callable[[UUID], Awaitable[bool]]:
    """Seed ensure_adguid_itsystem with arguments from context.

    Args:
        context: dictionary to extract arguments from.

    Returns:
        ensure_adguid_itsystem that only takes an UUID.
    """
    return partial(
        ensure_adguid_itsystem,
        settings=context["user_context"]["settings"],
        dataloaders=context["user_context"]["dataloaders"],
    )


@fastapi_router.post(
    "/trigger/all",
)
async def update_all_employees(request: Request) -> dict[str, Any]:
    """Call update_line_management on all org units."""
    context: Context = request.app.state.context
    gql_session = context["graphql_session"]
    query = gql("query EmployeeUUIDQuery { employees { uuid } }")
    result = await gql_session.execute(query)
    employee_uuids = map(UUID, map(itemgetter("uuid"), result["employees"]))
    employee_tasks = map(gen_ensure_adguid_itsystem(context), employee_uuids)
    num_changes = sum(await gather_with_concurrency(5, *employee_tasks))
    return {"status": "OK", "changes": num_changes}


@fastapi_router.post(
    "/trigger/{uuid}",
)
async def update_employee(
    request: Request,
    uuid: UUID = Query(..., description="UUID of the employee to recalculate"),
) -> dict[str, Any]:
    """Call ensure_adguid_itsystem on the provided employee."""
    context: Context = request.app.state.context
    all_ok = await gen_ensure_adguid_itsystem(context)(uuid)
    num_changes = 1 if all_ok else 0
    return {"status": "OK", "changes": num_changes}


@asynccontextmanager
async def open_ad_connection(ad_connection: Connection) -> AsyncIterator[None]:
    """Open the AD connection during FastRAMQPI lifespan.

    Yields:
        None
    """
    with ad_connection:
        yield


@asynccontextmanager
async def seed_dataloaders(fastramqpi: FastRAMQPI) -> AsyncIterator[None]:
    """Seed dataloaders during FastRAMQPI lifespan.

    Yields:
        None
    """
    context = fastramqpi.get_context()
    dataloaders = configure_dataloaders(context)
    fastramqpi.add_context(dataloaders=dataloaders)
    yield


def create_fastramqpi(**kwargs: Any) -> FastRAMQPI:
    """FastRAMQPI factory.

    Returns:
        FastRAMQPI system.
    """
    settings = Settings(**kwargs)
    fastramqpi = FastRAMQPI(application_name="adguidsync", settings=settings.fastramqpi)
    fastramqpi.add_context(settings=settings)

    ad_connection = configure_ad_connection(settings)
    fastramqpi.add_context(ad_connection=ad_connection)
    fastramqpi.add_healthcheck(name="ADConnection", healthcheck=ad_healthcheck)
    fastramqpi.add_lifespan_manager(open_ad_connection(ad_connection), 1500)

    fastramqpi.add_lifespan_manager(seed_dataloaders(fastramqpi), 2000)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    # TODO: This should be in FastRAMQPI
    @app.exception_handler(Exception)
    @app.exception_handler(ValueError)
    async def exception_callback(_: Request, exception: Exception) -> JSONResponse:
        # TODO: Use structlog v22.1 for proper JSON tracebacks
        trace = traceback.format_exc()
        logger.exception("Uncaught exception", exception=exception, trace=trace)
        return JSONResponse(
            {"status": "ERROR", "info": "Uncaught exception"}, status_code=500
        )

    return fastramqpi


def create_app(**kwargs: Any) -> FastAPI:
    """FastAPI application factory.

    Returns:
        FastAPI application.
    """
    fastramqpi = create_fastramqpi(**kwargs)
    return fastramqpi.get_app()
