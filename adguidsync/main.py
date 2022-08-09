# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Event handling."""
from functools import partial
from operator import itemgetter
from typing import Any
from typing import Awaitable
from typing import Callable
from uuid import UUID

from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import Query
from fastapi import Request
from fastramqpi.context import Context
from fastramqpi.main import FastRAMQPI
from gql import gql
from ra_utils.asyncio_utils import gather_with_concurrency

from .calculate import ensure_adguid_itsystem
from .config import Settings
from .dataloaders import seed_dataloaders
from .ldap import ad_connection


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


def create_app(**kwargs: Any) -> FastAPI:
    """FastAPI application factory.

    Returns:
        None
    """
    settings = Settings(**kwargs)
    fastramqpi = FastRAMQPI(application_name="adguidsync", settings=settings.fastramqpi)
    fastramqpi.add_context(settings=settings)

    fastramqpi.add_lifespan_manager(partial(ad_connection, fastramqpi)(), 1500)
    fastramqpi.add_lifespan_manager(partial(seed_dataloaders, fastramqpi)(), 2000)

    app = fastramqpi.get_app()
    app.include_router(fastapi_router)

    return fastramqpi.get_app()
