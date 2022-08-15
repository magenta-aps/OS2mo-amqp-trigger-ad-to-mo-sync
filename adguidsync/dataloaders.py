# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Dataloaders to bulk requests."""
import json
from functools import partial
from operator import itemgetter
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import cast
from uuid import UUID

from fastramqpi.context import Context
from gql import gql
from gql.client import AsyncClientSession
from ldap3 import Connection
from more_itertools import one
from more_itertools import unzip
from pydantic import BaseModel
from pydantic import parse_obj_as
from raclients.modelclient.mo import ModelClient
from ramodels.mo.details import ITUser as RAITUser
from strawberry.dataloader import DataLoader

from .utils import remove_duplicates


# pylint: disable=too-few-public-methods
class Dataloaders(BaseModel):
    """Collection of program dataloaders.

    Args:
        users_loader: Loads User models from UUIDs.
        itsystems_loader: Loads ITSystem UUIDs from user-keys.
        adguid_loader: Loads AD GUIDs (UUIDs) from CPR numbers.
    """

    class Config:
        """Arbitrary types need to be allowed to have DataLoader members."""

        arbitrary_types_allowed = True

    users_loader: DataLoader
    itsystems_loader: DataLoader
    adguid_loader: DataLoader
    ituser_uploader: DataLoader


# pylint: disable=too-few-public-methods
class ITUser(BaseModel):
    """Submodel for the GraphQL response from load_users."""

    itsystem_uuid: UUID
    uuid: UUID
    user_key: str


# pylint: disable=too-few-public-methods
class User(BaseModel):
    """Model for the GraphQL response from load_users."""

    itusers: list[ITUser]
    cpr_no: str
    user_key: str
    uuid: UUID


async def load_users(
    keys: list[UUID], graphql_session: AsyncClientSession
) -> list[User | None]:
    """Loads User models from UUIDs.

    Args:
        keys: List of user UUIDs.
        graphql_session: The GraphQL session to run queries on.

    Return:
        List of User models.
    """
    query = gql(
        """
        query User(
          $uuids: [UUID!]
        ) {
          employees(uuids: $uuids) {
            objects {
              itusers {
                itsystem_uuid
                uuid
                user_key
              }
              cpr_no
              user_key
              uuid
            }
          }
        }
        """
    )
    result = await graphql_session.execute(
        query, variable_values={"uuids": remove_duplicates(map(str, keys))}
    )
    users = parse_obj_as(
        list[User], list(map(one, map(itemgetter("objects"), result["employees"])))
    )
    user_map = {user.uuid: user for user in users}
    return [user_map.get(key) for key in keys]


async def load_itsystems(
    keys: list[str], graphql_session: AsyncClientSession
) -> list[UUID | None]:
    """Loads ITSystem UUIDs from user-keys.

    Args:
        keys: List of ITSystem user-keys.
        graphql_session: The GraphQL session to run queries on.

    Return:
        List of ITSystem UUIDs.
    """
    query = gql("query ITSystemsQuery { itsystems { uuid, user_key } }")
    result = await graphql_session.execute(query)
    user_keys, uuids = unzip(map(itemgetter("user_key", "uuid"), result["itsystems"]))
    uuids = map(UUID, uuids)
    # NOTE: This assumes ITSystem user-keys are unique
    itsystems_map = dict(zip(user_keys, uuids))
    return [itsystems_map.get(key) for key in keys]


def ad_response_to_cpr_uuid_map(
    ad_response: dict[str, Any], cpr_attribute: str
) -> dict[str, UUID]:
    """Convert our AD Response to a CPR-->UUID dictionary.

    Example input:
        ```Python
        {
            "entries": [
                {
                    "attributes": {
                        "extensionAttribute3": "0101709999",
                        "objectGUID": "{ccc5f858-5044-4093-a4c2-b2ecb595201e}"
                    },
                    "dn": "CN=John Efternavn,OU=...,DC=Kommune,DC=net"
                },
                {
                    "attributes": {
                        "extensionAttribute3": "3112700000",
                        "objectGUID": "{d34513c5-2649-4045-b0a3-038da5d3765b}"
                    },
                    "dn": "CN=Hanne Efternavn,OU=...,DC=Kommune,DC=net"
                }
            ]
        }
        ```

    Args:
        ad_response: The JSON-parsed response from the AD.

    Returns:
        mapping from CPR-numbers to AD GUIDs.
    """
    users = list(map(itemgetter("attributes"), ad_response["entries"]))
    cpr_nos = map(itemgetter(cpr_attribute), users)
    guids = map(
        lambda guid_str: UUID(guid_str.strip("{}")),
        map(itemgetter("objectGUID"), users),
    )
    # NOTE: This assumes CPR numbers are unique
    return dict(zip(cpr_nos, guids))


async def load_adguid(
    keys: list[str],
    ad_connection: Connection,
    cpr_attribute: str,
    search_base: str,
) -> list[UUID | None]:
    """Loads AD GUIDs (UUIDs) from CPR numbers.

    Args:
        keys: List of CPR numbers.
        ad_connection: The AD connection to run queries on.
        cpr_attribute: The AD field which contains the CPR Number.
        search_base: The AD search base to use for all queries.

    Return:
        List of ADGUIDs.
    """
    # Construct our search filter by OR'ing all CPR numbers together
    cpr_conditions = "".join(map(lambda cpr: f"({cpr_attribute}={cpr})", keys))
    search_filter = "(&(objectclass=user)(|" + cpr_conditions + "))"

    ad_connection.search(
        search_base=search_base,
        search_filter=search_filter,
        # Search in the entire subtree of search_base
        search_scope="SUBTREE",
        # Fetch only CPR and objectGUID attributes
        attributes=[cpr_attribute, "objectGUID"],
    )
    json_str = ad_connection.response_to_json()
    ad_response = json.loads(json_str)

    cpr_to_uuid_map = ad_response_to_cpr_uuid_map(ad_response, cpr_attribute)
    return [cpr_to_uuid_map.get(key) for key in keys]


# TODO: Trim down the return value here by understanding model_client
async def upload_itusers(
    keys: list[RAITUser],
    model_client: ModelClient,
) -> list[Any | None]:
    """Uploads ITUser models via the model_client.

    Args:
        keys: List of ITUser models.
        model_client: The ModelClient to upload via.

    Return:
        List of response.
    """
    return cast(list[Any | None], await model_client.upload(keys))


def configure_dataloaders(context: Context) -> Dataloaders:
    """Construct our dataloaders from the FastRAMQPI context.

    Args:
        context: The FastRAMQPI context to configure our dataloaders with.

    Returns:
        Dataloaders required for ensure_adguid_itsystem.
    """
    # NOTE: Dataloaders should use call_later instead of call_soon within their
    #       implementation for greater bulking performance at the cost of worse latency.
    #       In this integration latency is not of great concern.
    graphql_loader_functions: dict[
        str, Callable[[list[Any], AsyncClientSession], Awaitable[Any]]
    ] = {
        "users_loader": load_users,
        "itsystems_loader": load_itsystems,
    }

    graphql_session = context["graphql_session"]
    graphql_dataloaders = {
        key: DataLoader(
            load_fn=partial(value, graphql_session=graphql_session), cache=False
        )
        for key, value in graphql_loader_functions.items()
    }

    settings = context["user_context"]["settings"]
    ad_connection = context["user_context"]["ad_connection"]
    adguid_loader = DataLoader(
        load_fn=partial(
            load_adguid,
            ad_connection=ad_connection,
            cpr_attribute=settings.ad_cpr_attribute,
            search_base=settings.ad_search_base,
        ),
        cache=False,
    )

    model_client = context["model_client"]
    ituser_uploader = DataLoader(
        load_fn=partial(
            upload_itusers,
            model_client=model_client,
        ),
        cache=False,
    )

    return Dataloaders(
        **graphql_dataloaders,
        adguid_loader=adguid_loader,
        ituser_uploader=ituser_uploader,
    )
