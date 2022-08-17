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
from pydantic import Field
from pydantic import BaseModel
from pydantic import parse_obj_as
from raclients.modelclient.mo import ModelClient
from ramodels.mo.details import Address as RAAddress
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
    classes_loader: DataLoader
    adattribute_loader: DataLoader
    address_uploader: DataLoader


# pylint: disable=too-few-public-methods
class ITUser(BaseModel):
    """Submodel for the GraphQL response from load_users."""

    itsystem_uuid: UUID
    user_key: str


# pylint: disable=too-few-public-methods
class Validity(BaseModel):
    """Submodel for the GraphQL response from load_users."""

    from_time: str = Field(alias="from")
    to_time: str | None = Field(alias="to")


# pylint: disable=too-few-public-methods
class Address(BaseModel):
    """Submodel for the GraphQL response from load_users."""

    uuid: UUID
    address_type_uuid: UUID
    value: str
    validity: Validity


# pylint: disable=too-few-public-methods
class User(BaseModel):
    """Model for the GraphQL response from load_users."""

    itusers: list[ITUser]
    addresses: list[Address]
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
                user_key
              }
              addresses {
                uuid
                address_type_uuid
                value
                validity {
                  from
                  to
                }
              }
              uuid
            }
          }
        }
        """
    )
    result = await graphql_session.execute(
        query, variable_values={"uuids": remove_duplicates(map(str, keys))}
    )
    print(result)
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


async def load_classes(
    keys: list[str], graphql_session: AsyncClientSession
) -> list[UUID | None]:
    """Loads ITSystem UUIDs from user-keys.

    Args:
        keys: List of ITSystem user-keys.
        graphql_session: The GraphQL session to run queries on.

    Return:
        List of ITSystem UUIDs.
    """
    query = gql("query ClassesQuery { classes { uuid, user_key } }")
    result = await graphql_session.execute(query)
    user_keys, uuids = unzip(map(itemgetter("user_key", "uuid"), result["classes"]))
    uuids = map(UUID, uuids)
    # NOTE: This assumes Class user-keys are unique
    classes_map = dict(zip(user_keys, uuids))
    return [classes_map.get(key) for key in keys]


def ad_response_to_cpr_uuid_map(
    ad_response: dict[str, Any]
) -> dict[UUID, Any]:
    """Convert our AD Response to a UUID-->dictionary mapping.

    Example input:
        ```Python
        {
            "entries": [
                {
                    "attributes": {
                        ...
                        "objectGUID": "{ccc5f858-5044-4093-a4c2-b2ecb595201e}"
                    },
                    "dn": "CN=John Efternavn,OU=...,DC=Kommune,DC=net"
                },
                {
                    "attributes": {
                        ...
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
        mapping from GUID to attribute dictionary.
    """
    users = list(map(itemgetter("attributes"), ad_response["entries"]))
    guids = map(
        lambda guid_str: UUID(guid_str.strip("{}")),
        map(itemgetter("objectGUID"), users),
    )
    return dict(zip(guids, users))


async def load_adattributes(
    keys: list[UUID],
    attributes: set[str],
    ad_connection: Connection,
    search_base: str,
) -> list[dict[str, Any]]:
    """Loads AD attributes from ADGUID (UUID).

    Args:
        keys: List of ADGUIDs.
        ad_connection: The AD connection to run queries on.
        search_base: The AD search base to use for all queries.

    Return:
        List of AD attribute dicts.
    """
    # Construct our search filter by OR'ing all CPR numbers together
    guid_conditions = "".join(map(lambda guid: f"(objectGUID={guid})", keys))
    search_filter = "(&(objectclass=user)(|" + guid_conditions + "))"

    ad_connection.search(
        search_base=search_base,
        search_filter=search_filter,
        # Search in the entire subtree of search_base
        search_scope="SUBTREE",
        # Fetch only requested attributes and objectGUID
        attributes=set(["objectGUID"]).union(attributes),
    )
    json_str = ad_connection.response_to_json()
    ad_response = json.loads(json_str)

    guid_to_attributes_map = ad_response_to_cpr_uuid_map(ad_response)
    return [guid_to_attributes_map.get(key) for key in keys]


# TODO: Trim down the return value here by understanding model_client
async def upload_addresses(
    keys: list[RAAddress],
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
        "classes_loader": load_classes,
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
    adattribute_loader = DataLoader(
        load_fn=partial(
            load_adattributes,
            attributes={"*"},
            ad_connection=ad_connection,
            search_base=settings.ad_search_base,
        ),
        cache=False,
    )

    model_client = context["model_client"]
    address_uploader = DataLoader(
        load_fn=partial(
            upload_addresses,
            model_client=model_client,
        ),
        cache=False,
    )

    return Dataloaders(
        **graphql_dataloaders,
        adattribute_loader=adattribute_loader,
        address_uploader=address_uploader,
    )
