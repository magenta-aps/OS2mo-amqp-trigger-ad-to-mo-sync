# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Business logic."""
from asyncio import gather
from datetime import date
from operator import attrgetter
from typing import Any
from uuid import UUID

import structlog
from more_itertools import all_unique
from more_itertools import only
from ramodels.mo.details import Address

from .config import ADMapping
from .config import Settings
from .dataloaders import Dataloaders


async def insert(
    to_insert: set[UUID],
    write_address_map: dict[UUID, str],
    user_uuid: UUID,
    dataloaders: Dataloaders,
) -> list[Any | None]:
    insert_addresses = []
    for uuid in to_insert:
        insert_addresses.append(
            Address.from_simplified_fields(
                value=write_address_map[uuid],
                address_type_uuid=uuid,
                from_date=date.today().isoformat(),
                person_uuid=user_uuid,
            )
        )

    if not insert_addresses:
        return []

    response = await dataloaders.address_uploader.load_many(insert_addresses)
    return response


async def edit(
    to_edit: set[UUID],
    write_address_map: dict[UUID, str],
    current_addresses: list,
    user_uuid: UUID,
    dataloaders: Dataloaders,
) -> list[Any | None]:
    current_address_map = {
        address.address_type_uuid: address for address in current_addresses
    }

    edit_addresses = []
    for uuid in to_edit:
        edit_addresses.append(
            Address.from_simplified_fields(
                value=write_address_map[uuid],
                address_type_uuid=uuid,
                uuid=current_address_map[uuid].uuid,
                from_date=current_address_map[uuid].validity.from_time,
                to_date=current_address_map[uuid].validity.to_time,
                person_uuid=user_uuid,
            )
        )

    if not edit_addresses:
        return []

    response = await dataloaders.address_uploader.load_many(edit_addresses)
    return response


async def ensure_ad2mosynced(
    user_uuid: UUID,
    settings: Settings,
    dataloaders: Dataloaders,
) -> bool:
    """Ensure that all AD-to-MO synchronizations are up-to-date.

    Args:
        user_uuid: UUID of the user to ensure synchronizations for.
        settings: Pydantic settings objects.
        dataloaders: Dataloaders for MO and AD.

    Returns:
        None
    """
    logger = structlog.get_logger().bind(user_uuid=user_uuid)

    itsystem_uuid = settings.adguid_itsystem_uuid
    itsystem_user_key = settings.adguid_itsystem_user_key
    if itsystem_uuid is None:
        itsystem_uuid = await dataloaders.itsystems_loader.load(itsystem_user_key)
        if itsystem_uuid is None:
            message = "Unable to find itsystem by user-key"
            logger.warn(message, itsystem_user_key=itsystem_user_key)
            raise ValueError(message)

    user = await dataloaders.users_loader.load(user_uuid)
    if user is None:
        message = "Unable to find user by uuid"
        logger.warn(message)
        raise ValueError(message)

    ituser = only(
        filter(lambda ituser: ituser.itsystem_uuid == itsystem_uuid, user.itusers)
    )
    if ituser is None:
        logger.info("Unable to find ADGUID itsystem on user")
        return False
    adguid = UUID(ituser.user_key)

    logger = logger.bind(adguid=adguid)
    logger.info("Synchronizing user")

    addresses = settings.ad_mappings

    # Verify that address_type_user_key is unique for each type
    # TODO: This should be verified via a pydantic validator
    address_type_user_keys = map(attrgetter("mo_address_type_user_key"), addresses)
    address_type_user_keys = filter(None.__ne__, address_type_user_keys)
    assert all_unique(address_type_user_keys)

    async def fetch_mo_address_type_uuid(entry: ADMapping) -> ADMapping:
        if entry.mo_address_type_uuid is not None:
            return entry
        user_key = entry.mo_address_type_user_key
        uuid = await dataloaders.classes_loader.load(user_key)
        if uuid is None:
            message = "Unable to find class by user-key"
            logger.warn(message, mo_address_type_user_key=user_key)
            raise ValueError(message)
        return entry.copy(update={"mo_address_type_uuid": uuid})

    # TODO: Return a new pydantic model with enforced address_type_uuid here
    addresses = await gather(*map(fetch_mo_address_type_uuid, addresses))

    address_type_uuids = list(map(attrgetter("mo_address_type_uuid"), addresses))
    assert all_unique(address_type_uuids)

    current_addresses = list(
        filter(
            lambda address: address.address_type_uuid in address_type_uuids,
            user.addresses,
        )
    )
    current_address_types = {address.address_type_uuid for address in current_addresses}

    current_address_map = {
        address.address_type_uuid: address.value for address in current_addresses
    }
    # Assert uniqueness on existing addresses
    assert len(current_address_map) == len(current_addresses)

    result = await dataloaders.adattribute_loader.load(adguid)
    # print(result)

    write_address_map = {
        address["mo_address_type_uuid"]: result[address["ad_field"]]
        for address in addresses
    }

    print(current_address_map)
    print(write_address_map)

    write_address_types = set(address_type_uuids)

    # TODO: Delete should be consider all 'owned' address types
    to_delete = current_address_types.difference(write_address_types)
    potential_to_edit = current_address_types.intersection(write_address_types)
    to_insert = write_address_types.difference(current_address_types)

    to_edit = set(
        filter(
            lambda key: current_address_map[key] != write_address_map[key],
            potential_to_edit,
        )
    )

    print("INSERT", to_insert)
    print("POTENTIAL_EDIT", potential_to_edit)
    print("EDIT", to_edit)
    print("DELETE", to_delete)

    insert_task = insert(to_insert, write_address_map, user_uuid, dataloaders)
    edit_task = edit(
        to_edit, write_address_map, current_addresses, user_uuid, dataloaders
    )
    # delete_task = delete(to_delete, write_address_map, current_address_map)

    results = await gather(insert_task, edit_task)  # , delete_task)
    print(results)

    # print(address_writes)
    return True
