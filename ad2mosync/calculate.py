# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Business logic."""
from asyncio import gather
from datetime import date
from itertools import chain
from operator import attrgetter
from uuid import UUID

import structlog
from more_itertools import only
from pydantic import parse_obj_as
from ramodels.mo.details import Address
from strawberry.dataloader import DataLoader

from .config import ADMapping
from .config import ADMappingList
from .config import Settings
from .dataloaders import Dataloaders


def construct_insert_addresses(
    to_insert: set[UUID],
    write_address_map: dict[UUID, str],
    user_uuid: UUID,
) -> list[Address]:
    """Construct addresses to be inserted.

    Args:
        to_insert: UUID of address_type for address to be inserted.
        write_address_map: Map of information to construct insert addresses with.
        user_uuid: The user to insert addresses for.

    Returns:
        List of Address objects to be inserted.
    """
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
    return insert_addresses


def construct_edit_addresses(
    to_edit: set[UUID],
    write_address_map: dict[UUID, str],
    current_addresses: list,
    user_uuid: UUID,
) -> list[Address]:
    """Construct addresses to be edited.

    Args:
        to_edit: UUID of address_type for address to be edited.
        write_address_map: Map of information to construct edit addresses with.
        current_addresses: List of current addresses for the user.
        user_uuid: The user to edit addresses for.

    Returns:
        List of Address objects to be edited.
    """
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
    return edit_addresses


async def ensure_mo_address_type_uuids(
    mappings: ADMappingList,
    classes_loader: DataLoader,
) -> ADMappingList:
    """Ensure that APMappingList only contains entries with mo_address_type_uuid set.

    If entries without mo_address_type_uuid exists in the input, the uuids are fetched
    via the user-keys from MO.

    Args:
        mappings: Mapping list with elements that may not have mo_address_type_uuid set.
        classes_loader: Dataloader to load class UUIDs from MO by user-key.

    Raises:
        ValueError: If unable to find class by user-key.

    Returns:
        mappings: Mapping list with elements that have mo_address_type_uuid set.
    """

    async def fetch_mo_address_type_uuid(entry: ADMapping) -> ADMapping:
        if entry.mo_address_type_uuid is not None:
            return entry
        user_key = entry.mo_address_type_user_key
        uuid = await classes_loader.load(user_key)
        if uuid is None:
            message = "Unable to find class by user-key"
            logger = structlog.get_logger()
            logger.warn(message, user_key=user_key)
            raise ValueError(message)
        return entry.copy(update={"mo_address_type_uuid": uuid})

    # TODO: Consider having a ADMappingList with a submodel that has a non-optional
    #       mo_address_type_uuid field on it.
    ensured_mappings = parse_obj_as(
        ADMappingList, await gather(*map(fetch_mo_address_type_uuid, mappings))
    )
    return ensured_mappings


async def get_itsystem_uuid(
    itsystem_user_key: str,
    itsystem_uuid: UUID | None,
    itsystems_loader: DataLoader,
) -> UUID:
    """Get the ADGUID ITSystem UUID.

    Args:
        itsystem_user_key: User-key for the ITSystem to load.
        itsystem_uuid: UUID for the ITSystem to load.
        itsystems_loader: Dataloader to load ITSystems from MO.

    Raises:
        ValueError: If unable to find itsystem by user-key.

    Returns:
        UUID of the ADGUID ITSystem
    """
    if itsystem_uuid is not None:
        return itsystem_uuid

    itsystem_uuid = await itsystems_loader.load(itsystem_user_key)
    if itsystem_uuid is None:
        message = "Unable to find itsystem by user-key"
        logger = structlog.get_logger()
        logger.warn(message, user_key=itsystem_user_key)
        raise ValueError(message)
    return itsystem_uuid


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

    itsystem_uuid = await get_itsystem_uuid(
        settings.adguid_itsystem_user_key,
        settings.adguid_itsystem_uuid,
        dataloaders.itsystems_loader,
    )

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

    mappings = await ensure_mo_address_type_uuids(
        settings.ad_mappings, dataloaders.classes_loader
    )

    address_type_uuids = list(map(attrgetter("mo_address_type_uuid"), mappings))

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
        mapping.mo_address_type_uuid: result[mapping.ad_field] for mapping in mappings
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

    insert_addresses = construct_insert_addresses(
        to_insert, write_address_map, user_uuid
    )
    edit_addresses = construct_edit_addresses(
        to_edit, write_address_map, current_addresses, user_uuid
    )
    # TODO: Handle DELETE

    upsert_addresses = list(chain(insert_addresses, edit_addresses))
    if not upsert_addresses:
        return False

    response = await dataloaders.address_uploader.load_many(upsert_addresses)
    print(response)
    return True
