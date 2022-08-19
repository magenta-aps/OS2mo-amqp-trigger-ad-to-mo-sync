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
    insert_address_map: dict[UUID, str],
    user_uuid: UUID,
) -> list[Address]:
    """Construct addresses to be inserted.

    Args:
        write_address_map: Map of information to construct insert addresses with.
        user_uuid: The user to insert addresses for.

    Returns:
        List of Address objects to be inserted.
    """
    insert_addresses = []
    for address_type_uuid, value in insert_address_map.items():
        insert_addresses.append(
            Address.from_simplified_fields(
                value=value,
                address_type_uuid=address_type_uuid,
                from_date=date.today().isoformat(),
                person_uuid=user_uuid,
            )
        )
    return insert_addresses


def construct_edit_addresses(
    edit_address_map: dict[UUID, str],
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
    for address_type_uuid, value in edit_address_map.items():
        edit_addresses.append(
            Address.from_simplified_fields(
                value=value,
                address_type_uuid=address_type_uuid,
                uuid=current_address_map[address_type_uuid].uuid,
                # We can only guarantee this data is true from today
                from_date=date.today().isoformat(),
                to_date=current_address_map[address_type_uuid].validity.to_time,
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


# TODO: Remove this and break function up further
# pylint: disable=too-many-locals
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
    mapping_address_type_uuids = set(map(attrgetter("mo_address_type_uuid"), mappings))

    # TODO: This should probably be a setting instead, defining all the address-types
    #       that are managed by the program. For now we assume only the configured ones
    #       are managed, and thus no deletion will ever occur.
    managed_address_type_uuids = mapping_address_type_uuids

    # Only operate on current-addresses that are managed by us, all addresses unmanaged
    # are left untouched by the program.
    current_address_type_uuids = set(
        map(attrgetter("address_type_uuid"), user.addresses)
    )
    current_address_type_uuids = current_address_type_uuids.intersection(
        managed_address_type_uuids
    )

    edit_address_type_uuids = current_address_type_uuids.intersection(
        mapping_address_type_uuids
    )
    insert_address_type_uuids = mapping_address_type_uuids.difference(
        current_address_type_uuids
    )
    delete_address_type_uuids = current_address_type_uuids.difference(
        managed_address_type_uuids
    )

    logger.debug("Might edit", address_type_uuids=edit_address_type_uuids)
    logger.debug("Gonna insert", address_type_uuids=insert_address_type_uuids)
    logger.debug("Gonna delete", address_type_uuids=delete_address_type_uuids)

    # TODO: This is true until managed_address_type_uuids is set from settings.
    assert delete_address_type_uuids == set()

    current_address_tuples = [
        (address.address_type_uuid, address.value)
        for address in user.addresses
        if address.address_type_uuid in current_address_type_uuids
    ]
    current_address_map = dict(current_address_tuples)
    # Assert address_type uniqueness on managed existing addresses
    if len(current_address_tuples) != len(current_address_map):
        message = "Non uniqueness on managed existing addresses found"
        # TODO: Include the non-unique address types
        logger.warn(message)
        raise ValueError(message)

    # Fetch AD fields and construct write map
    result = await dataloaders.adattribute_loader.load(adguid)
    # print(result)
    write_address_map = {
        mapping.mo_address_type_uuid: result[mapping.ad_field] for mapping in mappings
    }

    # address_maps are mappings from address-type to address value
    # current is for the current addresses in MO
    # write is for the desired addresses in MO (i.e. from the mapping)

    # print(current_address_map)
    # print(write_address_map)

    # We only wanna edit, if there are updates on the value
    edit_address_type_uuids = set(
        filter(
            lambda key: current_address_map[key] != write_address_map[key],
            edit_address_type_uuids,
        )
    )
    logger.debug("Gonna edit", address_type_uuids=edit_address_type_uuids)

    # Filter write_address_map by insert_address_type_uuids
    insert_address_map = {
        key: value
        for key, value in write_address_map.items()
        if key in insert_address_type_uuids
    }
    insert_addresses = construct_insert_addresses(insert_address_map, user_uuid)

    # Filter write_address_map by edit_address_type_uuids
    edit_address_map = {
        key: value
        for key, value in write_address_map.items()
        if key in edit_address_type_uuids
    }
    edit_addresses = construct_edit_addresses(
        edit_address_map, user.addresses, user_uuid
    )
    # TODO: Handle DELETE

    upsert_addresses = list(chain(insert_addresses, edit_addresses))
    if not upsert_addresses:
        return False

    response = await dataloaders.address_uploader.load_many(upsert_addresses)
    print(response)
    return True
