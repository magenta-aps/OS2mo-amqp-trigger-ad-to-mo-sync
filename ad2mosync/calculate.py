# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Business logic."""
from uuid import UUID

import structlog
from more_itertools import only

from .config import Settings
from .dataloaders import Dataloaders


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
    adguid = ituser.user_key

    logger = logger.bind(adguid=adguid)
    logger.info("Synchronizing user")
    # TODO: Parse mapping yaml file and pull relevant MO and AD fields?

    return True
