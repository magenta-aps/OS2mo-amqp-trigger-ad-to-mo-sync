# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Business logic."""
from datetime import date
from uuid import UUID

import structlog
from raclients.modelclient.mo import ModelClient
from ramodels.mo.details import ITUser

from .config import Settings
from .dataloaders import Dataloaders


async def ensure_adguid_itsystem(
    user_uuid: UUID,
    settings: Settings,
    dataloaders: Dataloaders,
    model_client: ModelClient,
) -> bool:
    """Ensure that an ADGUID IT-system exists in MO for the given user.

    Args:
        user_uuid: UUID of the user to ensure existence for.

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

    has_ituser = any(
        map(lambda ituser: ituser.itsystem_uuid == itsystem_uuid, user.itusers)
    )
    if has_ituser:
        # TODO: Should we verify its value?
        logger.info("ITUser already exists")
        return False

    adguid = await dataloaders.adguid_loader.load(user.cpr_no)
    if adguid is None:
        logger.info("Unable to find ad user by cpr number")
        return False

    logger = logger.bind(adguid=adguid)

    ituser = ITUser.from_simplified_fields(
        user_key=str(adguid),
        itsystem_uuid=itsystem_uuid,
        person_uuid=user.uuid,
        from_date=date.today().isoformat(),
    )
    logger.info("Creating ITUser for user")
    # TODO: Upload dataloader?
    response = await model_client.upload([ituser])
    logger.debug("Creating ITUser response", response=response)
    return True
