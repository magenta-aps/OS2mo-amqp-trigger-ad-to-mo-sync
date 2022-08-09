# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Business logic."""
from datetime import date
from uuid import UUID

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
    itsystem_uuid = settings.adguid_itsystem_uuid
    if itsystem_uuid is None:
        itsystem_uuid = await dataloaders.itsystems_loader.load(
            settings.adguid_itsystem_bvn
        )

    user = await dataloaders.users_loader.load(user_uuid)
    has_ituser = any(
        map(lambda ituser: ituser.itsystem_uuid == itsystem_uuid, user.itusers)
    )
    if has_ituser:
        # TODO: Should we verify its value?
        print("ITUser already exists")
        return False

    adguid = await dataloaders.adguid_loader.load(user.cpr_no)
    if adguid is None:
        print("No ADGUID found!")
        return False

    ituser = ITUser.from_simplified_fields(
        user_key=str(adguid),
        itsystem_uuid=itsystem_uuid,
        person_uuid=user.uuid,
        from_date=date.today().isoformat(),
    )
    print(ituser)
    # TODO: Upload dataloader?
    response = await model_client.upload([ituser])
    print(response)
    return True
