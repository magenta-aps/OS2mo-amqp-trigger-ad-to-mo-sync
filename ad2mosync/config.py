# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""Settings handling."""
from collections.abc import Iterator
from typing import Any
from typing import TYPE_CHECKING
from uuid import UUID

from fastramqpi.config import Settings as FastRAMQPISettings
from more_itertools import all_unique
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import ConstrainedList
from pydantic import Field
from pydantic import root_validator
from pydantic import SecretStr
from pydantic.types import T

if TYPE_CHECKING:
    from pydantic.typing import CallableGenerator


class ServerConfig(BaseModel):
    """Settings model for domain controllers."""

    class Config:
        """Settings are frozen."""

        frozen = True

    host: str = Field(..., description="Hostname / IP to establish connection with")
    port: int | None = Field(
        None,
        description=(
            "Port to utilize when establishing a connection. Defaults to 636 for SSL"
            " and 389 for non-SSL"
        ),
    )
    use_ssl: bool = Field(False, description="Whether to establish a SSL connection")
    insecure: bool = Field(False, description="Whether to verify SSL certificates")
    timeout: int = Field(5, description="Number of seconds to wait for connection")


class ServerList(ConstrainedList):
    """Constrainted list for domain controllers."""

    min_items = 1
    unique_items = True

    item_type = ServerConfig
    __args__ = (ServerConfig,)


class ADMapping(BaseModel):
    """Settings model for field mapping."""

    class Config:
        """Settings are frozen."""

        frozen = True

    ad_field: str = Field(..., description="Name of the AD field to read data from.")
    mo_address_type_user_key: str | None = Field(
        None,
        description="User-key of the address type in MO to maintain the address under.",
    )
    mo_address_type_uuid: UUID | None = Field(
        None,
        description=(
            "UUID of the address type in MO to maintain the address under, if unset"
            " falls back to user_key"
        ),
    )
    # TODO: Add a templating method here in the future?
    # TODO: Add support for visibility?

    @root_validator
    def either_user_key_or_uuid_must_be_set(
        cls, values: dict[str, Any]
    ) -> dict[str, Any]:
        """Ensure either user_key or uuid is set."""
        if (
            values.get("mo_address_type_user_key") is None
            and values.get("mo_address_type_uuid") is None
        ):
            raise ValueError(
                "One of 'mo_address_type_user_key' or "
                "'mo_address_type_uuid' must be set."
            )
        return values


class ADMappingList(ConstrainedList):
    """Constrainted list for field mappings."""

    min_items = 1
    unique_items = True

    item_type = ADMapping
    __args__ = (ADMapping,)

    @classmethod
    def __get_validators__(cls) -> "CallableGenerator":
        yield from super().__get_validators__()
        yield cls.address_types_must_be_unique

    @classmethod
    def address_types_must_be_unique(cls, values: list[T] | None) -> list[T] | None:
        """Ensure that user_key is unique accross all submodels."""
        if values is None:
            return None

        # If we were not given dicts, let the ADMapping validator handle it
        all_dict = all(map(lambda val: isinstance(val, dict), values))
        if not all_dict:
            return values

        # mypy is too dumb to realize the below is now an invariant
        addresses: list[dict[str, Any]] = values  # type: ignore
        address_type_user_keys: Iterator[str | None] = map(
            lambda address: address.get("mo_address_type_user_key"), addresses
        )
        address_type_user_keys = filter(None.__ne__, address_type_user_keys)
        if all_unique(address_type_user_keys) is False:
            raise ValueError(
                "'mo_address_type_user_key' must be unique across entire list."
            )

        address_type_uuids: Iterator[str | None] = map(
            lambda address: address.get("mo_address_type_uuid"), addresses
        )
        address_type_uuids = filter(None.__ne__, address_type_uuids)
        if all_unique(address_type_uuids) is False:
            raise ValueError(
                "'mo_address_type_uuid' must be unique across entire list."
            )

        return values


class Settings(BaseSettings):
    """Settings for the ADGUID Sync integration."""

    class Config:
        """Settings are frozen."""

        frozen = True
        env_nested_delimiter = "__"

    fastramqpi: FastRAMQPISettings = Field(
        default_factory=FastRAMQPISettings, description="FastRAMQPI settings"
    )

    ad_controllers: ServerList = Field(
        ..., description="List of domain controllers to query"
    )
    ad_domain: str = Field(
        ..., description="Domain to use when authenticating with the domain controller"
    )
    ad_user: str = Field(
        "os2mo",
        description="Username to use when authenticating with the domain controller",
    )
    ad_password: SecretStr = Field(
        ...,
        description="Password to use when authenticating with the domain controller",
    )
    ad_cpr_attribute: str = Field(
        ..., description="AD attribute which contains the CPR number"
    )
    ad_search_base: str = Field(
        ..., description="Search base to utilize for all AD requests"
    )

    adguid_itsystem_uuid: UUID | None = Field(
        None,
        description=(
            "UUID of the ADGUID IT-system in OS2mo, if unset falls back to user_key"
        ),
    )
    adguid_itsystem_user_key: str = Field(
        "ADGUID", description="User-key of the ADGUID IT-system in OS2mo"
    )

    ad_mappings: ADMappingList = Field(..., description="List of AD to MO mappings")
