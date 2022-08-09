# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""Settings handling."""
from uuid import UUID

from fastramqpi.config import Settings as FastRAMQPISettings
from pydantic import BaseModel
from pydantic import BaseSettings
from pydantic import Field


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


class Settings(BaseSettings):
    """Settings for the ADGUID Sync integration."""

    class Config:
        """Settings are frozen."""

        frozen = True
        env_nested_delimiter = "__"

    fastramqpi: FastRAMQPISettings = Field(
        default_factory=FastRAMQPISettings, description="FastRAMQPI settings"
    )

    ad_controllers: list[ServerConfig] = Field(
        ..., description="List of domain controllers to query"
    )
    ad_domain: str = Field(
        ..., description="Domain to use when authenticating with the domain controller"
    )
    ad_user: str = Field(
        ...,
        description="Username to use when authenticating with the domain controller",
    )
    ad_password: str = Field(
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
        description="UUID of the ADGUID IT-system in OS2mo, if unset falls back to bvn",
    )
    adguid_itsystem_bvn: str = Field(
        "ADGUID", description="User-key of the ADGUID IT-system in OS2mo"
    )
