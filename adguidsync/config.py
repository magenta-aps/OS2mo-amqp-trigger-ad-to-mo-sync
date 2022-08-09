# SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=too-few-public-methods
"""Settings handling."""
from fastramqpi import FastRAMQPISettings
from pydantic import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Settings for the ADGUID Sync integration."""

    class Config:
        """Settings are frozen."""

        frozen = True
        env_nested_delimiter = "__"

    fastramqpi: FastRAMQPISettings = Field(
        default_factory=FastRAMQPISettings, description="FastRAMQPI settings"
    )
