# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Event handling."""
from typing import Any
from typing import cast

from fastapi import FastAPI
from fastramqpi.main import FastRAMQPI


def create_app(**kwargs: Any) -> FastAPI:
    """FastAPI application factory.

    Returns:
        None
    """
    fastramqpi = FastRAMQPI(application_name="adguidsync", **kwargs)
    return cast(FastAPI, fastramqpi.get_app())
