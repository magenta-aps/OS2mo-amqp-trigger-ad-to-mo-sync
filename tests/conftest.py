# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
"""This module contains pytest specific code, fixtures and helpers."""
import os
from collections.abc import Iterator

import pytest

from ad2mosync.config import Settings


@pytest.fixture
def settings_overrides() -> Iterator[dict[str, str]]:
    """Fixture to construct dictionary of minimal overrides for valid settings.

    Yields:
        Minimal set of overrides.
    """
    overrides = {
        "CLIENT_SECRET": "Hunter2",
        "AD_CONTROLLERS": '[{"host": "localhost"}]',
        "AD_DOMAIN": "Kommune",
        "AD_PASSWORD": "Hunter2",
        "AD_CPR_ATTRIBUTE": "extensionAttribute3",
        "AD_SEARCH_BASE": "OU=Fiktiv kommune,DC=fiktiv,DC=net",
    }
    yield overrides


@pytest.fixture
def load_settings_overrides(
    settings_overrides: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> Iterator[dict[str, str]]:
    """Fixture to set happy-path settings overrides as environmental variables.

    Note:
        Only loads environmental variables, if variables are not already set.

    Args:
        settings_overrides: The list of settings to load in.
        monkeypatch: Pytest MonkeyPatch instance to set environmental variables.

    Yields:
        Minimal set of overrides.
    """
    for key, value in settings_overrides.items():
        if os.environ.get(key) is not None:
            continue
        monkeypatch.setenv(key, value)
    yield settings_overrides


@pytest.fixture
def settings(load_settings_overrides: dict[str, str]) -> Iterator[Settings]:
    """Fixture to construct settings.

    Args:
        load_settings_overrides: Unused variable, called purely for side-effect.

    Yields:
        Happy-path settings.
    """
    settings = Settings()
    yield settings
