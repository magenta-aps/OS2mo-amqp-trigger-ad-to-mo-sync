# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Various utilities."""
from collections.abc import Sequence
from typing import Any


def remove_duplicates(xs: Sequence[Any] | map[Any]) -> list[Any]:
    return list(dict.fromkeys(xs))
