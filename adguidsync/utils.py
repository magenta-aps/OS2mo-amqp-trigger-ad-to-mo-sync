# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Various utilities."""
from collections.abc import Iterable
from collections.abc import Iterator
from typing import TypeVar


T = TypeVar("T")


def remove_duplicates(iterator: Iterable[T] | Iterator[T]) -> list[T]:
    """Remove duplicates from the input and return the result as a list.

    Args:
        iterator: Input to remove duplicated elements from.

    Return:
        Deduplicated list of results.
    """
    return list(dict.fromkeys(iterator))
