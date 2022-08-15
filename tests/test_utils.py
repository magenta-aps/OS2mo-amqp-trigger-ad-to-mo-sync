# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
"""Testing various utilities."""
from collections import Counter
from collections.abc import Iterable
from collections.abc import Iterator
from itertools import tee

import pytest
from hypothesis import given

from adguidsync.utils import remove_duplicates


@pytest.mark.parametrize(
    "iterator,expected",
    [
        # Empty lists are handled nicely
        ([], []),
        # One element lists are handled nicely
        ([1], [1]),
        ([2], [2]),
        # Two element lists are handled nicely, perserving order
        ([1, 2], [1, 2]),
        ([2, 1], [2, 1]),
        # Duplicates are removed
        ([1, 1], [1]),
        ([2, 2], [2]),
        # Duplicates are removed, while perserving order
        ([1, 1, 1], [1]),
        ([1, 1, 2], [1, 2]),
        ([1, 2, 2], [1, 2]),
        ([2, 1, 1], [2, 1]),
        ([2, 1, 2], [2, 1]),
        ([2, 2, 1], [2, 1]),
        ([2, 2, 2], [2]),
    ],
)
def test_remove_duplicates_fixpoint(iterator: list[int], expected: list[int]) -> None:
    """Ensure that the output of remove_duplicates is as expected.

    Args:
        iterator: Input to verify remove_duplicates with.
        expected: The expected output of running remove_duplicates on the iterator.

    Return:
        None
    """
    result = remove_duplicates(iterator)
    assert result == expected


@given(...)
def test_remove_duplicates_count(iterator: Iterable[int] | Iterator[int]) -> None:
    """Ensure that the output has no duplicates.

    Args:
        iterator: Input to verify remove_duplicates with.

    Return:
        None
    """
    result = remove_duplicates(iterator)

    # Count elements and assert that there is only one of each
    counter = Counter(result)
    assert all(count == 1 for element, count in counter.most_common())


@given(...)
def test_remove_duplicates_elements(iterator: Iterable[int] | Iterator[int]) -> None:
    """Ensure that the output has the same elements as the input.

    Args:
        iterator: Input to verify remove_duplicates with.

    Return:
        None
    """
    it1, it2 = tee(iterator, 2)
    result = set(remove_duplicates(it1))
    expected = set(it2)
    assert result == expected


@given(...)
def test_remove_duplicates_order(iterator: Iterable[int] | Iterator[int]) -> None:
    """Ensure that the output has the inputs order.

    Args:
        iterator: Input to verify remove_duplicates with.

    Return:
        None
    """
    it1, it2 = tee(iterator, 2)
    result = remove_duplicates(it1)

    for element in it2:
        # The previous test ensure that the elements are the same, thus if we cannot
        # find an element in the result it must be one we have already have been seen.
        if element not in result:
            continue
        assert element == result.pop(0)
