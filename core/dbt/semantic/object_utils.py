from __future__ import annotations
import pprint
import textwrap
import itertools
import random
import string
from collections import OrderedDict, defaultdict, deque
from collections.abc import Mapping
from dataclasses import is_dataclass, fields
import datetime
from hashlib import sha1
from typing import Sequence, TypeVar, Tuple, NoReturn, Union

from dbt.dataclass_schema import dbtClassMixin


def assert_values_exhausted(value: NoReturn) -> NoReturn:
    """Helper method to allow MyPy to guarantee an exhaustive switch through an enumeration or literal

    DO NOT MODIFY THE TYPE SIGNATURE OF THIS FUNCTION UNLESS MYPY CHANGES HOW IT HANDLES THINGS

    To use this function correctly you MUST do an exhaustive switch through ALL values, using `is` for comparison
    (doing x == SomeEnum.VALUE will not work, nor will `x in (SomeEnum.VALUE_1, SomeEnum.VALUE_2)`).

    If mypy raises an error of the form:
      `x has incompatible type SomeEnum; expected NoReturn`
    the switch is not constructed correctly. Fix your switch statement to use `is` for all comparisons.

    If mypy raises an error of the form
      `x has incompatible type Union[Literal...]` expected NoReturn`
    the switch statement is non-exhaustive, and the values listed in the error message need to be accounted for.

    See https://mypy.readthedocs.io/en/stable/literal_types.html#exhaustiveness-checks
    For an enum example, see issue:
    https://github.com/python/mypy/issues/6366#issuecomment-560369716
    """
    assert False, f"Should be unreachable, but got {value}"


def assert_exactly_one_arg_set(**kwargs) -> None:  # type: ignore
    """Throws an assertion error if 0 or more than 1 argument is not None."""
    num_set = 0
    for value in kwargs.values():
        if value is not None:
            num_set += 1

    assert num_set == 1, f"{num_set} argument(s) set instead of 1 in arguments: {kwargs}"


def is_hashable_base_model(obj):  # type:ignore # noqa: D
    return isinstance(obj, dbtClassMixin)


def _to_pretty_printable_object(obj):  # type: ignore
    """Convert the object that will look nicely when fed into the PrettyPrinter.

    Main change is that dataclasses will have a field with the class name. In Python 3.10, the pretty printer class will
    support dataclasses, so we can remove this once we're on 3.10. Also tried the prettyprint package with dataclasses,
    but that prints full names for the classes e.g. a.b.MyClass and it also always added line breaks, even if an object
    could fit on one line, so preferred to not use that.

    e.g.
    metricflow.specs.DimensionSpec(
        name='country',
        identifier_links=()
    ),

    Instead, the below will print something like:

    {'class': 'DimensionSpec',
     'name': 'country_latest',
     'identifier_links': ({'class': 'IdentifierSpec',
                           'name': 'listing',
                           'identifier_links': ()},)}
    """
    if obj is None:
        return None

    elif isinstance(obj, (str, int, float)):
        return obj

    elif isinstance(obj, (list, tuple)):
        result = []
        for item in obj:
            result.append(_to_pretty_printable_object(item))

        if isinstance(obj, list):
            return result
        elif isinstance(obj, tuple):
            return tuple(result)

        assert False

    elif isinstance(obj, Mapping):
        result = {}
        for key, value in obj.items():
            result[_to_pretty_printable_object(key)] = _to_pretty_printable_object(value)
        return result

    elif is_dataclass(obj):
        result = {"class": type(obj).__name__}

        for field in fields(obj):
            result[field.name] = _to_pretty_printable_object(getattr(obj, field.name))
        return result
    elif is_hashable_base_model(obj):
        result = {"class": type(obj).__name__}

        for field_name, value in obj.dict().items():
            result[field_name] = _to_pretty_printable_object(value)
        return result

    # Can't make it more pretty.
    return obj


def pretty_format(obj) -> str:  # type: ignore
    """Return the object as a string that looks pretty."""
    if isinstance(obj, str):
        return obj
    return pprint.pformat(_to_pretty_printable_object(obj), width=80, sort_dicts=False)


def pformat_big_objects(*args, **kwargs) -> str:  # type: ignore
    """Prints a series of objects with many fields in a pretty way.

    See _to_pretty_printable_object() for more context on this format. Looks like:

    measure_recipe:
    {'class': 'MeasureRecipe',
     'measure_node': ReadSqlSourceNode(node_id=rss_140),
     'required_local_linkable_specs': ({'class': 'DimensionSpec',
                                        'name': 'is_instant',
                                        'identifier_links': ()},),
     'join_linkable_instances_recipes': ()}

    """
    items = []
    for arg in args:
        items.append(pretty_format(arg))
    for key, value in kwargs.items():
        items.append(f"{key}:")
        items.append(textwrap.indent(pretty_format(value), prefix="    "))
    return "\n".join(items)


SequenceT = TypeVar("SequenceT")


def flatten_nested_sequence(
    sequence_of_sequences: Sequence[Sequence[SequenceT]],
) -> Tuple[SequenceT, ...]:
    """Convert a nested sequence into a flattened tuple.

    e.g. ((1,2), (3,4)) -> (1, 2, 3, 4)
    """
    return tuple(itertools.chain.from_iterable(sequence_of_sequences))


def flatten_and_dedupe(
    sequence_of_sequences: Sequence[Sequence[SequenceT]],
) -> Tuple[SequenceT, ...]:
    """Convert a nested sequence into a flattened tuple, with de-duping.

    e.g. ((1,2), (2,3)) -> (1, 2, 3)
    """
    items = flatten_nested_sequence(sequence_of_sequences)
    return tuple(OrderedDict.fromkeys(items))


def random_id() -> str:
    """Generates an 8-digit random alphanumeric string."""
    alphabet = string.ascii_lowercase + string.digits
    # Characters that go below the line are visually unappealing, so don't use those.
    filtered_alphabet = [x for x in alphabet if x not in "gjpqy"]
    return "".join(random.choices(filtered_alphabet, k=8))


def hash_items(items: Sequence[SqlColumnType]) -> str:
    """Produces a hash from a list of strings."""
    hash_builder = sha1()
    for item in items:
        hash_builder.update(str(item).encode("utf-8"))
    return hash_builder.hexdigest()


SqlColumnType = Union[str, int, float, datetime.datetime, datetime.date, bool]


class iter_bucket:
    """Wrap *iterable* and return an object that buckets it iterable into
    child iterables based on a *key* function.
        >>> iterable = ['a1', 'b1', 'c1', 'a2', 'b2', 'c2', 'b3']
        >>> s = bucket(iterable, key=lambda x: x[0])  # Bucket by 1st character
        >>> sorted(list(s))  # Get the keys
        ['a', 'b', 'c']
        >>> a_iterable = s['a']
        >>> next(a_iterable)
        'a1'
        >>> next(a_iterable)
        'a2'
        >>> list(s['b'])
        ['b1', 'b2', 'b3']
    The original iterable will be advanced and its items will be cached until
    they are used by the child iterables. This may require significant storage.
    By default, attempting to select a bucket to which no items belong  will
    exhaust the iterable and cache all values.
    If you specify a *validator* function, selected buckets will instead be
    checked against it.
        >>> from itertools import count
        >>> it = count(1, 2)  # Infinite sequence of odd numbers
        >>> key = lambda x: x % 10  # Bucket by last digit
        >>> validator = lambda x: x in {1, 3, 5, 7, 9}  # Odd digits only
        >>> s = bucket(it, key=key, validator=validator)
        >>> 2 in s
        False
        >>> list(s[2])
        []
    """

    def __init__(self, iterable, key, validator=None):
        self._it = iter(iterable)
        self._key = key
        self._cache = defaultdict(deque)
        self._validator = validator or (lambda x: True)

    def __contains__(self, value):
        if not self._validator(value):
            return False

        try:
            item = next(self[value])
        except StopIteration:
            return False
        else:
            self._cache[value].appendleft(item)

        return True

    def _get_values(self, value):
        """
        Helper to yield items from the parent iterator that match *value*.
        Items that don't match are stored in the local cache as they
        are encountered.
        """
        while True:
            # If we've cached some items that match the target value, emit
            # the first one and evict it from the cache.
            if self._cache[value]:
                yield self._cache[value].popleft()
            # Otherwise we need to advance the parent iterator to search for
            # a matching item, caching the rest.
            else:
                while True:
                    try:
                        item = next(self._it)
                    except StopIteration:
                        return
                    item_value = self._key(item)
                    if item_value == value:
                        yield item
                        break
                    elif self._validator(item_value):
                        self._cache[item_value].append(item)

    def __iter__(self):
        for item in self._it:
            item_value = self._key(item)
            if self._validator(item_value):
                self._cache[item_value].append(item)

        yield from self._cache.keys()

    def __getitem__(self, value):
        if not self._validator(value):
            return iter(())

        return self._get_values(value)
