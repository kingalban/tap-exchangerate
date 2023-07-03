from __future__ import annotations

import requests
from math import ceil
from typing import Iterable, Tuple, Union, Generator, Optional, overload, TypeVar, NamedTuple
from datetime import datetime, timedelta, date

T = TypeVar("T")
# exchange_row = NamedTuple("exchange_row", ("date", "currency", "rate"))
DateLikeType = Union[str, datetime, date]


class ExchangeRow(NamedTuple):
    date: date
    currency: str
    rate: float


@overload
def as_date(date_like: str, format=str) -> date: ...


@overload
def as_date(date_like: Union[date, datetime], format=None) -> date: ...


def as_date(date_like: DateLikeType, format="%Y-%m-%d") -> date:
    if isinstance(date_like, datetime):
        return date_like.date()

    elif isinstance(date_like, date):
        return date_like

    elif isinstance(date_like, str):
        return datetime.strptime(date_like, format)

    raise ValueError(f"{date_like!r} of type {type(date_like)} not accepted")


@overload
def generate_date_range(start: DateLikeType, end: DateLikeType, _format: None) -> Generator[date, None, None]: ...


@overload
def generate_date_range(start: DateLikeType, end: DateLikeType, _format: str) -> Generator[str, None, None]: ...


def generate_date_range(start: DateLikeType,
                        end: DateLikeType,
                        _format: Union[str, None] = "%Y-%m-%d") -> Union[Generator[str, None, None],
                                                                         Generator[date, None, None]]:
    """ Generates dates from the start to end inclusive.
        if start > end then no dates are generated. If start == end then one date is generated

        Args:
            start: either a ISO-8601 formatted string, or a datetime.date object
            end: either a ISO-8601 formatted string, or a datetime.date object
            _format: optional string format. Default ISO-8601. If None then returns datetime.date objects.

        Returns:
            a generator of dates. If format is None then items are of type datetime.date, otherwise str.

        Raises:
            ValueError: if start or end are string not conforming to ISO-8601 (with default format)
    """

    start = as_date(start, _format)
    end = as_date(end, _format)
    step = 1 if start <= end else -1

    for days in range(0, (end - start).days + step, step):
        if _format is None:
            yield start + timedelta(days=days)

        else:
            yield (start + timedelta(days=days)).strftime(_format)


def generate_item_pairs(iterator: Iterable[T], n: int) -> Generator[T, None, None]:
    """ generates n tuples, selected from 'iterator' such that they make pairs that span 'iterator'.
        eg: [first, second, ..., final] -> ((first_item,  tenth), (tenth, twentieth), ..., (thirtieth, final))
        eg: [0,1,2,3,4,5,6,7,8,9] -> [(0, 4), (4, 8), (8, 9)]
    """
    if n == 0:
        return

    all_items = list(iterator)

    narrowed_items = all_items[::ceil(len(all_items) / n)]

    narrowed_items_with_final = narrowed_items + ([all_items[-1]] if all_items[-1] not in narrowed_items else [])

    yield from zip(narrowed_items_with_final, narrowed_items_with_final[1:])


def generate_date_pairs(start_date: DateLikeType,
                        end_date: DateLikeType,
                        n: int) -> Generator[Tuple[date, date], None, None]:
    yield from generate_item_pairs(generate_date_range(start_date, end_date), n)


def zip_is_last(iterable: Iterable[T]) -> Generator[Tuple[T, bool], None, None]:
    it = iter(iterable)

    try:
        next_value = next(it)

    except StopIteration:
        return

    while True:
        current_value = next_value

        try:
            next_value = next(it)
            yield current_value, False

        except StopIteration:
            yield current_value, True
            return


def get_exr_timeseries(start_date: str, end_date: str) -> Generator[ExchangeRow, None, None]:
    def get_exr_366_days(start_date: DateLikeType, end_date: DateLikeType) -> Generator[ExchangeRow, None, None]:
        """ get exchange rates for a range of up to 366 days """
        assert (as_date(end_date) - as_date(start_date)).days <= 365, \
            f"too many days: from {start_date} to {end_date} is {(as_date(start_date) - as_date(end_date)).days} days"

        response = requests.get(f'https://api.exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}')
        response.raise_for_status()

        if "rates" in (json_resp := response.json()):
            for record_date, rates in json_resp["rates"].items():
                for currency, rate in rates.items():
                    yield ExchangeRow(record_date, currency, float(rate))
        else:
            raise ValueError(f"response did not include rates. {json_resp=}")

    n_cuts = ceil(abs((as_date(end_date) - as_date(start_date)).days) / 365)

    for date_pair, is_last_pair in zip_is_last(generate_date_pairs(start_date, end_date, n_cuts)):
        # generate_date_pairs gives pairs with overlapping ends. This will give duplicates
        # unless it's the last date, shift the later date back one day.
        day_offset = 0 if is_last_pair else 1
        yield from get_exr_366_days(
            date_pair[0],
            (as_date(date_pair[1]) - timedelta(days=day_offset)).strftime("%Y-%m-%d")
        )
