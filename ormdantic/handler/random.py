import datetime
import math
import random
import string

from pydantic.fields import ModelField

from ormdantic.types import AnyNumber, default_max_length


def _random_str_value(model_field: ModelField) -> str:
    """Get a random string."""
    target_length = _get_target_length(
        model_field.field_info.min_length, model_field.field_info.max_length
    )
    choices = string.ascii_letters + string.digits
    return _random_str(choices, target_length)


def _random_number_value(model_field: ModelField) -> AnyNumber:
    """Get a random number."""
    default_max_difference = 256
    iter_size = model_field.field_info.multiple_of or 1
    # Determine lower bound.
    lower = 0
    if ge := model_field.field_info.ge:
        while lower < ge:
            lower += iter_size
    if gt := model_field.field_info.gt:
        while lower <= gt:
            lower += iter_size
    # Determine upper bound.
    upper = lower + iter_size * default_max_difference
    if le := model_field.field_info.le:
        while upper > le:
            upper -= iter_size
    if lt := model_field.field_info.lt:
        while upper >= lt:
            upper -= iter_size
    # Ensure lower bound is not greater than upper bound.
    if (
        not model_field.field_info.ge
        and not model_field.field_info.gt
        and lower > upper
    ):
        lower = upper - iter_size * default_max_difference
    # Ensure upper bound is not less than lower bound.
    if not model_field.field_info.multiple_of:
        return random.randint(lower, upper)
    max_iter_distance = abs(math.floor((upper - lower) / iter_size))
    return lower + iter_size * random.randint(1, max_iter_distance)


def _random_datetime_value() -> datetime.datetime:
    """Get a random datetime."""
    dt = datetime.datetime.fromordinal(_random_date_value().toordinal())
    dt += _random_timedelta_value()
    return dt


def _random_date_value() -> datetime.date:
    """Get a random date."""
    return datetime.date(
        year=random.randint(1, 9999),
        month=random.randint(1, 12),
        day=random.randint(1, 28),
    )


def _random_time_value() -> datetime.time:
    """Get a random time."""
    return datetime.time(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
    )


def _random_timedelta_value() -> datetime.timedelta:
    """Get a random timedelta."""
    return datetime.timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


def _random_str(choices: str, target_length: int) -> str:
    """Get a random string."""
    return "".join(random.choice(choices) for _ in range(target_length))


def _get_target_length(min_length: int | None, max_length: int | None) -> int:
    """Get a random target length."""
    if not min_length:
        if max_length is not None:
            min_length = random.randint(0, max_length - 1)
        else:
            min_length = random.randint(0, default_max_length)
    max_length = max_length or random.randint(1, default_max_length) + min_length
    return random.choice(range(min_length, max_length))
