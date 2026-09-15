"""Virtual time in whole hours.

Nothing reads the wall clock.  PyIceberg stamps snapshots with real
milliseconds, so the table wrapper keeps its own ledger of virtual commit
times and expires snapshots by id.

S3 rounds lifecycle actions to midnight UTC:

    "Amazon S3 calculates the time by adding the number of days specified in
    the rule to the time when the new successor version of the object is
    created and rounding up the resulting time to the next day at midnight
    UTC."  -- Amazon S3 User Guide, "Lifecycle configuration elements"

``ceil_to_midnight`` is that rounding; an instant already on a midnight stays
(AWS's worked example: 1/15 10:30 + 3 d -> 1/19 00:00).  lhbench used a day
clock; ``day_of`` / ``at`` convert, so its policies run unchanged on hours.
"""

HOURS_PER_DAY = 24


def day_of(hour: int) -> int:
    return hour // HOURS_PER_DAY


def hour_of_day(hour: int) -> int:
    return hour % HOURS_PER_DAY


def at(day: int, hour: int = 0) -> int:
    return day * HOURS_PER_DAY + hour


def ceil_to_midnight(hour: int) -> int:
    """Round up to the next midnight; a midnight rounds to itself."""
    return -(-hour // HOURS_PER_DAY) * HOURS_PER_DAY


def days(hours: float) -> float:
    return hours / HOURS_PER_DAY


def next_at_or_after(times, t):
    """First calendar entry >= t, or +inf when the calendar is exhausted."""
    for p in times:
        if p >= t:
            return p
    return float("inf")


def calendar(predicate, hour: int, total_days: int):
    """Hours at which a job runs: every day d for which predicate(d) holds."""
    return [at(d, hour) for d in range(total_days) if predicate(d)]
