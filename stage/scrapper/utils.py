from datetime import datetime, timezone

def urljoin(base_start: str, *parts: str) -> str:
    base = base_start
    for part in filter(None, parts):
        base = '{}/{}'.format(base.rstrip('/'), part.lstrip('/'))
    return base

def str_to_datetime_utc(str):
    """Converts a string into UTC datetime object.

    Args:
        str (str): String timestamp.

    Returns:
        datetime: Datetime object.
    """
    return datetime.fromisoformat(str).replace(tzinfo=timezone.utc)


def datetime_utc(year, month, day, hour, minute):
    """Returns a datetime object in UTC timezone

    Args:
        year (int): eg. 2021
        month (int): between 1-12
        day (int): between 1-7
        hour (int): between 0-23
        minute (int): between 0-59

    Returns:
        datetime
    """
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
