from datetime import date
from flows.helpers import get_last_occurrence_of


def test_get_last_occurrence_of_when_day_is_tomorrow(mocker):
    mocker.patch("flows.helpers.date").today.return_value = date(
        2023, 2, 13  # A Monday
    )
    weekday_int = 1
    assert date(2023, 2, 7) == get_last_occurrence_of(weekday_int)


def test_get_last_occurrence_of_when_day_is_today(mocker):
    mocker.patch("flows.helpers.date").today.return_value = date(
        2023, 2, 14  # A Tuesday
    )
    weekday_int = 1
    assert date(2023, 2, 14) == get_last_occurrence_of(weekday_int)


def test_get_last_occurrence_of_when_day_is_yesterday(mocker):
    mocker.patch("flows.helpers.date").today.return_value = date(
        2023, 2, 15  # A Wednesday
    )
    weekday_int = 1
    assert date(2023, 2, 14) == get_last_occurrence_of(weekday_int)
