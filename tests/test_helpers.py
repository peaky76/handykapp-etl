from pendulum import parse
from flows.helpers import get_last_occurrence_of


def test_get_last_occurrence_of_when_day_is_tomorrow(mocker):
    mocker.patch("flows.helpers.pendulum").now.return_value = parse(
        "2023-02-13"
    )  # A Monday
    weekday_int = 2
    assert parse("2023-02-07") == get_last_occurrence_of(weekday_int)


def test_get_last_occurrence_of_when_day_is_today(mocker):
    mocker.patch("flows.helpers.pendulum").now.return_value = parse(
        "2023-02-14"
    )  # A Tuesday
    weekday_int = 2
    assert parse("2023-02-14") == get_last_occurrence_of(weekday_int)


def test_get_last_occurrence_of_when_day_is_yesterday(mocker):
    mocker.patch("flows.helpers.pendulum").now.return_value = parse(
        "2023-02-15"
    )  # A Wednesday
    weekday_int = 2
    assert parse("2023-02-14") == get_last_occurrence_of(weekday_int)
