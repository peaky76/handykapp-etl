from pendulum import parse
from src.helpers.helpers import get_last_occurrence_of, log_validation_problem

PENDULUM_IMPORT = "src.helpers.helpers.pendulum"


def test_get_last_occurrence_of_when_day_is_tomorrow(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-13")  # A Monday
    weekday_int = 2
    assert parse("2023-02-07").date() == get_last_occurrence_of(weekday_int)


def test_get_last_occurrence_of_when_day_is_today(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-14")  # A Tuesday
    weekday_int = 2
    assert parse("2023-02-14").date() == get_last_occurrence_of(weekday_int)


def test_get_last_occurrence_of_when_day_is_yesterday(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-15")  # A Wednesday
    weekday_int = 2
    assert parse("2023-02-14").date() == get_last_occurrence_of(weekday_int)


def test_log_validation_problem(mocker):
    logger = mocker.patch("src.helpers.helpers.get_run_logger")
    problem = {
        "name": "name_str",
        "field": "year",
        "row": "1",
        "value": "foobar",
        "error": "ValueError",
    }
    log_validation_problem(problem)
    assert logger().error.called
