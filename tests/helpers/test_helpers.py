import pytest
from pendulum import TUESDAY, parse

from src.helpers.helpers import (
    apply_newmarket_workaround,
    fetch_content,
    get_last_occurrence_of,
    log_validation_problem,
)

PENDULUM_IMPORT = "src.helpers.helpers.pendulum"


def test_fetch_content_when_successful(mocker):
    resp = mocker.patch("src.helpers.helpers.get")
    resp.return_value.content = "foobar"
    assert fetch_content("https://example.com") == "foobar"


def test_fetch_content_when_unsuccessful(mocker):
    resp = mocker.patch("src.helpers.helpers.get")
    resp.return_value.raise_for_status.side_effect = Exception
    with pytest.raises(Exception):
        fetch_content("https://example.com")


def test_get_last_occurrence_of_when_day_is_tomorrow(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-13")  # A Monday
    assert get_last_occurrence_of(TUESDAY) == parse("2023-02-07").date()


def test_get_last_occurrence_of_when_day_is_today(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-14")  # A Tuesday
    assert get_last_occurrence_of(TUESDAY) == parse("2023-02-14").date()


def test_get_last_occurrence_of_when_day_is_yesterday(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-15")  # A Wednesday
    assert get_last_occurrence_of(TUESDAY) == parse("2023-02-14").date()


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
    assert logger().warning.called


def test_apply_newmarket_workaround_for_early_rowley():
    assert apply_newmarket_workaround(parse("2023-05-01")) == "Newmarket Rowley"


def test_apply_newmarket_workaround_for_early_july():
    assert apply_newmarket_workaround(parse("2023-06-11")) == "Newmarket July"


def test_apply_newmarket_workaround_for_late_july():
    assert apply_newmarket_workaround(parse("2023-08-15")) == "Newmarket July"


def test_apply_newmarket_workaround_for_late_rowley():
    assert apply_newmarket_workaround(parse("2023-09-01")) == "Newmarket Rowley"
