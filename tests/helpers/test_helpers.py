from pendulum import parse
import pendulum
import pytest
from src.helpers.helpers import (
    fetch_content,
    get_all_files,
    get_last_occurrence_of,
    log_validation_problem,
)

PENDULUM_IMPORT = "src.helpers.helpers.pendulum"


def test_fetch_content_when_successful(mocker):
    resp = mocker.patch("src.helpers.helpers.get")
    resp.return_value.content = "foobar"
    assert "foobar" == fetch_content("https://example.com")


def test_fetch_content_when_unsuccessful(mocker):
    resp = mocker.patch("src.helpers.helpers.get")
    resp.return_value.raise_for_status.side_effect = Exception
    with pytest.raises(Exception):
        fetch_content("https://example.com")


def test_get_all_files(mocker):
    client = mocker.patch("src.helpers.helpers.client")
    client.list_objects_v2.return_value = {
        "Contents": [{"Key": "foo.csv"}, {"Key": "bar.csv"}],
        "NextContinuationToken": None,
    }
    assert ["foo.csv", "bar.csv"] == list(get_all_files("dir"))


def test_get_all_files_modified_after(mocker):
    client = mocker.patch("src.helpers.helpers.client")
    client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "foo.csv", "LastModified": pendulum.parse("2019-01-01 00:00")},
            {"Key": "bar.csv", "LastModified": pendulum.parse("2020-01-01 00:00")},
        ],
        "NextContinuationToken": None,
    }
    assert ["bar.csv"] == list(get_all_files("dir", pendulum.parse("2019-07-01 00:00")))


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
