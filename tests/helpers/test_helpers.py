import pendulum
import pytest
from pendulum import parse

from src.helpers.helpers import (
    fetch_content,
    get_files,
    get_last_occurrence_of,
    log_validation_problem,
    read_file,
    stream_file,
    write_file,
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


def test_get_files(mocker):
    client = mocker.patch("src.helpers.helpers.client")
    client.list_objects_v2.return_value = {
        "Contents": [{"Key": "foo.csv"}, {"Key": "bar.csv"}],
        "NextContinuationToken": "",
    }
    assert ["foo.csv", "bar.csv"] == list(get_files("dir"))


def test_get_files_modified_after(mocker):
    client = mocker.patch("src.helpers.helpers.client")
    client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "foo.csv", "LastModified": pendulum.parse("2019-01-01 00:00")},
            {"Key": "bar.csv", "LastModified": pendulum.parse("2020-01-01 00:00")},
        ],
        "NextContinuationToken": None,
    }
    assert ["bar.csv"] == list(get_files("dir", pendulum.parse("2019-07-01 00:00")))


def test_get_last_occurrence_of_when_day_is_tomorrow(mocker):
    mocker.patch(PENDULUM_IMPORT).now.return_value = parse("2023-02-13")  # A Monday
    weekday_int = 2
    assert parse("2023-02-07").date() == get_last_occurrence_of(weekday_int)


def test_read_file_for_csv(mocker):
    mocker.patch("src.helpers.helpers.stream_file").return_value = bytes(
        "foo,bar,baz", "utf-8"
    )
    assert [["foo", "bar", "baz"]] == read_file("foo.csv")


def test_read_file_for_json(mocker):
    mocker.patch("src.helpers.helpers.stream_file").return_value = bytes(
        '{"foo": "bar"}', "utf-8"
    )
    assert {"foo": "bar"} == read_file("foo.json")


def test_stream_file(mocker):
    client = mocker.patch("src.helpers.helpers.client")
    body = client.get_object.return_value["Body"]
    body.read = lambda: bytes("foobar", "utf-8")

    actual = stream_file("foo.csv")
    assert isinstance(actual, bytes)
    assert actual.decode("utf-8") == "foobar"


def test_write_file(mocker):
    client = mocker.patch("src.helpers.helpers.client")
    write_file("foobar", "foo.csv")
    assert client.put_object.called


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
    assert logger().warning.called
