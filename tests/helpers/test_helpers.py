from unittest.mock import MagicMock

import pendulum
import pytest
from pendulum import TUESDAY, parse

from src.clients.spaces_client import SpacesClient
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


@pytest.fixture
def mock_spaces_client(mocker):
    mock_client = mocker.patch("src.helpers.helpers.SpacesClient.get")
    return mock_client.return_value

def test_fetch_content_when_successful(mocker):
    resp = mocker.patch("src.helpers.helpers.get")
    resp.return_value.content = "foobar"
    assert fetch_content("https://example.com") == "foobar"


def test_fetch_content_when_unsuccessful(mocker):
    resp = mocker.patch("src.helpers.helpers.get")
    resp.return_value.raise_for_status.side_effect = Exception
    with pytest.raises(Exception):
        fetch_content("https://example.com")


def test_get_files(mock_spaces_client):
    mock_spaces_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "foo.csv"}, {"Key": "bar.csv"}],
        "NextContinuationToken": "",
    }
    assert list(get_files("dir")) == ["foo.csv", "bar.csv"]


def test_get_files_modified_after(mock_spaces_client):
    mock_spaces_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "foo.csv", "LastModified": pendulum.parse("2019-01-01 00:00")},
            {"Key": "bar.csv", "LastModified": pendulum.parse("2020-01-01 00:00")},
        ],
        "NextContinuationToken": None,
    }
    assert list(get_files("dir", pendulum.parse("2019-07-01 00:00"))) == ["bar.csv"]


def test_read_file_for_csv(mock_spaces_client):
    mock_spaces_client.get_object.return_value = {"Body": MagicMock(read=lambda: bytes("foo,bar,baz", "utf-8"))}
    assert read_file("foo.csv") == [["foo", "bar", "baz"]]


def test_read_file_for_json(mock_spaces_client):
    mock_spaces_client.get_object.return_value = {"Body": MagicMock(read=lambda: bytes('{"foo": "bar"}', "utf-8"))}
    assert read_file("foo.json") == {"foo": "bar"}


def test_stream_file(mock_spaces_client):
    mock_spaces_client.get_object.return_value = {"Body": MagicMock(read=lambda: bytes("foobar", "utf-8"))}
    actual = stream_file("foo.csv")
    assert isinstance(actual, bytes)
    assert actual.decode("utf-8") == "foobar"


def test_write_file(mock_spaces_client):
    write_file("foobar", "foo.csv")
    assert mock_spaces_client.put_object.called


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
