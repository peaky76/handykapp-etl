from unittest.mock import MagicMock

import pendulum
import pytest

from src.clients.spaces_client import SpacesClient


@pytest.fixture
def mock_spaces_client(mocker):
    mock_client = mocker.patch("src.clients.spaces_client.SpacesClient.get")
    return mock_client.return_value


def test_get_files(mock_spaces_client):
    mock_spaces_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "foo.csv"}, {"Key": "bar.csv"}],
        "NextContinuationToken": "",
    }
    assert list(SpacesClient.get_files("dir")) == ["foo.csv", "bar.csv"]


def test_get_files_modified_after(mock_spaces_client):
    mock_spaces_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "foo.csv", "LastModified": pendulum.parse("2019-01-01 00:00")},
            {"Key": "bar.csv", "LastModified": pendulum.parse("2020-01-01 00:00")},
        ],
        "NextContinuationToken": None,
    }
    assert list(SpacesClient.get_files("dir", pendulum.parse("2019-07-01 00:00"))) == [
        "bar.csv"
    ]


def test_read_file_for_csv(mock_spaces_client):
    mock_spaces_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: bytes("foo,bar,baz", "utf-8"))
    }
    assert SpacesClient.read_file("foo.csv") == [["foo", "bar", "baz"]]


def test_read_file_for_json(mock_spaces_client):
    mock_spaces_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: bytes('{"foo": "bar"}', "utf-8"))
    }
    assert SpacesClient.read_file("foo.json") == {"foo": "bar"}


def test_stream_file(mock_spaces_client):
    mock_spaces_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: bytes("foobar", "utf-8"))
    }
    actual = SpacesClient.stream_file("foo.csv")
    assert isinstance(actual, bytes)
    assert actual.decode("utf-8") == "foobar"


def test_write_file(mock_spaces_client):
    SpacesClient.write_file("foobar", "foo.csv")
    assert mock_spaces_client.put_object.called
