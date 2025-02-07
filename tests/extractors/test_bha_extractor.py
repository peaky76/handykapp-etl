import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.extractors.bha_extractor import (
    DESTINATION,
    SOURCE,
    fetch,
    save,
)


def test_bha_source():
    expected = "https://api09.horseracing.software/bha/v1/ratings/csv/ratings"
    assert expected == SOURCE


def test_bha_destination():
    expected = "handykapp/bha/"
    assert expected == DESTINATION


def test_fetch(mocker):
    mocker.patch("src.extractors.bha_extractor.fetch_content").return_value = "foobar"
    mocker.patch("src.extractors.bha_extractor.FILES").return_value = {"foo": "bar"}
    assert fetch.fn("foo") == "foobar"


def test_save(mocker):
    write_file = mocker.patch("src.extractors.bha_extractor.write_file")
    mocker.patch("src.extractors.bha_extractor.DESTINATION", "example/")
    mocker.patch("src.extractors.bha_extractor.LAST_UPDATE_STR", "20210101")

    save.fn("foo", "foobar")

    assert write_file.call_count == 1
    assert mocker.call("foobar", "example/bha_foo_20210101.csv") == write_file.call_args
