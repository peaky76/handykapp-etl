from src.extractors.bha_extractor import fetch, SOURCE, DESTINATION
from src.helpers.helpers import fetch_content


def test_bha_source():
    expected = "https://www.britishhorseracing.com/feeds/v4/ratings/csv/"
    assert expected == SOURCE


def test_bha_destination():
    expected = "handykapp/bha/"
    assert expected == DESTINATION


def test_fetch(mocker):
    mocker.patch("src.extractors.bha_extractor.fetch_content").return_value = "foobar"
    mocker.patch("src.extractors.bha_extractor.FILES").return_value = {"foo": "bar"}
    assert "foobar" == fetch.fn("foo")
