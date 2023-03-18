import pendulum
from src.extractors.theracingapi_extractor import (
    SOURCE,
    DESTINATION,
    extract_countries,
    extract_racecards,
    get_headers,
)


def test_theracingapi_source():
    expected = "https://the-racing-api1.p.rapidapi.com/v1/"
    assert expected == SOURCE


def test_theracingapi_destination():
    expected = "handykapp/theracingapi/"
    assert expected == DESTINATION


def test_get_headers(mocker):
    mocker.patch(
        "src.extractors.theracingapi_extractor.Secret.load"
    ).return_value.get.return_value = "<key>"
    headers = get_headers()
    assert "the-racing-api1.p.rapidapi.com" == headers["x-rapidapi-host"]
    assert "<key>" == headers["x-rapidapi-key"]


def test_extract_countries(mocker):
    mocker.patch("src.extractors.theracingapi_extractor.fetch_content").return_value = {
        "name": "foobaristan"
    }
    assert {"name": "foobaristan"} == extract_countries.fn()


def test_extract_racecards_for_tomorrow_as_default(mocker):
    write_file = mocker.patch("src.extractors.theracingapi_extractor.write_file")
    mocker.patch("src.extractors.theracingapi_extractor.fetch_content").return_value = [
        {}
    ]
    mocker.patch("src.extractors.theracingapi_extractor.DESTINATION", "dir/")
    mocker.patch("pendulum.now").return_value = pendulum.parse("2020-01-01")

    extract_racecards.fn()

    expected_destination = "dir/racecards/theracingapi_racecards_20200102.json"
    assert 1 == write_file.call_count
    assert mocker.call([{}], expected_destination) == write_file.call_args
