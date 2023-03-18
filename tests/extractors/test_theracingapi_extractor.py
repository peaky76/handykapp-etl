from src.extractors.theracingapi_extractor import (
    SOURCE,
    DESTINATION,
    extract_countries,
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
