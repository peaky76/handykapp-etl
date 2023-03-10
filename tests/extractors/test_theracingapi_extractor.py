from src.extractors.theracingapi_extractor import SOURCE, DESTINATION


def test_theracingapi_source():
    expected = "https://the-racing-api1.p.rapidapi.com/v1/"
    assert expected == SOURCE


def test_theracingapi_destination():
    expected = "handykapp/theracingapi/"
    assert expected == DESTINATION
