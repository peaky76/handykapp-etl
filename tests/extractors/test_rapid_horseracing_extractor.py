from src.extractors.rapid_horseracing_extractor import (
    LIMITS,
    RACECARDS_DESTINATION,
    RESULTS_DESTINATION,
    SOURCE,
    get_file_date,
)

LIMITS = {"day": 50, "minute": 10}


def test_rapid_horseracing_source():
    expected = "https://horse-racing.p.rapidapi.com/"
    assert expected == SOURCE


def test_rapid_horseracing_racecards_destination():
    expected = "handykapp/rapid_horseracing/racecards/"
    assert expected == RACECARDS_DESTINATION


def test_rapid_horseracing_racecards_destination():
    expected = "handykapp/rapid_horseracing/results/"
    assert expected == RESULTS_DESTINATION


def test_rapid_horseracing_has_limits():
    expected = ["day", "minute"]
    assert expected == list(LIMITS.keys())


def test_get_file_date():
    filename = "rapid_api_racecards_20200101.json"
    expected = "20200101"
    assert expected == get_file_date(filename)
