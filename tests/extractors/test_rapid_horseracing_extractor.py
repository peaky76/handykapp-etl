from src.extractors.rapid_horseracing_extractor import (
    LIMITS,
    RACECARDS_DESTINATION,
    RESULTS_DESTINATION,
    SOURCE,
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
