import pendulum
import pytest

from src.transformers.betfair_transformer import (
    get_places_from_place_detail,
    transform_betfair_pnl_data,
    validate_betfair_pnl_data,
)


@pytest.fixture
def mock_data():
    return [
        row.split(",")
        for row in [
            "\ufeffMarket,Start time,Settled date,Profit/Loss (£)",
            "Horse Racing / Brighton 30th Apr : 1m2f Hcap,30-Apr-24 16:10,30-Apr-24 16:13,78.60"
        ]
    ]

def test_get_places_from_place_detail_when_tbp_in_detail():
    actual = get_places_from_place_detail("3 TBP")
    assert actual == 3

def test_get_places_from_place_detail_when_tbp_not_in_detail():
    actual = get_places_from_place_detail("Places")
    assert actual is None

def test_get_places_from_place_detail_when_tbp_has_no_value():
    actual = get_places_from_place_detail("TBP")
    assert actual is None

def test_transform_betfair_pnl_data_returns_correct_output(mock_data):
    actual = transform_betfair_pnl_data.fn(mock_data)[0]
    
    assert actual.racecourse == "Brighton"
    assert actual.race_datetime == pendulum.datetime(2024, 4, 30, 16, 10, 0, tz="UTC")
    assert actual.profit_loss == 78.60
    assert actual.places == 1
    assert actual.race_description == "1m2f Hcap"


def test_validate_betfair_pnl_data_returns_no_problems_for_correct_data(mock_data):
    problems = validate_betfair_pnl_data.fn(mock_data)
    assert len(problems.dicts()) == 0


def test_validate_betfair_pnl_data_returns_problems_for_incorrect_data(mock_data):
    mock_data[1][3] = "Lots of cash"
    problems = validate_betfair_pnl_data.fn(mock_data)
    assert len(problems.dicts()) == 1
    assert problems.dicts()[0]["field"] == "Profit/Loss (£)"
