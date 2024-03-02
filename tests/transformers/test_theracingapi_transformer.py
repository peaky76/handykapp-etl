import pendulum
import petl
import pytest
from transformers.theracingapi_transformer import (
    build_datetime,
    transform_horse,
    transform_races,
)


@pytest.fixture()
def horse_1_data():
    return {
        "horse": "Hortzadar",
        "age": "8",
        "sex": "gelding",
        "sex_code": "G",
        "colour": "b",
        "region": "GB",
        "dam": "Clouds Of Magellan",
        "sire": "Sepoy",
        "damsire": "Dynaformer",
        "trainer": "David Omeara",
        "owner": "Akela Thoroughbreds Limited",
        "number": "1",
        "draw": "4",
        "headgear": "",
        "lbs": "141",
        "ofr": "76",
        "jockey": "Mark Winn",
        "last_run": "12",
        "form": "476601",
    }


@pytest.fixture()
def horse_2_data():
    return {
        "horse": "Tele Red",
        "age": "6",
        "sex": "filly",
        "sex_code": "F",
        "colour": "b",
        "region": "GB",
        "dam": "Hardy Blue",
        "sire": "Telescope",
        "damsire": "Red Clubs",
        "trainer": "K R Burke",
        "owner": "John Kenny",
        "number": "2",
        "draw": "7",
        "headgear": "bl",
        "lbs": "141",
        "ofr": "76",
        "jockey": "Brandon Wilkie(5)",
        "last_run": "12",
        "form": "113246",
    }


@pytest.fixture()
def racecard_data(horse_1_data, horse_2_data):
    return {
        "course": "Wolverhampton (AW)",
        "date": "2023-10-03",
        "off_time": "1:42",
        "race_name": "Virgin Bet Apprentice Handicap",
        "distance_f": "8.0",
        "region": "GB",
        "pattern": "",
        "race_class": "Class 5",
        "type": "Flat",
        "age_band": "3yo+",
        "rating_band": "0-75",
        "prize": "£4,187",
        "field_size": "7",
        "going": "Soft",
        "surface": "AW",
        "runners": [
            horse_1_data,
            horse_2_data,
        ],
    }


def test_build_datetime_with_morning_race():
    assert build_datetime("2023-10-03", "11:15") == "2023-10-03T11:15:00+00:00"


def test_build_datetime_with_afternoon_race():
    assert build_datetime("2023-10-03", "3:15") == "2023-10-03T15:15:00+00:00"


def test_build_datetime_with_evening_race():
    assert build_datetime("2023-10-03", "9:15") == "2023-10-03T21:15:00+00:00"


def test_build_datetime_with_minutes_below_ten():
    assert build_datetime("2023-10-03", "3:05") == "2023-10-03T15:05:00+00:00"


def test_transform_horse_returns_correct_output_when_professional_jockey(
    horse_1_data, mocker
):
    mocker.patch("pendulum.now", return_value=pendulum.parse("2023-10-03"))
    expected = {
        "name": "HORTZADAR",
        "sex": "M",
        "country": "GB",
        "year": 2015,
        "colour": "Bay",
        "sire": "SEPOY",
        "dam": "CLOUDS OF MAGELLAN",
        "damsire": "DYNAFORMER",
        "trainer": "David Omeara",
        "owner": "Akela Thoroughbreds Limited",
        "jockey": "Mark Winn",
        "allowance": 0,
        "saddlecloth": 1,
        "draw": 4,
        "headgear": None,
        "lbs_carried": 141,
        "official_rating": 76,
    }
    actual = transform_horse(
        petl.fromdicts([horse_1_data]), pendulum.parse("2023-10-03")
    )
    assert actual == expected


def test_transform_horse_returns_correct_output_when_apprentice_jockey(
    horse_2_data, mocker
):
    mocker.patch("pendulum.now", return_value=pendulum.parse("2023-10-03"))
    expected = {
        "name": "TELE RED",
        "sex": "F",
        "country": "GB",
        "year": 2017,
        "colour": "Bay",
        "sire": "TELESCOPE",
        "dam": "HARDY BLUE",
        "damsire": "RED CLUBS",
        "trainer": "K R Burke",
        "owner": "John Kenny",
        "jockey": "Brandon Wilkie",
        "allowance": 5,
        "saddlecloth": 2,
        "draw": 7,
        "headgear": "Blinkers",
        "lbs_carried": 141,
        "official_rating": 76,
    }
    actual = transform_horse(
        petl.fromdicts([horse_2_data]), pendulum.parse("2023-10-03")
    )
    assert actual == expected


def test_transform_races_returns_correct_output(racecard_data):
    expected = {
        "course": "Wolverhampton",
        "surface": "AW",
        "code": "Flat",
        "datetime": "2023-10-03T13:42:00+00:00",
        "title": "Virgin Bet Apprentice Handicap",
        "is_handicap": True,
        "obstacle": None,
        "distance_description": "1m",
        "race_grade": None,
        "race_class": 5,
        "age_restriction": "3yo+",
        "rating_restriction": "0-75",
        "going_description": "Soft",
        "prize": "£4187",
    }

    actual = transform_races(petl.fromdicts([racecard_data]))[0]

    assert len(actual["runners"]) == 2
    actual.pop("runners")

    assert actual == expected


# def test_validate_racecard_returns_no_problems_for_correct_data(racecard_data):
#     problems = validate_races.fn(petl.fromdicts([racecard_data]))
#     assert 0 == len(problems.dicts())


# def test_validate_racecard_returns_problems_for_incorrect_data(racecard_data):
#     racecard_data["course"] = 365
#     problems = validate_races.fn(petl.fromdicts([racecard_data]))
#     assert 1 == len(problems.dicts())
#     assert "horses" == problems.dicts()[0]["field"]
