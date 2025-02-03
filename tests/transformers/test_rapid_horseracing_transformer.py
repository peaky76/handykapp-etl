import pendulum
import petl
import pytest
from transformers.rapid_horseracing_transformer import (
    transform_horse,
    transform_results,
    validate_results,
)


@pytest.fixture
def horse_data():
    return {
        "horse": "Dobbin(IRE)",
        "id_horse": "123456",
        "jockey": "A Jockey",
        "trainer": "A Trainer",
        "age": "3",
        "weight": "10-0",
        "number": "1",
        "last_ran_days_ago": "1",
        "non_runner": "0",
        "form": "1-2-3",
        "position": "1",
        "distance_beaten": "1 1/2",
        "owner": "A Owner",
        "sire": "THE SIRE",
        "dam": "THE DAM(FR)",
        "OR": "",
        "sp": "8",
        "odds": [],
    }


@pytest.fixture
def result_data():
    return {
        "id_race": "123456",
        "course": "Lucksin Downs",
        "date": "2020-01-01 16:00:00",
        "title": "LUCKSIN HANDICAP (5)",
        "distance": "1m2f",
        "age": "3",
        "going": "Soft (Good to Soft in places)",
        "finished": "1",
        "canceled": "0",
        "finish_time": "",
        "prize": "\u00a32794",
        "class": "5",
        "horses": [],
    }


def test_transform_horse_returns_correct_output(horse_data):
    expected = {
        "name": "DOBBIN",
        "country": "IRE",
        "year": 2020,
        "rapid_id": "123456",
        "jockey": "A Jockey",
        "trainer": "A Trainer",
        "lbs_carried": 140,
        "saddlecloth": "1",
        "days_since_prev_run": 1,
        "non_runner": False,
        "form": "1-2-3",
        "position": "1",
        "distance_beaten": "1 1/2",
        "owner": "A Owner",
        "sire": "THE SIRE",
        "sire_country": "GB",
        "dam": "THE DAM",
        "dam_country": "FR",
        "official_rating": None,
        "sp": "8",
        "odds": [],
        "finishing_time": None,
    }
    actual = transform_horse(petl.fromdicts([horse_data]), pendulum.parse("2023-03-08"))
    assert actual == expected


def test_transform_results_returns_correct_output(result_data):
    expected = {
        "rapid_id": "123456",
        "course": "Lucksin Downs",
        "datetime": "2020-01-01T16:00:00+00:00",
        "title": "LUCKSIN HANDICAP (5)",
        "is_handicap": True,
        "obstacle": None,
        "surface": "Turf",
        "code": "Flat",
        "distance_description": "1m2f",
        "age_restriction": "3",
        "going_description": "Soft (Good to Soft in places)",
        "finished": True,
        "cancelled": False,
        "prize": "£2794",
        "class": "5",
        "runners": [],
    }
    actual = transform_results(petl.fromdicts([result_data]))[0]
    assert actual == expected


def test_validate_results_returns_no_problems_for_correct_data(result_data, horse_data):
    result_data["horses"] = [horse_data]
    problems = validate_results(petl.fromdicts([result_data]))
    assert len(problems.dicts()) == 0


def test_validate_results_returns_problems_for_incorrect_data(result_data):
    problems = validate_results(petl.fromdicts([result_data]))
    assert len(problems.dicts()) == 1
    assert problems.dicts()[0]["field"] == "horses"
