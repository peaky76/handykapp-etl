import pendulum
import petl
import pytest
from transformers.rapid_horseracing_transformer import (
    transform_horse_data,
    transform_results_data,
    validate_results_data,
)


@pytest.fixture()
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


@pytest.fixture()
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


def test_transform_horse_data_returns_correct_output(horse_data):
    expected = {
        "name": "DOBBIN",
        "country": "IRE",
        "year": 2020,
        "sex": None,
        "breed": None,
        "colour": None,
        "operations": None,
        "jockey": {
            "name": "A Jockey",
            "role": "jockey",
            "sex": None,
            "references": {
                "rapid": "A Jockey"
            }
        },
        "trainer": {
            "name": "A Trainer",
            "role": "trainer",
            "sex": None,
            "references": {
                "rapid": "A Trainer"
            }
        },
        "lbs_carried": 140,
        "saddlecloth": 1,
        "draw": None,
        "position": "1",
        "distance_beaten": "1 1/2",
        "owner": "A Owner",
        "sire": {
            "name": "THE SIRE",
            "country": "GB",
            "year": None,
            "sex": "M",
            "source": "rapid"
        },
        "dam": {
            "name": "THE DAM",
            "country": "FR",
            "year": None,
            "sex": "F",
            "source": "rapid"
        },
        "allowance": None,
        "damsire": None,
        "headgear": None,
        "official_rating": None,
        "ratings": None,
        "source": "rapid",
        "sp": "8",
    }
    actual = transform_horse_data(petl.fromdicts([horse_data]), pendulum.parse("2023-03-08"))
    assert actual.model_dump() == expected


def test_transform_results_data_returns_correct_output(result_data):
    expected = {
        "racecourse": {
            "name": "Lucksin Downs",
            "formal_name": None,
            "country": "GB",
            "obstacle": None,
            "surface": "Turf",
            "code": "Flat",
            "handedness": None,
            "contour": None,
            "shape": None,
            "style": None,
            "source": "rapid",
            "references": {
                "rapid": "Lucksin Downs"
            }
        },
        "datetime": pendulum.parse("2020-01-01 16:00"),
        "title": "LUCKSIN HANDICAP (5)",
        "is_handicap": True,
        "distance_description": "1m2f",
        "age_restriction": None,
        "rating_restriction": None,
        "race_class": None,
        "race_grade": None,
        "number_of_runners": None,
        "going_description": "Soft (Good to Soft in places)",
        "prize": "£2794",
        "runners": [],
        "references": {
            "rapid": "123456"
        },
        "source": "rapid"
    }
    actual = transform_results_data(petl.fromdicts([result_data]))[0]
    assert actual.model_dump() == expected


def test_validate_results_data_returns_no_problems_for_correct_data(result_data, horse_data):
    result_data["horses"] = [horse_data]
    problems = validate_results_data(petl.fromdicts([result_data]))
    assert len(problems.dicts()) == 0


def test_validate_results_data_returns_problems_for_incorrect_data(result_data):
    problems = validate_results_data(petl.fromdicts([result_data]))
    assert len(problems.dicts()) == 1
    assert problems.dicts()[0]["field"] == "horses"
