import pendulum
from transformers.rapid_horseracing_transformer import (
    transform_horses,
    transform_results,
)
import petl


def test_transform_horses_returns_correct_output():
    horse_data = petl.fromdicts(
        [
            {
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
        ]
    )
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
    assert expected == transform_horses(horse_data, pendulum.parse("2023-03-08"))


def test_transform_results_returns_correct_output():
    result_data = petl.fromdicts(
        [
            {
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
        ]
    )
    expected = {
        "rapid_id": "123456",
        "venue": "Lucksin Downs",
        "datetime": "2020-01-01 16:00:00",
        "title": "LUCKSIN HANDICAP (5)",
        "is_handicap": True,
        "obstacle": None,
        "distance": {
            "description": "1m2f",
            "official_yards": 2200,
            "actual_yards": None,
        },
        "age_restriction": "3",
        "going": {
            "description": "Soft (Good to Soft in places)",
            "official_main": "SOFT",
            "official_secondary": "GOOD TO SOFT",
        },
        "finished": True,
        "cancelled": False,
        "prize": "£2794",
        "class": "5",
        "result": [],
    }
    assert expected == transform_results.fn(result_data)[0]
