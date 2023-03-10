import pendulum
from transformers.rapid_horseracing_transformer import (
    transform_horse,
    transform_result,
    validate_date,
    validate_distance,
    validate_going,
    validate_handicap,
    validate_prize,
    validate_weight,
)
import petl


def test_validate_date_fails_for_none():
    assert not validate_date(None)


def test_validate_date_passes_for_valid_date():
    assert validate_date("2020-01-01")


def test_validate_date_fails_for_invalid_date():
    assert not validate_date("2020-01-32")


def test_validate_distance_passes_for_miles_and_furlongs():
    assert validate_distance("1m2f")


def test_validate_distance_passes_for_miles_and_furlongs_with_space():
    assert validate_distance("1m 2f")


def test_validate_distance_passes_for_miles():
    assert validate_distance("1m")


def test_validate_distance_passes_for_furlongs():
    assert validate_distance("6f")


def test_validate_distance_fails_for_none():
    assert not validate_distance(None)


def test_validate_distance_fails_for_invalid_string():
    assert not validate_distance("1m2f3f")


def test_validate_distance_fails_for_furlongs_before_miles():
    assert not validate_distance("6f1m")


def test_validate_distance_fails_for_furlongs_over_eight():
    assert not validate_distance("9f")


def test_validate_distance_fails_for_furlongs_under_four():
    assert not validate_distance("3f")


def test_validate_distance_fails_for_miles_over_four():
    assert not validate_distance("6m")


def test_validate_going_passes_for_straight_going():
    assert validate_going("good")


def test_validate_going_passes_for_going_to_going():
    assert validate_going("good to firm")


def test_validate_going_passes_for_going_with_in_places():
    assert validate_going("good (good to soft in places)")


def test_validate_going_fails_for_none():
    assert not validate_going(None)


def test_validate_going_fails_for_invalid_string():
    assert not validate_going("moist to tricky")


def test_validate_handicap_passes_if_name_contains_handicap():
    assert validate_handicap("LUCKSIN HANDICAP (5)")


def test_validate_handicap_passes_if_name_contains_handicap_abbr():
    assert validate_handicap("LUCKSIN H'CAP (5) ")


def test_validate_handicap_fails_if_name_does_not_contain_handicap():
    assert not validate_handicap("LUCKSIN STAKES (5)")


def test_validate_prize_passes_for_sterling():
    assert validate_prize("£1234")


def test_validate_prize_passes_for_sterling_with_comma():
    assert validate_prize("£1,234")


def test_validate_prize_passes_for_dollar():
    assert validate_prize("$1234")


def test_validate_prize_fails_for_none():
    assert not validate_prize(None)


def test_validate_prize_fails_for_invalid_string():
    assert not validate_prize("£1234$")


def test_validate_weight_fail_for_none():
    assert not validate_weight(None)


def test_validate_weight_fails_for_weight_without_hyphen():
    assert not validate_weight("10")


def test_validate_weight_fails_for_weight_with_more_than_one_hyphen():
    assert not validate_weight("10-0-0")


def test_validate_weight_fails_for_weight_with_letters():
    assert not validate_weight("10-0a")


def test_validate_weight_fails_for_weight_with_invalid_lbs():
    assert not validate_weight("10-15")


def test_transform_horse_returns_correct_output():
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
    assert expected == transform_horse(horse_data, pendulum.parse("2023-03-08"))


def test_transform_result_returns_correct_output():
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
    assert expected == transform_result.fn(result_data)[0]
