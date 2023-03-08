from transformers.rapid_horseracing_transformer import (
    parse_distance,
    parse_going,
    transform_horse,
    validate_date,
    validate_distance,
    validate_going,
    validate_prize,
)
import petl


def test_parse_distance_returns_correct_value_for_miles_and_furlongs():
    assert parse_distance("1m2f") == 2011.680


def test_parse_distance_returns_correct_value_for_miles():
    assert parse_distance("1m") == 1609.344


def test_parse_distance_returns_correct_value_for_furlongs():
    assert parse_distance("6f") == 1207.008


def test_parse_distance_returns_correct_value_for_none():
    assert parse_distance(None) == 0


def test_parse_going_returns_correct_value_for_straight_going():
    assert parse_going("GOOD") == {"main": "GOOD", "secondary": None}


def test_parse_going_returns_correct_value_for_going_to_going():
    assert parse_going("GOOD TO FIRM") == {"main": "GOOD TO FIRM", "secondary": None}


def test_parse_going_returns_correct_value_for_going_with_in_places():
    assert parse_going("GOOD (GOOD TO SOFT IN PLACES)") == {
        "main": "GOOD",
        "secondary": "GOOD TO SOFT",
    }


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


def test_transform_horse_returns_correct_output():
    input = petl.fromdicts(
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
        "rapid_id": "123456",
        "jockey": "A Jockey",
        "trainer": "A Trainer",
        "age": "3",
        "weight": "10-0",
        "saddlecloth": "1",
        "days_since_prev_run": "1",
        "non_runner": "0",
        "form": "1-2-3",
        "position": "1",
        "distance_beaten": "1 1/2",
        "owner": "A Owner",
        "sire": "THE SIRE",
        "sire_country": None,
        "dam": "THE DAM",
        "dam_country": "FR",
        "official_rating": "",
        "sp": "8",
        "odds": [],
    }
    assert expected == transform_horse.fn(input)[0]
