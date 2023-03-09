import pendulum
from transformers.parsers import (
    parse_going,
    parse_horse,
    parse_sex,
    parse_weight,
    parse_yards,
    yob_from_age,
)


def test_parse_going_returns_correct_value_for_none():
    assert parse_going(None) == {"main": None, "secondary": None}


def test_parse_going_returns_correct_value_for_straight_going():
    assert parse_going("GOOD") == {"main": "GOOD", "secondary": None}


def test_parse_going_returns_correct_value_for_going_to_going():
    assert parse_going("GOOD TO FIRM") == {"main": "GOOD TO FIRM", "secondary": None}


def test_parse_going_returns_correct_value_for_going_with_in_places():
    assert parse_going("GOOD (GOOD TO SOFT IN PLACES)") == {
        "main": "GOOD",
        "secondary": "GOOD TO SOFT",
    }


def test_parse_horse_returns_correct_tuple_when_none():
    assert (None, None) == parse_horse(None)


def test_parse_horse_returns_correct_tuple_when_country_not_supplied():
    assert ("DOBBIN", None) == parse_horse("DOBBIN")


def test_parse_horse_returns_correct_tuple_when_name_lowercase():
    assert ("DOBBIN", None) == parse_horse("Dobbin")


def test_parse_horse_returns_correct_tuple_when_country_supplied_with_space():
    assert ("DOBBIN", "IRE") == parse_horse("DOBBIN (IRE)")


def test_parse_horse_returns_correct_tuple_when_country_supplied_without_space():
    assert ("DOBBIN", "IRE") == parse_horse("DOBBIN(IRE)")


def test_parse_horse_returns_correct_tuple_when_country_supplied_as_a_default():
    assert ("DOBBIN", "IRE") == parse_horse("DOBBIN", "IRE")


def test_parse_sex_returns_correct_value_for_gelding():
    assert "M" == parse_sex("GELDING")


def test_parse_sex_returns_correct_value_for_colt():
    assert "M" == parse_sex("COLT")


def test_parse_sex_returns_correct_value_for_stallion():
    assert "M" == parse_sex("STALLION")


def test_parse_sex_returns_correct_value_for_rig():
    assert "M" == parse_sex("RIG")


def test_parse_sex_returns_correct_value_for_filly():
    assert "F" == parse_sex("FILLY")


def test_parse_sex_returns_correct_value_for_mare():
    assert "F" == parse_sex("MARE")


def test_parse_yards_returns_correct_value_for_miles_and_furlongs():
    assert parse_yards("1m2f") == 2200


def test_parse_yards_returns_correct_value_for_miles():
    assert parse_yards("1m") == 1760


def test_parse_yards_returns_correct_value_for_furlongs():
    assert parse_yards("6f") == 1320


def test_parse_yards_returns_correct_value_for_none():
    assert parse_yards(None) == 0


def test_parse_weight_returns_correct_value_for_st_lbs_with_hyphen():
    assert 145 == parse_weight("10-5")


def test_yob_from_age_returns_correct_value_with_default_to_today():
    assert 2020 == yob_from_age(3)


def test_yob_from_age_returns_correct_value_with_specified_date():
    assert 2020 == yob_from_age(3, pendulum.parse("2023-03-08"))
