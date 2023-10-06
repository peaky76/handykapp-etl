import pendulum
from transformers.parsers import (
    parse_horse,
    parse_obstacle,
)


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


def test_parse_obstacle_returns_correct_value_for_none():
    assert None is parse_obstacle("A RACE")


def test_parse_obstacle_returns_correct_value_for_chase():
    assert "CHASE" == parse_obstacle("A CHASE")


def test_parse_obstacle_returns_correct_value_for_steeplechase():
    assert "CHASE" == parse_obstacle("A STEEPLECHASE")


def test_parse_obstacle_returns_correct_value_for_embedded_use_of_chase():
    assert None is parse_obstacle("A PURCHASE")


def test_parse_obstacle_returns_none_if_name_is_none():
    assert None is parse_obstacle(None)


def test_parse_obstacle_returns_correct_value_for_hurdle():
    assert "HURDLE" == parse_obstacle("A HURDLE")


def test_parse_obstacle_returns_correct_value_for_cross_country():
    assert "CROSS-COUNTRY" == parse_obstacle("A CROSS COUNTRY")


def test_parse_obstacle_returns_correct_value_for_cross_country_chase():
    assert "CROSS-COUNTRY" == parse_obstacle("A CROSS COUNTRY CHASE")


def test_parse_obstacle_returns_correct_value_for_national_hunt_flat():
    assert None is parse_obstacle("A NATIONAL HUNT FLAT")


def test_parse_obstacle_returns_correct_value_for_flat_race():
    assert None is parse_obstacle("A BIG STAKES RACE")


def test_parse_obstacle_is_case_insensitive():
    assert "CHASE" == parse_obstacle("a chase")
