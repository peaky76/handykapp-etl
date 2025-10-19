from transformers.parsers import (
    parse_code,
    parse_obstacle,
)


def test_parse_code_returns_correct_value_for_obstacle():
    assert parse_code("CHASE", None) == "National Hunt"


def test_parse_code_returns_correct_value_for_national_hunt_in_title():
    assert parse_code(None, "Big National Hunt Flat Race") == "National Hunt"


def test_parse_code_works_case_insensitively():
    assert parse_code(None, "BIG NATIONAL HUNT FLAT RACE") == "National Hunt"


def test_parse_code_returns_correct_value_for_nh_in_title():
    assert parse_code(None, "Big NHF Race") == "National Hunt"


def test_parse_code_returns_correct_value_for_dot_separated_nh_in_title():
    assert parse_code(None, "Big N.H.F Race") == "National Hunt"


def test_parse_code_returns_correct_value_when_not_obstacle_or_nh():
    assert parse_code(None, "Big Handicap") == "Flat"


def test_parse_obstacle_returns_correct_value_for_none():
    assert None is parse_obstacle("A RACE")


def test_parse_obstacle_returns_correct_value_for_chase():
    assert parse_obstacle("A CHASE") == "CHASE"


def test_parse_obstacle_returns_correct_value_for_steeplechase():
    assert parse_obstacle("A STEEPLECHASE") == "CHASE"


def test_parse_obstacle_returns_correct_value_for_embedded_use_of_chase():
    assert None is parse_obstacle("A PURCHASE")


def test_parse_obstacle_returns_none_if_name_is_none():
    assert None is parse_obstacle(None)


def test_parse_obstacle_returns_correct_value_for_hurdle():
    assert parse_obstacle("A HURDLE") == "HURDLE"


def test_parse_obstacle_returns_correct_value_for_cross_country():
    assert parse_obstacle("A CROSS COUNTRY") == "CROSS-COUNTRY"


def test_parse_obstacle_returns_correct_value_for_cross_country_chase():
    assert parse_obstacle("A CROSS COUNTRY CHASE") == "CROSS-COUNTRY"


def test_parse_obstacle_returns_correct_value_for_national_hunt_flat():
    assert None is parse_obstacle("A NATIONAL HUNT FLAT")


def test_parse_obstacle_returns_correct_value_for_flat_race():
    assert None is parse_obstacle("A BIG STAKES RACE")


def test_parse_obstacle_is_case_insensitive():
    assert parse_obstacle("a chase") == "CHASE"
