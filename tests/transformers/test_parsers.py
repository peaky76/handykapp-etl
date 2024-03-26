import pendulum
from transformers.parsers import (
    parse_code,
    parse_days_since_run,
    parse_horse,
    parse_obstacle,
    parse_variant,
)


def test_parse_code_returns_correct_value_for_obstacle():
    assert parse_code(True, None) == "NH"


def test_parse_code_returns_correct_value_for_national_hunt_in_title():
    assert parse_code(False, "Big National Hunt Flat Race") == "NH"


def test_parse_code_returns_correct_value_for_nh_in_title():
    assert parse_code(False, "Big NHF Race") == "NH"


def test_parse_code_returns_correct_value_when_not_obstacle_or_nh():
    assert parse_code(False, "Big Handicap") == "Flat"


def test_parse_days_since_run_returns_correct_value_for_none():
    assert parse_days_since_run(None, None) is None


def test_parse_days_since_run_returns_correct_value_when_same_code_int():
    assert parse_days_since_run(pendulum.parse("2022-05-30"), 5) == pendulum.parse("2022-05-25")


def test_parse_days_since_run_returns_correct_value_when_same_code_str():
    assert parse_days_since_run(pendulum.parse("2022-05-30"), "5") == pendulum.parse("2022-05-25")


def test_parse_days_since_run_returns_correct_value_when_different_code_str():
    assert parse_days_since_run(pendulum.parse("2022-05-30"), "105 (20F)") == pendulum.parse("2022-05-10")


def test_parse_horse_returns_correct_tuple_when_none():
    assert parse_horse(None) == (None, None)


def test_parse_horse_returns_correct_tuple_when_country_not_supplied():
    assert parse_horse("DOBBIN") == ("DOBBIN", None)


def test_parse_horse_returns_correct_tuple_when_name_lowercase():
    assert parse_horse("Dobbin") == ("DOBBIN", None)


def test_parse_horse_returns_correct_tuple_when_country_supplied_with_space():
    assert parse_horse("DOBBIN (IRE)") == ("DOBBIN", "IRE")


def test_parse_horse_returns_correct_tuple_when_country_supplied_without_space():
    assert parse_horse("DOBBIN(IRE)") == ("DOBBIN", "IRE")


def test_parse_horse_returns_correct_tuple_when_country_supplied_as_a_default():
    assert parse_horse("DOBBIN", "IRE") == ("DOBBIN", "IRE")


def test_parse_obstacle_returns_correct_value_for_none():
    assert None is parse_obstacle("A RACE")


def test_parse_obstacle_returns_correct_value_for_chase():
    assert parse_obstacle("A CHASE") == "Chase"


def test_parse_obstacle_returns_correct_value_for_steeplechase():
    assert parse_obstacle("A STEEPLECHASE") == "Chase"


def test_parse_obstacle_returns_correct_value_for_embedded_use_of_chase():
    assert None is parse_obstacle("A PURCHASE")


def test_parse_obstacle_returns_none_if_name_is_none():
    assert None is parse_obstacle(None)


def test_parse_obstacle_returns_correct_value_for_hurdle():
    assert parse_obstacle("A HURDLE") == "Hurdle"


def test_parse_obstacle_returns_correct_value_for_cross_country():
    assert parse_obstacle("A CROSS COUNTRY") == "Cross-Country"


def test_parse_obstacle_returns_correct_value_for_cross_country_chase():
    assert parse_obstacle("A CROSS COUNTRY CHASE") == "Cross-Country"


def test_parse_obstacle_returns_correct_value_for_national_hunt_flat():
    assert None is parse_obstacle("A NATIONAL HUNT FLAT")


def test_parse_obstacle_returns_correct_value_for_flat_race():
    assert None is parse_obstacle("A BIG STAKES RACE")


def test_parse_obstacle_is_case_insensitive():
    assert parse_obstacle("a chase") == "Chase"


def test_parse_variant_with_uppercase_variant():
    assert parse_variant("STR") == "Straight"


def test_parse_variant_if_not_present_in_converter():
    assert parse_variant("new") == "New"