from transformers.validators import (
    validate_date,
    validate_distance,
    validate_going,
    validate_handicap,
    validate_prize,
    validate_weight,
)


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


def test_validate_handicap_passes_if_name_contains_handicap_in_titlecase():
    assert validate_handicap("Lucksin Handicap (5) ")


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


def test_validate_prize_passes_for_euro():
    assert validate_prize("€1234")


def test_validate_prize_passes_for_yen():
    assert validate_prize("¥1234")


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
