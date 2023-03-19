from transformers.validators import (
    validate_class,
    validate_date,
    validate_distance,
    validate_going,
    validate_horse,
    validate_prize,
    validate_rating,
    validate_sex,
    validate_weight,
    validate_year,
)


def test_validate_class_passes_for_empty_string():
    assert validate_class("")


def test_validate_class_passes_for_value_under_8():
    assert validate_class("7")


def test_validate_class_fails_for_value_over_7():
    assert not validate_class("8")


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


def test_validate_going_passes_for_going_with_chase_and_hurdle():
    assert validate_going("good (good to soft in places chase course)")


def test_validate_going_fails_for_none():
    assert not validate_going(None)


def test_validate_going_fails_for_invalid_string():
    assert not validate_going("moist to tricky")


def test_validate_horse_fails_for_none():
    assert not validate_horse(None)


def test_validate_horse_fails_for_empty_string():
    assert not validate_horse("")


def test_validate_horse_passes_for_valid_string():
    assert validate_horse("DOBBIN (IRE)")


def test_validate_horse_fails_for_string_without_country():
    assert not validate_horse("DOBBIN")


def test_validate_horse_fails_for_string_over_30_chars():
    assert not validate_horse("DOBBINTHEREALLYEXTREMELYGOODHORSEWITHALOVELYMANE (IRE)")


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


def test_validate_rating_passes_for_empty_string():
    assert validate_rating("")


def test_validate_rating_passes_for_str_in_range():
    assert validate_rating("99")


def test_validate_rating_fails_for_str_below_range():
    assert not validate_rating("-1")


def test_validate_rating_fails_for_str_above_range():
    assert not validate_rating("999")


def test_validate_sex_passes_for_sex_in_list():
    assert validate_sex("MARE")


def test_validate_sex_fails_for_none():
    assert not validate_sex(None)


def test_validate_sex_fails_for_invalid_string():
    assert not validate_sex("PUPPY")


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


def test_validate_year_fails_for_none():
    assert not validate_year(None)


def test_validate_year_passes_for_str_in_range():
    assert validate_year("2020")


def test_validate_year_fails_for_str_below_range():
    assert not validate_year("1599")


def test_validate_year_fails_for_str_above_range():
    assert not validate_year("2101")
