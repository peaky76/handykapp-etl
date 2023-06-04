from extractors.formdata_extractor import (
    extract_prize,
    is_dist_going,
    is_horse,
    is_race_date,
    parse_middle_details,
)


def test_extract_prize():
    assert ("2CG", "1156") == extract_prize("2CG1156")


def test_is_dist_going_true_for_turf_going():
    assert is_dist_going("5G")


def test_is_dist_going_true_for_aw_going():
    assert is_dist_going("5d")


def test_is_dist_going_true_for_decimal_dist():
    assert is_dist_going("9.1G")


def test_is_dist_going_false_with_non_dist_going():
    assert not is_dist_going("85")


def test_is_horse_true_without_country():
    assert is_horse("AADDEEY")


def test_is_horse_true_with_country():
    assert is_horse("AADDEEY (IRE)")


def test_is_horse_true_with_multiple_word_name():
    assert is_horse("HURRICANE LANE")


def test_is_horse_not_true_for_dist_going():
    assert not is_horse("5G")


def test_is_horse_not_true_for_title():
    assert not is_horse("FORMDATA")


def test_is_horse_not_true_for_trainer():
    assert not is_horse("D B O'Meara")


def test_is_horse_not_true_for_trainer_initials():
    assert not is_horse("D B O")


def test_is_horse_not_true_for_date_range():
    assert not is_horse("21 MAY 22 - 21 MAY 23")


def test_is_horse_not_true_for_trainer():
    assert not is_horse("D B O'Meara")


def test_is_race_date_true_when_single_digit_day():
    assert is_race_date("3May23")


def test_is_race_date_true_when_double_digit_day():
    assert is_race_date("20Apr23")


def test_is_race_date_false_with_non_date():
    assert not is_race_date("JMitchell")


def test_parse_middle_details_when_jockey_and_single_digit_position():
    expected = {
        "headgear": None,
        "allowance": None,
        "jockey": "JFanning",
        "position": "3",
    }

    assert expected == parse_middle_details("JFanning3")


def test_parse_middle_details_when_jockey_and_double_digit_position():
    expected = {
        "headgear": None,
        "allowance": None,
        "jockey": "JFanning",
        "position": "12",
    }

    assert expected == parse_middle_details("JFanning12")


def test_parse_middle_details_when_headgear_jockey_position():
    expected = {
        "headgear": "t",
        "allowance": None,
        "jockey": "JFanning",
        "position": "12",
    }

    assert expected == parse_middle_details("tJFanning12")


def test_parse_middle_details_when_allowance_jockey_position():
    expected = {
        "headgear": None,
        "allowance": "3",
        "jockey": "HDavies",
        "position": "12",
    }

    assert expected == parse_middle_details("3HDavies12")


def test_parse_middle_details_when_headgear_allowance_jockey_position():
    expected = {
        "headgear": "t",
        "allowance": "3",
        "jockey": "HDavies",
        "position": "12",
    }

    assert expected == parse_middle_details("t3HDavies12")
