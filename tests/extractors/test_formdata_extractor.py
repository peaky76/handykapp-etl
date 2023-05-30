from extractors.formdata_extractor import (
    extract_run,
    extract_title,
    is_dist_going,
    is_horse,
    is_race_date,
)


def test_extract_run():
    input = "9-6 BMcHugh 11 6.3 65 6.1G 78 12Oct22 H 7 Not 12 8-11 CMurtagh 10 4.6 67 5D 74 22Apr23 H"
    expected = [
        "12Oct22",
        "H",
        "7",
        "Not",
        "12",
        "8-11",
        "CMurtagh",
        "10",
        "4.6",
        "67",
        "5D",
        "74",
    ]
    assert expected == extract_run(input)


def test_extract_title_when_all_elements_present():
    words = [
        "FLAT FORMDATA",
        "21 MAY 22 - 21 MAY 23",
        "5",
        "AAD",
        "AADDEEY (IRE)",
        "6",
        "Archie Watson",
    ]
    assert "FLAT FORMDATA 21 MAY 22 - 21 MAY 23 5 AAD" == extract_title(words)


def test_extract_title_when_not_title():
    words = [
        "Archie Watson",
        "F2",
        "£15462",
        "4Jun22",
        "H",
        "39",
        "Eps",
        "11",
        "9-4",
        "t5HarryBurns",
        "7",
        "16.5",
        "63",
        "12G",
        "87",
    ]
    assert None == extract_title(words)


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


def test_is_horse_not_true_for_dist_going():
    assert not is_horse("5G")


def test_is_horse_not_true_for_title():
    assert not is_horse("FORMDATA")


def test_is_horse_not_true_for_date_range():
    assert not is_horse("21 MAY 22 - 21 MAY 23")


def test_is_race_date_true_when_single_digit_day():
    assert is_race_date("3May23")


def test_is_race_date_true_when_double_digit_day():
    assert is_race_date("20Apr23")


def test_is_race_date_false_with_non_date():
    assert not is_race_date("JMitchell")
