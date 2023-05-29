from extractors.formdata_extractor import is_race_date


def test_is_race_date_true_when_single_digit_day():
    assert is_race_date("3May23")


def test_is_race_date_true_when_double_digit_day():
    assert is_race_date("20Apr23")


def test_is_race_date_with_non_date_returns_false():
    assert not is_race_date("JMitchell")
