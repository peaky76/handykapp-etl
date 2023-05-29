from extractors.formdata_extractor import is_dist_going, is_race_date


def test_is_dist_going_true_for_turf_going():
    assert is_dist_going("5G")


def test_is_dist_going_true_for_aw_going():
    assert is_dist_going("5d")


def test_is_dist_going_true_for_decimal_dist():
    assert is_dist_going("9.1G")


def test_is_dist_going_false_with_non_dist_going():
    assert not is_dist_going("85")


def test_is_race_date_true_when_single_digit_day():
    assert is_race_date("3May23")


def test_is_race_date_true_when_double_digit_day():
    assert is_race_date("20Apr23")


def test_is_race_date_false_with_non_date():
    assert not is_race_date("JMitchell")
