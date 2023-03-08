import pendulum
from transformers.parsers import parse_horse, parse_sex, yob_from_age
from transformers.rapid_horseracing_transformer import parse_weight


def test_parse_horse_returns_correct_tuple_when_country_not_supplied():
    assert ("DOBBIN", None) == parse_horse("DOBBIN")


def test_parse_horse_returns_correct_tuple_when_name_lowercase():
    assert ("DOBBIN", None) == parse_horse("Dobbin")


def test_parse_horse_returns_correct_tuple_when_country_supplied_with_space():
    assert ("DOBBIN", "IRE") == parse_horse("DOBBIN (IRE)")


def test_parse_horse_returns_correct_tuple_when_country_supplied_without_space():
    assert ("DOBBIN", "IRE") == parse_horse("DOBBIN(IRE)")


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


def test_parse_weight_returns_correct_value_for_st_lbs_with_hyphen():
    assert 145 == parse_weight("10-5")


def test_yob_from_age_returns_correct_value_with_default_to_today():
    assert 2020 == yob_from_age(3)


def test_yob_from_age_returns_correct_value_with_specified_date():
    assert 2020 == yob_from_age(3, pendulum.parse("2023-03-08"))
