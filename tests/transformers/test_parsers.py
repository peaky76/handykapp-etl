from transformers.parsers import parse_horse, parse_sex


def test_parse_horse_returns_correct_tuple_when_country_not_supplied():
    assert ("DOBBIN", None) == parse_horse("DOBBIN")


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
