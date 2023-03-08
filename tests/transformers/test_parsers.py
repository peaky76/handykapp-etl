from transformers.parsers import parse_horse


def test_parse_horse_returns_correct_dict_when_country_not_supplied():
    assert ("DOBBIN", None) == parse_horse("DOBBIN")


def test_parse_horse_returns_correct_dict_when_country_supplied_with_space():
    assert ("DOBBIN", "IRE") == parse_horse("DOBBIN (IRE)")


def test_parse_horse_returns_correct_dict_when_country_supplied_without_space():
    assert ("DOBBIN", "IRE") == parse_horse("DOBBIN(IRE)")
