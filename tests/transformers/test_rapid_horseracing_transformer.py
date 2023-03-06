from transformers.rapid_horseracing_transformer import validate_date


def test_validate_date_fails_for_none():
    assert not validate_date(None)


def test_validate_date_passes_for_valid_date():
    assert validate_date("2020-01-01")


def test_validate_date_fails_for_invalid_date():
    assert not validate_date("2020-01-32")
