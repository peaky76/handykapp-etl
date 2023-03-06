import pytest
from src.transformers.bha_transformer import (
    get_csv,
    parse_horse,
    parse_sex,
    validate_horse,
    validate_rating,
    validate_sex,
    validate_year,
)


@pytest.fixture
def mock_csv_data():
    return [
        "Name,Year,Sex,Sire,Dam,Trainer,Flat rating,Diff Flat,Flat Clltrl,AWT rating,Diff AWT,AWT Clltrl,Chase rating,Diff Chase,Chase Clltrl,Hurdle rating,Diff Hurdle,Hurdle Clltrl"
        "A BOY NAMED IVY (IRE),2018,GELDING,MARKAZ (IRE),ST ATHAN (GB),,79,,,,,,,,,,,",
        "A DAY TO DREAM (IRE),2020,GELDING,ADAAY (IRE),TARA TOO (IRE),Ollie Pears,49,,,,,,,,,,,",
        "A DEFINITE GETAWAY (IRE),2018,GELDING,GETAWAY (GER),DEF IT VIC (IRE),Ben Pauling,,,,,,,,,,112,,",
    ]


@pytest.fixture
def mock_csv_bytearray(mock_csv_data):
    return bytearray("\n".join(mock_csv_data), "utf-8")


def test_parse_horse_returns_correct_dict_when_country_supplied():
    assert ("DOBBIN", "IRE") == parse_horse("DOBBIN (IRE)")


def test_parse_horse_returns_correct_dict_when_country_not_supplied():
    assert ("DOBBIN", None) == parse_horse("DOBBIN")


def test_parse_sex_returns_correct_value_for_gelding():
    assert "M" == parse_sex("GELDING")


def test_parse_sex_returns_correct_value_for_colt():
    assert "M" == parse_sex("COLT")


def test_parse_sex_returns_correct_value_for_stallion():
    assert "M" == parse_sex("STALLION")


def test_parse_sex_returns_correct_value_for_filly():
    assert "F" == parse_sex("FILLY")


def test_parse_sex_returns_correct_value_for_mare():
    assert "F" == parse_sex("MARE")


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


def test_validate_year_fails_for_none():
    assert not validate_year(None)


def test_validate_year_passes_for_str_in_range():
    assert validate_year("2020")


def test_validate_year_fails_for_str_below_range():
    assert not validate_year("1599")


def test_validate_year_fails_for_str_above_range():
    assert not validate_year("2101")


def test_get_csv_returns_latest_ratings_by_default(mocker):
    mocker.patch(
        "src.transformers.bha_transformer.get_files",
        return_value=[
            "handykapp/bha/bha_ratings_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_rating_changes_20200301.csv",
        ],
    )
    assert "handykapp/bha/bha_ratings_20200201.csv" == get_csv.fn()


def test_get_csv_returns_perf_figs_if_requested(mocker):
    mocker.patch(
        "src.transformers.bha_transformer.get_files",
        return_value=[
            "handykapp/bha/bha_perf_figs_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_ratings_20200301.csv",
        ],
    )
    assert "handykapp/bha/bha_perf_figs_20200101.csv" == get_csv.fn(type="perf_figs")


def test_get_csv_returns_ratings_for_date_if_requested(mocker):
    mocker.patch(
        "src.transformers.bha_transformer.get_files",
        return_value=[
            "handykapp/bha/bha_ratings_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_ratings_20200301.csv",
        ],
    )
    assert "handykapp/bha/bha_ratings_20200201.csv" == get_csv.fn(date="20200201")


def test_get_csv_returns_none_if_no_files_found(mocker):
    mocker.patch("src.transformers.bha_transformer.get_files", return_value=[])
    assert None == get_csv.fn()
