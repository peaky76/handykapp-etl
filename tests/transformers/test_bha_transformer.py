from prefect.testing.utilities import prefect_test_harness
from src.transformers.bha_transformer import (
    get_file,
    parse_horse,
    parse_sex,
    select_dams,
    select_sires,
    SOURCE,
)


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


def test_get_file_returns_latest_ratings_by_default(mocker):
    mocker.patch(
        "src.transformers.bha_transformer.get_files",
        return_value=[
            "handykapp/bha/bha_ratings_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_rating_changes_20200301.csv",
        ],
    )
    assert "handykapp/bha/bha_ratings_20200201.csv" == get_file.fn()


def test_get_file_returns_perf_figs_if_requested(mocker):
    mocker.patch(
        "src.transformers.bha_transformer.get_files",
        return_value=[
            "handykapp/bha/bha_perf_figs_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_ratings_20200301.csv",
        ],
    )
    assert "handykapp/bha/bha_perf_figs_20200101.csv" == get_file.fn(type="perf_figs")


def test_get_file_returns_ratings_for_date_if_requested(mocker):
    mocker.patch(
        "src.transformers.bha_transformer.get_files",
        return_value=[
            "handykapp/bha/bha_ratings_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_ratings_20200301.csv",
        ],
    )
    assert "handykapp/bha/bha_ratings_20200201.csv" == get_file.fn(date="20200201")


def test_get_file_returns_none_if_no_files_found(mocker):
    mocker.patch("src.transformers.bha_transformer.get_files", return_value=[])
    assert None == get_file.fn()


def test_select_dams():
    data = [
        {"dam": "DAM1"},
        {"dam": "DAM2"},
        {"dam": "DAM1"},
        {"dam": "DAM3"},
    ]
    assert ["DAM1", "DAM2", "DAM3"] == select_dams.fn(data)


def test_select_sires():
    data = [
        {"sire": "SIRE1"},
        {"sire": "SIRE2"},
        {"sire": "SIRE1"},
        {"sire": "SIRE3"},
    ]
    assert ["SIRE1", "SIRE2", "SIRE3"] == select_sires.fn(data)
