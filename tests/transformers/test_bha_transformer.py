from prefect.testing.utilities import prefect_test_harness
from transformers.bha_transformer import (
    get_ratings_files,
    parse_horse,
    parse_sex,
    prune_ratings_csv,
)
import petl


def test_get_ratings_files_only_returns_ratings_files(mocker):
    with prefect_test_harness():
        mocker.patch("transformers.bha_transformer.get_files").return_value = [
            "bha_ratings_20210101.csv",
            "bha_rating_changes_20210101.csv",
            "bha_perf_figs_20210101.csv",
        ]
        assert ["bha_ratings_20210101.csv"] == get_ratings_files.fn()


def test_get_ratings_files_only_returns_files_for_specified_date(mocker):
    with prefect_test_harness():
        mocker.patch("transformers.bha_transformer.get_files").return_value = [
            "bha_ratings_20210101.csv",
            "bha_ratings_20210202.csv",
            "bha_ratings_20210303.csv",
        ]
        assert ["bha_ratings_20210101.csv"] == get_ratings_files.fn("2021-01-01")


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
