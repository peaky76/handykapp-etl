from prefect.testing.utilities import prefect_test_harness
from transformers.bha_transformer import get_ratings_files, prune_ratings_csv
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
