import petl
import pytest

from src.transformers.bha_transformer import (
    get_csv,
    read_csv,
    transform_ratings_data,
    validate_ratings_data,
)

GET_FILES_IMPORT = "src.transformers.bha_transformer.get_files"


@pytest.fixture()
def mock_data():
    return [
        row.split(",")
        for row in [
            "Name,Year,Sex,Sire,Dam,Trainer,Flat rating,Diff Flat,Flat Clltrl,AWT rating,Diff AWT,AWT Clltrl,Chase rating,Diff Chase,Chase Clltrl,Hurdle rating,Diff Hurdle,Hurdle Clltrl",
            "A DAY TO DREAM (IRE),2020,GELDING,ADAAY (IRE),TARA TOO (IRE),Ollie Pears,49,,,,,,,,,,,",
        ]
    ]


def test_get_csv_returns_latest_ratings_by_default(mocker):
    mocker.patch(
        GET_FILES_IMPORT,
        return_value=[
            "handykapp/bha/bha_ratings_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_rating_changes_20200301.csv",
        ],
    )
    assert get_csv.fn() == "handykapp/bha/bha_ratings_20200201.csv"


def test_get_csv_returns_perf_figs_if_requested(mocker):
    mocker.patch(
        GET_FILES_IMPORT,
        return_value=[
            "handykapp/bha/bha_perf_figs_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_ratings_20200301.csv",
        ],
    )
    assert (
        get_csv.fn(csv_type="perf_figs") == "handykapp/bha/bha_perf_figs_20200101.csv"
    )


def test_get_csv_returns_ratings_for_date_if_requested(mocker):
    mocker.patch(
        GET_FILES_IMPORT,
        return_value=[
            "handykapp/bha/bha_ratings_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_ratings_20200301.csv",
        ],
    )
    assert get_csv.fn(date="20200201") == "handykapp/bha/bha_ratings_20200201.csv"


def test_get_csv_returns_none_if_no_files_found(mocker):
    mocker.patch(GET_FILES_IMPORT, return_value=[])
    assert None is get_csv.fn()


def test_read_csv(mocker):
    mocker.patch(
        "src.transformers.bha_transformer.stream_file",
        return_value=bytes("foo,bar,baz", "utf-8"),
    )
    assert petl.header(read_csv.fn("foobar.csv")) == ("foo", "bar", "baz")


def test_transform_ratings_data_returns_correct_output(mock_data):
    expected = {
        "name": "A DAY TO DREAM (IRE)",
        "year": 2020,
        "sex": "M",
        "trainer": "Ollie Pears",
        "sire": "ADAAY (IRE)",
        "dam": "TARA TOO (IRE)",
        "operations": [{"type": "gelding", "date": None}],
        "ratings": {
            "flat": 49,
            "aw": None,
            "chase": None,
            "hurdle": None,
        },
    }
    actual = transform_ratings_data.fn(mock_data)[0]
    assert expected == actual


def test_validate_ratings_data_returns_no_problems_for_correct_data(mock_data):
    problems = validate_ratings_data.fn(mock_data)
    assert len(problems.dicts()) == 0


def test_validate_ratings_data_returns_problems_for_incorrect_data(mock_data):
    mock_data[1][0] = "A DAY TO DREAM"
    problems = validate_ratings_data.fn(mock_data)
    assert len(problems.dicts()) == 1
    assert problems.dicts()[0]["field"] == "Name"
