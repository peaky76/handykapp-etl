import pendulum
import petl
import pytest

from models import BHARatingsRecord
from src.transformers.bha_transformer import (
    convert_header_to_field_name,
    csv_row_to_dict,
    get_csv,
    read_csv,
    transform_ratings,
)

GET_FILES_IMPORT = "src.transformers.bha_transformer.SpacesClient.get_files"


@pytest.fixture
def mock_data():
    rows = [
        row.split(",")
        for row in [
            "Name,Year,Sex,Sire,Dam,Trainer,Flat rating,Diff Flat,Flat Clltrl,AWT rating,Diff AWT,AWT Clltrl,Chase rating,Diff Chase,Chase Clltrl,Hurdle rating,Diff Hurdle,Hurdle Clltrl",
            "A DAY TO DREAM (IRE),2020,GELDING,ADAAY (IRE),TARA TOO (IRE),Ollie Pears,49,,,,,,,,,,,",
        ]
    ]
    header = [convert_header_to_field_name(col) for col in rows[0]]
    return BHARatingsRecord(**csv_row_to_dict(header, rows[1]))


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
        "src.transformers.bha_transformer.SpacesClient.stream_file",
        return_value=bytes("foo,bar,baz", "utf-8"),
    )
    assert petl.header(read_csv.fn("foobar.csv")) == ("foo", "bar", "baz")


def test_transform_ratings_returns_correct_output(mock_data):
    date_time = pendulum.now()
    expected = {
        "name": "A DAY TO DREAM",
        "country": "IRE",
        "year": 2020,
        "sex": "M",
        "colour": None,
        "owner": None,
        "trainer": "Ollie Pears",
        "sire": "ADAAY (IRE)",
        "dam": "TARA TOO (IRE)",
        "gelded_from": date_time.date(),
        "ratings": {
            "flat": 49,
            "aw": None,
            "chase": None,
            "hurdle": None,
        },
    }
    actual = transform_ratings.fn(mock_data, date_time).model_dump()
    assert expected == actual
