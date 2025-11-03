import datetime

import pendulum
import petl
import pytest
from peak_utility.listish import compact

from models import BHARatingsRecord, PreMongoHorse
from models.bha_perf_figs_record import BHAPerfFigsRecord
from src.loaders.bha_loader import (
    convert_header_to_field_name,
    csv_row_to_dict,
    get_csv,
    read_csv,
)
from src.transformers.bha_transformer import (
    transform_historic_rating,
    transform_perf_figs,
    transform_ratings,
)

GET_FILES_IMPORT = "src.loaders.bha_loader.SpacesClient.get_files"


@pytest.fixture
def date():
    return pendulum.Date(2020, 1, 1)


@pytest.fixture
def mock_perf_figs_data(date):
    rows = [
        row.split(",")
        for row in [
            "Racehorse,YOF,Sex,Trainer,Latest,2 runs ago,3 runs ago,4 runs ago,5 runs ago,6 runs ago",
            "A DAY TO DREAM (IRE),2020,GELDING,Ollie Pears,T:55,A:x,-,-,-,-",
        ]
    ]
    header = [convert_header_to_field_name(col) for col in rows[0]]
    row_dict = csv_row_to_dict(header, rows[1])
    row_dict["date"] = date
    return BHAPerfFigsRecord(**row_dict)


@pytest.fixture
def mock_ratings_data():
    rows = [
        row.split(",")
        for row in [
            "Name,Year,Sex,Sire,Dam,Trainer,Flat rating,Diff Flat,Flat Clltrl,AWT rating,Diff AWT,AWT Clltrl,Chase rating,Diff Chase,Chase Clltrl,Hurdle rating,Diff Hurdle,Hurdle Clltrl",
            "A DAY TO DREAM (IRE),2020,GELDING,ADAAY (IRE),TARA TOO (IRE),Ollie Pears,49,,,,,,,,,,,",
        ]
    ]
    header = [convert_header_to_field_name(col) for col in rows[0]]
    row_dict = csv_row_to_dict(header, rows[1])
    now = pendulum.now()
    row_dict["date"] = datetime.date(now.year, now.month, now.day)
    return BHARatingsRecord(**row_dict)


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
        "src.loaders.bha_loader.SpacesClient.stream_file",
        return_value=bytes("foo,bar,baz", "utf-8"),
    )
    assert petl.header(read_csv.fn("foobar.csv")) == ("foo", "bar", "baz")


def test_transform_historic_rating_when_all_weather(date):
    expected = {
        "date_before": date,
        "races_before": 1,
        "surface": "All Weather",
        "obstacle": None,
        "rating": 55,
    }
    actual = transform_historic_rating("A:55", 1, date).model_dump()
    assert expected == actual


def test_transform_historic_rating_when_turf(date):
    expected = {
        "date_before": date,
        "races_before": 1,
        "surface": "Turf",
        "obstacle": None,
        "rating": 55,
    }
    actual = transform_historic_rating("T:55", 1, date).model_dump()
    assert expected == actual


def test_transform_historic_rating_when_hurdle(date):
    expected = {
        "date_before": date,
        "races_before": 1,
        "surface": "Turf",
        "obstacle": "Hurdle",
        "rating": 55,
    }
    actual = transform_historic_rating("H:55", 1, date).model_dump()
    assert expected == actual


def test_transform_historic_rating_when_chase(date):
    expected = {
        "date_before": date,
        "races_before": 1,
        "surface": "Turf",
        "obstacle": "Chase",
        "rating": 55,
    }
    actual = transform_historic_rating("C:55", 1, date).model_dump()
    assert expected == actual


def test_transform_historic_rating_when_value_is_x(date):
    expected = {
        "date_before": date,
        "races_before": 1,
        "surface": "All Weather",
        "obstacle": None,
        "rating": None,
    }
    actual = transform_historic_rating("A:x", 1, date).model_dump()
    assert expected == actual


def test_transform_historic_rating_when_absent(date):
    actual = transform_historic_rating("-", 1, date)
    assert actual is None


def test_transform_perf_figs_returns_correct_output(mock_perf_figs_data, date):
    expected = {
        "name": "A DAY TO DREAM",
        "country": "IRE",
        "year": 2020,
        "sex": "M",
        "historic_ratings": [
            {
                "date_before": date,
                "races_before": 0,
                "surface": "Turf",
                "obstacle": None,
                "rating": 55,
            },
            {
                "date_before": date,
                "races_before": 1,
                "surface": "All Weather",
                "obstacle": None,
                "rating": None,
            },
        ],
    }
    actual = compact(transform_perf_figs(mock_perf_figs_data).model_dump())
    assert expected == actual


def test_transform_ratings_returns_correct_output(mock_ratings_data):
    # transform_ratings now takes only the BHARatingsRecord and returns PreMongoHorse
    sire = PreMongoHorse(name="ADAAY", country="IRE", sex="M")
    dam = PreMongoHorse(name="TARA TOO", country="IRE", sex="F")
    expected = {
        "name": "A DAY TO DREAM",
        "country": "IRE",
        "year": 2020,
        "sex": "M",
        "colour": None,
        "owner": None,
        "trainer": "Ollie Pears",
        "sire": sire.model_dump(),
        "dam": dam.model_dump(),
        "damsire": None,
        "gelded_from": mock_ratings_data.date,
        "ratings": {
            "flat": 49,
            "aw": None,
            "chase": None,
            "hurdle": None,
        },
    }
    actual = transform_ratings(mock_ratings_data).model_dump()
    assert expected == actual
