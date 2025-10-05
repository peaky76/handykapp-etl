import pytest

from loaders.bha_loader import (
    convert_header_to_field_name,
    csv_row_to_dict,
    get_csv,
)


def test_convert_header_to_field_name():
    assert convert_header_to_field_name("Flat rating") == "flat_rating"
    assert convert_header_to_field_name("AWT rating") == "awt_rating"
    assert convert_header_to_field_name("Name") == "name"


def test_csv_row_to_dict():
    header = ["name", "year", "sex"]
    data_row = ["HORSE (IRE)", "2020", "GELDING"]
    result = csv_row_to_dict(header, data_row)
    expected = {"name": "HORSE (IRE)", "year": "2020", "sex": "GELDING"}
    assert result == expected


def test_get_csv_returns_latest_ratings_by_default(mocker):
    mocker.patch(
        "loaders.bha_loader.SpacesClient.get_files",
        return_value=[
            "handykapp/bha/bha_ratings_20200101.csv",
            "handykapp/bha/bha_ratings_20200201.csv",
            "handykapp/bha/bha_rating_changes_20200301.csv",
        ],
    )
    assert get_csv.fn() == "handykapp/bha/bha_ratings_20200201.csv"
