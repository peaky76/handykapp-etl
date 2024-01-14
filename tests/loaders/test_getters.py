import pytest
from bson.objectid import ObjectId
from loaders.getters import lookup_racecourse_id


@pytest.fixture()
def mock_find_one(mocker):
    return mocker.patch("pymongo.collection.Collection.find_one")

def test_lookup_racecourse_id(mock_find_one):
    mock_find_one.return_value = {"_id": ObjectId("abcdef123456abcdef123456")}

    result = lookup_racecourse_id("Course", "Turf", "123", "No")

    mock_find_one.assert_called_once_with(
        {
            "name": "Course",
            "surface": {"$in": ["Turf"]},
            "code": "123",
            "obstacle": "No",
        },
        {"_id": 1},
    )
    assert result == ObjectId("abcdef123456abcdef123456")
